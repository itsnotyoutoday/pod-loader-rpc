"""RunPod REST API — provision, inspect and terminate pods.

The provider in `provider.py` ATTACHES to a pod that exists. This module is what brings one
into existence, so `runctl create --provider runpod` no longer requires you to click through
the console first.

## What the API will and will not do

It accepts `imageName` and an optional `containerRegistryAuthId`. It does NOT accept a
Dockerfile or a build context — there is no way to ask RunPod to build an image for a Pod,
and a Pod cannot build one itself (it is a container, with no nested Docker daemon). The
image must already be in a registry.

## Network volume = the S3 bucket

A RunPod network volume is exposed two ways: over an S3-compatible API from outside, and as
an ordinary directory at /workspace on the pod. They are the same bytes. So a corpus
uploaded via `runctl push` is simply present on the pod — no staging leg, no second copy.

## Money

`create()` starts billing. `terminate()` stops it. A stopped pod still bills for its disk;
only termination ends the charge, so `runctl destroy` says so explicitly rather than
implying the record's removal was enough.
"""
from __future__ import annotations

import json
import os
import pathlib
import urllib.error
import urllib.request

API = "https://rest.runpod.io/v1"
KEY_FILES = ("runpod.key", "../runpod.key", "/run/secrets/runpod.key")

def _search_up(name: str, start: pathlib.Path | None = None,
               levels: int = 6) -> pathlib.Path | None:
    """Look for `name` in cwd and each parent above it.

    Fixed relative paths like "../runpod.key" encode how deep the caller happens to be, and
    broke the moment the repos moved one level down into backend/. Searching upward is
    robust to layout, and stops at the home directory so it can never wander into
    somebody else's tree.
    """
    cur = (start or pathlib.Path.cwd()).resolve()
    home = pathlib.Path.home().resolve()
    for _ in range(levels):
        cand = cur / name
        if cand.is_file():
            return cand
        if cur == home or cur.parent == cur:
            break
        cur = cur.parent
    return None



def load_key(path: str | None = None) -> str | None:
    """Read the API key. Accepts a bare `rpa_…` line or `name=value` form.

    Order: an explicit path, then RUNPOD_API_KEY, then the fixed candidates, then an
    upward search. The upward search exists because the fixed paths encoded how deep the
    caller happened to sit, and broke the moment the repos moved into backend/.
    """
    env = os.environ.get("RUNPOD_API_KEY", "").strip()
    if env and not path:
        return env
    found = _search_up("runpod.key")
    for c in ([path] if path else []) + list(KEY_FILES) + ([str(found)] if found else []):
        if not c:
            continue
        p = pathlib.Path(c)
        if not p.is_file():
            continue
        text = p.read_text(encoding="utf-8").strip()
        if not text:
            continue
        if "=" in text and not text.startswith("rpa_"):
            for line in text.splitlines():
                k, _, v = line.strip().partition("=")
                if k.strip().lower() in ("api_key", "api", "key", "runpod_api_key"):
                    return v.strip()
        else:
            return text.splitlines()[0].strip()
    return None


class RunPodAPI:
    def __init__(self, key: str | None = None):
        self.key = key or load_key()

    @property
    def available(self) -> bool:
        return bool(self.key)

    def _call(self, method: str, path: str, body: dict | None = None,
              timeout: int = 60) -> dict:
        if not self.key:
            raise RuntimeError("no RunPod API key (runpod.key)")
        req = urllib.request.Request(
            f"{API}{path}", method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Authorization": f"Bearer {self.key}",
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")[:400]
            raise RuntimeError(f"HTTP {e.code} {method} {path}: {detail}") from None

    # -- inspection ---------------------------------------------------------------------

    def pods(self) -> list[dict]:
        d = self._call("GET", "/pods")
        return d if isinstance(d, list) else d.get("data", [])

    def pod(self, pod_id: str) -> dict:
        return self._call("GET", f"/pods/{pod_id}")

    @staticmethod
    def uptime_seconds(pod: dict) -> float | None:
        """Seconds since the pod started.

        REST v1 exposes no `runtime` object — that shape belongs to the GraphQL API, and
        reading `runtime.uptimeInSeconds` here silently yields None forever. What REST does
        give is `lastStartedAt` (falling back to `createdAt`), so derive it.
        """
        import datetime as _dt
        import re

        stamp = pod.get("lastStartedAt") or pod.get("createdAt")
        if not stamp:
            return None
        # "2026-08-12 20:36:44.623 +0000 UTC" — Go's format, not ISO 8601.
        m = re.match(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", str(stamp))
        if not m:
            return None
        try:
            started = _dt.datetime.strptime(f"{m.group(1)} {m.group(2)}",
                                            "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=_dt.timezone.utc)
        except ValueError:
            return None
        return max(0.0, (_dt.datetime.now(_dt.timezone.utc) - started).total_seconds())

    def volumes(self) -> list[dict]:
        d = self._call("GET", "/networkvolumes")
        return d if isinstance(d, list) else d.get("data", [])

    # -- registry credentials -----------------------------------------------------------

    def registry_auths(self) -> list[dict]:
        # An empty collection comes back as a bare `null` body, not `[]` or `{"data": []}`.
        d = self._call("GET", "/containerregistryauth")
        if not d:
            return []
        return d if isinstance(d, list) else (d.get("data") or [])

    def add_registry_auth(self, name: str, username: str, password: str) -> dict:
        """Store credentials for a PRIVATE image. Returns a record whose `id` is what
        `create()` needs — the id, not the name."""
        return self._call("POST", "/containerregistryauth",
                          {"name": name, "username": username, "password": password})

    # -- lifecycle ----------------------------------------------------------------------

    def create(self, *, name: str, image: str, gpu_type_ids: list[str] | None = None,
               network_volume_id: str | None = None, gpu_count: int = 1,
               registry_auth_id: str | None = None, volume_mount_path: str = "/workspace",
               container_disk_gb: int = 30, ports: list[str] | None = None,
               env: dict | None = None, cloud_type: str = "SECURE",
               entrypoint: list[str] | None = None,
               start_cmd: list[str] | None = None,
               compute_type: str = "GPU",
               vcpu_count: int = 16,
               cpu_flavor_ids: list[str] | None = None,
               start_ssh: bool = False) -> dict:
        """Provision a pod. THIS STARTS BILLING.

        `gpu_type_ids` is a preference list — RunPod picks the first with capacity, which
        avoids a hard failure when one model is sold out.

        ## The container must not exit immediately

        A pod is only "up" while its process runs. An image whose CMD prints help and
        returns leaves a pod that bills but never reports a runtime, never opens a port and
        never appears usable in the console — which is exactly what happened the first time.
        Either pass a `start_cmd` that does the work (batch style, pod exits when finished)
        or one that blocks (`sleep infinity`) if you intend to connect to it.

        ## Batch style is preferred here

        The network volume is the same storage as the S3 bucket, so a pod can write its log
        and results to /workspace and they are readable from outside immediately. That
        removes the need for SSH entirely, and the pod stops billing when the job ends.
        """
        body: dict = {
            "name": name,
            "imageName": image,
            "containerDiskInGb": container_disk_gb,
            "cloudType": cloud_type,
            "env": env or {},
        }
        if compute_type == "CPU":
            # Every stage here is CPU: ffmpeg, MFA (Kaldi), librosa/pyin, and a CPU-only
            # torch build. Renting a GPU bought nothing and made launches fail — US-NC-1
            # had no GPU capacity while CPU was freely available, and the network volume
            # pins us to that datacentre. GPU fields are ignored when computeType=CPU.
            body["computeType"] = "CPU"
            body["vcpuCount"] = vcpu_count
            if cpu_flavor_ids:
                body["cpuFlavorIds"] = cpu_flavor_ids
        else:
            body["gpuTypeIds"] = gpu_type_ids or []
            body["gpuCount"] = gpu_count
        if ports:
            body["ports"] = ports
        if network_volume_id:
            body["networkVolumeId"] = network_volume_id
            body["volumeMountPath"] = volume_mount_path
        if registry_auth_id:
            body["containerRegistryAuthId"] = registry_auth_id
        if entrypoint is not None:
            body["dockerEntrypoint"] = entrypoint
        if start_cmd is not None:
            body["dockerStartCmd"] = start_cmd
        if start_ssh:
            body["env"] = {**body["env"], "PUBLIC_KEY": _public_key() or ""}
            body.setdefault("ports", ["22/tcp"])
        return self._call("POST", "/pods", body, timeout=180)

    def terminate(self, pod_id: str) -> dict:
        """Delete the pod and STOP BILLING. Stopping alone still bills for disk."""
        return self._call("DELETE", f"/pods/{pod_id}")

    def ssh_target(self, pod_id: str) -> dict:
        """Extract an ssh host/port once the pod is running."""
        p = self.pod(pod_id)
        status = p.get("desiredStatus") or p.get("status")
        ip = p.get("publicIp")
        port = None
        for pm in (p.get("portMappings") or {}).items():
            if str(pm[0]) == "22":
                port = pm[1]
        if not port:
            for pm in (p.get("runtime") or {}).get("ports", []) or []:
                if pm.get("privatePort") == 22:
                    ip, port = pm.get("ip", ip), pm.get("publicPort")
        return {"pod_id": pod_id, "status": status, "ip": ip, "port": port,
                "ready": bool(ip and port),
                "ssh": f"root@{ip}" if ip else None}


def _public_key() -> str | None:
    for name in ("id_ed25519.pub", "id_rsa.pub"):
        p = pathlib.Path.home() / ".ssh" / name
        if p.exists():
            return p.read_text().strip()
    return None
