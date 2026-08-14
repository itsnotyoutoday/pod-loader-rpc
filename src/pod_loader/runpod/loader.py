"""Launch onto a rented RunPod pod. The expensive destination, so it carries the guards."""
from __future__ import annotations

from ..ids import pod_name

import time

from ..base_loader import BaseLoader, PodInfo, Running


class RunPodLoader(BaseLoader):
    name = "runpod"

    def preflight(self, cfg) -> list[str]:
        problems = []
        try:
            from .api import RunPodAPI
            RunPodAPI()
        except Exception as e:
            problems.append(f"RunPod API unavailable: {str(e)[:90]}")
        if getattr(cfg, "max_lifetime_min", 0) <= 0:
            problems.append(
                "MAX_LIFETIME_MIN must be positive. An in-pod timeout cannot terminate a "
                "RunPod pod — runpodctl from inside returns Unauthorized — so a pod that "
                "reports no liveness and has no lifetime cap can never be stopped."),
        return problems

    def plan(self, cfg) -> str:
        from . import reaper
        kw = {"compute": cfg.compute, "clouds": cfg.clouds,
              "fallback_to_gpu": cfg.fallback_to_gpu}
        if cfg.flavors:
            kw["flavors"] = cfg.flavors
        if cfg.gpu_types:
            kw["gpu_types"] = cfg.gpu_types
        n = len(list(reaper.placements(**kw)))
        ceiling = f", refusing anything above ${cfg.max_cost_hr}/hr" if cfg.max_cost_hr else ""
        return f"would try {n} placements{ceiling}"

    def _registry_auth_id(self, cfg) -> str:
        """Credentials for pulling our image, registered with RunPod once and reused.

        Not about privacy — our images are public. It is about RATE LIMITS: Docker Hub
        allows 10 anonymous pulls per hour PER IP, and RunPod datacenter addresses are
        shared with every other tenant on that machine. Anonymous pulls there fail with
        429 at random, which is a worse problem than the slow GHCR pulls this is meant to
        replace: a slow pull eventually succeeds. An authenticated free account gets 100
        pulls/hour counted against the ACCOUNT rather than the shared address.

        Registered lazily and matched by name, so this costs one API call the first time
        and none afterwards.
        """
        import pathlib as _pl

        keyfile = getattr(cfg, "registry_keyfile", "") or ""
        if not keyfile:
            for c in ("dockerhub.key", "../dockerhub.key", "~/dockerhub.key"):
                p = _pl.Path(c).expanduser()
                if p.is_file():
                    keyfile = str(p)
                    break
            else:
                from .volume import _search_up
                found = _search_up(("dockerhub.key",))
                keyfile = str(found) if found else ""
        if not keyfile:
            return ""

        kv = {}
        for line in _pl.Path(keyfile).expanduser().read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if "=" in line:
                k, v = line.split("=", 1)
                kv[k.strip().lower()] = v.strip()
        user = kv.get("username") or kv.get("user")
        token = kv.get("token") or kv.get("password") or kv.get("pat")
        if not user or not token:
            return ""

        from .api import RunPodAPI
        api = RunPodAPI()
        label = f"dockerhub-{user}"
        for a in api.registry_auths():
            if a.get("name") == label:
                return a.get("id", "")
        rec = api.add_registry_auth(label, user, token)
        print(f"  registered Docker Hub credentials with RunPod as {label}", flush=True)
        return rec.get("id", "")

    def create_kwargs(self, cfg, *, env: dict) -> dict:
        from . import volume
        auth = self._registry_auth_id(cfg)
        kw = {"name": "queued-job", "image": cfg.image,
              **({"registry_auth_id": auth} if auth else {}),
              "container_disk_gb": cfg.disk_gb, "vcpu_count": cfg.vcpu,
              "ports": ["8000/http"], "env": env}
        # Only when the launch file asked for one. `volume.load(None)` searches upward
        # for a key file, so an unset value used to attach a volume anyway — and with it a
        # datacenter pin. Measured 2026-08-13: identical shapes place without the volume
        # and return "no capacity" with it, four rounds running, at the same instant.
        if cfg.volume_declared:
            vol = volume.load(cfg.runpod_volume or None)
            if vol:
                kw.update(vol.create_kwargs())
        return kw

    def start(self, cfg, *, job_id: str, env: dict, spec_key: str) -> Running:
        from .. import launchfile
        from . import reaper, volume

        # The name is required by the API and is also how the reaper's sweep recognises
        # its own work, so it is set here rather than left to the caller.
        auth = self._registry_auth_id(cfg)
        if auth:
            self._say("pulling authenticated — Docker Hub allows only 10 anonymous "
                      "pulls/hour per IP, and RunPod addresses are shared")
        create = {"name": pod_name(job_id), "image": cfg.image,
                  **({"registry_auth_id": auth} if auth else {}),
                  "container_disk_gb": cfg.disk_gb, "vcpu_count": cfg.vcpu,
                  "ports": ["8000/http"], "env": env}
        vol = volume.load(cfg.runpod_volume or None) if cfg.volume_declared else None
        if vol:
            vol.require_provider("runpod")
            create.update(vol.create_kwargs())
            self._say(f"volume: {vol.volume_id} in {vol.datacenter} "
                      f"(PINS compute to that datacenter)")
        else:
            self._say("volume: none — free to land in any datacenter")

        # Not a plain create: walks flavor x cloud x compute type, and refuses a placement
        # above MAX_COST_HR rather than silently taking a 12x pricier slot.
        api = reaper._api()
        pod, used = reaper.create_with_capacity(
            api, create, **launchfile.capacity_kwargs(cfg))
        pod_id = pod["id"]
        cost = float(pod.get("costPerHr") or 0)

        # Journalled BEFORE anything else can fail, so a pod whose launcher dies is still
        # discoverable. A pod nobody journaled is a pod nobody can find.
        deadline = time.time() + cfg.max_lifetime_min * 60
        reaper.journal(pod_id, pod_name(job_id), cost, deadline)
        _arm_deadline(pod_id, deadline, cost)
        _register_with_control(cfg, pod_id, deadline, cost, job_id)
        shape = (used.get("cpu_flavor_ids") or used.get("gpu_type_ids") or ["?"])[0]
        return Running(
            target=self.name, job_id=job_id, handle=pod_id,
            endpoint=f"https://{pod_id}-8000.proxy.runpod.net", cost_hr=cost,
            detail={"placement": f"{used.get('cloud_type','')}/{shape}",
                    "max_lifetime_min": cfg.max_lifetime_min,
                    "datacenter": vol.datacenter if vol else None})

    def stop(self, handle: str) -> dict:
        from .api import RunPodAPI
        RunPodAPI().terminate(handle)
        return {"terminated": handle}

    def list_pods(self) -> list[PodInfo]:
        from .api import RunPodAPI
        return [PodInfo(pod_id=p["id"], name=p.get("name", ""),
                        cost_hr=float(p.get("costPerHr") or 0),
                        age_sec=_age_sec(p), target=self.name,
                        running=(p.get("desiredStatus") == "RUNNING"))
                for p in RunPodAPI().pods()]


def _age_sec(pod: dict) -> float:
    """Uptime from whichever timestamp RunPod supplies.

    An unparseable date reads as age ZERO, never as old. A watcher protects young pods
    with a grace period, so "unknown" must land on the side that does not terminate
    somebody's running job over a date format.
    """
    import datetime as dt
    for key in ("lastStartedAt", "createdAt"):
        raw = (pod.get(key) or "").strip()
        if not raw:
            continue
        for fmt in ("%Y-%m-%d %H:%M:%S.%f %z %Z", "%Y-%m-%d %H:%M:%S.%f %z",
                    "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                t = dt.datetime.strptime(raw, fmt)
                if t.tzinfo is None:
                    t = t.replace(tzinfo=dt.timezone.utc)
                return max(0.0, (dt.datetime.now(dt.timezone.utc) - t).total_seconds())
            except ValueError:
                continue
    return 0.0


def _arm_deadline(pod_id: str, deadline_ts: float, cost_hr: float) -> None:
    """A daemon thread that terminates this pod when its budget runs out.

    `runctl launch` returns as soon as the pod is created, so there is no context manager
    holding it — which meant, briefly, that a pod created this way had NO automatic
    termination at all. That is the exact failure this project keeps circling: an in-pod
    timeout cannot terminate a RunPod pod, so if nothing out here is counting, nothing is.

    A daemon thread dies with the shell, so this alone is not sufficient — it covers the
    common case where the terminal stays open. `_register_with_control` covers the case
    where it does not, and `reaper.journal` covers both by leaving a trail a later
    `sweep()` can act on. Three partial guarantees that fail in different ways.
    """
    import threading

    def kill():
        remaining = deadline_ts - time.time()
        if remaining > 0:
            time.sleep(remaining)
        try:
            from .api import RunPodAPI
            RunPodAPI().terminate(pod_id)
            print(f"\n  [deadline] terminated {pod_id} — budget exhausted "
                  f"(${cost_hr}/hr)", flush=True)
        except Exception as e:
            print(f"\n  [deadline] COULD NOT terminate {pod_id}: {e}\n"
                  f"  Kill it manually: python runctl.py kill --pod {pod_id}", flush=True)

    threading.Thread(target=kill, daemon=True, name=f"deadline-{pod_id}").start()


def _register_with_control(cfg, pod_id: str, deadline_ts: float, cost_hr: float,
                           job_id: str) -> None:
    """Hand the deadline to pod-control, which outlives this shell.

    Registered AFTER creation here, which leaves a window where the pod exists and
    pod-control has not heard of it. That window is why the sweep has a grace period for
    unregistered pods — without it, this pod would be reaped as an orphan seconds after
    being created.
    """
    if not getattr(cfg, "control_url", ""):
        return
    import json
    import urllib.request

    from ..base_loader import (_control_token, _pinned_context, _verify_pin,
                               control_pod_token)
    try:
        req = urllib.request.Request(
            cfg.control_url.rstrip("/") + "/v1/register",
            # The token the pod already has in its environment. Registered AFTER
            # creation because the environment is fixed at creation, which is before this
            # pod exists to be registered — the sweep's grace period covers that window.
            data=json.dumps({"pod_id": pod_id, "provider": "runpod",
                             "deadline_ts": deadline_ts, "cost_hr": cost_hr,
                             "job_id": job_id, "name": pod_name(job_id),
                             "pod_token": control_pod_token(cfg, job_id)}).encode(),
            headers={"Content-Type": "application/json",
                     "X-Podh-Token": _control_token(cfg)}, method="POST")
        ctx = _pinned_context(cfg)
        with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
            _verify_pin(r, ctx)
        print(f"  registered with pod-control (lifetime cap {cfg.max_lifetime_min:.0f}min)",
              flush=True)
    except Exception as e:
        # Loud, not fatal. The local thread and the journal still hold, but the operator
        # should know the durable guarantee is the one that failed.
        print(f"  ⚠️  could not register with pod-control: {type(e).__name__}. "
              f"The deadline is now only as good as this shell staying open.", flush=True)
