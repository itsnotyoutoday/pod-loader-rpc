"""`BaseLoader` — everything a launch does that is the same everywhere.

## The split

Most of launching a job has nothing to do with where it runs. Publishing the code,
staging the spec, composing the environment, validating both against the harness contract
— identical whether the work lands on a rented pod, a container on this laptop, or
something that does not exist yet.

So that work lives here, once, and a subclass supplies only the three things that genuinely
differ:

    preflight()   can this destination take work right now?
    start()       create the thing and return where to reach it
    stop()        destroy it

`launch()` is `final` in spirit: it runs the generic sequence and calls the hooks at the
points where the destination matters. A new backend therefore cannot accidentally skip
contract validation or forget to publish code, because it never writes that part.

## Why inheritance rather than a branch

This was the requirement from the first day: *"use object based interfaces and inheritance
to define local or runpod depending upon what object is used… the theory should allow us to
essentially add an adapter other than runpod one day."*

A branch on `TARGET` would put every future destination inside one function, so adding the
fourth risks the three that already work. A subclass adds a file and touches nothing else.

## Why `stop()` is on the base interface rather than optional

A local container that leaks costs nothing. A rented pod that leaks bills while you sleep,
and an in-pod timeout cannot terminate a RunPod pod — verified, `runpodctl` from inside
returns `Unauthorized`. Termination has to come from out here, so every destination answers
for it explicitly instead of by omission.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PodInfo:
    """One thing that currently exists and may be costing money."""

    pod_id: str
    name: str
    cost_hr: float = 0.0
    age_sec: float = 0.0
    target: str = ""
    running: bool = True

    @property
    def key(self) -> str:
        """Target-qualified. Two providers can hand out the same id string, and a
        collision would mean terminating the wrong machine."""
        return f"{self.target}:{self.pod_id}"

    def describe(self) -> dict:
        return {"target": self.target, "pod_id": self.pod_id, "name": self.name,
                "cost_hr": self.cost_hr, "age_min": round(self.age_sec / 60, 1),
                "running": self.running}


@dataclass
class Running:
    """A started job, and how to reach it."""

    target: str
    job_id: str
    handle: str                       # pod id, container id, pid — whatever the target uses
    endpoint: str = ""                # base URL for /v1, when there is one
    cost_hr: float = 0.0
    detail: dict = field(default_factory=dict)

    def describe(self) -> dict:
        return {"target": self.target, "job_id": self.job_id, "handle": self.handle,
                "endpoint": self.endpoint, "cost_hr": self.cost_hr, **self.detail}


class LoaderError(RuntimeError):
    pass


class BaseLoader:
    """Generic launch sequence. Subclass and implement the three hooks."""

    name = "base"

    # -- hooks a destination must supply -------------------------------------------------

    def preflight(self, cfg) -> list[str]:
        """Problems that would waste a launch, found before anything is created."""
        return []

    def start(self, cfg, *, job_id: str, env: dict, spec_key: str) -> Running:
        raise NotImplementedError(f"{type(self).__name__} must implement start()")

    def stop(self, handle: str) -> dict:
        raise NotImplementedError(f"{type(self).__name__} must implement stop()")

    def list_pods(self) -> list[PodInfo]:
        """Everything this target currently has running.

        Lives here rather than in a watchdog because it is provider-specific knowledge,
        and provider-specific knowledge belongs in one place. A separate service that
        reimplemented enumeration would be a second definition of the same API call — the
        shape of bug this project has already paid for repeatedly.

        MUST raise on failure rather than returning []. An empty list and a failed call
        are indistinguishable to a caller, and anything that quietly concludes "nothing is
        running" stops protecting you at the moment it matters most.
        """
        raise NotImplementedError(f"{type(self).__name__} cannot enumerate pods")

    def plan(self, cfg) -> str:
        """One line describing what start() would do. Shown by --dry-run."""
        return f"start one {self.name} instance"

    # -- the generic sequence, shared by every destination -------------------------------

    def publish_code(self, cfg) -> str:
        """Push the workload's code/ and return the root the pod should import from."""
        if not cfg.workload:
            return ""
        from . import sync
        r = sync.publish(cfg.workload)
        self._say(f"code: {r['files']} files → {r['root']}"
                  + ("   (MUTABLE dev path — commit to pin it)" if r["mutable"] else ""))
        return r["root"]

    def stage_spec(self, cfg, spec: dict, job_id: str) -> str:
        """Write the spec where the harness will read it, BEFORE anything is created.

        Before, not after: the pod is told its spec path at creation and never discovers
        it, so the object has to exist first or the harness starts and finds nothing.
        """
        from .objectstore import get_storage
        st = get_storage(cfg.store or None)
        key = f"runs/{job_id}/spec.json"
        st.client.put_object(Bucket=st.require().bucket, Key=key,
                             Body=json.dumps(spec).encode())
        self._say(f"spec: {key}")
        return key

    def validate(self, cfg, spec: dict, env: dict) -> list[str]:
        """Check the spec and the environment against the harness's own contract.

        Both, because they fail differently: a bad spec is rejected by the API with a
        useful message, while a missing environment variable stops the harness from
        booting at all — and a pod that never boots reports nothing.
        """
        from . import contract
        problems = [f"spec: {p}" for p in contract.validate_spec(spec)]
        problems += [f"env: {m}" for m in contract.check_env(env)]
        return problems

    def launch(self, cfg, *, spec: dict, job_id: str = "", dry_run: bool = False) -> Running | None:
        """The whole sequence. Subclasses do not override this."""
        from . import launchfile

        job_id = job_id or f"job{int(time.time())}"
        blocked = self.preflight(cfg)
        if blocked:
            raise LoaderError(f"target {self.name!r} is not usable:\n" +
                              "\n".join(f"    {b}" for b in blocked))

        code_root = self.publish_code(cfg)
        if code_root:
            spec = {**spec, "code": {"root": code_root,
                                     "rev": code_root.rsplit("/", 1)[-1]}}

        spec_key = self.stage_spec(cfg, spec, job_id) if not dry_run else \
            f"runs/{job_id}/spec.json"
        env = launchfile.pod_env(cfg, job_id=job_id, spec_key=spec_key,
                                 code_root=code_root)

        problems = self.validate(cfg, spec, env)
        if problems:
            raise LoaderError("this launch would fail on the harness:\n" +
                              "\n".join(f"    {p}" for p in problems) +
                              "\n  Caught here for free.")

        # Hand off to pod-control when one is configured. Everything above this line
        # already ran, so a submitted job is validated exactly as strictly as a local one
        # — the control plane inherits a checked spec rather than re-deriving trust.
        if cfg.control_url:
            if dry_run:
                self._say(f"--dry-run: would submit to {cfg.control_url} "
                          f"(queue deadline {cfg.queue_deadline_min:.0f}min)")
                return None
            return self.submit_to_control(cfg, job_id=job_id, env=env, spec_key=spec_key)

        if dry_run:
            self._say(f"--dry-run: validated for {self.name}; {self.plan(cfg)}")
            return None
        return self.start(cfg, job_id=job_id, env=env, spec_key=spec_key)

    def submit_to_control(self, cfg, *, job_id: str, env: dict,
                          spec_key: str) -> Running:
        """Queue the job with pod-control instead of provisioning it here.

        The queue, the placement walk and the deadline all move off this machine, which is
        the point: a laptop that sleeps stops being part of the critical path. The job
        waits there until capacity AND price are acceptable, or until its queue deadline
        expires.
        """
        import json
        import urllib.error
        import urllib.request

        from . import launchfile

        body = {
            "job_id": job_id,
            "spec_key": spec_key,
            "target": self.name,
            "env": env,
            "create": self.create_kwargs(cfg, env=env),
            "capacity": launchfile.capacity_kwargs(cfg),
            "max_cost_hr": cfg.max_cost_hr,
            "budget_min": cfg.max_lifetime_min,
            "queue_deadline_min": cfg.queue_deadline_min,
            "idempotency_key": cfg.idempotency_key or job_id,
        }
        req = urllib.request.Request(
            cfg.control_url.rstrip("/") + "/v1/jobs",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json",
                     "X-Podh-Token": _control_token(cfg)},
            method="POST")
        ctx = _pinned_context(cfg)
        try:
            with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
                _verify_pin(r, ctx)
                out = json.loads(r.read())
        except urllib.error.HTTPError as e:
            raise LoaderError(
                f"pod-control refused the job ({e.code}): "
                f"{e.read().decode()[:300]}") from None
        except Exception as e:
            raise LoaderError(
                f"cannot reach pod-control at {cfg.control_url}: {type(e).__name__}\n"
                f"  Unset PODH_CONTROL_URL to provision directly from here instead."
            ) from None

        self._say(f"queued with pod-control: {out.get('job_id')} "
                  f"({'already existed' if out.get('existing') else 'new'})")
        return Running(target=f"control:{self.name}", job_id=out.get("job_id", job_id),
                       handle=out.get("job_id", job_id), endpoint=cfg.control_url,
                       detail={"state": out.get("state", "queued"),
                               "queue_deadline_min": cfg.queue_deadline_min})

    def create_kwargs(self, cfg, *, env: dict) -> dict:
        """The provider-shaped creation arguments, so pod-control can create it later.

        Built here rather than there because it is provider knowledge, and provider
        knowledge lives in this package.
        """
        return {}

    # -- output --------------------------------------------------------------------------

    quiet = False

    def _say(self, msg: str) -> None:
        if not self.quiet:
            print(f"  {msg}", flush=True)


#: Registry. A new destination is an entry here plus a class — nothing in the launch path
#: changes, which is the whole point of the base class above.
#:
#:     runpod   rented pods
#:     docker   a container on this machine, same image
#:     direct   the harness in this interpreter
#:
#: RunPod is one vendor among several that rent containers by the hour — Vast.ai, Lambda,
#: Paperspace, CoreWeave, or a Kubernetes cluster you already own. Each is a subclass:
#: implement preflight/start/stop, register it here, and every part of the system above
#: this line is unchanged, because they all speak the same /v1 to the same harness image.
#: That portability is the reason the harness shares no code with this package and the two
#: agree only on a contract.
def _pinned_context(cfg):
    """Verify pod-control by pinned certificate, exactly as the pod does.

    The control plane has no DNS name, so no CA can vouch for it. Both ends are ours, so
    the client admits ONE certificate rather than trusting every CA to have never
    mis-issued. Without a pin configured this returns None and verification stays at the
    default — a self-signed endpoint then simply fails to connect, which is correct: the
    token must never travel over a connection nobody checked.
    """
    import hashlib
    import ssl

    want = (getattr(cfg, "control_fingerprint", "") or "").replace(":", "").lower()
    if not want:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx._podh_expected = want
    return ctx


def _verify_pin(response, ctx) -> None:
    """Confirm the certificate actually served matches the pin, after connecting."""
    import hashlib
    import ssl

    if ctx is None:
        return
    sock = getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None)
    if sock is None:
        return
    got = hashlib.sha256(sock.getpeercert(binary_form=True)).hexdigest()
    if got != ctx._podh_expected:
        raise ssl.SSLError(
            f"pod-control certificate does not match PODH_CONTROL_FINGERPRINT.\n"
            f"  expected {ctx._podh_expected[:24]}…\n  got      {got[:24]}…")


def _control_token(cfg) -> str:
    """Shared secret for pod-control. From a file or the environment, never a config
    value — a launch file sits beside the job and gets committed."""
    import os
    from pathlib import Path
    if getattr(cfg, "control_token_file", ""):
        p = Path(cfg.control_token_file).expanduser()
        if p.is_file():
            return p.read_text().strip()
    return os.environ.get("PODH_CONTROL_TOKEN", "")


def _registry() -> dict:
    from .local.loader import DirectLoader, DockerLoader
    from .runpod.loader import RunPodLoader
    return {l.name: l for l in (RunPodLoader(), DockerLoader(), DirectLoader())}


def get_loader(name: str) -> BaseLoader:
    """Resolve `TARGET` from the launch file."""
    reg = _registry()
    key = (name or "runpod").lower()
    if key not in reg:
        raise ValueError(
            f"unknown TARGET {name!r}; have {', '.join(sorted(reg))}.\n"
            f"  A new destination is a subclass of BaseLoader implementing preflight/"
            f"start/stop, registered in base_loader._registry(). Nothing else in the "
            f"launch path needs to change.")
    return reg[key]


if __name__ == "__main__":                      # python -m pod_loader.base_loader
    for n, l in sorted(_registry().items()):
        bad = l.preflight(type("C", (), {"budget_min": 60, "image": "", "workload": ""})())
        print(f"  {n:<8} {'ready' if not bad else 'unavailable: ' + bad[0].splitlines()[0]}")
