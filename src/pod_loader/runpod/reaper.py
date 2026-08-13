"""Guaranteed pod termination — the external janitor, now known to be mandatory.

## Why this is not optional

Verified on a real pod (2026-08-13): `runpodctl` invoked from INSIDE a RunPod pod returns
`Error: Unauthorized`. There is no pod-scoped credential. This settles the contradiction
between plexus's `trainer.Dockerfile` (which claimed self-delete works) and its
`runpod_cleanup.py` (which said it does not) — the latter was right.

The consequence is sharper than it first looks: `lingua-watchdog`'s MAX_LIFE_SEC ceiling
CANNOT terminate a RunPod pod. It will faithfully detect the timeout, call self-delete, be
refused, and log that it was refused — while the pod keeps billing. An in-pod ceiling is a
detection mechanism, not a cost control. Treating it as one is how a pod runs overnight.

So termination must come from outside, and it must not depend on a human noticing.

## Three layers, each covering the previous one's failure

    with pod(...) as p:      terminates on success, exception, KeyboardInterrupt, SIGTERM
    deadline thread          terminates when wall-clock exceeds the budget, even if the
                             main thread is blocked on a hung HTTP call
    sweep()                  terminates anything left behind by a process that died before
                             its finally block could run — the case the first two cannot
                             cover, because a killed process runs no cleanup

Every launch is journaled to disk BEFORE the API call returns, so a pod that exists but
whose creating process vanished is still discoverable. A pod that is never journaled is a
pod nobody can find.
"""
from __future__ import annotations

import atexit
import json
import os
import signal
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

REGISTRY = Path(os.environ.get(
    "PODH_POD_REGISTRY",
    Path.home() / ".lingua" / "pods.jsonl"))

# ## Ephemeral pods are named differently from real work, and that distinction is safety
#
# `runners/batch_pod.py` names real jobs `lingua-<job_id>`. A sweep keyed on the generic
# `lingua-` prefix would therefore collect a deliberate multi-hour corpus build the moment
# it passed the age limit — destroying exactly the work this janitor exists to protect the
# budget for.
#
# So EPHEMERAL is a distinct, longer prefix. Only pods launched by `pod()` for tests and
# probes carry it, and only those are ever swept. A pod launched to run for days is simply
# not named this, and is invisible to the janitor by construction.
EPHEMERAL_PREFIX = "lingua-test-"

# Kept for callers that want to inspect real jobs; NEVER used as a sweep default.
JOB_PREFIX = "lingua-"
NAME_PREFIX = EPHEMERAL_PREFIX


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _api():
    # A relative import needs no sys.path surgery. This used to prepend an absolute path
    # to the pre-split monolith on one particular laptop — dead after the repo split, and
    # it leaked a home directory into every checkout.
    from .api import RunPodAPI
    return RunPodAPI()


def journal(pod_id: str, name: str, cost_hr: float, deadline_ts: float) -> None:
    """Record a launch. Written before anything else can fail."""
    REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    with REGISTRY.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"pod_id": pod_id, "name": name, "cost_hr": cost_hr,
                            "launched": _now(), "deadline_ts": deadline_ts,
                            "pid": os.getpid()}) + "\n")
        f.flush()
        os.fsync(f.fileno())


def terminate(pod_id: str, *, why: str = "") -> bool:
    """Idempotent. A 404 means it is already gone, which is success."""
    try:
        _api().terminate(pod_id)
        print(f"  [reaper] terminated {pod_id} {why}".rstrip(), flush=True)
        return True
    except Exception as exc:
        if "404" in str(exc) or "not found" in str(exc).lower():
            print(f"  [reaper] {pod_id} already gone", flush=True)
            return True
        print(f"  [reaper] FAILED to terminate {pod_id}: {exc}", flush=True)
        return False


def list_pods() -> list[dict]:
    api = _api()
    try:
        pods = api.list_pods() if hasattr(api, "list_pods") else api._call("GET", "/pods")
    except Exception as exc:
        print(f"  [reaper] cannot list pods: {exc}", flush=True)
        return []
    if isinstance(pods, dict):
        pods = pods.get("data") or pods.get("pods") or []
    return pods or []


def sweep(*, max_age_min: float = 30.0, prefix: str = EPHEMERAL_PREFIX,
          dry_run: bool = False, force: bool = False) -> dict:
    """Kill EPHEMERAL pods older than max_age_min. The backstop for a dead launcher.

    Deliberately narrow. It only touches names starting with `lingua-test-`, which only
    `pod()` assigns. A corpus build named `lingua-neutro_v3` is invisible here no matter how
    long it has been running — a job that is supposed to take days must never be at the
    mercy of a janitor's age limit.

    Broadening the prefix requires `force=True`, because `prefix="lingua-"` would match real
    work and the mistake is unrecoverable.
    """
    if not prefix.startswith(EPHEMERAL_PREFIX) and not force:
        raise ValueError(
            f"refusing to sweep with prefix {prefix!r}: it can match real jobs "
            f"(batch_pod.py names them 'lingua-<job_id>'). Ephemeral pods use "
            f"{EPHEMERAL_PREFIX!r}. Pass force=True only if you are certain.")
    killed, kept, cost = [], [], 0.0
    for p in list_pods():
        name = p.get("name") or ""
        pid = p.get("id")
        c = float(p.get("costPerHr") or 0)
        rt = p.get("runtime") or {}
        up = (rt.get("uptimeInSeconds") if isinstance(rt, dict) else None) or 0
        if not name.startswith(prefix):
            kept.append({"pod_id": pid, "name": name, "why": "not ours"})
            continue
        if up / 60.0 <= max_age_min:
            kept.append({"pod_id": pid, "name": name, "age_min": round(up / 60, 1)})
            continue
        cost += c
        if dry_run:
            killed.append({"pod_id": pid, "name": name, "age_min": round(up / 60, 1),
                           "dry_run": True})
        elif terminate(pid, why=f"(sweep: age {up/60:.1f}min > {max_age_min}min)"):
            killed.append({"pod_id": pid, "name": name, "age_min": round(up / 60, 1)})
    return {"killed": killed, "kept": kept, "reclaimed_per_hr": round(cost, 3)}


#: CPU shapes, in preference order. A pinned datacenter runs out of one shape while others
#: sit free — observed repeatedly on US-NC-1, which a network volume pins you to.
FLAVORS = ("cpu3c", "cpu3g", "cpu5c", "cpu5g", "cpu3m", "cpu5m")

#: GPU types, cheapest-capable first. The plexus pattern: RunPod takes the first with
#: capacity, so a sold-out model degrades instead of failing.
GPU_TYPES = ("NVIDIA RTX A4000", "NVIDIA RTX A4500", "NVIDIA RTX A5000",
             "NVIDIA GeForce RTX 4090")


def _is_capacity_error(exc) -> bool:
    """RunPod reports a full rack as HTTP 500, indistinguishable from a real server error
    unless you read the body."""
    return "no longer any instances" in str(exc) or "no instances available" in str(exc)


class TooExpensive(RuntimeError):
    """A placement was available but cost more than the job allows. Waiting is correct."""


def placements(*, compute: str = "CPU", flavors=FLAVORS, gpu_types=GPU_TYPES,
               clouds=("SECURE", "COMMUNITY"), fallback_to_gpu: bool = False):
    """Every shape worth trying, in the order to try them.

    Three dimensions, not one. plexus walked GPU models only, and that would not have
    saved today: all six CPU flavours were exhausted on both clouds while GPU had
    capacity. A list within one compute type is not enough when a volume pins you to a
    single datacenter and removes every other region as an option.

        flavor   cpu3c -> cpu3g -> cpu5c -> cpu5g -> cpu3m -> cpu5m
        cloud    SECURE -> COMMUNITY
        type     CPU -> GPU          <- the dimension plexus did not have

    GPU fallback is OPT-IN because it is a ~12x price jump ($0.06/hr -> $0.74/hr). A
    four-minute benchmark does not care; a six-hour corpus build is $0.36 against $4.44.
    Silently taking it would be the expensive kind of helpful.
    """
    want_gpu = compute.upper() == "GPU"
    for cloud in clouds:
        for flavor in (flavors if not want_gpu else ()):
            yield {"compute_type": "CPU", "cpu_flavor_ids": [flavor],
                   "cloud_type": cloud, "_label": f"{cloud}/CPU/{flavor}"}
    if want_gpu or fallback_to_gpu:
        for cloud in clouds:
            for gpu in gpu_types:
                yield {"compute_type": "GPU", "gpu_type_ids": [gpu], "gpu_count": 1,
                       "cloud_type": cloud, "_label": f"{cloud}/GPU/{gpu}"}


def create_with_capacity(api, create_kwargs: dict, *, compute: str = "CPU",
                         flavors=FLAVORS, gpu_types=GPU_TYPES,
                         clouds=("SECURE", "COMMUNITY"), fallback_to_gpu: bool = False,
                         max_cost_hr: float = 0.0, verbose: bool = True):
    """Create a pod, walking placements until one is available AND affordable.

    There is no way to ASK RunPod whether a shape is free — no dry run, no availability
    endpoint that reflects reality — so the only honest probe is to try creating, and to
    treat a capacity 500 as "next placement" rather than an outage.

    Cost and capacity resolve together into one decision. If the only free slot costs more
    than the job allows, that is not a placement — it is a reason to keep waiting. Checking
    them separately is how you end up paying 12x for a job that was happy to wait.
    """
    tried, too_dear = [], []
    for place in placements(compute=compute, flavors=flavors, gpu_types=gpu_types,
                            clouds=clouds, fallback_to_gpu=fallback_to_gpu):
        label = place.pop("_label")
        kw = {**create_kwargs, **place}
        try:
            pod = api.create(**kw)
        except Exception as exc:
            if not _is_capacity_error(exc):
                raise
            tried.append(label)
            if verbose:
                print(f"  [capacity] {label} full", flush=True)
            continue

        cost = float(pod.get("costPerHr") or 0)
        if max_cost_hr and cost > max_cost_hr:
            # Terminate immediately: it exists and is billing from this instant.
            try:
                api.terminate(pod["id"])
            except Exception:
                pass
            too_dear.append(f"{label} @ ${cost}/hr")
            if verbose:
                print(f"  [capacity] {label} available but ${cost}/hr exceeds "
                      f"${max_cost_hr}/hr — released", flush=True)
            continue

        if verbose and tried:
            print(f"  [capacity] got {label} @ ${cost}/hr after {len(tried)} full",
                  flush=True)
        return pod, kw

    msg = "no acceptable placement.\n"
    if tried:
        msg += f"  full: {', '.join(tried)}\n"
    if too_dear:
        msg += (f"  available but over the ${max_cost_hr}/hr ceiling: "
                f"{', '.join(too_dear)}\n"
                f"  Raise MAX_COST_HR to take one, or wait for a cheaper shape.\n")
    if not fallback_to_gpu and tried:
        msg += "  GPU fallback is off; set FALLBACK_TO_GPU=true to widen the search.\n"
    msg += ("  A network volume pins compute to its datacenter, so there is no other "
            "region to fall back to. Either wait, or run without a volume.")
    raise TooExpensive(msg) if too_dear and not tried else RuntimeError(msg)


@contextmanager
def pod(create_kwargs: dict, *, budget_min: float = 15.0, name: str | None = None,
        capacity: dict | None = None):
    """Launch a pod that CANNOT outlive its budget.

    Termination is attempted from four places, because each covers a failure the others
    do not:

        finally         normal exit and ordinary exceptions
        signal handlers SIGINT / SIGTERM — an operator pressing ctrl-C
        atexit          interpreter shutdown paths that skip finally
        deadline thread wall clock, even while the main thread is blocked on a hung
                        network call — the case that actually bills overnight

    The deadline thread is a daemon and holds the pod id directly, so it does not depend
    on any state the main thread might be stuck mutating.
    """
    api = _api()
    nm = name or f"{EPHEMERAL_PREFIX}{int(time.time())}"
    create_kwargs = {**create_kwargs, "name": nm}

    p, create_kwargs = create_with_capacity(api, create_kwargs, **(capacity or {}))
    pid = p.get("id")
    if not pid:
        raise RuntimeError(f"pod create returned no id: {p}")
    deadline = time.time() + budget_min * 60
    journal(pid, nm, float(p.get("costPerHr") or 0), deadline)
    print(f"  [reaper] launched {pid} ({nm}) ${p.get('costPerHr')}/hr "
          f"budget={budget_min}min", flush=True)

    done = threading.Event()

    def _watch():
        # Poll rather than one long sleep, so the budget is honoured even if the process
        # was suspended.
        while not done.wait(5):
            if time.time() >= deadline:
                print(f"  [reaper] BUDGET EXCEEDED ({budget_min}min) — killing {pid}",
                      flush=True)
                terminate(pid, why="(budget)")
                os._exit(2)      # hard exit: the main thread is presumed wedged

    threading.Thread(target=_watch, daemon=True).start()

    def _bail(*_a):
        terminate(pid, why="(signal)")
        os._exit(130)

    prev = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    for s in prev:
        try:
            signal.signal(s, _bail)
        except ValueError:
            pass                 # not on the main thread; finally still covers us
    atexit.register(lambda: terminate(pid, why="(atexit)"))

    try:
        yield {"pod_id": pid, "name": nm, "cost_hr": p.get("costPerHr"), "raw": p}
    finally:
        done.set()
        terminate(pid, why="(finally)")
        for s, h in prev.items():
            try:
                signal.signal(s, h)
            except (ValueError, TypeError):
                pass


def http_endpoint(pod_id: str, port: int = 8000) -> str:
    """Where an HTTP port actually lives on RunPod.

    Asking for `"8000/http"` does NOT populate publicIp or portMappings — RunPod proxies
    HTTP ports instead, and the API fields stay empty forever. During the first pod test
    that made a perfectly healthy pod look dead for two minutes. TCP ports behave the other
    way round (publicIp plus a mapped port), so the distinction is worth encoding rather
    than rediscovering.
    """
    return f"https://{pod_id}-{port}.proxy.runpod.net"


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="pod janitor")
    ap.add_argument("cmd", choices=["list", "sweep", "kill"])
    ap.add_argument("--max-age-min", type=float, default=30.0)
    ap.add_argument("--prefix", default=EPHEMERAL_PREFIX)
    ap.add_argument("--force", action="store_true",
                    help="allow a prefix that could match real jobs (dangerous)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--pod-id")
    a = ap.parse_args()

    if a.cmd == "list":
        pods = list_pods()
        if not pods:
            print("  no pods running")
            return 0
        total = 0.0
        for p in pods:
            c = float(p.get("costPerHr") or 0)
            total += c
            rt = p.get("runtime") or {}
            up = (rt.get("uptimeInSeconds") if isinstance(rt, dict) else None) or 0
            print(f"  {p.get('id')}  {p.get('name'):32}  ${c}/hr  {up/60:.1f}min")
        print(f"  TOTAL ${total:.3f}/hr")
        return 0
    if a.cmd == "kill":
        if not a.pod_id:
            print("--pod-id required")
            return 1
        return 0 if terminate(a.pod_id, why="(manual)") else 1
    r = sweep(max_age_min=a.max_age_min, prefix=a.prefix, dry_run=a.dry_run,
              force=a.force)
    print(json.dumps(r, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
