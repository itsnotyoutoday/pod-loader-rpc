"""Typed pieces of a job submission (docs/APP-API.md r3, `POST /v1/jobs`).

Each dataclass serialises to exactly the JSON the API takes, leaving out what was not set,
so the server's defaults apply rather than ours. Times are seconds everywhere. Anything
the dataclasses do not name yet goes in `extra` and is merged in unchanged, so a field the
server learns tomorrow does not need a client release today.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any, Optional


def _clean(obj) -> Any:
    """Dataclass → dict, recursively, dropping None and merging `extra`."""
    if hasattr(obj, "__dataclass_fields__"):
        out = {}
        for f in fields(obj):
            if f.name == "extra":
                continue
            v = _clean(getattr(obj, f.name))
            if v is not None:
                out[f.name] = v
        out.update(getattr(obj, "extra", None) or {})
        return out
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    return obj


class _Spec:
    def to_dict(self) -> dict:
        return _clean(self)


@dataclass
class Gpu(_Spec):
    """`gpu`: what card the pod needs. Omit the whole thing for a CPU pod."""
    count: int = 1
    min_vram_gb: Optional[int] = None
    types: Optional[list] = None            # explicit ordered list; overrides the catalogue
    clouds: Optional[list] = None
    max_cost_hr: Optional[float] = None
    allowed_cuda: Optional[list] = None
    min_ram_gb: Optional[int] = None
    datacenter_only: Optional[bool] = None
    interruptible: Optional[bool] = None    # spot: may be preempted, is never parked
    extra: dict = field(default_factory=dict)


@dataclass
class Boot(_Spec):
    """`boot`: what must be true before the pod is `ready`.

    The nested parts stay plain dicts on purpose: they are the spec's own shapes
    (`models: {"profile", "paths"}`, `addons: {"set", "version"}`, `app: {"fetch", "run",
    "serve"}`, `workload: {...}`) and wrapping each would only add a second place to read.
    `Boot.models_from(profile, paths)` is the one shorthand, because it is the part that
    changes on every launch.
    """
    gpu_check: Optional[bool] = None
    binaries: Optional[list] = None
    models: Optional[dict] = None
    addons: Optional[dict] = None
    app: Optional[dict] = None
    workload: Optional[dict] = None
    extra: dict = field(default_factory=dict)

    @staticmethod
    def models_from(profile: str, paths: list) -> dict:
        return {"profile": profile, "paths": list(paths)}


@dataclass
class Policy(_Spec):
    """`policy`: how long the pod may boot, idle, stay warm, stay parked and live."""
    boot_stall_s: Optional[int] = None
    boot_timeout_s: Optional[int] = None
    idle_stop_s: Optional[int] = None
    keep_warm_until: Optional[str] = None   # ISO time; the starting value only, see warm()
    park_ttl_s: Optional[int] = None        # 0 = never park, terminate on idle
    max_life_s: Optional[int] = None
    extra: dict = field(default_factory=dict)


@dataclass
class JobSpec(_Spec):
    """A whole `POST /v1/jobs` body."""
    lane: str
    image: str
    gpu: Optional[Gpu] = None
    cpu: Optional[dict] = None
    disk: Optional[dict] = None
    ports: Optional[list] = None
    env: Optional[dict] = None              # never read back by the API
    boot: Optional[Boot] = None
    policy: Optional[Policy] = None
    reuse: Optional[str] = None             # "never" (server default) | "parked-first"
    labels: Optional[dict] = None
    job_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    queue_deadline_s: Optional[int] = 900
    extra: dict = field(default_factory=dict)


def spec_dict(spec) -> dict:
    """A JobSpec or an already-built dict, as the JSON body."""
    if isinstance(spec, dict):
        return _clean(spec)
    if hasattr(spec, "to_dict"):
        return spec.to_dict()
    raise TypeError(f"a job spec is a JobSpec or a dict, not {type(spec).__name__}")


# -- tasks (docs/TASK-API.md, `POST /v1/tasks`) ----------------------------------------------

@dataclass
class Needs(_Spec):
    """`needs`: what a pod must have to claim the task.

    `models` are paths or `{"path", "sha256"?, "size"?, "source"?}` dicts; a `source`
    (`https://`, `hf://`, `civitai://`) lets pod-control seed a file its store lacks.
    `addons`, when given, must equal the pool template's `boot.addons`. `loads` is the
    affinity list (`["ckpt:<id>", "wf:<id>"]`, the first entry weighted x2)."""
    min_vram_gb: Optional[float] = None
    models: Optional[list] = None
    addons: Optional[dict] = None
    nvenc: Optional[bool] = None
    loads: Optional[list] = None
    fits_window_s: Optional[float] = None
    extra: dict = field(default_factory=dict)


@dataclass
class TaskSpec(_Spec):
    """A whole `POST /v1/tasks` body. `lane` may be left out for a token confined to one
    lane. Server defaults when unset: priority 0, deadline_s 86400, max_attempts 2,
    lease_s 180, timeout_s 1800 (0 = none for a pool with no workload), max_releases 5."""
    pool: str
    kind: str                               # "comfy" or "<app>.<handler>"
    lane: Optional[str] = None
    task_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    priority: Optional[int] = None          # higher first; FIFO within a priority
    deadline_s: Optional[float] = None      # expire if not claimed by then
    max_attempts: Optional[int] = None
    lease_s: Optional[float] = None
    timeout_s: Optional[float] = None
    max_releases: Optional[int] = None
    needs: Optional[Needs] = None
    after: Optional[list] = None            # task ids that must succeed first
    workflow: Optional[dict] = None         # {"ref", "inline", "patch", "map"}
    inputs: Optional[dict] = None           # name -> store:// | https:// | {"ref", "sha256"?, "size"?} | {"task", "artifact"|"result"}
    outputs: Optional[dict] = None          # {"to", "keep", "names"} or {"put_url"}
    payload: Optional[dict] = None          # opaque to pod-control
    labels: Optional[dict] = None
    fallback: Optional[str] = None          # "app": never launches, ends `returned`
    fallback_after_s: Optional[float] = None
    extra: dict = field(default_factory=dict)


def task_dict(spec) -> dict:
    """A TaskSpec or an already-built dict, as the JSON body."""
    if isinstance(spec, dict):
        return _clean(spec)
    if hasattr(spec, "to_dict"):
        return spec.to_dict()
    raise TypeError(f"a task spec is a TaskSpec or a dict, not {type(spec).__name__}")
