"""The Task API half of FakeControl (docs/TASK-API.md r2 + §9 server notes): pools, tasks,
the autoscaler's launch bookkeeping, and hooks that play a pool pod's worker.

Mixed into `pod_loader.app.testing.FakeControl`; its module docstring lists what this
follows and simplifies. Shapes follow pod-control's taskapi.py / tasks.py / pools.py.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from typing import Optional

from ._fake_errors import FakeError, _HTTPError

#: A task on a pod: it holds a lease and keeps the pod busy.
TASK_ACTIVE = ("claimed", "held", "running")
TASK_OPEN = ("queued", "held", "claimed", "running")
TASK_FINAL = ("succeeded", "failed", "cancelled", "expired", "returned")

TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
KIND_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}(\.[A-Za-z0-9][A-Za-z0-9_-]{0,63})?$")
INPUT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
POOL_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SOURCES = ("https://", "hf://", "civitai://")

TASK_DEFAULTS = {"priority": 0, "deadline_s": 86400.0, "max_attempts": 2, "lease_s": 180.0,
                 "timeout_s": 1800.0, "max_releases": 5}
SUBMIT_KEYS = {"task_id", "idempotency_key", "lane", "pool", "kind", "priority", "deadline_s",
               "max_attempts", "lease_s", "timeout_s", "max_releases", "needs", "after",
               "workflow", "inputs", "outputs", "payload", "labels", "fallback",
               "fallback_after_s"}
NEEDS_KEYS = {"min_vram_gb", "models", "addons", "nvenc", "loads", "fits_window_s"}
WORKFLOW_KEYS = {"ref", "inline", "patch", "map"}
SCALE_KEYS = {"min_pods", "max_pods", "launch_when", "prelaunch", "never_launch",
              "tasks_per_pod", "cost_share"}
MATCH_KEYS = {"affinity_wait_s", "accepts_from"}

MAX_TASK_BYTES = 256 * 1024
MAX_RESULT_BYTES = 256 * 1024
MAX_EVENT_BYTES = 16 * 1024
MAX_ARTIFACTS = 500
PROGRESS_EVERY_S = 5.0
EVENT_EVERY_S = 1.0
POOL_IDLE_STOP_S = 300
POOL_QUEUE_DEADLINE_S = 900
EXPECT_COOLDOWN_S, EXPECT_COOLDOWN_MAX_S = 900.0, 4 * 3600.0
EXPECT_GRACE_S = 600.0
MIN_POD_WARM_S = 600.0


def _dumps(v) -> str:
    return json.dumps(v, separators=(",", ":"), sort_keys=True)


def payload_sha(payload, workflow) -> str:
    """The server's attempt hash of what a pod was handed."""
    return hashlib.sha256(_dumps({"payload": payload, "workflow": workflow}).encode()).hexdigest()


def _num(v, what: str, lo: float, hi: float, integer: bool = False):
    if isinstance(v, bool) or not isinstance(v, (int, float)) or (integer and int(v) != v):
        raise _HTTPError(422, f"{what} must be a {'whole ' if integer else ''}number")
    if not lo <= v <= hi:
        raise _HTTPError(422, f"{what} must be {lo}-{hi}")
    return int(v) if integer else float(v)


def _only(d, keys: set, what: str) -> dict:
    if not isinstance(d, dict):
        raise _HTTPError(422, f"{what} must be an object")
    extra = set(d) - keys
    if extra:
        raise _HTTPError(422, f"{what}: unknown keys {sorted(extra)}")
    return d


def _strs(v, what: str, max_n: int) -> list:
    if not isinstance(v, list) or len(v) > max_n or not all(isinstance(x, str) and x for x in v):
        raise _HTTPError(422, f"{what} must be a list of at most {max_n} strings")
    return list(v)


def _rel(p, what: str) -> str:
    if not isinstance(p, str) or not p or p.startswith("/") or "\\" in p \
            or ".." in p.split("/"):
        raise _HTTPError(422, f"{what} {p!r} must be a relative path without '..'")
    return p


def _store_ref(ref, what: str) -> str:
    if not isinstance(ref, str) or not ref.startswith("store://") \
            or "/" not in ref[len("store://"):]:
        raise _HTTPError(422, f"{what} must be store://<store>/<key>")
    return ref


@dataclass
class _Pool:
    lane: str
    name: str
    version: int
    job: dict
    scale: dict
    match: dict
    updated_ts: float
    launches: list = field(default_factory=list)
    expect: Optional[dict] = None
    expect_wrong: int = 0
    cooldown_until: float = 0.0
    last_error: str = ""


@dataclass
class _Task:
    task_id: str
    lane: str
    pool: str
    kind: str
    priority: int
    idem: str
    deadline_s: float
    deadline_ts: float
    max_attempts: int
    lease_s: float
    timeout_s: float
    max_releases: int
    fallback: str
    fallback_after_s: float
    needs: dict
    workflow: dict
    inputs: dict
    outputs: dict
    payload: dict
    labels: dict
    after: list
    submitted_at: float
    queued_at: float
    qseq: int
    state: str = "queued"
    attempt: int = 1
    job_id: str = ""
    pod_id: str = ""
    pod: dict = field(default_factory=dict)
    lease_until: float = 0.0
    claimed_at: float = 0.0
    started_at: float = 0.0
    ended_at: float = 0.0
    held_reason: str = ""
    held_since: float = 0.0
    held_s: float = 0.0
    progress: float = 0.0
    message: str = ""
    result: object = None
    artifacts: list = field(default_factory=list)
    error: Optional[dict] = None
    usage: dict = field(default_factory=dict)
    cancel_reason: str = ""
    releases: int = 0
    released_jobs: list = field(default_factory=list)
    progress_ev_ts: float = 0.0
    event_ev_ts: float = 0.0
    affinity_hit: bool = False
    prefetched: bool = False
    attempts: list = field(default_factory=list)
    seeds: list = field(default_factory=list)       # model paths seeded for this task

    def held_now(self, now: float) -> float:
        h = self.held_s
        if self.state == "held" and self.held_since:
            h += max(0.0, now - self.held_since)
        return h


def _affinity(want: list, loaded: list) -> int:
    if not want or not loaded:
        return 0
    have = set(loaded)
    return sum(1 for x in want if x in have) + (1 if want[0] in have else 0)


def _pct(xs: list, p: float):
    if not xs:
        return None
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))], 3)


class TaskFake:
    """Mixin: needs FakeControl's `now`, `jobs`, `event_log`, `_submit`, `_settle`, `_iso`,
    `_spend`, `lane_backoff`, `lane_caps`, `ledger`, `park_cost_hr`, `_may`, `advance`."""

    def _tasks_init(self) -> None:
        self.pools: dict = {}            # (lane, name) -> _Pool
        self.task_rows: dict = {}        # task_id -> _Task, in submit order
        self.seeds: dict = {}            # model path -> {"state", "error", "source"}
        self._pod_loaded: dict = {}      # pod job id -> its last `have.loaded`
        self._qseq = 0

    # =================================================================== conveniences

    def task(self, task_id: str) -> dict:
        """The task as `GET /v1/tasks/{id}` shows it."""
        return self._task_view(self._t(task_id))

    def task_state(self, task_id: str) -> str:
        return self._t(task_id).state

    def pool_launches(self, lane: str, name: str) -> list:
        """Every pod the autoscaler asked for: `[{"job_id", "reason", "at", "version"}]`.
        Each is a real fake job (labels pool=, lane=) that the test places and readies
        like any other."""
        p = self.pools.get((lane, name))
        if p is None:
            raise FakeError(f"no pool {lane}/{name}")
        return [dict(x) for x in p.launches]

    def seed_done(self, path: str) -> None:
        """The store now has `path`: tasks held for seeding it go back to the queue."""
        self.seeds.setdefault(path, {})["state"] = "done"
        self.advance(0)

    def seed_fail(self, path: str, error: str = "seed failed") -> None:
        """Seeding `path` gave up: the tasks waiting on it fail at stage `seeding`."""
        s = self.seeds.setdefault(path, {})
        s["state"], s["error"] = "failed", error
        self.advance(0)

    # =================================================================== pod-side hooks

    def claim_tasks(self, pool: str, *, pod: Optional[str] = None, capacity: int = 1,
                    have_loaded: Optional[list] = None, have_models: Optional[list] = None,
                    vram_gb: Optional[float] = None, nvenc: Optional[bool] = None,
                    window_s: Optional[float] = None, cost_hr: Optional[float] = None) -> list:
        """`POST /v1/pod/tasks/claim` from a pod of `pool` ("lane/name", or a name unique
        across lanes). Returns the claim views (what the worker gets).

        `pod` is a job id the pool launched (it must be ready/busy/idle; a draining or
        parked one gets nothing), or any other string naming a virtual pod that never
        ends. Left out: the pool's first serving pod, else the virtual pod
        "<lane>-<name>-pod". VRAM, NVENC and cost come from the real pod's facts (virtual:
        48 GB, unknown NVENC, $0.50/h) unless given; `window_s` (for fits_window_s) is
        computed from a real pod's policy, unbounded for a virtual one."""
        p = self._pool_ref(pool)
        if isinstance(capacity, bool) or not isinstance(capacity, int) or not 0 <= capacity <= 16:
            raise FakeError("capacity must be a whole number 0-16")
        pid, job = self._claiming_pod(p, pod)
        if job is not None and (job.draining or job.state == "parked"):
            return []
        loaded = list(have_loaded or [])
        self._pod_loaded[pid] = loaded
        if capacity == 0:
            return []
        if job is not None:
            facts = job.pod or {}
            vram = float(vram_gb if vram_gb is not None else facts.get("vram_gb") or 0)
            pod_nvenc = nvenc if nvenc is not None else facts.get("nvenc")
            rate = float(cost_hr if cost_hr is not None else facts.get("cost_hr") or 0)
            win = window_s if window_s is not None else self._window(job)
            pod_view = {"job_id": pid, "pod_id": facts.get("id"),
                        "machine_id": facts.get("machine_id"), "gpu": facts.get("gpu"),
                        "vram_gb": vram or None, "nvenc": pod_nvenc, "cost_hr": rate,
                        "pool": p.name}
            present = {m for m, st in job.models.items() if st == "present"}
        else:
            vram = float(vram_gb if vram_gb is not None else 48)
            pod_nvenc = nvenc
            rate = float(cost_hr if cost_hr is not None else 0.50)
            win = window_s if window_s is not None else math.inf
            pod_view = {"job_id": pid, "pod_id": pid, "machine_id": None,
                        "gpu": "FAKE-GPU-48G", "vram_gb": vram, "nvenc": pod_nvenc,
                        "cost_hr": rate, "pool": p.name}
            present = set()
        holding = self._holding(pid)
        served = [p.name] + ([q.name for q in self.pools.values() if q.lane == p.lane
                              and q.name != p.name and p.name in q.match.get("accepts_from", [])]
                             if holding == 0 else [])
        tmpl_boot = p.job.get("boot") or {}
        jaddons = tmpl_boot.get("addons") or {}
        jstore = (tmpl_boot.get("models") or {}).get("profile") or ""
        have = set(have_models or [])
        picks = []
        for t in self.task_rows.values():
            if t.lane != p.lane or t.pool not in served or t.state != "queued":
                continue
            if self.now > t.deadline_ts or pid in t.released_jobs:
                continue
            n = t.needs
            if n.get("min_vram_gb") and vram < n["min_vram_gb"]:
                continue
            if n.get("nvenc") and pod_nvenc is not True:
                continue
            if n.get("addons") and n["addons"] != {k: jaddons.get(k) for k in ("set", "version")}:
                continue
            if n.get("models") and n.get("store") != jstore:
                continue
            if n.get("fits_window_s") and win < n["fits_window_s"]:
                continue
            if not self._deps_ok(t):
                continue
            score = _affinity(n.get("loads") or [], loaded)
            picks.append(((-t.priority, -score, t.queued_at, t.qseq), score, t))
        picks.sort(key=lambda x: x[0])
        out = []
        for _, score, t in picks:
            if len(out) >= capacity:
                break
            missing = [m for m in t.needs.get("models") or []
                       if m["path"] not in have and m["path"] not in present]
            t.state = "held" if missing else "claimed"
            t.job_id, t.pod_id, t.pod = pid, pod_view["pod_id"] or pid, dict(pod_view)
            t.lease_until = self.now + t.lease_s
            t.claimed_at = self.now
            t.held_reason = "pod_fetch" if missing else ""
            t.held_since = self.now if missing else 0.0
            t.progress, t.message = 0.0, ""
            t.affinity_hit = score > 0
            t.prefetched = holding + len(out) > 0
            t.attempts.append({"n": len(t.attempts) + 1, "attempt": t.attempt,
                               "payload_sha": payload_sha(t.payload, t.workflow),
                               "job_id": pid, "pod_id": t.pod_id, "claimed_at": self.now,
                               "started_at": None, "ended_at": None, "outcome": None,
                               "error": None})
            if missing and job is not None:
                added = False
                for m in missing:
                    if job.models.get(m["path"]) not in ("present", "fetching"):
                        job.models[m["path"]] = "fetching"
                        added = True
                if added:
                    job.models_rev += 1
            self._temit("task.claimed", t, state="claimed", attempt=t.attempt, affinity=score)
            if missing:
                self._temit("task.held", t, held_reason="pod_fetch",
                            paths=[m["path"] for m in missing])
            try:
                out.append(self._claim_view(t, missing))
            except _HTTPError as e:
                self._tfail(t, stage="inputs", reason=e.error[:300])
        self._sync_pod(pid)
        self.advance(0)
        return out

    def claim_task(self, pool: str, **kw) -> Optional[dict]:
        """`claim_tasks(pool, capacity=1, ...)`: the one claimed task, or None."""
        got = self.claim_tasks(pool, capacity=1, **kw)
        return got[0] if got else None

    def task_start(self, task_id: str) -> dict:
        """`POST /v1/pod/tasks/{id}/start`: claimed/held → running. On a task no longer
        on its pod (e.g. cancelled) the reply is the server's 409 body, `cancel: True`."""
        t = self._t(task_id)
        if t.state == "running":
            return {"state": "running", "already": True}
        if t.state not in ("claimed", "held"):
            return {"error": f"task {task_id} is {t.state}", "cancel": True, "state": t.state}
        t.held_s = t.held_now(self.now)
        t.state, t.started_at = "running", self.now
        t.held_reason, t.held_since = "", 0.0
        t.lease_until = self.now + t.lease_s
        if t.attempts and t.attempts[-1]["ended_at"] is None:
            t.attempts[-1]["started_at"] = self.now
        self._temit("task.running", t, attempt=t.attempt)
        self.advance(0)
        return {"state": "running", "lease_until": t.lease_until}

    def task_progress(self, task_id: str, progress: Optional[float] = None,
                      message: Optional[str] = None) -> dict:
        """A beat (`POST /v1/pod/tasks/{id}/beat`): renews the lease; `task.progress` at
        most once per 5 s. The reply's `cancel` is what the worker acts on."""
        t = self._t(task_id)
        if t.state not in TASK_ACTIVE:
            return {"cancel": True, "state": t.state}
        t.lease_until = self.now + t.lease_s
        loud = False
        if isinstance(progress, (int, float)) and not isinstance(progress, bool):
            t.progress = min(1.0, max(0.0, float(progress)))
            loud = True
        if isinstance(message, str):
            t.message = message[:500]
            loud = True
        if loud and self.now - t.progress_ev_ts >= PROGRESS_EVERY_S:
            t.progress_ev_ts = self.now
            self._temit("task.progress", t, progress=t.progress, message=t.message or None)
        self.advance(0)
        return {"cancel": False, "state": t.state, "lease_until": t.lease_until}

    task_beat = task_progress

    def task_event(self, task_id: str, data) -> dict:
        """`ctx.emit(data)`: a `task.event`, at most 1/s per task (extras are dropped)."""
        t = self._t(task_id)
        if t.state not in TASK_ACTIVE:
            return {"cancel": True, "state": t.state, "emitted": False}
        if len(json.dumps(data, default=str)) > MAX_EVENT_BYTES:
            raise FakeError(f"an event is at most {MAX_EVENT_BYTES} bytes (the server 422s)")
        if self.now - t.event_ev_ts < EVENT_EVERY_S:
            return {"cancel": False, "emitted": False, "reason": "rate limited (1/s)"}
        t.event_ev_ts = self.now
        self._temit("task.event", t, data=data)
        return {"cancel": False, "emitted": True}

    def task_artifact(self, task_id: str, name: str, ref: Optional[str] = None, *,
                      size: Optional[int] = None, sha256: Optional[str] = None) -> dict:
        """A mid-run upload: shows in GET at once, with a `task.artifact` event. With
        `outputs.to` the server names it `<to><name>` (a given `ref` is ignored, as the
        server does); otherwise `ref` is its https url (or, leniently, a store:// ref)."""
        t = self._t(task_id)
        if t.state not in TASK_ACTIVE:
            return {"cancel": True, "state": t.state}
        a = self._artifact(t, {"name": name, "ref": ref, "size": size, "sha256": sha256})
        t.artifacts = self._merge_artifacts(t, [a])
        self._temit("task.artifact", t, artifact=a)
        return {"cancel": False, "artifacts": len(t.artifacts)}

    def task_done(self, task_id: str, ok: bool, *, result=None, artifacts: Optional[list] = None,
                  usage: Optional[dict] = None, error: Optional[dict] = None,
                  retryable: bool = False) -> dict:
        """`POST /v1/pod/tasks/{id}/done`. ok → succeeded. Not ok → failed; or, with
        `retryable`, requeued at attempt + 1 (failed at max_attempts); or `returned` for a
        fallback "app" task. A task no longer on its pod is not changed (`accepted:
        False`, `cancel` when it was cancelled)."""
        t = self._t(task_id)
        if t.state not in TASK_ACTIVE:
            return {"accepted": False, "state": t.state, "cancel": t.state == "cancelled"}
        if result is not None and len(json.dumps(result, default=str)) > MAX_RESULT_BYTES:
            raise FakeError(f"result is at most {MAX_RESULT_BYTES} bytes (the server 422s)")
        t.artifacts = self._merge_artifacts(t, [self._artifact(t, a) for a in artifacts or []])
        t.usage = self._usage(t, usage)
        t.result = result
        t.held_s = t.held_now(self.now)
        t.held_since = self.now if t.state == "held" else 0.0
        pid = t.job_id
        if ok is True:
            t.state, t.ended_at, t.progress, t.lease_until = "succeeded", self.now, 1.0, 0.0
            self._close_attempt(t, "succeeded")
            self._temit("task.succeeded", t, result=result, artifacts=t.artifacts, usage=t.usage)
            state = "succeeded"
        else:
            e = error if isinstance(error, dict) else {}
            err = {"stage": str(e.get("stage") or "run")[:64],
                   "reason": str(e.get("reason") or "")[:500],
                   "log_tail": str(e["log_tail"])[-1024:] if e.get("log_tail") else None}
            if t.fallback == "app":
                state = self._give_back(t, f"failed on pod: {err['reason']}")
            elif retryable:
                state = self._lose(t, f"retryable: {err['reason']}", stage=err["stage"])
            else:
                self._tfail(t, stage=err["stage"], reason=err["reason"], log_tail=err["log_tail"])
                state = "failed"
        self._sync_pod(pid)
        self.advance(0)
        return {"accepted": True, "state": state}

    def task_release(self, task_id: str, reason: str = "released") -> dict:
        """The pod gives it back: requeued without counting an attempt, never offered to
        that pod again. "no handler" counts toward max_releases. A fallback "app" task is
        returned instead; a cancelled one stays cancelled."""
        t = self._t(task_id)
        if t.state not in TASK_ACTIVE:
            return {"released": False, "state": t.state}
        reason = str(reason or "released")[:200]
        pid = t.job_id
        if t.fallback == "app":
            st = self._give_back(t, f"released: {reason}")
        else:
            releases = t.releases + (1 if reason == "no handler" else 0)
            if releases >= t.max_releases:
                self._tfail(t, stage="handler", reason=f"{reason} (released {releases} times)")
                st = "failed"
            else:
                t.releases = releases
                t.released_jobs = (t.released_jobs + [pid])[-50:]
                self._close_attempt(t, "released", reason)
                self._temit("task.requeued", t, state="queued", reason=f"released: {reason}",
                            attempt=t.attempt, counted=False)
                self._requeue(t)
                st = "queued"
        self._sync_pod(pid)
        self.advance(0)
        return {"released": True, "state": st}

    def lose_lease(self, task_id: str) -> str:
        """The task's lease lapsed (the pod went quiet): requeued at attempt + 1, or
        failed at max_attempts; returned for a fallback "app" task. Nothing happens to a
        task that is not on a pod (cancel stays final). Returns the state after."""
        t = self._t(task_id)
        pid = t.job_id
        st = self._lose(t, "lease lost")
        self._sync_pod(pid)
        self.advance(0)
        return st

    # =================================================================== transitions

    def _t(self, task_id: str) -> _Task:
        t = self.task_rows.get(task_id)
        if t is None:
            raise FakeError(f"no task {task_id}")
        return t

    def _temit(self, kind: str, t: _Task, state: str = "", **detail) -> dict:
        ev = {"seq": len(self.event_log) + 1, "ts": self.now, "kind": kind,
              "job_id": t.task_id, "pod_id": t.pod_id or "", "state": state or t.state,
              "detail": {"task_id": t.task_id, "lane": t.lane, "pool": t.pool, "kind": t.kind,
                         **({"pod_job": t.job_id} if t.job_id else {}), **detail}}
        self.event_log.append(ev)
        return ev

    def _next_qseq(self) -> int:
        self._qseq += 1
        return self._qseq

    def _close_attempt(self, t: _Task, outcome: str, error: str = "") -> None:
        if t.attempts and t.attempts[-1]["ended_at"] is None:
            t.attempts[-1].update(ended_at=self.now, outcome=outcome, error=error[:300] or None)

    def _unbind(self, t: _Task) -> None:
        t.job_id, t.pod_id, t.pod = "", "", {}
        t.lease_until, t.held_reason, t.held_since = 0.0, "", 0.0

    def _requeue(self, t: _Task) -> None:
        t.held_s = t.held_now(self.now)
        self._unbind(t)
        t.state, t.progress, t.message = "queued", 0.0, ""
        t.queued_at, t.qseq = self.now, self._next_qseq()

    def _end_task(self, t: _Task, state: str, **fields) -> None:
        t.held_s = t.held_now(self.now)
        t.held_since = 0.0
        t.state, t.ended_at, t.lease_until = state, self.now, 0.0
        for k, v in fields.items():
            setattr(t, k, v)

    def _tfail(self, t: _Task, *, stage: str, reason: str, log_tail=None) -> bool:
        if t.fallback == "app":
            return self._give_back(t, f"{stage}: {reason}") == "returned"
        if t.state not in TASK_OPEN:
            return False
        was = t.state
        err = {"stage": stage, "reason": reason, "log_tail": log_tail}
        self._end_task(t, "failed", error=err)
        if was in TASK_ACTIVE:
            self._close_attempt(t, "failed", reason)
        self._temit("task.failed", t, state="failed", error=err, attempt=t.attempt)
        return True

    def _give_back(self, t: _Task, why: str) -> str:
        if t.state not in TASK_OPEN:
            return t.state
        was = t.state
        self._end_task(t, "returned", error={"stage": "fallback", "reason": why[:500],
                                             "log_tail": None})
        if was in TASK_ACTIVE:
            self._close_attempt(t, "returned", why)
        self._temit("task.returned", t, state="returned", reason=why)
        return "returned"

    def _lose(self, t: _Task, why: str, stage: str = "lease") -> str:
        """Lost by its pod: requeued at attempt + 1, or failed at max_attempts. Only from
        a pod-bound state, so never from cancelled."""
        if t.state not in TASK_ACTIVE or not t.job_id:
            return t.state
        if t.fallback == "app":
            return self._give_back(t, why)
        if t.attempt >= t.max_attempts:
            err = {"stage": stage, "reason": f"{why}; max_attempts ({t.max_attempts}) reached",
                   "log_tail": None}
            self._end_task(t, "failed", error=err)
            self._close_attempt(t, "lost", why)
            self._temit("task.failed", t, state="failed", error=err, attempt=t.attempt)
            return "failed"
        self._close_attempt(t, "lost", why)
        self._temit("task.requeued", t, state="queued", reason=why, attempt=t.attempt + 1,
                    counted=True)
        t.attempt += 1
        self._requeue(t)
        return "queued"

    def _tcancel(self, t: _Task, reason: str = "") -> bool:
        if t.state not in TASK_OPEN:
            return False
        was = t.state
        self._end_task(t, "cancelled", cancel_reason=str(reason or "")[:300])
        if was in TASK_ACTIVE:
            self._close_attempt(t, "cancelled")
        self._temit("task.cancelled", t, state="cancelled", was=was, reason=reason or None)
        self._sync_pod(t.job_id)
        return True

    def _holding(self, pid: str) -> int:
        return sum(1 for x in self.task_rows.values()
                   if x.job_id == pid and x.state in TASK_ACTIVE)

    def _sync_pod(self, pid: str) -> None:
        """A pod holding a task counts as busy (its heartbeat says so); when it holds none
        it is not, and its idle clock runs."""
        j = self.jobs.get(pid) if pid else None
        if j is None or j.state not in ("ready", "busy", "idle", "draining"):
            return
        want = self._holding(pid) > 0
        if j.busy != want:
            j.busy = want
            j.heartbeat_seen = True
            self._settle(j)

    def _window(self, j) -> float:
        pol = j.policy
        idle = float(pol.get("idle_stop_s") or 0)
        if not idle:
            w = math.inf
        else:
            since = j.idle_since if j.idle_since is not None else self.now
            w = max(0.0, idle - (self.now - since))
            w = max(w, (j.keep_warm_until or 0) - self.now)
        life = float(pol.get("max_life_s") or 0)
        if life and j.placed_at is not None:
            w = min(w, max(0.0, j.placed_at + life - self.now))
        return w

    def _deps_ok(self, t: _Task) -> bool:
        return all(a in self.task_rows and self.task_rows[a].state == "succeeded"
                   for a in t.after)

    def _pool_ref(self, pool: str) -> _Pool:
        if "/" in pool:
            lane, _, name = pool.partition("/")
            p = self.pools.get((lane, name))
        else:
            hits = [p for p in self.pools.values() if p.name == pool]
            if len(hits) > 1:
                raise FakeError(f"pool {pool!r} is in several lanes; say 'lane/{pool}'")
            p = hits[0] if hits else None
        if p is None:
            raise FakeError(f"no pool {pool}")
        return p

    def _claiming_pod(self, p: _Pool, pod: Optional[str]):
        if pod is not None:
            j = self.jobs.get(pod)
            if j is None:
                return pod, None                          # a virtual pod of that name
            if j.pool != (p.lane, p.name):
                raise FakeError(f"{pod} is not a pod of pool {p.lane}/{p.name}")
            if j.state not in ("ready", "busy", "idle", "draining", "parked"):
                raise FakeError(f"{pod} is {j.state}; only a ready pod claims tasks")
            return pod, j
        for j in self.jobs.values():
            if j.pool == (p.lane, p.name) and j.state in ("ready", "busy", "idle") \
                    and not j.draining:
                return j.job_id, j
        return f"{p.lane}-{p.name}-pod", None

    # =================================================================== views

    @staticmethod
    def _presign(ref: str) -> str:
        if ref.startswith("https://"):
            return ref
        return "https://fake-store.test/" + ref[len("store://"):] + "?X-Fake-Signature=1"

    def _claim_view(self, t: _Task, missing) -> dict:
        outputs = {k: v for k, v in t.outputs.items() if k != "names"}
        if t.outputs.get("names"):
            outputs["put"] = {n: self._presign(t.outputs["to"] + n) for n in t.outputs["names"]}
        if t.outputs.get("to"):
            outputs["sign"] = f"/v1/pod/tasks/{t.task_id}/outputs"
        return {"task_id": t.task_id, "kind": t.kind, "lane": t.lane, "pool": t.pool,
                "state": t.state, "attempt": t.attempt, "lease_s": t.lease_s,
                "lease_until": t.lease_until, "timeout_s": t.timeout_s or None,
                "needs": json.loads(json.dumps(t.needs)),
                "workflow": json.loads(json.dumps(t.workflow)),
                "payload": json.loads(json.dumps(t.payload)), "labels": dict(t.labels),
                "inputs": self._resolve_inputs(t), "outputs": outputs,
                "held_reason": t.held_reason or None,
                "missing_models": [m["path"] for m in missing]}

    def _resolve_inputs(self, t: _Task) -> dict:
        out = {}
        for name, ref in t.inputs.items():
            if isinstance(ref, dict) and "ref" in ref:
                r = ref["ref"]
                out[name] = {"url": self._presign(r),
                             **({"ref": r} if r.startswith("store://") else {}),
                             **{k: ref[k] for k in ("sha256", "size") if k in ref}}
                continue
            if isinstance(ref, dict):
                dep = self.task_rows.get(ref["task"])
                if dep is None or dep.state != "succeeded":
                    raise _HTTPError(409, f"inputs.{name}: task {ref['task']} has not succeeded")
                if "result" in ref:
                    cur = dep.result
                    for part in [x for x in ref["result"].split(".") if x]:
                        if isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
                            cur = cur[int(part)]
                        elif isinstance(cur, dict) and part in cur:
                            cur = cur[part]
                        else:
                            raise _HTTPError(409, f"result path {ref['result']!r} is not in the "
                                                  f"dependency's result")
                    out[name] = {"value": cur}
                    continue
                a = {x["name"]: x for x in dep.artifacts}.get(ref["artifact"])
                if not a:
                    raise _HTTPError(409, f"inputs.{name}: task {ref['task']} has no artifact "
                                          f"{ref['artifact']!r}")
                src = a.get("ref") or a.get("url")
                out[name] = {"url": self._presign(src), "ref": src}
            else:
                out[name] = {"url": self._presign(ref),
                             **({"ref": ref} if ref.startswith("store://") else {})}
        return out

    def _position(self, t: _Task) -> Optional[int]:
        if t.state != "queued":
            return None
        return 1 + sum(1 for x in self.task_rows.values()
                       if x.lane == t.lane and x.pool == t.pool and x.state == "queued"
                       and (x.priority > t.priority or (x.priority == t.priority
                                                        and (x.queued_at, x.qseq)
                                                        < (t.queued_at, t.qseq))))

    def _task_view(self, t: _Task, sign: bool = True) -> dict:
        arts = []
        for a in t.artifacts:
            v = dict(a)
            if a.get("ref"):
                v["url"] = self._presign(a["ref"]) if sign else None
            arts.append(v)
        seeding = None
        if t.held_reason == "seeding":
            seeding = [{"path": pth, "state": (self.seeds.get(pth) or {}).get("state", "pending"),
                        "error": (self.seeds.get(pth) or {}).get("error") or None}
                       for pth in t.seeds
                       if (self.seeds.get(pth) or {}).get("state") != "done"] or None
        return {"task_id": t.task_id, "lane": t.lane, "pool": t.pool, "kind": t.kind,
                "state": t.state, "held_reason": t.held_reason or None,
                "attempt": t.attempt, "max_attempts": t.max_attempts,
                "priority": t.priority, "position": self._position(t),
                "pod": dict(t.pod) or None, "progress": t.progress,
                "message": t.message or None, "result": t.result, "artifacts": arts,
                "error": t.error, "usage": dict(t.usage) or None,
                "timings": {"queued_at": t.queued_at or None, "claimed_at": t.claimed_at or None,
                            "started_at": t.started_at or None, "ended_at": t.ended_at or None,
                            "held_s": round(t.held_now(self.now), 3)},
                "deadline_at": t.deadline_ts or None, "lease_until": t.lease_until or None,
                "timeout_s": t.timeout_s or None, "fallback": t.fallback or None,
                "labels": dict(t.labels), "after": list(t.after),
                "needs": json.loads(json.dumps(t.needs)), "seeding": seeding,
                "payload_purged": False, "cancel_reason": t.cancel_reason or None,
                "attempts": [dict(a) for a in t.attempts]}

    def _artifact(self, t: _Task, a) -> dict:
        if not isinstance(a, dict):
            raise FakeError("each artifact is {name, size?, sha256?, url?|ref?}")
        name = a.get("name")
        try:
            _rel(name, "artifact name")
        except _HTTPError as e:
            raise FakeError(e.error)
        out = {"name": name}
        if isinstance(a.get("size"), int) and not isinstance(a["size"], bool) and a["size"] >= 0:
            out["size"] = a["size"]
        if isinstance(a.get("sha256"), str) and re.match(r"^[0-9a-fA-F]{64}$", a["sha256"]):
            out["sha256"] = a["sha256"].lower()
        given = a.get("url") or a.get("ref")
        if t.outputs.get("to"):
            out["ref"] = t.outputs["to"] + name
        elif isinstance(given, str) and given.startswith("https://"):
            out["url"] = given[:4096]
        elif isinstance(given, str) and given.startswith("store://"):
            out["ref"] = given
        else:
            raise FakeError(f"artifact {name!r}: this task has no store prefix, so give its "
                            f"https url (the server 422s)")
        return out

    @staticmethod
    def _merge_artifacts(t: _Task, new: list) -> list:
        have = {a["name"]: a for a in t.artifacts}
        for a in new:
            have[a["name"]] = a
        if len(have) > MAX_ARTIFACTS:
            raise FakeError(f"at most {MAX_ARTIFACTS} artifacts per task")
        return list(have.values())

    def _share(self, t: _Task) -> float:
        p = self.pools.get((t.lane, t.pool))
        w = ((p.scale.get("cost_share") if p else None) or {}).get(t.kind, 1)
        return float(w) if isinstance(w, (int, float)) and not isinstance(w, bool) and w >= 0 \
            else 1.0

    def _usage(self, t: _Task, body_usage) -> dict:
        j = self.jobs.get(t.job_id)
        rate = float(((j.pod or {}).get("cost_hr") if j else None) or t.pod.get("cost_hr") or 0)
        wall = max(0.0, self.now - (t.claimed_at or self.now))
        mine = self._share(t)
        total = mine + sum(self._share(x) for x in self.task_rows.values()
                           if x.job_id == t.job_id and x.state in TASK_ACTIVE and x is not t)
        share = (mine / total) if total > 0 else 1.0
        u = body_usage if isinstance(body_usage, dict) else {}
        phases = u.get("phases") if isinstance(u.get("phases"), dict) else {}
        out = {"gpu_type": t.pod.get("gpu"), "cost_usd": round(rate * wall / 3600 * share, 6),
               "wall_s": round(wall, 3), "held_s": round(t.held_now(self.now), 3),
               "queued_s": round(max(0.0, (t.claimed_at or self.now) - (t.queued_at or self.now)), 3),
               "affinity_hit": t.affinity_hit, "prefetched": t.prefetched,
               "phases": {k: float(v) for k, v in phases.items()
                          if k in ("fetch_s", "run_s", "upload_s")
                          and isinstance(v, (int, float)) and not isinstance(v, bool)}}
        if isinstance(u.get("vram_peak_mb"), (int, float)) and not isinstance(u["vram_peak_mb"], bool):
            out["vram_peak_mb"] = float(u["vram_peak_mb"])
        return out

    # =================================================================== the clock

    def _task_timers(self):
        """(due, task, action) for deadlines, fallbacks, timeouts and leases."""
        for t in self.task_rows.values():
            if t.state in ("queued", "held") and not t.job_id:
                yield t.deadline_ts, t, self._t_expire
            if t.state == "queued" and t.fallback == "app" and t.fallback_after_s:
                yield t.queued_at + t.fallback_after_s, t, \
                    (lambda x: self._give_back(x, f"unclaimed for {x.fallback_after_s:.0f}s"))
            if t.state == "running" and t.timeout_s and t.started_at:
                yield t.started_at + t.timeout_s, t, self._t_timeout
            if t.state in TASK_ACTIVE and t.job_id and t.lease_until:
                yield t.lease_until, t, self._t_lease

    def _t_expire(self, t: _Task) -> None:
        self._end_task(t, "expired")
        self._temit("task.expired", t, state="expired", waited_s=round(self.now - t.queued_at))

    def _t_timeout(self, t: _Task) -> None:
        self._tfail(t, stage="timeout", reason=f"ran past timeout_s {t.timeout_s:.0f}")
        self._sync_pod(t.job_id)

    def _t_lease(self, t: _Task) -> None:
        pid = t.job_id
        self._lose(t, "lease lost")
        self._sync_pod(pid)

    def _task_pass(self) -> None:
        """What the server's sweep and autoscaler do each pass, run after every advance
        (so after every API call and hook): dead dependencies, pods that ended, finished
        or failed seeds, then each pool's autoscale."""
        if not self.task_rows and not self.pools:
            return
        for _ in range(20):
            changed = False
            for t in list(self.task_rows.values()):
                if t.state in ("queued", "held") and not t.job_id:
                    dead = [(a, self.task_rows[a].state) for a in t.after if a in self.task_rows
                            and self.task_rows[a].state in ("failed", "cancelled", "expired",
                                                            "returned")]
                    if dead and self._tfail(t, stage="after",
                                            reason=f"dependency {dead[0][0]} ended {dead[0][1]}"):
                        changed = True
                        continue
                if t.state in TASK_ACTIVE and t.job_id in self.jobs:
                    j = self.jobs[t.job_id]
                    if j.state not in ("booting", "ready", "busy", "idle", "draining"):
                        self._lose(t, "pod ended")
                        changed = True
                        continue
                if t.state == "held" and t.held_reason == "seeding":
                    waiting = [pth for pth in t.seeds
                               if (self.seeds.get(pth) or {}).get("state") != "done"]
                    bad = [pth for pth in waiting
                           if (self.seeds.get(pth) or {}).get("state") == "failed"]
                    if bad:
                        self._tfail(t, stage="seeding",
                                    reason=f"{bad[0]}: {self.seeds[bad[0]].get('error')}"[:500])
                        changed = True
                    elif not waiting:
                        t.held_s = t.held_now(self.now)
                        t.state, t.held_reason, t.held_since = "queued", "", 0.0
                        t.qseq = self._next_qseq()
                        self._temit("task.queued", t, state="queued", seeded=True)
                        changed = True
            if not changed:
                break
        for p in list(self.pools.values()):
            self._scale_one(p)

    # =================================================================== autoscaler

    def _pool_jobs(self, p: _Pool, *, parked: bool = False) -> list:
        live = ("queued", "placing", "booting", "ready", "busy", "idle", "draining") \
            + (("parked",) if parked else ())
        return [j for j in self.jobs.values() if j.pool == (p.lane, p.name) and j.state in live]

    def _demand(self, p: _Pool) -> dict:
        rows = [t for t in self.task_rows.values() if t.lane == p.lane and t.pool == p.name
                and t.fallback != "app" and t.state in TASK_OPEN]
        queued = [t for t in rows if t.state == "queued" and self._deps_ok(t)]
        on_pods = [t for t in rows if t.state in ("claimed", "running")
                   or (t.state == "held" and t.held_reason != "seeding")]
        oldest = min((t.queued_at for t in queued), default=None)
        return {"queued": len(queued), "on_pods": len(on_pods),
                "oldest_wait_s": (self.now - oldest) if oldest is not None else 0.0}

    def _judge_expect(self, p: _Pool) -> Optional[dict]:
        hint = p.expect
        if not hint:
            return None
        if self.now < hint["judge_at"]:
            return hint
        came = any(t.lane == p.lane and t.pool == p.name and t.submitted_at >= hint["at"]
                   for t in self.task_rows.values())
        p.expect = None
        if came:
            p.expect_wrong = 0
        else:
            p.expect_wrong += 1
            p.cooldown_until = self.now + min(EXPECT_COOLDOWN_MAX_S,
                                              EXPECT_COOLDOWN_S * 2 ** (p.expect_wrong - 1))
        return None

    def _scale_one(self, p: _Pool) -> None:
        sc = p.scale
        for j in self.jobs.values():
            if j.pool == (p.lane, p.name) and not j.pool_noted and j.state in (
                    "done", "failed", "cancelled", "expired", "preempted"):
                j.pool_noted = True
                self._emit("pool.scaled", j, state="stopped",
                           detail={"lane": p.lane, "pool": p.name, "action": "stopped",
                                   "job_id": j.job_id, "reason": j.reason or j.state})
        hint = self._judge_expect(p)
        jobs = self._pool_jobs(p)
        serving = [j for j in jobs if not j.draining]
        if sc.get("min_pods"):
            for j in [j for j in serving if j.state in ("booting", "ready", "busy", "idle")
                      ][:int(sc["min_pods"])]:
                if (j.keep_warm_until or 0) < self.now + MIN_POD_WARM_S / 2:
                    j.keep_warm_until = self.now + MIN_POD_WARM_S
        if sc.get("never_launch"):
            return
        d = self._demand(p)
        tpp = int(sc.get("tasks_per_pod") or 1)
        lw = sc.get("launch_when") or {}
        want, reason = 0, ""
        if d["queued"] and (d["oldest_wait_s"] >= float(lw.get("queued_s") or 0)
                            or d["queued"] >= int(lw.get("queued_tasks") or 1)):
            want, reason = math.ceil((d["queued"] + d["on_pods"]) / tpp), "demand"
        if hint and sc.get("prelaunch"):
            h = math.ceil(int(hint["tasks"]) / tpp)
            if h > want:
                want, reason = h, "expect"
        if int(sc.get("min_pods") or 0) > want:
            want, reason = int(sc["min_pods"]), "min_pods"
        cap = int(sc.get("max_pods") or 0)
        want = min(want, cap)
        need = min(want - len(serving), cap - len(jobs))
        if need <= 0:
            return
        held = ""
        until = self.lane_backoff(p.lane)
        if until:
            held = f"lane {p.lane} is in boot-failure backoff until {self._iso(until)}"
        elif self._spend(p.lane)["breached"]:
            held = f"lane {p.lane} is over its daily cap"
        if held:
            p.last_error = held
            return
        lane_cap = (self.lane_caps.get(p.lane) or {}).get("max_pods")
        if lane_cap is not None:
            waiting = sum(1 for j in self.jobs.values() if j.lane == p.lane and j.state == "queued")
            need = min(need, max(0, int(lane_cap) - self._live(p.lane) - waiting))
        for _ in range(max(0, need)):
            if not self._launch(p, reason):
                break
        else:
            if need > 0:
                p.last_error = ""

    def _launch(self, p: _Pool, reason: str) -> bool:
        body = json.loads(json.dumps(p.job))
        body["lane"] = p.lane
        body["labels"] = {**(body.get("labels") or {}), "pool": p.name, "lane": p.lane}
        body.setdefault("queue_deadline_s", POOL_QUEUE_DEADLINE_S)
        self._n += 1
        body["job_id"] = f"{p.lane}-{p.name}-{self._n:04d}"
        try:
            reply = self._submit(body, None)
        except _HTTPError as e:
            p.last_error = f"template: {e.error}"
            return False
        j = self.jobs[reply["job_id"]]
        j.pool, j.pool_version = (p.lane, p.name), p.version
        rec = {"job_id": j.job_id, "reason": reason, "at": self.now, "version": p.version}
        p.launches.append(rec)
        self._emit("pool.scaled", j, state="launched",
                   detail={"lane": p.lane, "pool": p.name, "action": "launched",
                           "job_id": j.job_id, "reason": reason, "version": p.version,
                           **({"resumed_from": reply["resumed_from"]}
                              if reply.get("resumed_from") else {})})
        return True

    # =================================================================== HTTP

    def _task_route(self, m: str, p: list, q: dict, body, who):
        if p == ["pools"] and m == "GET":
            return {"pools": [{"lane": x.lane, "name": x.name, "version": x.version,
                               "updated_ts": x.updated_ts}
                              for (lane, _), x in sorted(self.pools.items())
                              if self._may(who, lane)]}, 200
        if p[0] == "pools" and len(p) >= 3:
            lane, name = p[1], p[2]
            if len(p) == 3 and m == "PUT":
                return self._pool_put(lane, name, body, who), 200
            pool = self._pool_for(who, lane, name)
            if len(p) == 3 and m == "GET":
                return self._pool_view(pool), 200
            if len(p) == 4 and p[3] == "expect" and m == "POST":
                return self._expect(pool, body or {}), 200
            if len(p) == 4 and p[3] == "stats" and m == "GET":
                since = q.get("since") or 0
                try:
                    since = float(since)
                except ValueError:
                    raise _HTTPError(422, "since must be a number")
                return self._stats(pool, kind=q.get("kind") or "", since=since), 200
        if p == ["tasks"] and m == "POST":
            return self._task_submit(body, who), 202
        if p == ["tasks"] and m == "GET":
            return self._task_list(q, who), 200
        if p == ["tasks"] and m == "DELETE":
            return self._bulk(who, lane=q.get("lane") or "", label=q.get("label") or "",
                              state=q.get("state") or "", ids=None,
                              reason=q.get("reason") or ""), 200
        if p == ["tasks", "cancel"] and m == "POST":
            body = body or {}
            return self._bulk(who, lane=str(body.get("lane") or ""),
                              label=str(body.get("label") or ""),
                              state=str(body.get("state") or ""), ids=body.get("ids"),
                              reason=str(body.get("reason") or "")), 200
        if p[0] == "tasks" and len(p) >= 2:
            t = self.task_rows.get(p[1])
            if t is None or not self._may(who, t.lane):
                raise _HTTPError(404, f"no task {p[1]}")
            if len(p) == 2 and m == "GET":
                return self._task_view(t), 200
            if len(p) == 2 and m == "DELETE":
                return self._cancel_one(t, q.get("reason") or ""), 200
            if len(p) == 2 and m == "PATCH":
                return self._patch(t, body or {}), 200
            if len(p) == 3 and p[2] == "retry" and m == "POST":
                return self._retry(t, body or {}), 200
        raise _HTTPError(404, f"no route {m} /v1/{'/'.join(p)}")

    def _pool_for(self, who, lane: str, name: str) -> _Pool:
        pool = self.pools.get((lane, name))
        if pool is None or not self._may(who, lane):
            raise _HTTPError(404, f"no pool {lane}/{name}")
        return pool

    # -- pools ----------------------------------------------------------------------------

    def _pool_put(self, lane: str, name: str, body, who) -> dict:
        if not self._may(who, lane):
            raise _HTTPError(403, f"this token may not write lane {lane!r}")
        if not POOL_NAME_RE.match(lane) or not POOL_NAME_RE.match(name):
            raise _HTTPError(422, f"lane and name must match {POOL_NAME_RE.pattern}")
        if not isinstance(body, dict):
            raise _HTTPError(422, "the body must be an object")
        extra = set(body) - {"job", "scale", "match", "accepts_from"}
        if extra:
            raise _HTTPError(422, f"unknown keys {sorted(extra)}; have job, scale, match")
        job = body.get("job")
        if not isinstance(job, dict):
            raise _HTTPError(422, "job is required: an APP-API submit body without job_id")
        for k in ("job_id", "idempotency_key"):
            if k in job:
                raise _HTTPError(422, f"job.{k} is set per pod by the autoscaler; leave it out")
        if job.get("lane") not in (None, lane):
            raise _HTTPError(422, f"job.lane is {job['lane']!r} but the pool is in lane {lane!r}")
        job = json.loads(json.dumps({k: v for k, v in job.items() if k != "lane"}))
        if not job.get("image") and not job.get("spec_key"):
            raise _HTTPError(422, "job: image is required")
        boot = job.setdefault("boot", {}) if isinstance(job.get("boot"), dict) else None
        if boot is None:
            job["boot"] = boot = {}
        if not isinstance(boot.get("app"), dict):
            boot["app"] = {}
        boot["app"].setdefault("serve", ["podh-worker"])
        a = boot.get("addons") or {}
        if a.get("set") and a.get("version") is None:
            raise _HTTPError(422, "job: boot.addons.version is required")
        if not isinstance(job.get("policy"), dict):
            job["policy"] = {}
        job["policy"].setdefault("idle_stop_s", POOL_IDLE_STOP_S)
        _num(job.get("queue_deadline_s", POOL_QUEUE_DEADLINE_S), "job.queue_deadline_s",
             30, 86400)
        sc = _only(body.get("scale") or {}, SCALE_KEYS, "scale")
        s = {"min_pods": _num(sc.get("min_pods", 0), "scale.min_pods", 0, 64, True),
             "max_pods": _num(sc.get("max_pods", 1), "scale.max_pods", 0, 64, True),
             "tasks_per_pod": _num(sc.get("tasks_per_pod", 1), "scale.tasks_per_pod", 1, 16, True),
             "prelaunch": sc.get("prelaunch", False), "never_launch": sc.get("never_launch", False)}
        for flag in ("prelaunch", "never_launch"):
            if not isinstance(s[flag], bool):
                raise _HTTPError(422, f"scale.{flag} must be true or false")
        if s["min_pods"] > s["max_pods"]:
            raise _HTTPError(422, "scale.min_pods cannot exceed scale.max_pods")
        lw = _only(sc.get("launch_when") or {}, {"queued_s", "queued_tasks"}, "scale.launch_when")
        s["launch_when"] = {"queued_s": _num(lw.get("queued_s", 0), "scale.launch_when.queued_s",
                                             0, 86400),
                            "queued_tasks": _num(lw.get("queued_tasks", 1),
                                                 "scale.launch_when.queued_tasks", 1, 10000, True)}
        share = sc.get("cost_share") or {}
        if not isinstance(share, dict) or not all(
                isinstance(k, str) and isinstance(v, (int, float)) and not isinstance(v, bool)
                and 0 <= v <= 1000 for k, v in share.items()):
            raise _HTTPError(422, "scale.cost_share maps kind -> weight (0-1000)")
        s["cost_share"] = share
        mt = _only(body.get("match") or {}, MATCH_KEYS, "match")
        mm = {"affinity_wait_s": _num(mt.get("affinity_wait_s", 0), "match.affinity_wait_s",
                                      0, 3600)}
        acc = body.get("accepts_from", mt.get("accepts_from")) or []
        mm["accepts_from"] = _strs(acc, "accepts_from", 16) if acc else []
        old = self.pools.get((lane, name))
        ver = old.version + 1 if old else 1
        if old:
            old.version, old.job, old.scale, old.match = ver, job, s, mm
            old.updated_ts, old.last_error = self.now, ""
        else:
            self.pools[(lane, name)] = _Pool(lane, name, ver, job, s, mm, self.now)
        return {"lane": lane, "name": name, "version": ver}

    def _pool_view(self, p: _Pool) -> dict:
        counts = {s: 0 for s in TASK_OPEN}
        for t in self.task_rows.values():
            if t.lane == p.lane and t.pool == p.name and t.state in TASK_OPEN:
                counts[t.state] += 1
        pods = []
        for j in self._pool_jobs(p, parked=True):
            pods.append({"job_id": j.job_id, "state": j.state,
                         "pod_id": (j.pod or {}).get("id"), "version": j.pool_version,
                         "tasks": self._holding(j.job_id), "busy": bool(j.busy),
                         "draining": bool(j.draining),
                         "loaded": list(self._pod_loaded.get(j.job_id, [])),
                         "cost_hr": (j.pod or {}).get("cost_hr"),
                         "gpu": (j.pod or {}).get("gpu")})
        ids = {j.job_id for j in self.jobs.values() if j.pool == (p.lane, p.name)}
        since = self.now - 86400
        run = park = 0.0
        rows = [r for r in self.ledger if r[1] in ids]
        burn = 0.0
        for j in self.jobs.values():
            if j.job_id in ids and j.bill_kind and j.bill_since is not None:
                rate = j.pod["cost_hr"] if j.bill_kind == "running" else self.park_cost_hr
                rows.append((j.lane, j.job_id, j.bill_kind, j.bill_since, self.now, rate))
                burn += rate
        for _, _, kind, t0, t1, rate in rows:
            usd = max(0.0, t1 - max(t0, since)) / 3600.0 * rate
            if kind == "running":
                run += usd
            else:
                park += usd
        return {"lane": p.lane, "name": p.name, "version": p.version,
                "job": json.loads(json.dumps(p.job)), "scale": json.loads(json.dumps(p.scale)),
                "match": json.loads(json.dumps(p.match)),
                "queued": counts["queued"], "held": counts["held"],
                "claimed": counts["claimed"], "running": counts["running"], "pods": pods,
                "spend": {"rolling_24h_usd": round(run + park, 4), "running_usd": round(run, 6),
                          "parked_usd": round(park, 6), "burn_usd_hr": round(burn, 6)},
                "expect": dict(p.expect) if p.expect else None,
                "expect_cooldown_until": p.cooldown_until or None,
                "last_error": p.last_error or None}

    def _expect(self, p: _Pool, body: dict) -> dict:
        if not p.scale.get("prelaunch"):
            raise _HTTPError(409, "this pool does not honour expect hints",
                             hint="set scale.prelaunch: true")
        in_s = _num(body.get("in_s", 0), "in_s", 0, 86400)
        n = _num(body.get("tasks", 1), "tasks", 1, 10000, True)
        needs = body.get("needs") if isinstance(body.get("needs"), dict) else {}
        if p.cooldown_until > self.now:
            return {"accepted": False, "cooldown_until": p.cooldown_until,
                    "reason": f"{p.expect_wrong} wrong hint(s) in a row"}
        p.expect = {"at": self.now, "in_s": in_s, "tasks": n, "needs": needs,
                    "judge_at": self.now + in_s + EXPECT_GRACE_S}
        return {"accepted": True, "expect": dict(p.expect)}

    def _stats(self, p: _Pool, *, kind: str, since: float) -> dict:
        since = since or self.now - 7 * 86400
        by: dict = {}
        for t in self.task_rows.values():
            if t.lane != p.lane or t.pool != p.name or t.submitted_at < since \
                    or (kind and t.kind != kind):
                continue
            k = by.setdefault(t.kind, {"counts": {}, "queued_s": [], "held_s": [], "run_s": []})
            k["counts"][t.state] = k["counts"].get(t.state, 0) + 1
            if t.claimed_at and t.queued_at:
                k["queued_s"].append(max(0.0, t.claimed_at - t.queued_at))
            if t.held_s:
                k["held_s"].append(t.held_s)
            if t.state == "succeeded" and t.started_at and t.ended_at:
                k["run_s"].append(max(0.0, t.ended_at - t.started_at))
        out = {kd: {"counts": v["counts"],
                    **{m: {"n": len(v[m]), "p50": _pct(v[m], 50), "p90": _pct(v[m], 90),
                           "p99": _pct(v[m], 99)} for m in ("queued_s", "held_s", "run_s")}}
               for kd, v in by.items()}
        return {"lane": p.lane, "pool": p.name, "since": since, "kinds": out}

    # -- tasks ----------------------------------------------------------------------------

    def _task_submit(self, body, who) -> dict:
        if not isinstance(body, dict):
            raise _HTTPError(422, "the body must be an object")
        lane = (str(body.get("lane") or "") or (who[0] if who else "batch")).strip()
        if not self._may(who, lane):
            raise _HTTPError(403, f"this token may not submit to lane {lane!r}")
        pname = body.get("pool")
        if not isinstance(pname, str) or not pname:
            raise _HTTPError(422, "pool is required")
        pool = self.pools.get((lane, pname))
        if pool is None:
            raise _HTTPError(422, f"no pool {lane}/{pname}", hint="PUT /v1/pools/{lane}/{name} first")
        idem = body.get("idempotency_key") or ""
        if idem:
            if not isinstance(idem, str) or len(idem) > 200:
                raise _HTTPError(422, "idempotency_key must be a string (at most 200)")
            for t in reversed(list(self.task_rows.values())):
                if t.lane == lane and t.idem == idem:
                    return {"task_id": t.task_id, "state": t.state, "lane": lane,
                            "pool": t.pool, "existing": True}
        tid = body.get("task_id")
        if tid is not None and (not isinstance(tid, str) or not TASK_ID_RE.match(tid)):
            raise _HTTPError(422, f"task_id must match {TASK_ID_RE.pattern}")
        if tid and tid in self.task_rows:
            raise _HTTPError(409, f"task {tid} already exists",
                             hint="use idempotency_key to make retries safe")
        v = self._parse_task(body, lane=lane, pool=pool)
        if not tid:
            self._n += 1
            tid = f"task_{self._n:06d}"
        t = _Task(task_id=tid, lane=lane, pool=pname, kind=v["kind"], priority=v["priority"],
                  idem=idem, deadline_s=v["deadline_s"], deadline_ts=self.now + v["deadline_s"],
                  max_attempts=v["max_attempts"], lease_s=v["lease_s"], timeout_s=v["timeout_s"],
                  max_releases=v["max_releases"], fallback=v["fallback"],
                  fallback_after_s=v["fallback_after_s"], needs=v["needs"],
                  workflow=v["workflow"], inputs=v["inputs"], outputs=v["outputs"],
                  payload=v["payload"], labels=v["labels"], after=v["after"],
                  submitted_at=self.now, queued_at=self.now, qseq=self._next_qseq())
        t.seeds = [m["path"] for m in v["needs"].get("models") or [] if m.get("source")]
        for m in v["needs"].get("models") or []:
            if m.get("source"):
                self.seeds.setdefault(m["path"], {"state": "pending", "source": m["source"]})
        self.task_rows[tid] = t
        self._temit("task.queued", t, labels=t.labels, priority=t.priority)
        if any((self.seeds.get(pth) or {}).get("state") != "done" for pth in t.seeds):
            t.state, t.held_reason, t.held_since = "held", "seeding", self.now
            self._temit("task.held", t, held_reason="seeding", paths=list(t.seeds))
        return {"task_id": tid, "state": t.state, "lane": lane, "pool": pname, "existing": False}

    def _parse_task(self, body: dict, *, lane: str, pool: _Pool) -> dict:
        extra = set(body) - SUBMIT_KEYS
        if extra:
            raise _HTTPError(422, f"unknown keys {sorted(extra)}")
        if len(json.dumps(body, default=str)) > MAX_TASK_BYTES:
            raise _HTTPError(422, f"a task is at most {MAX_TASK_BYTES} bytes; put bulk data in a store")
        kind = body.get("kind")
        if not isinstance(kind, str) or not KIND_RE.match(kind):
            raise _HTTPError(422, "kind must be 'comfy' or '<app>.<name>'")
        d = TASK_DEFAULTS
        boot = pool.job.get("boot") or {}
        out = {"kind": kind,
               "priority": _num(body.get("priority", 0), "priority", -1000, 1000, True),
               "deadline_s": _num(body.get("deadline_s", d["deadline_s"]), "deadline_s",
                                  1, 30 * 86400),
               "max_attempts": _num(body.get("max_attempts", d["max_attempts"]), "max_attempts",
                                    1, 20, True),
               "lease_s": _num(body.get("lease_s", d["lease_s"]), "lease_s", 15, 3600),
               "timeout_s": _num(body.get("timeout_s", d["timeout_s"] if boot.get("workload")
                                          else 0), "timeout_s", 0, 30 * 86400),
               "max_releases": _num(body.get("max_releases", d["max_releases"]), "max_releases",
                                    1, 100, True)}
        fb = body.get("fallback")
        if fb not in (None, "", "app"):
            raise _HTTPError(422, "fallback must be 'app' (or absent)")
        out["fallback"] = fb or ""
        out["fallback_after_s"] = _num(body.get("fallback_after_s", 0), "fallback_after_s",
                                       0, 30 * 86400)
        if out["fallback_after_s"] and not out["fallback"]:
            raise _HTTPError(422, "fallback_after_s needs fallback: 'app'")
        out["needs"] = self._parse_needs(body.get("needs"), boot)
        after = body.get("after") or []
        after = _strs(after, "after", 50) if after else []
        for a in after:
            if a == body.get("task_id"):
                raise _HTTPError(422, "a task cannot run after itself")
            dep = self.task_rows.get(a)
            if dep is None or dep.lane != lane:
                raise _HTTPError(422, f"after: no task {a!r} in lane {lane!r}")
        out["after"] = list(dict.fromkeys(after))
        w = _only(body.get("workflow") if body.get("workflow") is not None else {},
                  WORKFLOW_KEYS, "workflow")
        wf = {}
        for k in ("ref", "map"):
            if w.get(k) is not None:
                wf[k] = _rel(w[k], f"workflow.{k}")
        for k in ("inline", "patch"):
            if w.get(k) is not None:
                if not isinstance(w[k], dict):
                    raise _HTTPError(422, f"workflow.{k} must be an object")
                wf[k] = w[k]
        out["workflow"] = wf
        out["inputs"] = self._parse_inputs(body.get("inputs"), out["after"])
        out["outputs"] = self._parse_outputs(body.get("outputs"))
        payload = body.get("payload", {})
        out["payload"] = payload if payload is not None else {}
        lb = body.get("labels") or {}
        if (not isinstance(lb, dict) or len(lb) > 32
                or not all(isinstance(k, str) and 0 < len(k) <= 64
                           and isinstance(v, (str, int, float, bool)) and len(str(v)) <= 256
                           for k, v in lb.items())):
            raise _HTTPError(422, "labels: at most 32 string keys with short scalar values")
        out["labels"] = dict(lb)
        return json.loads(json.dumps(out))

    @staticmethod
    def _parse_needs(n, boot: dict) -> dict:
        n = _only(n if n is not None else {}, NEEDS_KEYS, "needs")
        store = (boot.get("models") or {}).get("profile") or ""
        out: dict = {}
        if n.get("min_vram_gb") is not None:
            out["min_vram_gb"] = _num(n["min_vram_gb"], "needs.min_vram_gb", 0, 1024)
        if n.get("models"):
            if not isinstance(n["models"], list):
                raise _HTTPError(422, "needs.models must be a list")
            models = []
            for it in n["models"]:
                if isinstance(it, str):
                    models.append({"path": _rel(it, "needs.models path")})
                    continue
                if not isinstance(it, dict) or not it.get("path"):
                    raise _HTTPError(422, "needs.models: a path or {path, sha256?, size?, source?}")
                m = {"path": _rel(it["path"], "needs.models path")}
                for k in ("sha256", "size"):
                    if it.get(k) is not None:
                        m[k] = it[k]
                if "source" in it:
                    src = it["source"]
                    if not isinstance(src, str) or not src.startswith(SOURCES) or len(src) > 2048:
                        raise _HTTPError(422, f"needs.models: source must start with one of {SOURCES}")
                    m["source"] = src
                models.append(m)
            if not store:
                raise _HTTPError(422, "needs.models: this pool's job template names no model "
                                      "store (boot.models), so its pods cannot fetch models")
            out["models"], out["store"] = models, store
        if n.get("addons") is not None:
            a = _only(n["addons"], {"set", "version"}, "needs.addons")
            want = {"set": a.get("set"), "version": a.get("version")}
            if want != {k: (boot.get("addons") or {}).get(k) for k in ("set", "version")}:
                raise _HTTPError(422, "needs.addons must match the pool's job template (boot.addons "
                                      f"is {boot.get('addons')!r}); update the pool first")
            out["addons"] = want
        if n.get("nvenc") is not None:
            if not isinstance(n["nvenc"], bool):
                raise _HTTPError(422, "needs.nvenc must be true or false")
            out["nvenc"] = n["nvenc"]
        if n.get("loads") is not None:
            out["loads"] = _strs(n["loads"], "needs.loads", 32)
        if n.get("fits_window_s") is not None:
            out["fits_window_s"] = _num(n["fits_window_s"], "needs.fits_window_s", 0, 30 * 86400)
        return out

    @staticmethod
    def _parse_inputs(i, after: list) -> dict:
        if i is None:
            return {}
        if not isinstance(i, dict) or len(i) > 64:
            raise _HTTPError(422, "inputs must map name -> ref (at most 64)")
        out = {}
        for name, ref in i.items():
            if not isinstance(name, str) or not INPUT_NAME_RE.match(name):
                raise _HTTPError(422, f"inputs: {name!r} is not a valid input name")
            if isinstance(ref, dict) and "ref" in ref:
                # {ref, sha256?, size?}: a concrete input with its content hash (TASK-API §8.2).
                _only(ref, {"ref", "sha256", "size"}, f"inputs.{name}")
                r = ref["ref"]
                if not (isinstance(r, str) and r.startswith("https://") and len(r) <= 4096):
                    r = _store_ref(r, f"inputs.{name}.ref")
                v = {"ref": r}
                if ref.get("sha256") is not None:
                    if not isinstance(ref["sha256"], str) or not re.match(r"^[0-9a-fA-F]{64}$",
                                                                          ref["sha256"]):
                        raise _HTTPError(422, f"inputs.{name}.sha256 must be 64 hex characters")
                    v["sha256"] = ref["sha256"].lower()
                if ref.get("size") is not None:
                    if isinstance(ref["size"], bool) or not isinstance(ref["size"], int) \
                            or ref["size"] < 0:
                        raise _HTTPError(422, f"inputs.{name}.size must be a non-negative integer")
                    v["size"] = ref["size"]
                out[name] = v
            elif isinstance(ref, dict):
                _only(ref, {"task", "artifact", "result"}, f"inputs.{name}")
                if ref.get("task") not in after:
                    raise _HTTPError(422, f"inputs.{name}: task {ref.get('task')!r} must be "
                                          f"listed in after")
                if ("artifact" in ref) == ("result" in ref):
                    raise _HTTPError(422, f"inputs.{name}: give exactly one of artifact, result")
                out[name] = {"task": ref["task"], **({"artifact": str(ref["artifact"])}
                                                     if "artifact" in ref
                                                     else {"result": str(ref["result"])})}
            elif isinstance(ref, str) and ref.startswith("https://") and len(ref) <= 4096:
                out[name] = ref
            else:
                out[name] = _store_ref(ref, f"inputs.{name}")
        return out

    @staticmethod
    def _parse_outputs(o) -> dict:
        if o is None:
            return {}
        o = _only(o, {"to", "keep", "names", "put_url"}, "outputs")
        out: dict = {}
        if o.get("put_url") is not None:
            if o.get("to") is not None:
                raise _HTTPError(422, "outputs: give to (a store prefix) or put_url, not both")
            if not isinstance(o["put_url"], str) or not o["put_url"].startswith("https://"):
                raise _HTTPError(422, "outputs.put_url must be https")
            out["put_url"] = o["put_url"]
        if o.get("to") is not None:
            _store_ref(o["to"], "outputs.to")
            out["to"] = o["to"] if o["to"].endswith("/") else o["to"] + "/"
        out["keep"] = _strs(o.get("keep") or [], "outputs.keep", 32) if o.get("keep") else []
        names = _strs(o.get("names") or [], "outputs.names", 200) if o.get("names") else []
        for n in names:
            _rel(n, "outputs.names")
        if names and not out.get("to"):
            raise _HTTPError(422, "outputs.names needs outputs.to (a store prefix to presign them in)")
        out["names"] = names
        return out

    def _task_list(self, q: dict, who) -> dict:
        states = [s.strip() for s in (q.get("state") or "").split(",") if s.strip()]
        lane, pool, label = q.get("lane") or "", q.get("pool") or "", q.get("label") or ""
        limit = int(q.get("limit") or 100)
        if limit > 1000:
            raise _HTTPError(422, "limit: at most 1000")
        rows = [t for t in reversed(list(self.task_rows.values()))
                if self._may(who, t.lane) and (not lane or t.lane == lane)
                and (not pool or t.pool == pool) and (not states or t.state in states)]
        if label:
            k, _, v = label.partition(":")
            rows = [t for t in rows if str(t.labels.get(k, "\0")) == v]
        return {"tasks": [self._task_view(t, sign=False) for t in rows[:limit]]}

    def _bulk(self, who, *, lane: str, label: str, state: str, ids, reason: str) -> dict:
        if ids:
            if not isinstance(ids, list) or len(ids) > 1000:
                raise _HTTPError(422, "ids: a list of at most 1000 task ids")
            rows = [self.task_rows[str(i)] for i in ids if str(i) in self.task_rows
                    and self._may(who, self.task_rows[str(i)].lane)]
        else:
            if not lane or not label:
                raise _HTTPError(422, "bulk cancel needs ids, or lane and label",
                                 hint="DELETE /v1/tasks?lane=&label=k:v[&state=]")
            if not self._may(who, lane):
                raise _HTTPError(404, f"no lane {lane}")
            states = tuple(s.strip() for s in state.split(",") if s.strip()) or TASK_OPEN
            k, _, v = label.partition(":")
            rows = [t for t in self.task_rows.values() if t.lane == lane and t.state in states
                    and str(t.labels.get(k, "\0")) == v]
        return {"cancelled": sum(1 for t in rows if self._tcancel(t, reason))}

    def _cancel_one(self, t: _Task, reason: str) -> dict:
        if t.state == "cancelled":
            return {"cancelled": True, "already": True, "task_id": t.task_id}
        if t.state in TASK_FINAL:
            raise _HTTPError(409, f"task {t.task_id} is {t.state}")
        was = t.state
        done = self._tcancel(t, reason)
        return {"cancelled": done, "task_id": t.task_id, "was": was,
                **({"note": "the pod hears it in its next beat reply"} if was in TASK_ACTIVE
                   else {})}

    def _retry(self, t: _Task, body: dict) -> dict:
        if t.state not in ("failed", "cancelled", "expired", "returned"):
            raise _HTTPError(409, f"task {t.task_id} is {t.state}",
                             hint="only a failed, cancelled, expired or returned task can be retried")
        patch = body.get("patch")
        if patch is not None and not isinstance(patch, dict):
            raise _HTTPError(422, "patch must be an object")
        if body.get("payload") is not None:
            t.payload = json.loads(json.dumps(body["payload"]))
        if patch is not None:
            t.workflow = {**t.workflow, "patch": json.loads(json.dumps(patch))}
        self._unbind(t)
        t.state, t.attempt, t.releases, t.released_jobs = "queued", 1, 0, []
        t.progress, t.message, t.result, t.artifacts = 0.0, "", None, []
        t.error, t.usage, t.cancel_reason = None, {}, ""
        t.queued_at, t.qseq = self.now, self._next_qseq()
        t.claimed_at = t.started_at = t.ended_at = t.held_s = 0.0
        t.deadline_ts = self.now + t.deadline_s
        self._temit("task.queued", t, retry=True, payload_sha=payload_sha(t.payload, t.workflow))
        waiting = [pth for pth in t.seeds if (self.seeds.get(pth) or {}).get("state") != "done"]
        if waiting:
            t.state, t.held_reason, t.held_since = "held", "seeding", self.now
            for pth in waiting:
                if self.seeds[pth].get("state") == "failed":
                    self.seeds[pth].update(state="pending", error="")
        return {"task_id": t.task_id, "state": t.state, "attempt": t.attempt}

    def _patch(self, t: _Task, body: dict) -> dict:
        if set(body) != {"priority"}:
            raise _HTTPError(422, "only priority can be changed")
        p = _num(body["priority"], "priority", -1000, 1000, True)
        if t.state != "queued":
            raise _HTTPError(409, f"task {t.task_id} is {t.state}",
                             hint="priority can be changed only while queued")
        t.priority = p
        return {"task_id": t.task_id, "priority": p, "position": self._position(t)}
