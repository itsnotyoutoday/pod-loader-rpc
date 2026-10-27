"""FakeControl: an in-memory pod-control for testing apps that use `pod_loader.app`.

THIS IS A TEST DOUBLE. It tracks docs/APP-API.md **r3** (the app API, §2 and §7) and
nothing else: no pods are rented, no network is touched, no time passes unless the test
says so. Serve it through httpx's MockTransport:

    fake = FakeControl()
    pc = Client("http://control.test", fake.token, transport=fake.transport)
    job = pc.submit(spec)
    fake.place(job)                      # pod-control found capacity
    fake.boot_line(job, "models done", stage="models")
    fake.ready(job)
    fake.set_busy(job, True); fake.set_busy(job, False)
    fake.advance(300)                    # idle_stop_s passes → parked (or done)

## What it follows

States queued → placing → booting → ready ⇄ busy / idle → parked / done, draining,
preempted, and the terminal done / failed / cancelled / expired. `policy` timers
(boot_stall_s, boot_timeout_s, idle_stop_s, park_ttl_s, max_life_s) and the queue deadline
run on a manual clock (`advance(seconds)`), firing in time order, so a chain such as idle →
parked → done(park TTL) lands at the right moments within one `advance`. Never idle while
booting or while any model is fetching: the idle clock starts only when the pod is not
busy AND nothing is fetching. Drain, warm (`until` or `delta_s` relative to the current
warm-until, capped by max_life_s), park, resume, cancel from any state (a pod that lands
after a cancel mid-placement is terminated on arrival), mid-life model additions with
`models_rev`, spend per lane with a cap and `breached`, `reuse: "parked-first"` with the
spec's matching rules, lane backoff after 3 consecutive pre-ready failures (5, then 15,
then 30 min), addon sets with validation and short-pin resolution, events with `seq`.

## What it simplifies (read before trusting a green test)

- Placement is manual: nothing leaves `queued` until the test calls `place()` (or
  `placing()` first). `place()` raises `FakeHeld` where the real dispatcher would simply
  leave the job queued: lane backoff, a breached budget, or the lane at `max_pods`.
- No GPU catalogue, prices or hosts: `place()` takes the pod facts the test wants
  (defaults: one 48 GB data-centre card at $0.50/h). Only `reuse` matching reads them.
- Busy/idle comes only from `set_busy()`, standing in for the pod's heartbeat. A `ready`
  pod that never heartbeats counts as not busy, so its idle clock runs from `ready()`.
- When a warm hold ends on an idle pod, the idle stop fires at max(idle_since +
  idle_stop_s, warm-until); the idle clock is not restarted.
- Drain clears any warm hold. A max-life drain stops (`done`, reason `max_life`) as soon
  as the pod is not busy, without waiting out idle_stop_s and without parking.
- Spend: running pods accrue their `cost_hr`, parked pods `park_cost_hr` (one flat rate,
  not per GB). `today_usd` is since UTC midnight of the fake clock; `running_usd` and
  `parked_usd` are the rolling-24 h spend split by kind. A budget breach is checked after
  each `advance()` and each API call, not at the exact second it is crossed.
- `GET /v1/events` never blocks: `wait` is accepted and ignored. No SSE, no webhooks.
- `pod.boot` events are rate-limited as specified (one per 10 s per job, or a stage
  change); every line is still kept on the job (last 200).
- Short commit pins resolve against `fake.known_commits` ({repo: [full shas]}), standing
  in for GitHub. Unknown or ambiguous → 422 naming the item.
- `boot.addons` with a `set` must carry a `version` (the spec's rule "outside test pods";
  the fake has no test pods).
- A resume that gets no GPU is not timed: the test calls `resume_without_gpu()`, which
  terminates the pod and sends the job back to `queued` for a fresh placement.
- Errors use FastAPI's shape `{"detail": {"error", "hint"}}` with the server's status
  codes (401 token, 403 lane, 404 job, 409 state, 422 body). Wording is the fake's own.
- Not implemented: chains, webhooks, `/v1/models/{profile}`, hosts endpoints beyond
  recording blocks in `fake.blocked_hosts`, the pod API (§3) and kits.
"""
from __future__ import annotations

import datetime as _dt
import json
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import unquote

try:
    import httpx
except ImportError:                      # pragma: no cover
    import httpx2 as httpx               # type: ignore[no-redef]

from .client import Client

TERMINAL = ("done", "failed", "cancelled", "expired", "preempted")
#: States in which a pod exists and runs (bills at cost_hr).
RUNNING = ("booting", "ready", "busy", "idle", "draining")
#: States in which the pod has reported ready and its busy flag means something.
SERVING = ("ready", "busy", "idle", "draining")

BACKOFF_AFTER = 3
BACKOFF_STEPS_S = (300, 900, 1800)
BOOT_EVENT_EVERY_S = 10
BOOT_LINES_KEPT = 200
PARK_DEFAULT_TTL_S = 3600

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
REPO_RE = re.compile(r"^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?(\.git)?$")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHORT_RE = re.compile(r"^[0-9a-f]{7,39}$")


class FakeError(RuntimeError):
    """A test hook was used in a way the real system could not produce."""


class FakeHeld(FakeError):
    """`place()` on a job the real dispatcher would leave queued."""


class _HTTPError(Exception):
    def __init__(self, status: int, error: str, hint: str = ""):
        self.status, self.error, self.hint = status, error, hint


@dataclass
class _Job:
    job_id: str
    lane: str
    spec: dict
    created_at: float
    queue_deadline_ts: Optional[float]
    queue_deadline_s: float = 0.0
    resuming: bool = False
    state: str = "queued"
    pod: Optional[dict] = None
    models: dict = field(default_factory=dict)
    boot_set: list = field(default_factory=list)
    models_rev: int = 0
    model_errors: dict = field(default_factory=dict)
    boot: list = field(default_factory=list)
    placed_at: Optional[float] = None
    boot_started_at: Optional[float] = None
    last_line_at: Optional[float] = None
    last_stage: Optional[str] = None
    last_boot_event_at: Optional[float] = None
    ready_at: Optional[float] = None
    busy: bool = False
    heartbeat_seen: bool = False
    idle_since: Optional[float] = None
    keep_warm_until: Optional[float] = None
    draining: bool = False
    stop_on_idle: bool = False          # max_life: drain, then stop (not park)
    max_life_fired: bool = False
    park_until: Optional[float] = None
    last_error: Optional[str] = None
    reason: Optional[str] = None
    stage: Optional[str] = None
    log_tail: Optional[str] = None
    resumed_from: Optional[str] = None
    ended_at: Optional[float] = None
    cancelled_in_placement: bool = False
    bill_kind: Optional[str] = None     # "running" | "parked" | None
    bill_since: Optional[float] = None

    @property
    def policy(self) -> dict:
        return self.spec.get("policy") or {}

    @property
    def interruptible(self) -> bool:
        return bool((self.spec.get("gpu") or {}).get("interruptible"))

    @property
    def fetching(self) -> bool:
        return any(v == "fetching" for v in self.models.values())


def _path_of(m) -> str:
    return m["path"] if isinstance(m, dict) else str(m)


class FakeControl:
    """In-memory pod-control. See the module docstring for what it follows and simplifies.

    `token` is a master token; `tokens` maps more tokens to the lanes they are confined
    to. `lane_caps` is {lane: {"daily_usd": x, "max_pods": n}}.
    """

    def __init__(self, *, token: str = "test-token", tokens: Optional[dict] = None,
                 start: float = 1_800_000_000.0, lane_caps: Optional[dict] = None,
                 park_cost_hr: float = 0.02):
        self.token = token
        self.tokens: dict = {token: None, **(tokens or {})}
        self.now = float(start)
        self.lane_caps: dict = {k: dict(v) for k, v in (lane_caps or {}).items()}
        self.park_cost_hr = float(park_cost_hr)
        self.jobs: dict = {}
        self.event_log: list = []
        self.ledger: list = []           # (lane, job_id, kind, t0, t1, rate_hr)
        self.addon_sets: dict = {}       # (lane, name) -> [version dicts]
        self.known_commits: dict = {}    # repo -> [full sha]
        self.blocked_hosts: dict = {}    # machine_id -> reason
        self.terminated_pods: list = []  # pods killed on arrival after a cancel
        self._fail_streak: dict = {}     # lane -> consecutive pre-ready failures
        self._backoff_until: dict = {}   # lane -> ts
        self._breached: set = set()
        self._n = 0
        self.transport = httpx.MockTransport(self._handle)

    # =================================================================== conveniences

    def client(self, **kw) -> Client:
        """A Client wired to this fake."""
        return Client("http://pod-control.test", kw.pop("token", self.token),
                      transport=self.transport, **kw)

    def job(self, job_id: str) -> dict:
        """The job as `GET /v1/jobs/{id}` shows it."""
        return self._view(self._get(job_id))

    def state(self, job_id: str) -> str:
        return self._get(job_id).state

    def events_of(self, kind: str = "", job_id: str = "") -> list:
        return [e for e in self.event_log if (not kind or e["kind"] == kind)
                and (not job_id or e["job_id"] == job_id)]

    def set_lane_cap(self, lane: str, *, daily_usd: Optional[float] = None,
                     max_pods: Optional[int] = None) -> None:
        cap = self.lane_caps.setdefault(lane, {})
        if daily_usd is not None:
            cap["daily_usd"] = float(daily_usd)
        if max_pods is not None:
            cap["max_pods"] = int(max_pods)

    def lane_backoff(self, lane: str) -> Optional[float]:
        """When the lane's hold ends (fake epoch seconds), or None if not held."""
        until = self._backoff_until.get(lane)
        return until if until and until > self.now else None

    def clear_backoff(self, lane: str) -> None:
        """`DELETE /v1/lanes/{lane}/backoff` (master)."""
        self._backoff_until.pop(lane, None)
        self._fail_streak[lane] = 0

    # =================================================================== the clock

    def advance(self, seconds: float) -> None:
        """Let `seconds` pass, firing every timer that falls due, in time order."""
        if seconds < 0:
            raise FakeError("the clock does not run backwards")
        target = self.now + float(seconds)
        while True:
            due = self._next_due()
            if due is None or due[0] > target:
                break
            t, job, action = due
            self.now = max(self.now, t)
            action(job)
        self.now = target
        self._check_budgets()

    def _next_due(self):
        best = None
        for j in self.jobs.values():
            for t, action in self._timers(j):
                if t is not None and (best is None or t < best[0]):
                    best = (t, j, action)
        return best

    def _timers(self, j: _Job):
        p = j.policy
        out = []
        if j.state == "queued" and j.queue_deadline_ts is not None:
            out.append((j.queue_deadline_ts, self._expire))
        if j.state == "booting":
            if p.get("boot_stall_s") and j.last_line_at is not None:
                out.append((j.last_line_at + float(p["boot_stall_s"]),
                            lambda job: self._fail(job, "boot_stall", job.last_stage)))
            if p.get("boot_timeout_s") and j.boot_started_at is not None:
                out.append((j.boot_started_at + float(p["boot_timeout_s"]),
                            lambda job: self._fail(job, "boot_timeout", job.last_stage)))
        if j.state in RUNNING and not j.max_life_fired and p.get("max_life_s") \
                and j.placed_at is not None:
            out.append((j.placed_at + float(p["max_life_s"]), self._max_life))
        if j.state in SERVING and j.idle_since is not None and not j.busy:
            if j.stop_on_idle:
                out.append((j.idle_since, lambda job: self._end(job, "done", "max_life")))
            elif p.get("idle_stop_s") is not None:
                t = j.idle_since + float(p["idle_stop_s"])
                if j.keep_warm_until is not None:
                    t = max(t, j.keep_warm_until)
                out.append((t, self._idle_stop))
        if j.state == "parked" and j.park_until is not None:
            out.append((j.park_until, lambda job: self._end(job, "done", "park_ttl")))
        return out

    # =================================================================== test hooks

    def placing(self, job_id: str) -> None:
        """pod-control picked the job up and is creating its pod (`placing`)."""
        j = self._get(job_id)
        if j.state != "queued":
            raise FakeError(f"{job_id} is {j.state}, not queued")
        self._hold_check(j)
        self._set_state(j, "placing", "job.assigned")

    def place(self, job_id: str, *, gpu: str = "FAKE-GPU-48G", gpu_count: Optional[int] = None,
              vram_gb: int = 48, cost_hr: float = 0.50, datacenter_class: bool = True,
              nvenc: bool = True, data_center: str = "DC-1", cuda: str = "12.8",
              driver: str = "570.00", machine_id: str = "", pod_id: str = "") -> dict:
        """A pod landed for the job: it is now `booting` and billing.

        Called on a job cancelled mid-placement, the pod is terminated on arrival (as the
        real dispatcher does) and recorded in `terminated_pods`; the job stays cancelled.
        """
        j = self._get(job_id)
        self._n += 1
        pod = {"id": pod_id or f"pod-{self._n}", "machine_id": machine_id or f"m-{self._n}",
               "gpu": gpu,
               "gpu_count": (gpu_count if gpu_count is not None
                             else int(j.spec["gpu"].get("count", 1)) if j.spec.get("gpu")
                             else 0),
               "vram_gb": vram_gb, "datacenter_class": datacenter_class, "nvenc": nvenc,
               "interruptible": j.interruptible, "data_center": data_center,
               "cost_hr": float(cost_hr), "cuda": cuda, "driver": driver}
        if j.cancelled_in_placement:
            self.terminated_pods.append(pod["id"])
            self._emit("pod.reaped", j, pod_id=pod["id"], state="cancelled",
                       detail={"reason": "cancelled during placement"})
            return pod
        if j.state not in ("queued", "placing"):
            raise FakeError(f"{job_id} is {j.state}; only a queued or placing job is placed")
        self._hold_check(j)
        j.pod = pod
        j.placed_at = self.now
        j.models = {p: "fetching" for p in j.boot_set}
        self._start_boot(j)
        self._set_state(j, "booting", "job.running")
        return pod

    def boot_line(self, job_id: str, line: str, stage: Optional[str] = None) -> None:
        """The pod reported a boot line (resets the stall clock)."""
        j = self._get(job_id)
        if j.state != "booting":
            raise FakeError(f"{job_id} is {j.state}; boot lines come while booting")
        j.boot.append({"at": self._iso(self.now), "stage": stage or j.last_stage, "line": line})
        del j.boot[:-BOOT_LINES_KEPT]
        j.last_line_at = self.now
        changed = stage is not None and stage != j.last_stage
        if stage:
            j.last_stage = stage
        if changed or j.last_boot_event_at is None \
                or self.now - j.last_boot_event_at >= BOOT_EVENT_EVERY_S:
            j.last_boot_event_at = self.now
            self._emit("pod.boot", j, state=j.last_stage or "booting",
                       detail={"line": line, "stage": j.last_stage})

    def ready(self, job_id: str) -> None:
        """The boot finished and the workload answers: `ready`."""
        j = self._get(job_id)
        if j.state != "booting":
            raise FakeError(f"{job_id} is {j.state}; only a booting pod becomes ready")
        failed = [p for p in j.boot_set if j.models.get(p) == "failed"]
        if failed:
            raise FakeError(f"boot-set model(s) failed: {failed}; use fail_boot()")
        for p in j.boot_set:
            if j.models.get(p) == "fetching":
                j.models[p] = "present"
        j.ready_at = self.now
        j.resuming = False
        j.busy = False
        j.heartbeat_seen = False
        self._fail_streak[j.lane] = 0
        self._backoff_until.pop(j.lane, None)
        self._set_state(j, "ready", "pod.ready")
        self._settle(j)
        self.advance(0)

    def set_busy(self, job_id: str, busy: bool, detail: str = "") -> None:
        """The pod's heartbeat carried `busy`."""
        j = self._get(job_id)
        if j.state not in SERVING:
            if j.state == "booting" and not busy:
                return              # a heartbeat while booting never makes the pod idle
            raise FakeError(f"{job_id} is {j.state}; busy/idle comes from a ready pod")
        j.busy = bool(busy)
        j.heartbeat_seen = True
        self._settle(j)
        self.advance(0)

    def model_state(self, job_id: str, path: str, state: str, error: str = "") -> None:
        """A model's fetch moved on: "present", "fetching" or "failed"."""
        if state not in ("present", "fetching", "failed"):
            raise FakeError(f"model state is present|fetching|failed, not {state!r}")
        j = self._get(job_id)
        if path not in j.models:
            raise FakeError(f"{job_id} has no model {path!r}")
        was = j.models[path]
        j.models[path] = state
        if state == "failed":
            j.model_errors[path] = error or "fetch failed"
        else:
            j.model_errors.pop(path, None)
        if was != state and state in ("present", "failed") and path not in j.boot_set:
            self._emit("pod.models", j, state=state,
                       detail={"path": path, "state": state,
                               "error": j.model_errors.get(path)})
        if j.state in SERVING:
            self._settle(j)
        self.advance(0)

    def fail_boot(self, job_id: str, stage: str, reason: str, log_tail: str = "") -> None:
        """The pod reported `done(rc != 0, reason, stage)`. Before `ready` this counts
        toward the lane's backoff; `stage="gpu_check"` blocks the machine."""
        j = self._get(job_id)
        if j.state not in RUNNING:
            raise FakeError(f"{job_id} is {j.state}; only a live pod fails")
        if stage == "gpu_check" and j.pod:
            self.blocked_hosts[j.pod["machine_id"]] = reason
            self._emit("host.blocked", j, state="blocked",
                       detail={"machine_id": j.pod["machine_id"], "reason": reason})
        self._fail(j, reason, stage, log_tail=log_tail or None)

    def preempt(self, job_id: str) -> None:
        """The provider took a spot pod back."""
        j = self._get(job_id)
        if j.state not in RUNNING and j.state != "placing":
            raise FakeError(f"{job_id} is {j.state}; only a live pod is preempted")
        self._end(j, "preempted", "preempted", kind="pod.preempted")

    def resume_without_gpu(self, job_id: str) -> None:
        """A resumed pod came back with no GPU: it is terminated and the job goes back to
        `queued` for a fresh placement."""
        j = self._get(job_id)
        if j.state != "booting" or not j.resuming:
            raise FakeError(f"{job_id} is not a booting resume")
        self.terminated_pods.append(j.pod["id"])
        self._bill(j, None)
        self._emit("pod.unhealthy", j, state="resume_no_gpu",
                   detail={"reason": "resume_no_gpu", "stage": "gpu_check",
                           "machine_id": j.pod["machine_id"], "log_tail": None})
        j.pod, j.placed_at, j.boot_started_at, j.last_line_at = None, None, None, None
        j.ready_at, j.max_life_fired, j.resuming = None, False, False
        j.models = {}
        j.queue_deadline_ts = self.now + j.queue_deadline_s
        self._set_state(j, "queued", "job.queued")

    # =================================================================== transitions

    def _emit(self, kind: str, j: Optional[_Job], *, state: str = "", pod_id: str = "",
              detail: Optional[dict] = None) -> dict:
        ev = {"seq": len(self.event_log) + 1, "ts": self.now, "kind": kind,
              "job_id": j.job_id if j else "",
              "pod_id": pod_id or ((j.pod or {}).get("id", "") if j else ""),
              "state": state or (j.state if j else ""), "detail": detail or {}}
        self.event_log.append(ev)
        return ev

    def _set_state(self, j: _Job, state: str, kind: str = "", **detail) -> None:
        j.state = state
        if kind:
            self._emit(kind, j, state=state, detail=detail or None)

    def _bill(self, j: _Job, kind: Optional[str]) -> None:
        if j.bill_kind and j.bill_since is not None and self.now > j.bill_since:
            rate = j.pod["cost_hr"] if j.bill_kind == "running" else self.park_cost_hr
            self.ledger.append((j.lane, j.job_id, j.bill_kind, j.bill_since, self.now, rate))
        j.bill_kind = kind
        j.bill_since = self.now if kind else None

    def _start_boot(self, j: _Job) -> None:
        j.boot_started_at = self.now
        j.last_line_at = self.now
        j.last_stage = None
        j.last_boot_event_at = None
        j.busy = False
        j.heartbeat_seen = False
        j.idle_since = None
        self._bill(j, "running")

    def _settle(self, j: _Job) -> None:
        """Derive ready/busy/idle/draining and the idle clock from busy + fetching.

        Busy → `busy` (or `draining`). Not busy and fetching → `ready`, clock stopped.
        Not busy, nothing fetching → `idle` once a heartbeat said so (`ready` before
        that), clock running."""
        if j.state not in SERVING:
            return
        if j.busy:
            new = "draining" if j.draining else "busy"
            j.idle_since = None
        elif j.fetching:
            new = "ready"
            j.idle_since = None
        else:
            new = "idle" if (j.heartbeat_seen or j.draining) else "ready"
            if j.idle_since is None:
                j.idle_since = self.now
        if new != j.state:
            kind = {"busy": "pod.busy", "idle": "pod.idle"}.get(new, "")
            self._set_state(j, new, kind)

    def _expire(self, j: _Job) -> None:
        self._end(j, "expired", "queue_deadline", kind="job.expired")

    def _max_life(self, j: _Job) -> None:
        j.max_life_fired = True
        if j.state == "booting":
            self._end(j, "done", "max_life")
            return
        j.draining = True
        j.stop_on_idle = True
        j.keep_warm_until = None
        self._emit("pod.draining", j, detail={"reason": "max_life"})
        self._settle(j)

    def _idle_stop(self, j: _Job) -> None:
        ttl = j.policy.get("park_ttl_s")
        if ttl and not j.interruptible:
            self._park(j, float(ttl))
        else:
            self._end(j, "done", "idle_stop")

    def _park(self, j: _Job, ttl_s: float) -> None:
        self._bill(j, "parked")
        j.park_until = self.now + ttl_s
        j.busy, j.idle_since, j.draining, j.stop_on_idle = False, None, False, False
        j.keep_warm_until = None
        self._set_state(j, "parked", "pod.parked", park_until=self._iso(j.park_until))

    def _fail(self, j: _Job, reason: str, stage: Optional[str], log_tail=None) -> None:
        before_ready = j.state == "booting"
        j.last_error, j.stage, j.log_tail = reason, stage, log_tail
        self._emit("pod.unhealthy", j, state=stage or reason,
                   detail={"reason": reason, "stage": stage,
                           "machine_id": (j.pod or {}).get("machine_id"),
                           "log_tail": log_tail})
        self._end(j, "failed", reason, kind="job.failed")
        if before_ready:
            n = self._fail_streak.get(j.lane, 0) + 1
            self._fail_streak[j.lane] = n
            if n >= BACKOFF_AFTER:
                hold = BACKOFF_STEPS_S[min(n - BACKOFF_AFTER, len(BACKOFF_STEPS_S) - 1)]
                self._backoff_until[j.lane] = self.now + hold
                self._emit("lane.backoff", None, state="held",
                           detail={"lane": j.lane, "failures": n, "hold_s": hold,
                                   "until": self._iso(self.now + hold)})

    def _end(self, j: _Job, state: str, reason: str, kind: str = "") -> None:
        self._bill(j, None)
        j.reason = reason
        j.ended_at = self.now
        j.busy, j.idle_since, j.park_until = False, None, None
        self._set_state(j, state, kind or ("job.done" if state == "done" else f"job.{state}"),
                        reason=reason)

    def _hold_check(self, j: _Job) -> None:
        until = self.lane_backoff(j.lane)
        if until:
            raise FakeHeld(f"lane {j.lane} is in boot-failure backoff until "
                           f"{self._iso(until)}")
        if self._spend(j.lane)["breached"]:
            raise FakeHeld(f"lane {j.lane} is over its daily cap")
        cap = (self.lane_caps.get(j.lane) or {}).get("max_pods")
        if cap is not None and self._live(j.lane) >= cap:
            raise FakeHeld(f"lane {j.lane} is at max_pods={cap}")

    def _live(self, lane: str) -> int:
        return sum(1 for x in self.jobs.values() if x.lane == lane and x.state in RUNNING)

    def _check_budgets(self) -> None:
        for lane in {x.lane for x in self.jobs.values()}:
            s = self._spend(lane)
            if not s["breached"]:
                self._breached.discard(lane)
                continue
            if lane in self._breached:
                continue
            self._breached.add(lane)
            self._emit("budget.breach", None, state="held",
                       detail={"lane": lane, "cap_usd": s["cap_usd"],
                               "rolling_24h_usd": s["rolling_24h_usd"]})
            for x in self.jobs.values():
                if x.lane == lane and x.state in SERVING and not x.draining:
                    x.draining = True
                    x.keep_warm_until = None
                    self._emit("pod.draining", x, detail={"reason": "budget"})
                    self._settle(x)

    # =================================================================== gpus

    #: A small stand-in catalogue, cheapest-capable first. Tests may replace `fake.gpus`.
    gpus = [
        {"gpu": "NVIDIA RTX A5000", "gpu_type_id": "NVIDIA RTX A5000", "vram_gb": 24, "datacenter": True, "cost_hr": 0.27},
        {"gpu": "NVIDIA GeForce RTX 4090", "gpu_type_id": "NVIDIA GeForce RTX 4090", "vram_gb": 24, "datacenter": False, "cost_hr": 0.69},
        {"gpu": "NVIDIA A40", "gpu_type_id": "NVIDIA A40", "vram_gb": 48, "datacenter": True, "cost_hr": 0.44},
        {"gpu": "NVIDIA L40S", "gpu_type_id": "NVIDIA L40S", "vram_gb": 48, "datacenter": True, "cost_hr": 0.86},
    ]

    def _gpus(self, q: dict) -> list:
        """The real server lists the walk order from its catalogue; the fake filters its
        stand-in list the same way (VRAM floor, data-centre only, explicit types) and
        flags, rather than drops, cards over `max_cost_hr`."""
        min_vram = float(q.get("min_vram_gb") or 0)
        dc_only = str(q.get("datacenter_only", "")).lower() in ("1", "true", "yes")
        types = [t for t in (q.get("types") or "").split(",") if t]
        cap = float(q["max_cost_hr"]) if q.get("max_cost_hr") else None
        rows = [dict(g) for g in self.gpus
                if g["vram_gb"] >= min_vram and (g["datacenter"] or not dc_only)]
        if types:
            rows = [g for t in types for g in rows if g["gpu_type_id"] == t]
        for g in rows:
            if cap is not None:
                g["over_max_cost"] = g["cost_hr"] is not None and g["cost_hr"] > cap
        return rows

    # =================================================================== spend

    def _spend(self, lane: str) -> dict:
        now = self.now
        day0 = _dt.datetime.fromtimestamp(now, _dt.timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        rows = [r for r in self.ledger if r[0] == lane]
        for x in self.jobs.values():
            if x.lane == lane and x.bill_kind and x.bill_since is not None:
                rate = x.pod["cost_hr"] if x.bill_kind == "running" else self.park_cost_hr
                rows.append((lane, x.job_id, x.bill_kind, x.bill_since, now, rate))

        def total(since, kind=None):
            s = 0.0
            for _, _, k, t0, t1, rate in rows:
                if kind and k != kind:
                    continue
                s += max(0.0, t1 - max(t0, since)) / 3600.0 * rate
            return round(s, 6)

        cap = self.lane_caps.get(lane) or {}
        rolling = total(now - 86400)
        cap_usd = cap.get("daily_usd")
        return {"lane": lane, "today_usd": total(day0), "rolling_24h_usd": rolling,
                "running_usd": total(now - 86400, "running"),
                "parked_usd": total(now - 86400, "parked"),
                "cap_usd": cap_usd, "max_pods": cap.get("max_pods"),
                "breached": bool(cap_usd is not None and rolling >= cap_usd),
                "live_pods": self._live(lane)}

    def _cost_so_far(self, j: _Job) -> float:
        s = sum((r[4] - r[3]) / 3600.0 * r[5] for r in self.ledger if r[1] == j.job_id)
        if j.bill_kind and j.bill_since is not None:
            rate = j.pod["cost_hr"] if j.bill_kind == "running" else self.park_cost_hr
            s += (self.now - j.bill_since) / 3600.0 * rate
        return round(s, 6)

    # =================================================================== views

    @staticmethod
    def _iso(t: Optional[float]) -> Optional[str]:
        if t is None:
            return None
        return _dt.datetime.fromtimestamp(t, _dt.timezone.utc).isoformat()

    def _view(self, j: _Job) -> dict:
        pod = None
        if j.pod and j.state not in ("queued", "placing"):
            pod = {**j.pod, "draining": j.draining}
        urls = {}
        if j.pod and j.state in RUNNING:
            for port in j.spec.get("ports") or []:
                num = str(port).split("/")[0]
                urls[num] = f"https://{j.pod['id']}-{num}.proxy.example"
        return {"job_id": j.job_id, "lane": j.lane, "state": j.state,
                "labels": j.spec.get("labels") or {}, "pod": pod,
                "draining": j.draining,
                "models": dict(j.models), "models_rev": j.models_rev,
                "model_errors": dict(j.model_errors),
                "urls": urls, "boot": list(j.boot),
                "placed_at": self._iso(j.placed_at), "ready_at": self._iso(j.ready_at),
                "idle_since": self._iso(j.idle_since), "park_until": self._iso(j.park_until),
                "keep_warm_until": self._iso(j.keep_warm_until),
                "last_error": j.last_error, "reason": j.reason, "stage": j.stage,
                "log_tail": j.log_tail, "cost_so_far": self._cost_so_far(j),
                "resumed_from": j.resumed_from,
                "image": j.spec.get("image"), "reuse": j.spec.get("reuse") or "never"}

    def _get(self, job_id) -> _Job:
        j = self.jobs.get(job_id)
        if j is None:
            raise FakeError(f"no job {job_id}")
        return j

    # =================================================================== HTTP

    def _handle(self, request) -> "httpx.Response":
        try:
            who = self._auth(request)
            out, status = self._route(request, who)
            self.advance(0)         # fire anything the call made due; checks budgets
            if isinstance(out, str):
                resp = httpx.Response(status, text=out,
                                      headers={"content-type": "application/json"})
            else:
                resp = httpx.Response(status, json=out)
        except _HTTPError as e:
            detail = {"error": e.error}
            if e.hint:
                detail["hint"] = e.hint
            resp = httpx.Response(e.status, json={"detail": detail})
        return resp

    def _auth(self, request):
        tok = request.headers.get("x-podh-token")
        if not tok or tok not in self.tokens:
            raise _HTTPError(401, "bad or missing X-Podh-Token")
        lanes = self.tokens[tok]
        return None if lanes is None else tuple(lanes)

    @staticmethod
    def _may(who, lane) -> bool:
        return who is None or lane in who

    def _job_for(self, who, job_id) -> _Job:
        j = self.jobs.get(job_id)
        if j is None or not self._may(who, j.lane):
            raise _HTTPError(404, f"no job {job_id}")
        return j

    def _route(self, request, who):
        m, path = request.method, request.url.path
        q = dict(request.url.params)
        body = {}
        if request.content:
            try:
                body = json.loads(request.content)
            except ValueError:
                raise _HTTPError(422, "body is not JSON")
        parts = [unquote(p) for p in path.strip("/").split("/")]
        if parts[:1] != ["v1"]:
            raise _HTTPError(404, f"no route {path}")
        p = parts[1:]
        if p == ["jobs"] and m == "POST":
            return self._submit(body, who), 202
        if p == ["jobs"] and m == "GET":
            return self._list(q, who), 200
        if len(p) >= 2 and p[0] == "jobs":
            j = self._job_for(who, p[1])
            if len(p) == 2 and m == "GET":
                return self._view(j), 200
            if len(p) == 2 and m == "DELETE":
                return self._cancel(j, q.get("reason", "")), 200
            if len(p) == 3 and m == "POST":
                op = {"drain": self._drain, "warm": self._warm, "park": self._park_req,
                      "resume": self._resume_req, "models": self._models}.get(p[2])
                if op:
                    return op(j, body), 200
        if p == ["gpus"] and m == "GET":
            return {"gpus": self._gpus(q),
                    "note": "cost_hr is a catalogue estimate, not a live price"}, 200
        if len(p) == 3 and p[0] == "lanes" and p[2] == "spend" and m == "GET":
            if not self._may(who, p[1]):
                raise _HTTPError(403, f"this token may not read lane {p[1]!r}")
            return self._spend(p[1]), 200
        if len(p) == 3 and p[0] == "lanes" and p[2] == "backoff" and m == "DELETE":
            if who is not None:
                raise _HTTPError(403, "clearing a backoff is master-only")
            self.clear_backoff(p[1])
            return {"cleared": True}, 200
        if p == ["events"] and m == "GET":
            return self._events(q), 200
        if p and p[0] == "addons":
            return self._addons(m, p[1:], body, who)
        raise _HTTPError(404, f"no route {m} {path}")

    # -- jobs ----------------------------------------------------------------------------

    def _submit(self, body: dict, who) -> dict:
        if not isinstance(body, dict):
            raise _HTTPError(422, "body must be an object")
        lane = (body.get("lane") or (who[0] if who else "batch")).strip()
        if not self._may(who, lane):
            raise _HTTPError(403, f"this token may not submit to lane {lane!r}",
                             hint=f"allowed: {list(who or ())}")
        if not body.get("image") and not body.get("spec_key"):
            raise _HTTPError(422, "image is required")
        if body.get("queue_deadline_s") is not None:
            deadline = float(body["queue_deadline_s"])
        elif body.get("queue_deadline_min") is not None:
            deadline = float(body["queue_deadline_min"]) * 60
        else:
            raise _HTTPError(422, "queue_deadline_s is required",
                             hint="it bounds how long this may wait for capacity")
        reuse = body.get("reuse") or "never"
        if reuse not in ("never", "parked-first"):
            raise _HTTPError(422, f"reuse is never|parked-first, not {reuse!r}")
        boot = body.get("boot") or {}
        addons = boot.get("addons") or {}
        if addons.get("set") and addons.get("version") is None:
            raise _HTTPError(422, "boot.addons.version is required",
                             hint="pin the addon set version explicitly")
        idem = body.get("idempotency_key")
        if idem:
            for x in self.jobs.values():
                if x.spec.get("idempotency_key") == idem:
                    if not self._may(who, x.lane):
                        raise _HTTPError(409, "idempotency_key already used in another lane")
                    return {"job_id": x.job_id, "state": x.state, "lane": x.lane,
                            "resumed_from": x.resumed_from, "existing": True}
        job_id = body.get("job_id")
        if job_id and job_id in self.jobs:
            raise _HTTPError(409, f"job {job_id} already exists",
                             hint="use idempotency_key to make retries safe")
        if not job_id:
            self._n += 1
            job_id = f"job_{self._n:06d}"
        j = _Job(job_id=job_id, lane=lane, spec=json.loads(json.dumps(body)),
                 created_at=self.now, queue_deadline_ts=self.now + deadline,
                 queue_deadline_s=deadline)
        j.boot_set = [_path_of(m) for m in ((boot.get("models") or {}).get("paths") or [])]
        self.jobs[job_id] = j
        self._emit("job.queued", j, state="queued", detail={"lane": lane})
        if reuse == "parked-first":
            src = self._match_parked(j)
            if src is not None:
                self._take_over(j, src)
        return {"job_id": j.job_id, "state": j.state, "lane": lane,
                "resumed_from": j.resumed_from}

    def _match_parked(self, j: _Job) -> Optional[_Job]:
        """Same lane, image, addon set and version, and binaries; the job's gpu satisfied
        by the parked pod's actual card. Models are not matched."""
        if self.lane_backoff(j.lane) or self._spend(j.lane)["breached"]:
            return None
        want_boot = j.spec.get("boot") or {}
        want_gpu = j.spec.get("gpu") or {}
        if want_gpu.get("interruptible"):
            return None
        for x in sorted(self.jobs.values(), key=lambda x: x.park_until or 0):
            if x.state != "parked" or x.lane != j.lane or not x.pod:
                continue
            have_boot = x.spec.get("boot") or {}
            if x.spec.get("image") != j.spec.get("image"):
                continue
            if (have_boot.get("addons") or None) != (want_boot.get("addons") or None):
                continue
            if (have_boot.get("binaries") or None) != (want_boot.get("binaries") or None):
                continue
            if not self._gpu_ok(want_gpu, x.pod, bool(j.spec.get("gpu"))):
                continue
            return x
        return None

    @staticmethod
    def _gpu_ok(want: dict, pod: dict, wants_gpu: bool) -> bool:
        if not wants_gpu:
            return pod.get("gpu_count", 0) == 0
        if pod.get("gpu_count", 0) < int(want.get("count", 1)):
            return False
        if want.get("min_vram_gb") and pod.get("vram_gb", 0) < want["min_vram_gb"]:
            return False
        if want.get("types") and pod.get("gpu") not in want["types"]:
            return False
        if want.get("max_cost_hr") is not None and pod.get("cost_hr", 0) > want["max_cost_hr"]:
            return False
        if want.get("datacenter_only") and not pod.get("datacenter_class"):
            return False
        if want.get("allowed_cuda") and pod.get("cuda") not in want["allowed_cuda"]:
            return False
        return True

    def _take_over(self, j: _Job, src: _Job) -> None:
        """The new job resumes the parked pod; the parked job ends."""
        pod = dict(src.pod)
        present = {p: s for p, s in src.models.items() if s == "present"}
        self._bill(src, None)
        src.reason, src.ended_at, src.park_until = f"reused_by:{j.job_id}", self.now, None
        self._set_state(src, "done", "job.done", reason=src.reason)
        j.pod = pod
        j.placed_at = self.now
        j.resumed_from = src.job_id
        j.resuming = True
        j.models = dict(present)
        j.boot_set = [p for p in j.boot_set if p in present]
        missing = [p for p in [_path_of(m) for m in
                               ((j.spec.get("boot") or {}).get("models") or {}).get("paths")
                               or []] if p not in present]
        if missing:
            j.models_rev += 1
            for p in missing:
                j.models[p] = "fetching"
        self._start_boot(j)
        self._set_state(j, "booting", "pod.resumed", resumed_from=src.job_id)

    def _list(self, q: dict, who) -> dict:
        states = [s for s in (q.get("state") or "").split(",") if s]
        lane = q.get("lane") or ""
        limit = int(q.get("limit") or 100)
        rows = [self._view(x) for x in self.jobs.values()
                if self._may(who, x.lane) and (not lane or x.lane == lane)
                and (not states or x.state in states)]
        rows.reverse()
        return {"jobs": rows[:limit]}

    def _cancel(self, j: _Job, reason: str) -> dict:
        was = j.state
        if was in TERMINAL:
            raise _HTTPError(409, f"job {j.job_id} is already {was}")
        if was == "placing":
            j.cancelled_in_placement = True
        if j.pod and was in RUNNING + ("parked",):
            self.terminated_pods.append(j.pod["id"])
        self._end(j, "cancelled", f"cancelled: {reason}" if reason else "cancelled",
                  kind="job.cancelled")
        return {"cancelled": True, "was": was, "pod_id": (j.pod or {}).get("id")}

    def _live_or_409(self, j: _Job, allowed, what: str) -> None:
        if j.state not in allowed:
            raise _HTTPError(409, f"cannot {what} a job that is {j.state}")

    def _drain(self, j: _Job, body: dict) -> dict:
        self._live_or_409(j, RUNNING, "drain")
        if not j.draining:
            j.draining = True
            j.keep_warm_until = None
            self._emit("pod.draining", j, detail={"reason": "asked"})
        self._settle(j)
        return {"job_id": j.job_id, "state": j.state, "draining": True}

    def _warm(self, j: _Job, body: dict) -> dict:
        self._live_or_409(j, ("queued", "placing") + RUNNING, "warm")
        if "until" in body and body["until"] is not None:
            try:
                t = _dt.datetime.fromisoformat(str(body["until"]).replace("Z", "+00:00"))
            except ValueError:
                raise _HTTPError(422, f"until is not an ISO time: {body['until']!r}")
            if t.tzinfo is None:
                t = t.replace(tzinfo=_dt.timezone.utc)
            until = t.timestamp()
        elif "delta_s" in body:
            try:
                delta = float(body["delta_s"])
            except (TypeError, ValueError):
                raise _HTTPError(422, "delta_s must be a number of seconds")
            base = j.keep_warm_until if j.keep_warm_until is not None else self.now
            until = base + delta
        else:
            raise _HTTPError(422, "warm takes until or delta_s")
        if j.policy.get("max_life_s") and j.placed_at is not None:
            until = min(until, j.placed_at + float(j.policy["max_life_s"]))
        j.keep_warm_until = until
        return {"job_id": j.job_id, "keep_warm_until": self._iso(until)}

    def _park_req(self, j: _Job, body: dict) -> dict:
        self._live_or_409(j, SERVING, "park")
        if j.interruptible:
            raise _HTTPError(409, "spot pods are not parked")
        ttl = body.get("ttl_s")
        if ttl is None:
            ttl = j.policy.get("park_ttl_s") or PARK_DEFAULT_TTL_S
        self._park(j, float(ttl))
        return {"job_id": j.job_id, "state": j.state, "park_until": self._iso(j.park_until)}

    def _resume_req(self, j: _Job, body: dict) -> dict:
        self._live_or_409(j, ("parked",), "resume")
        j.park_until = None
        j.draining = j.stop_on_idle = False
        j.resuming = True
        self._start_boot(j)
        self._set_state(j, "booting", "pod.resumed")
        return {"job_id": j.job_id, "state": j.state}

    def _models(self, j: _Job, body: dict) -> dict:
        if j.state in TERMINAL:
            raise _HTTPError(409, f"cannot add models to a job that is {j.state}")
        add = body.get("add")
        if not isinstance(add, list) or not add:
            raise _HTTPError(422, "add must be a non-empty list of paths")
        added = []
        for m in add:
            if not (isinstance(m, str) or (isinstance(m, dict) and m.get("path"))):
                raise _HTTPError(422, f"a model is a path or {{path, sha256, size}}: {m!r}")
            p = _path_of(m)
            if j.models.get(p) == "present" or j.models.get(p) == "fetching":
                continue
            j.models[p] = "fetching"
            j.model_errors.pop(p, None)
            added.append(p)
        if added:
            j.models_rev += 1
        self._settle(j)
        return {"job_id": j.job_id, "models_rev": j.models_rev, "added": added,
                "models": dict(j.models)}

    # -- events --------------------------------------------------------------------------

    def _events(self, q: dict) -> dict:
        since = int(q.get("since_seq") or 0)
        limit = int(q.get("limit") or 500)
        job_id, kind = q.get("job_id") or "", q.get("kind") or ""
        found = [e for e in self.event_log if e["seq"] > since
                 and (not job_id or e["job_id"] == job_id)
                 and (not kind or e["kind"] == kind)][:limit]
        head = len(self.event_log)
        return {"events": found, "next_seq": found[-1]["seq"] if found else since,
                "last_seq": head, "truncated": bool(found) and found[-1]["seq"] < head
                and len(found) == limit}

    # -- addon sets ----------------------------------------------------------------------

    def _addons(self, m: str, p: list, body: dict, who):
        if not p and m == "GET":
            sets = [{"lane": lane, "name": name, "version": vs[-1]["version"]}
                    for (lane, name), vs in sorted(self.addon_sets.items())
                    if self._may(who, lane)]
            return {"sets": sets}, 200
        if len(p) < 2:
            raise _HTTPError(404, "no route")
        lane, name = p[0], p[1]
        if not self._may(who, lane):
            if m == "GET":
                raise _HTTPError(404, f"no addon set {lane}/{name}")
            raise _HTTPError(403, f"this token may not write lane {lane!r}'s sets")
        versions = self.addon_sets.get((lane, name)) or []
        if len(p) == 2 and m == "PUT":
            return self._addon_put(lane, name, body, who), 200
        if not versions:
            raise _HTTPError(404, f"no addon set {lane}/{name}")
        if len(p) == 2 and m == "GET":
            return versions[-1], 200
        if len(p) == 3 and m == "GET" and p[2] == "export":
            v = versions[-1]
            return json.dumps({"lane": lane, "name": name, "version": v["version"],
                               "items": v["items"]}, indent=2, sort_keys=True) + "\n", 200
        if len(p) == 3 and m == "GET":
            try:
                n = int(p[2])
            except ValueError:
                raise _HTTPError(404, f"no version {p[2]!r}")
            for v in versions:
                if v["version"] == n:
                    return v, 200
            raise _HTTPError(404, f"no version {n} of {lane}/{name}")
        raise _HTTPError(404, "no route")

    def _addon_put(self, lane: str, name: str, body: dict, who) -> dict:
        if not NAME_RE.match(name) or name in (".", ".."):
            raise _HTTPError(422, f"bad set name {name!r}")
        items = body.get("items")
        if not isinstance(items, list):
            raise _HTTPError(422, "items must be a list")
        out = []
        for i, it in enumerate(items):
            label = f"item {i} ({it.get('name') if isinstance(it, dict) else it!r})"
            if not isinstance(it, dict):
                raise _HTTPError(422, f"{label}: must be an object")
            nm = it.get("name") or ""
            if not NAME_RE.match(nm) or nm in (".", ".."):
                raise _HTTPError(422, f"{label}: bad name")
            if not REPO_RE.match(it.get("repo") or ""):
                raise _HTTPError(422, f"{label}: repo must be https://github.com/<owner>/<repo>")
            commit = str(it.get("commit") or "").lower()
            if not SHA_RE.match(commit):
                if not SHORT_RE.match(commit):
                    raise _HTTPError(422, f"{label}: commit must be a hex SHA (7-40 chars)")
                repo = it["repo"][:-4] if it["repo"].endswith(".git") else it["repo"]
                hits = {c for c in (self.known_commits.get(repo, [])
                                    + self.known_commits.get(repo + ".git", []))
                        if c.startswith(commit)}
                if len(hits) != 1:
                    raise _HTTPError(422, f"{label}: commit {commit} is "
                                     f"{'ambiguous' if hits else 'unknown'}")
                commit = hits.pop()
            for ln in it.get("links") or []:
                for k in ("file", "to"):
                    v = str((ln or {}).get(k) or "")
                    v = v[len("comfy:"):] if k == "to" and v.startswith("comfy:") else v
                    if not v or v.startswith("/") or ".." in v.split("/"):
                        raise _HTTPError(422, f"{label}: link {k} {v!r} leaves its root")
            out.append({**it, "commit": commit,
                        "requirements": it.get("requirements", True),
                        "optional": bool(it.get("optional", False))})
        versions = self.addon_sets.setdefault((lane, name), [])
        v = {"lane": lane, "name": name, "version": len(versions) + 1, "items": out,
             "note": body.get("note") or "", "by": "master" if who is None else ",".join(who),
             "at": self._iso(self.now)}
        versions.append(v)
        return {"version": v["version"]}
