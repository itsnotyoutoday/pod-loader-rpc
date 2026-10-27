"""FakeControl's pod logs (APP-API §9), per-job timelines (§8.14) and statistics
(TASK-API §11.5). Mixed into FakeControl; see the module docstring of testing.py for what
is simplified."""
from __future__ import annotations

import datetime as _dt
import json
import re
import uuid
from typing import Optional

from ._fake_errors import FakeError, _HTTPError

LOG_REQUEST_TTL_S = 600
LOG_LINES_MAX = 2000
LOG_TAIL_MAX_BYTES = 256 * 1024
LOG_NAMES_MAX = 16
LOG_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
STATS_DEFAULT_SINCE_S = 7 * 86400
BOOT_GROUPS = ("gpu", "image", "data_center", "lane", "pool")
_ENDED = ("done", "failed", "cancelled", "expired", "preempted")


def _pct(xs: list, p: float):
    if not xs:
        return None
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))], 3)


def _dist(xs: list) -> dict:
    xs = [x for x in xs if x is not None]
    return {"n": len(xs), "p50": _pct(xs, 50), "p90": _pct(xs, 90),
            "max": round(max(xs), 3) if xs else None}


def _r(v):
    return round(v, 3) if isinstance(v, (int, float)) else v


def _short_gpu(name):
    if not name:
        return None
    return name[len("NVIDIA "):] if name.startswith("NVIDIA ") else name


def _epoch(iso: str) -> float:
    return _dt.datetime.fromisoformat(iso).timestamp()


def _valid_name(n) -> bool:
    return isinstance(n, str) and bool(LOG_NAME_RE.match(n)) and n not in (".", "..")


def _tail(lines: list) -> tuple:
    out, size = [], 0
    for line in reversed(lines):
        b = len(line.encode("utf-8", "replace")) + 1
        if size + b > LOG_TAIL_MAX_BYTES:
            if not out:
                keep = line.encode("utf-8", "replace")[-(LOG_TAIL_MAX_BYTES - 1):]
                out.append(keep.decode("utf-8", "ignore"))
            return list(reversed(out)), True
        out.append(line)
        size += b
    return list(reversed(out)), False


class LogsStatsFake:
    """Pod logs, timelines and /v1/stats/* over the fake's in-memory jobs and tasks."""

    def _logs_init(self) -> None:
        #: The log store name full logs go to (LOG_STORE); None = not configured.
        self.log_store: Optional[str] = None
        self._log_requests: dict = {}    # job_id -> {names, lines, req_id, expires_at}
        self._log_tails: dict = {}       # (job_id, name) -> {name, at, lines, truncated}
        self._log_refs: dict = {}        # (job_id, name) -> {name, ref, size?}

    # =================================================================== test hooks

    def log_request(self, job_id: str) -> Optional[dict]:
        """What the pod's `/v1/alive` reply carries as `logs` right now (None when no
        request is pending): `{"names", "lines", "req_id"}`."""
        self._get(job_id)
        r = self._pending_logs(job_id)
        return {k: r[k] for k in ("names", "lines", "req_id")} if r else None

    def push_logs(self, job_id: str, name: str, lines: list, *, truncated: bool = False,
                  req_id: Optional[str] = None) -> dict:
        """The pod pushed a tail (`POST /v1/pod/logs`): stored as the latest for that name
        (256 KB cap, newest lines kept), and the pending request is cleared (only the one
        whose req_id matches, when `req_id` is given)."""
        self._get(job_id)
        if not _valid_name(name):
            raise FakeError(f"{name!r} is not a log name")
        if not isinstance(lines, list) or not all(isinstance(x, str) for x in lines):
            raise FakeError("lines must be a list of strings")
        kept, cut = _tail(lines)
        self._log_tails[(job_id, name)] = {"name": name, "at": self.now, "lines": kept,
                                           "truncated": bool(truncated) or cut}
        cur = self._log_requests.get(job_id)
        cleared = bool(cur) and (req_id is None or cur["req_id"] == req_id)
        if cleared:
            del self._log_requests[job_id]
        self._emit("pod.logs", self._get(job_id),
                   detail={"names": [name], **({"req_id": req_id} if req_id else {})})
        return {"job_id": job_id, "stored": [name], "cleared": cleared}

    def upload_logs(self, job_id: str, names: list, sizes: Optional[dict] = None) -> dict:
        """The pod asked for upload URLs (`POST /v1/pod/logs/upload`) and uploaded: refs
        are recorded on the job (`timeline.logs`). With no `log_store` the server answers
        404 and the pod skips; this raises FakeError instead."""
        j = self._get(job_id)
        if not self.log_store:
            raise FakeError("no log store configured (set fake.log_store)")
        refs = {}
        for n in names:
            if not _valid_name(n):
                raise FakeError(f"{n!r} is not a log name")
            ref = f"store://{self.log_store}/logs/{j.lane}/{job_id}/{n}.log.gz"
            size = (sizes or {}).get(n)
            self._log_refs[(job_id, n)] = {"name": n, "ref": ref,
                                           **({"size": size} if size is not None else {})}
            refs[n] = ref
        return refs

    # =================================================================== logs over HTTP

    def _pending_logs(self, job_id: str) -> Optional[dict]:
        r = self._log_requests.get(job_id)
        if r and r["expires_at"] <= self.now:
            del self._log_requests[job_id]
            return None
        return r

    def _logs_route(self, m: str, j, p: list, q: dict, body):
        """p is the path after /v1/jobs/{id}: ["logs"], ["logs", "request"],
        ["logs", name, "download"]. None when it is not a logs route."""
        if p == ["logs", "request"] and m == "POST":
            return self._logs_request(j, body or {}), 202
        if p == ["logs"] and m == "GET":
            name = q.get("name") or ""
            logs = [dict(v) for (jid, n), v in sorted(self._log_tails.items())
                    if jid == j.job_id and (not name or n == name)]
            return {"job_id": j.job_id, "logs": logs,
                    "pending": self._pending_logs(j.job_id) is not None}, 200
        if len(p) == 3 and p[0] == "logs" and p[2] == "download" and m == "GET":
            if not self.log_store:
                return {"error": "no log store configured"}, 404
            ref = self._log_refs.get((j.job_id, p[1]))
            if not ref:
                raise _HTTPError(404, f"no uploaded log {p[1]!r} for job {j.job_id}")
            key = ref["ref"].split("/", 3)[3]
            return {"job_id": j.job_id, "name": p[1], "ref": ref["ref"],
                    "size": ref.get("size"),
                    "url": f"https://fake-store.test/{key}?sig=fake", "expires_s": 21600}, 200
        return None

    def _logs_request(self, j, body: dict) -> dict:
        if j.state in _ENDED:
            raise _HTTPError(409, f"job {j.job_id} is {j.state}",
                             "a pod that has ended cannot answer")
        names = body.get("names")
        if names is None:
            names = []
        if not isinstance(names, list) or len(names) > LOG_NAMES_MAX \
                or not all(_valid_name(n) for n in names):
            raise _HTTPError(422, f"names must be a list of at most {LOG_NAMES_MAX} log names")
        lines = body.get("lines", 200)
        if not isinstance(lines, int) or isinstance(lines, bool) or not 1 <= lines <= LOG_LINES_MAX:
            raise _HTTPError(422, f"lines must be a whole number 1-{LOG_LINES_MAX}")
        req = {"names": list(dict.fromkeys(names)), "lines": lines,
               "req_id": uuid.uuid4().hex[:16], "expires_at": self.now + LOG_REQUEST_TTL_S}
        self._log_requests[j.job_id] = req
        return {"job_id": j.job_id, **req}

    # =================================================================== timelines

    def _stages(self, j) -> list:
        marks = []                      # (stage, start)
        for b in j.boot:
            if b.get("stage") and (not marks or marks[-1][0] != b["stage"]):
                if b["stage"] not in [s for s, _ in marks]:
                    marks.append((b["stage"], _epoch(b["at"])))
        out = []
        for i, (stage, start) in enumerate(marks):
            if i + 1 < len(marks):
                end, outcome = marks[i + 1][1], "ok"
            elif j.ready_at is not None:
                end, outcome = j.ready_at, "ok"
            elif j.ended_at is not None:
                end, outcome = j.ended_at, "failed" if j.state == "failed" else "ended"
            else:
                end, outcome = None, "open"
            s = _r(end - start) if end is not None else None
            out.append({"boot_n": 0, "stage": stage, "start": start, "end": end,
                        "measured_s": s, "reported_s": None, "s": s, "bytes": None,
                        "files": None, "mb_s": None, "outcome": outcome})
        return out

    def _timeline(self, j) -> dict:
        pod = j.pod or {}
        placed = j.placed_at
        first = _epoch(j.boot[0]["at"]) if j.boot else None
        stages = self._stages(j)
        atts = [a for t in self.task_rows.values() for a in t.attempts
                if a.get("job_id") == j.job_id]
        claims = [a["claimed_at"] for a in atts if a.get("claimed_at")]
        starts = [a["started_at"] for a in atts if a.get("started_at")]
        failed_stage = None
        if j.state == "failed" and j.ready_at is None:
            failed_stage = j.stage or next((s["stage"] for s in reversed(stages)
                                            if s["outcome"] == "failed"), None) or (
                "placement" if placed is None else ("pull" if first is None else None))
        evs = [e["kind"] for e in self.event_log if e.get("job_id") == j.job_id]
        return {
            "job_id": j.job_id, "lane": j.lane, "pool": j.pool[1] if j.pool else None,
            "image": j.spec.get("image"), "image_digest": None,
            "gpu": _short_gpu(pod.get("gpu")), "gpu_type_id": pod.get("gpu") or None,
            "gpu_count": pod.get("gpu_count"), "data_center": pod.get("data_center"),
            "machine_id": pod.get("machine_id"), "cost_hr": pod.get("cost_hr"),
            "submitted_at": j.created_at,
            "placement": {"attempts": 1 if placed is not None else 0,
                          "outcomes": {"placed": 1} if placed is not None else {},
                          "first_attempt_at": placed, "walks": 1 if placed is not None else 0,
                          "walk_s": 0.0 if placed is not None else None,
                          "walk_total_s": 0.0 if placed is not None else None},
            "placed_at": placed,
            "placement_s": _r(placed - j.created_at) if placed is not None else None,
            "first_boot_line_at": first,
            "pull_s": _r(first - placed) if first is not None and placed is not None else None,
            "stages": stages, "ready_at": j.ready_at,
            "boot_total_s": _r(j.ready_at - placed) if j.ready_at is not None
            and placed is not None else None,
            "first_claim_at": min(claims) if claims else None,
            "first_task_started_at": min(starts) if starts else None,
            "tasks": {"attempts": len(atts),
                      "succeeded": sum(1 for a in atts if a.get("outcome") == "succeeded")},
            "busy_s": None, "idle_s": None, "parked_s": None,
            "parks": evs.count("pod.parked"), "resumes": evs.count("pod.resumed"),
            "resumed_from": j.resumed_from, "ended_at": j.ended_at, "state": j.state,
            "end_reason": j.reason if j.ended_at is not None else None,
            "end_stage": j.stage, "failed_stage": failed_stage,
            "life_s": _r((j.ended_at or self.now) - placed) if placed is not None else None,
            "cost_usd": self._cost_so_far(j),
            "logs": [dict(v) for (jid, _), v in sorted(self._log_refs.items())
                     if jid == j.job_id]}

    # =================================================================== stats over HTTP

    def _stats_window(self, q: dict) -> tuple:
        def when(v, default):
            if v in (None, ""):
                return default
            try:
                return float(v)
            except ValueError:
                pass
            try:
                t = _dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except ValueError:
                raise _HTTPError(422, f"since/until: {v!r} is not epoch seconds or ISO-8601")
            if t.tzinfo is None:
                t = t.replace(tzinfo=_dt.timezone.utc)
            return t.timestamp()
        a = when(q.get("since"), self.now - STATS_DEFAULT_SINCE_S)
        b = when(q.get("until"), self.now + 1)
        if b <= a:
            raise _HTTPError(422, "until must be after since")
        return a, b

    def _stat_lane(self, who, lane: str) -> None:
        if lane and not self._may(who, lane):
            raise _HTTPError(404, f"no lane {lane}")

    @staticmethod
    def _group_by(v: str, allowed) -> str:
        if v and v not in allowed:
            raise _HTTPError(422, f"group_by must be one of {sorted(allowed)}")
        return v

    def _stats_route(self, m: str, p: list, q: dict, who):
        if m != "GET":
            raise _HTTPError(405, "stats are read with GET")
        lane = q.get("lane") or ""
        self._stat_lane(who, lane)
        a, b = self._stats_window(q)
        if p in (["boots"], ["boots", "raw"]):
            raw = p == ["boots", "raw"]
            resumed = q.get("resumed") or ("all" if raw else "0")
            if resumed not in ("0", "1", "all"):
                raise _HTTPError(422, "resumed must be 0, 1 or all")
            limit = 20000
            if raw:
                try:
                    limit = int(q.get("limit") or 1000)
                except ValueError:
                    raise _HTTPError(422, "limit must be a whole number")
                if not 1 <= limit <= 20000:
                    raise _HTTPError(422, "limit must be 1-20000")
            g = "" if raw else self._group_by(q.get("group_by") or "", BOOT_GROUPS)
            tls = self._boot_timelines(who, q, a, b, resumed, limit)
            if raw:
                return "".join(json.dumps(t, separators=(",", ":")) + "\n" for t in tls), 200
            return {"since": a, "until": b, "group_by": g or None, "resumed": resumed,
                    "groups": self._boot_stats(tls, g),
                    "basis": "fake: the fake's own clock and boot lines"}, 200
        if p == ["tasks"]:
            g = self._group_by(q.get("group_by") or "", {"kind", "gpu"}) or "kind"
            return {"since": a, "until": b, "group_by": g,
                    "groups": self._task_stats(who, q, a, b, g)}, 200
        if p == ["spend"]:
            g = self._group_by(q.get("group_by") or "", {"day", "lane", "gpu", "pool"}) or "day"
            rows = self._spend_rows(who, lane, a, b, g)
            return {"since": a, "until": b, "group_by": g, "rows": rows,
                    "total_usd": round(sum(r["usd"] for r in rows), 4),
                    "basis": "fake: cost_hr x fake-clock time"}, 200
        raise _HTTPError(404, f"no route GET /v1/stats/{'/'.join(p)}")

    def _boot_timelines(self, who, q, a, b, resumed, limit) -> list:
        lane, image = q.get("lane") or "", q.get("image") or ""
        gpu, dc = (_short_gpu(q.get("gpu") or "") or "").lower(), q.get("data_center") or ""
        out = []
        for j in sorted(self.jobs.values(), key=lambda x: x.created_at):
            if not (a <= j.created_at < b) or not self._may(who, j.lane):
                continue
            if lane and j.lane != lane:
                continue
            tl = self._timeline(j)
            if image and tl["image"] != image:
                continue
            if gpu and gpu != (tl["gpu"] or "").lower():
                continue
            if dc and tl["data_center"] != dc:
                continue
            if (resumed == "0" and tl["resumed_from"]) or (resumed == "1" and not tl["resumed_from"]):
                continue
            out.append(tl)
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _boot_stats(tls: list, group_by: str) -> list:
        groups: dict = {}
        for tl in tls:
            key = tl.get(group_by) if group_by else "all"
            groups.setdefault(key if key is not None else "unknown", []).append(tl)
        out = []
        for key, g in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            placed = [t for t in g if t["placed_at"] is not None]
            ready = [t for t in placed if t["ready_at"] is not None]
            failed = [t for t in placed if t["ready_at"] is None and t["state"] == "failed"]
            other = [t for t in placed if t["ready_at"] is None and t["ended_at"] is not None
                     and t["state"] != "failed"]
            booting = [t for t in placed if t["ready_at"] is None and t["ended_at"] is None]
            decided = len(ready) + len(failed)
            stages: dict = {}
            for t in placed:
                for s in t["stages"]:
                    if s["outcome"] == "ok" and s["s"] is not None:
                        stages.setdefault(s["stage"], []).append(s["s"])
            fails: dict = {}
            for t in failed:
                k = (t["failed_stage"] or "unknown", (t["end_reason"] or "")[:80])
                fails[k] = fails.get(k, 0) + 1
            out.append({
                "key": key, "jobs": len(g), "n": len(placed), "unplaced": len(g) - len(placed),
                "ready": len(ready), "failed": len(failed), "ended_before_ready": len(other),
                "booting": len(booting),
                "success_rate": round(len(ready) / decided, 4) if decided else None,
                "placement_s": _dist([t["placement_s"] for t in placed]),
                "walk_s": _dist([t["placement"]["walk_s"] for t in placed]),
                "pull_s": _dist([t["pull_s"] for t in placed]),
                "boot_total_s": _dist([t["boot_total_s"] for t in placed]),
                "stages": {n: _dist(xs) for n, xs in stages.items()},
                "failures": [{"stage": k[0], "reason": k[1], "n": n}
                             for k, n in sorted(fails.items(), key=lambda kv: -kv[1])],
                "cost_usd": round(sum(t["cost_usd"] for t in g), 4)})
        return out

    def _task_stats(self, who, q, a, b, group_by) -> list:
        groups: dict = {}
        for t in self.task_rows.values():
            if not (a <= t.submitted_at < b) or not self._may(who, t.lane):
                continue
            if any(q.get(k) and q[k] != getattr(t, k) for k in ("lane", "pool", "kind")):
                continue
            u = t.usage or {}
            key = ((_short_gpu(u.get("gpu_type") or (t.pod or {}).get("gpu")) or "unknown")
                   if group_by == "gpu" else t.kind)
            g = groups.setdefault(key, {"n": 0, "outcomes": {}, "queued_s": [], "held_s": [],
                                        "run_s": [], "fetch_s": [], "upload_s": [],
                                        "wall_s": [], "cost": [], "claimed": 0, "hits": 0})
            g["n"] += 1
            g["outcomes"][t.state] = g["outcomes"].get(t.state, 0) + 1
            if t.claimed_at and t.queued_at:
                g["queued_s"].append(u.get("queued_s", max(0.0, t.claimed_at - t.queued_at)))
            if t.held_s:
                g["held_s"].append(t.held_s)
            if t.started_at and t.ended_at and t.state in ("succeeded", "failed"):
                g["run_s"].append(max(0.0, t.ended_at - t.started_at))
            ph = u.get("phases") or {}
            for k in ("fetch_s", "upload_s"):
                if isinstance(ph.get(k), (int, float)):
                    g[k].append(float(ph[k]))
            if isinstance(u.get("wall_s"), (int, float)):
                g["wall_s"].append(u["wall_s"])
            if isinstance(u.get("cost_usd"), (int, float)):
                g["cost"].append(u["cost_usd"])
            if u:
                g["claimed"] += 1
                g["hits"] += 1 if u.get("affinity_hit") else 0
        out = []
        for key, g in sorted(groups.items(), key=lambda kv: -kv[1]["n"]):
            cost = g["cost"]
            out.append({"key": key, "n": g["n"], "outcomes": g["outcomes"],
                        **{k: _dist(g[k]) for k in ("queued_s", "held_s", "run_s", "fetch_s",
                                                    "upload_s", "wall_s")},
                        "cost_usd": {"total": round(sum(cost), 6),
                                     "per_task": round(sum(cost) / len(cost), 6) if cost else None,
                                     "p50": _pct(cost, 50), "p90": _pct(cost, 90)},
                        "affinity_hit_rate": round(g["hits"] / g["claimed"], 4)
                        if g["claimed"] else None})
        return out

    def _spend_rows(self, who, lane: str, a: float, b: float, group_by: str) -> list:
        b = min(b, self.now)
        rows = list(self.ledger)
        for x in self.jobs.values():
            if x.bill_kind and x.bill_since is not None:
                rate = x.pod["cost_hr"] if x.bill_kind == "running" else self.park_cost_hr
                rows.append((x.lane, x.job_id, x.bill_kind, x.bill_since, self.now, rate))
        groups: dict = {}
        for ln, job_id, kind, t0, t1, rate in rows:
            if not self._may(who, ln) or (lane and ln != lane):
                continue
            s, e = max(t0, a), min(t1, b)
            if e <= s:
                continue
            pieces = []
            while s < e:
                d = _dt.datetime.fromtimestamp(s, _dt.timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0)
                nxt = min(e, d.timestamp() + 86400) if group_by == "day" else e
                pieces.append((s, nxt))
                s = nxt
            j = self.jobs.get(job_id)
            for p0, p1 in pieces:
                key = (_dt.datetime.fromtimestamp(p0, _dt.timezone.utc).strftime("%Y-%m-%d")
                       if group_by == "day" else ln if group_by == "lane"
                       else (_short_gpu(((j and j.pod) or {}).get("gpu")) or "unknown")
                       if group_by == "gpu" else ((j and j.pool and j.pool[1]) or "none"))
                g = groups.setdefault(key, {"run_usd": 0.0, "park_usd": 0.0, "pod_hours": 0.0})
                usd = rate * (p1 - p0) / 3600
                if kind == "parked":
                    g["park_usd"] += usd
                else:
                    g["run_usd"] += usd
                    g["pod_hours"] += (p1 - p0) / 3600
        out = [{"key": k, "usd": round(g["run_usd"] + g["park_usd"], 4),
                "run_usd": round(g["run_usd"], 4), "park_usd": round(g["park_usd"], 4),
                "pod_hours": round(g["pod_hours"], 3)} for k, g in groups.items()]
        return (sorted(out, key=lambda r: r["key"], reverse=True) if group_by == "day"
                else sorted(out, key=lambda r: -r["usd"]))
