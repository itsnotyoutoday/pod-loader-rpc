"""Client for the pod-control app API (docs/APP-API.md r3, §2) and its Task API
(docs/TASK-API.md: pools and tasks).

One method per endpoint, plain dicts back (the API's own JSON), and one exception family
whose message is the server's own error text. Authentication is the `X-Podh-Token` header.

TLS: pod-control may have no DNS name, so no CA can vouch for it. Give `fingerprint` (the
sha256 of its certificate, colons optional) and the connection admits that one
certificate, checked inside the handshake, before the token is sent. Without one, normal
CA verification applies.

Tests pass `transport=` (an httpx transport); `pod_loader.app.testing.FakeControl` provides
one that runs the whole API in memory.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json as _json
import re
from typing import Any, Iterable, Optional, Union
from urllib.parse import quote

try:                                     # the published httpx, or its renamed successor
    import httpx
except ImportError:                      # pragma: no cover - depends on the environment
    import httpx2 as httpx               # type: ignore[no-redef]

from ..base_loader import pinned_ssl_context
from .spec import spec_dict, task_dict

TOKEN_HEADER = "X-Podh-Token"


class AppAPIError(RuntimeError):
    """The server refused, or could not be reached. `status` is None when unreachable."""

    def __init__(self, status: Optional[int], error: str, *, hint: str = "",
                 method: str = "", path: str = "", body: Any = None):
        self.status, self.error, self.hint = status, error, hint
        self.method, self.path, self.body = method, path, body
        where = f"{method} {path}".strip()
        head = f"HTTP {status}" if status is not None else "unreachable"
        msg = f"{head} {where}: {error}" if where else f"{head}: {error}"
        super().__init__(msg + (f" (hint: {hint})" if hint else ""))


class Unauthorized(AppAPIError):
    """401/403: the token is wrong, or confined to other lanes."""


class NotFound(AppAPIError):
    """404: no such job, set or version (or one in a lane this token cannot see)."""


class Conflict(AppAPIError):
    """409: the request does not fit the job's state (e.g. resume a job that is not parked)."""


class Invalid(AppAPIError):
    """400/422: the body was refused; `error` names what."""


class Unreachable(AppAPIError):
    """No HTTP reply: refused connection, timeout, or a certificate that fails the pin."""


_BY_STATUS = {400: Invalid, 401: Unauthorized, 403: Unauthorized, 404: NotFound,
              409: Conflict, 422: Invalid}


def _error_from(resp, method: str, path: str) -> AppAPIError:
    """FastAPI wraps errors as {"detail": ...}; detail is {"error","hint"}, a string, or a
    list of validation problems. Keep the server's words in every case."""
    try:
        body = resp.json()
    except ValueError:
        body = resp.text
    detail = body.get("detail", body) if isinstance(body, dict) else body
    hint = ""
    if isinstance(detail, dict):
        error = str(detail.get("error") or detail.get("message") or detail)
        hint = str(detail.get("hint") or "")
    elif isinstance(detail, list):
        error = "; ".join(
            f"{'.'.join(str(x) for x in d.get('loc', []))}: {d.get('msg')}"
            if isinstance(d, dict) else str(d) for d in detail)
    else:
        error = str(detail or resp.reason_phrase or "error")[:1000]
    cls = _BY_STATUS.get(resp.status_code, AppAPIError)
    return cls(resp.status_code, error, hint=hint, method=method, path=path, body=body)


def _iso(t: Union[str, float, int, _dt.datetime]) -> str:
    if isinstance(t, str):
        return t
    if isinstance(t, _dt.datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=_dt.timezone.utc)
        return t.isoformat()
    return _dt.datetime.fromtimestamp(float(t), _dt.timezone.utc).isoformat()


def _model_item(m) -> Union[str, dict]:
    if isinstance(m, (str, dict)):
        return m
    if hasattr(m, "to_dict"):
        return m.to_dict()
    raise TypeError(f"a model is a path or a {{path, sha256, size}} dict, not {type(m).__name__}")


_FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
_REPO = re.compile(r"^https://github\.com/([^/]+)/(.+?)(?:\.git)?/?$")


def canonical_addon_item(it: dict) -> dict:
    """An addon item as the server stores it (pod-control addons.validate_items): every
    key present with its default, the repo as https://github.com/<owner>/<repo>, the
    commit lower-case. Unknown keys are kept, so they still count as a difference."""
    it = dict(it or {})
    repo = str(it.get("repo") or "")
    m = _REPO.match(repo)
    out = {**it,
           "name": it.get("name"),
           "repo": f"https://github.com/{m.group(1)}/{m.group(2)}" if m else repo,
           "commit": str(it.get("commit") or "").lower(),
           "pip": list(it.get("pip") or []),
           "requirements": it.get("requirements", True),
           "links": [{"file": (ln or {}).get("file"), "to": (ln or {}).get("to")}
                     for ln in it.get("links") or []],
           "probe": it.get("probe") or "",
           "optional": bool(it.get("optional", False)),
           "note": it.get("note") or ""}
    return out


def addon_items_hash(items: list) -> str:
    """sha256 of the canonical items as sorted-key JSON."""
    doc = [canonical_addon_item(it) for it in items]
    return hashlib.sha256(_json.dumps(doc, sort_keys=True, separators=(",", ":"))
                          .encode()).hexdigest()


class Client:
    """The app API.

        with Client("https://203.0.113.5:8443", token, fingerprint="ab:cd:…") as pc:
            job_id = pc.submit(spec)
            pc.get(job_id)["state"]
    """

    def __init__(self, base_url: str, token: str, *, fingerprint: str = "",
                 timeout: float = 30.0, transport=None, client=None):
        if not base_url:
            raise ValueError("base_url required")
        if client is not None:
            self._http = client
        else:
            kw: dict = {"base_url": base_url.rstrip("/"), "timeout": timeout,
                        "headers": {TOKEN_HEADER: token or ""}}
            if transport is not None:
                kw["transport"] = transport
            elif fingerprint:
                kw["verify"] = pinned_ssl_context(fingerprint)
            self._http = httpx.Client(**kw)
        self._own = client is None

    # -- plumbing ----------------------------------------------------------------------

    def close(self) -> None:
        if self._own:
            self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _req(self, method: str, path: str, *, json: Any = None, params: Optional[dict] = None,
             raw: bool = False, timeout: Optional[float] = None):
        kw: dict = {}
        if json is not None:
            kw["json"] = json
        if params:
            kw["params"] = {k: v for k, v in params.items() if v not in (None, "")}
        if timeout is not None:
            kw["timeout"] = timeout
        try:
            resp = self._http.request(method, path, **kw)
        except httpx.TransportError as e:
            raise Unreachable(None, f"{type(e).__name__}: {e}", method=method,
                              path=path) from None
        if resp.status_code >= 400:
            raise _error_from(resp, method, path)
        if raw:
            return resp.text
        if not resp.content:
            return {}
        try:
            return resp.json()
        except ValueError:
            raise AppAPIError(resp.status_code, f"non-JSON reply: {resp.text[:200]}",
                              method=method, path=path) from None

    @staticmethod
    def _id(job_id: str) -> str:
        if not job_id:
            raise ValueError("job_id required")
        return quote(job_id, safe="")

    # -- jobs --------------------------------------------------------------------------

    def submit_reply(self, spec) -> dict:
        """`POST /v1/jobs`, returning the whole reply
        (`{"job_id", "state", "lane", "resumed_from"}`, plus `existing` on a retry)."""
        return self._req("POST", "/v1/jobs", json=spec_dict(spec))

    def submit(self, spec) -> str:
        """Queue a job (a JobSpec or a dict). Returns its job id."""
        return self.submit_reply(spec)["job_id"]

    def get(self, job_id: str) -> dict:
        return self._req("GET", f"/v1/jobs/{self._id(job_id)}")

    def list(self, lane: str = "", states: Union[str, Iterable[str], None] = None,
             limit: Optional[int] = None) -> list:
        """`GET /v1/jobs?lane=&state=a,b`. Returns the list of jobs."""
        if states is not None and not isinstance(states, str):
            states = ",".join(states)
        out = self._req("GET", "/v1/jobs",
                        params={"lane": lane, "state": states, "limit": limit})
        return out.get("jobs", []) if isinstance(out, dict) else out

    def cancel(self, job_id: str, reason: str = "") -> dict:
        """Terminate now, from any state, including mid-placement."""
        return self._req("DELETE", f"/v1/jobs/{self._id(job_id)}",
                         params={"reason": reason})

    def drain(self, job_id: str) -> dict:
        return self._req("POST", f"/v1/jobs/{self._id(job_id)}/drain", json={})

    def warm(self, job_id: str, *, until=None, delta_s: Optional[float] = None) -> dict:
        """Hold the pod warm until a time (ISO string, datetime or epoch seconds), or move
        the current warm-until by `delta_s` (may be negative). Exactly one of the two."""
        if (until is None) == (delta_s is None):
            raise ValueError("warm takes exactly one of until= or delta_s=")
        body = {"until": _iso(until)} if until is not None else {"delta_s": delta_s}
        return self._req("POST", f"/v1/jobs/{self._id(job_id)}/warm", json=body)

    def park(self, job_id: str, ttl_s: Optional[int] = None) -> dict:
        body = {} if ttl_s is None else {"ttl_s": int(ttl_s)}
        return self._req("POST", f"/v1/jobs/{self._id(job_id)}/park", json=body)

    def resume(self, job_id: str) -> dict:
        return self._req("POST", f"/v1/jobs/{self._id(job_id)}/resume", json={})

    def add_models(self, job_id: str, models: Iterable) -> dict:
        """Mid-life additions: paths, or `{"path", "sha256", "size"}` dicts."""
        items = [_model_item(m) for m in models]
        if not items:
            raise ValueError("add_models needs at least one model")
        return self._req("POST", f"/v1/jobs/{self._id(job_id)}/models", json={"add": items})

    # -- lanes, events -----------------------------------------------------------------

    def spend(self, lane: str) -> dict:
        """`{"today_usd", "rolling_24h_usd", "running_usd", "parked_usd", "cap_usd",
        "max_pods", "breached", "live_pods"}`."""
        if not lane:
            raise ValueError("lane required")
        return self._req("GET", f"/v1/lanes/{quote(lane, safe='')}/spend")

    def gpus(self, *, min_vram_gb: Optional[float] = None, clouds: Optional[list] = None,
             datacenter_only: Optional[bool] = None, max_cost_hr: Optional[float] = None,
             types: Optional[list] = None) -> list:
        """The cards a submit with these `gpu` fields would try, in the order it would try
        them: `[{"gpu", "gpu_type_id", "vram_gb", "datacenter", "cost_hr", ...}]`.
        `cost_hr` is a catalogue estimate, not a live price; the walk checks each pod's
        real price against `max_cost_hr` after placing it."""
        params: dict = {}
        if min_vram_gb is not None:
            params["min_vram_gb"] = min_vram_gb
        if clouds:
            params["clouds"] = ",".join(clouds)
        if datacenter_only is not None:
            params["datacenter_only"] = "true" if datacenter_only else "false"
        if max_cost_hr is not None:
            params["max_cost_hr"] = max_cost_hr
        if types:
            params["types"] = ",".join(types)
        return self._req("GET", "/v1/gpus", params=params).get("gpus", [])

    def events(self, since_seq: int = 0, wait: float = 0, *, job_id: str = "",
               kind: str = "", limit: Optional[int] = None) -> dict:
        """`{"events", "next_seq", "last_seq", "truncated"}`. `wait` > 0 long-polls (≤ 60 s);
        the HTTP timeout is stretched to cover it."""
        return self._req("GET", "/v1/events",
                         params={"since_seq": since_seq, "wait": wait or None,
                                 "job_id": job_id, "kind": kind, "limit": limit},
                         timeout=(float(wait) + 15) if wait else None)

    # -- addon sets --------------------------------------------------------------------

    @staticmethod
    def _set_path(lane: str, name: str) -> str:
        if not lane or not name:
            raise ValueError("lane and name required")
        return f"/v1/addons/{quote(lane, safe='')}/{quote(name, safe='')}"

    def addons(self) -> Any:
        """`GET /v1/addons`: the sets this token can see."""
        return self._req("GET", "/v1/addons")

    def addon_get(self, lane: str, name: str, version: Optional[int] = None) -> dict:
        """One version of a set, the latest when `version` is None."""
        p = self._set_path(lane, name)
        return self._req("GET", p if version is None else f"{p}/{int(version)}")

    def addon_put(self, lane: str, name: str, items: list, note: str = "") -> int:
        """Create the next version of a set. Returns the new version number."""
        body: dict = {"items": list(items)}
        if note:
            body["note"] = note
        return int(self._req("PUT", self._set_path(lane, name), json=body)["version"])

    def addon_put_if_changed(self, lane: str, name: str, items: list, note: str = "") -> dict:
        """PUT the set only when its content differs from the latest version.

        Both sides are compared in the server's normalised form (every item key present
        with its default, repo without `.git`, commit lower-case), and a short commit pin
        counts as equal to the full SHA the server resolved it to when that SHA starts
        with it. Item order is significant (it is install order). `note` is not compared.
        Returns `{"version", "changed"}`."""
        try:
            latest = self.addon_get(lane, name)
        except NotFound:
            latest = None
        if latest is not None:
            have = [canonical_addon_item(it) for it in latest.get("items") or []]
            want = [canonical_addon_item(it) for it in items]
            by_name = {it["name"]: it for it in have}
            for it in want:
                old = by_name.get(it["name"])
                if (old and old["repo"] == it["repo"] and not _FULL_SHA.match(it["commit"])
                        and it["commit"] and old["commit"].startswith(it["commit"])):
                    it["commit"] = old["commit"]
            if addon_items_hash(want) == addon_items_hash(have):
                return {"version": int(latest["version"]), "changed": False}
        return {"version": self.addon_put(lane, name, items, note=note), "changed": True}

    def addon_export(self, lane: str, name: str) -> str:
        """The set as a file to commit into the app's repo (the server's text, verbatim)."""
        return self._req("GET", self._set_path(lane, name) + "/export", raw=True)

    # -- task pools (TASK-API §2, §9.4) ------------------------------------------------

    @staticmethod
    def _pool_path(lane: str, name: str) -> str:
        if not lane or not name:
            raise ValueError("lane and name required")
        return f"/v1/pools/{quote(lane, safe='')}/{quote(name, safe='')}"

    def pools(self) -> list:
        """`GET /v1/pools`: `[{"lane", "name", "version", "updated_ts"}]` this token sees."""
        return self._req("GET", "/v1/pools").get("pools", [])

    def pool_put(self, lane: str, name: str, job, scale: Optional[dict] = None,
                 match: Optional[dict] = None, cost_share: Optional[dict] = None) -> int:
        """Create or replace a pool. `job` is an APP-API submit body (a JobSpec or dict)
        without `job_id`/`idempotency_key`; `scale` is `{"min_pods", "max_pods",
        "launch_when": {"queued_s", "queued_tasks"}, "prelaunch", "never_launch",
        "tasks_per_pod"}`; `match` is `{"affinity_wait_s", "accepts_from"}`. `cost_share`
        (`{kind: weight}`) is folded into `scale`. Returns the new version."""
        body: dict = {"job": spec_dict(job)}
        sc = dict(scale or {})
        if cost_share is not None:
            sc["cost_share"] = dict(cost_share)
        if sc:
            body["scale"] = sc
        if match:
            body["match"] = dict(match)
        return int(self._req("PUT", self._pool_path(lane, name), json=body)["version"])

    def pool_get(self, lane: str, name: str) -> dict:
        """`{"version", "job", "scale", "match", "queued", "held", "claimed", "running",
        "pods": [...], "spend", "expect", "expect_cooldown_until", "last_error"}`."""
        return self._req("GET", self._pool_path(lane, name))

    def pool_expect(self, lane: str, name: str, in_s: float = 0, tasks: int = 1,
                    needs: Optional[dict] = None) -> dict:
        """A pre-launch hint (`scale.prelaunch` pools only, else Conflict). During a
        wrong-hint cooldown the reply is `{"accepted": False, "cooldown_until"}`, not an
        error."""
        body: dict = {"in_s": in_s, "tasks": int(tasks)}
        if needs:
            body["needs"] = needs.to_dict() if hasattr(needs, "to_dict") else dict(needs)
        return self._req("POST", self._pool_path(lane, name) + "/expect", json=body)

    def pool_stats(self, lane: str, name: str, *, kind: str = "",
                   since: Union[float, int, _dt.datetime, None] = None) -> dict:
        """Per-kind counts and wall percentiles (queued_s, held_s, run_s) since `since`
        (epoch seconds or a datetime; the server's default is 7 days)."""
        if isinstance(since, _dt.datetime):
            if since.tzinfo is None:
                since = since.replace(tzinfo=_dt.timezone.utc)
            since = since.timestamp()
        return self._req("GET", self._pool_path(lane, name) + "/stats",
                         params={"kind": kind, "since": since or None})

    # -- tasks (TASK-API §2, §8, §9) ----------------------------------------------------

    @staticmethod
    def _label(label) -> str:
        """`"k:v"`, or a one-entry dict, as the API's `label=k:v`."""
        if label is None or label == "":
            return ""
        if isinstance(label, str):
            if ":" not in label:
                raise ValueError("a label filter is 'key:value'")
            return label
        if isinstance(label, dict):
            if len(label) != 1:
                raise ValueError("the server filters on one label; give one key")
            (k, v), = label.items()
            return f"{k}:{v}"
        raise TypeError(f"a label is 'key:value' or {{key: value}}, not {type(label).__name__}")

    @staticmethod
    def _states(states) -> Optional[str]:
        if states is None or isinstance(states, str):
            return states
        return ",".join(states)

    def submit_task_reply(self, spec) -> dict:
        """`POST /v1/tasks`: `{"task_id", "state", "lane", "pool", "existing"}`.
        `existing` is True when the idempotency key matched a task already there."""
        return self._req("POST", "/v1/tasks", json=task_dict(spec))

    def submit_task(self, spec) -> str:
        """Queue a task (a TaskSpec or a dict). Returns its task id."""
        return self.submit_task_reply(spec)["task_id"]

    def task(self, task_id: str) -> dict:
        """The task record: state, attempt, position, progress, result, artifacts (with
        fresh presigned urls), error, usage, timings, attempts[]."""
        return self._req("GET", f"/v1/tasks/{self._id(task_id)}")

    def tasks(self, lane: str = "", pool: str = "",
              states: Union[str, Iterable[str], None] = None,
              labels: Union[str, dict, None] = None, limit: Optional[int] = None) -> list:
        """`GET /v1/tasks`, newest first. The server filters on one label; with a dict of
        several, the first is sent and the rest are matched here, the server's way
        (`str(value)` equality)."""
        rest: dict = {}
        label = ""
        if isinstance(labels, dict) and len(labels) > 1:
            first, *more = labels.items()
            label = self._label(dict([first]))
            rest = dict(more)
        else:
            label = self._label(labels)
        out = self._req("GET", "/v1/tasks",
                        params={"lane": lane, "pool": pool, "state": self._states(states),
                                "label": label, "limit": limit})
        rows = out.get("tasks", []) if isinstance(out, dict) else out
        if rest:
            rows = [r for r in rows if all(str((r.get("labels") or {}).get(k, "\0")) == str(v)
                                           for k, v in rest.items())]
        return rows

    def cancel_task(self, task_id: str, reason: str = "") -> dict:
        """Cancel from any open state; final (no automatic path requeues it). Cancelling a
        cancelled task is an idempotent 200 (`already`); any other final state is
        Conflict."""
        return self._req("DELETE", f"/v1/tasks/{self._id(task_id)}",
                         params={"reason": reason})

    def cancel_tasks(self, lane: str = "", label: Union[str, dict, None] = None,
                     state: Union[str, Iterable[str], None] = None, *,
                     ids: Optional[Iterable[str]] = None, reason: str = "") -> int:
        """Bulk cancel: by `ids`, or by `lane` and `label` (optionally narrowed to
        `state`, default every open state). Returns how many were cancelled."""
        if ids is not None:
            body: dict = {"ids": list(ids)}
            if reason:
                body["reason"] = reason
            return int(self._req("POST", "/v1/tasks/cancel", json=body)["cancelled"])
        lab = self._label(label)
        if not lane or not lab:
            raise ValueError("bulk cancel needs ids, or lane and label")
        return int(self._req("DELETE", "/v1/tasks",
                             params={"lane": lane, "label": lab,
                                     "state": self._states(state),
                                     "reason": reason})["cancelled"])

    def retry_task(self, task_id: str, payload: Optional[dict] = None,
                   patch: Optional[dict] = None) -> dict:
        """Run a failed, cancelled, expired or returned task again: same id, attempt 1,
        and optionally a new payload or workflow patch (a re-roll; `attempts[]` keeps the
        old payload's hash). `{"task_id", "state", "attempt"}`."""
        body: dict = {}
        if payload is not None:
            body["payload"] = payload
        if patch is not None:
            body["patch"] = patch
        return self._req("POST", f"/v1/tasks/{self._id(task_id)}/retry", json=body)

    def set_priority(self, task_id: str, priority: int) -> dict:
        """Queued tasks only (else Conflict). `{"task_id", "priority", "position"}`."""
        return self._req("PATCH", f"/v1/tasks/{self._id(task_id)}",
                         json={"priority": int(priority)})

    # -- timelines and statistics (APP-API §8.14, TASK-API §11.5) ----------------------

    def job_timeline(self, job_id: str) -> dict:
        """The job's `timeline` from `GET /v1/jobs/{id}`: the placement walk, pull, each
        boot stage, ready, busy/idle, parks, the end, cost, and `logs` (uploaded full
        logs as `[{name, ref, size?}]`)."""
        return self.get(job_id).get("timeline") or {}

    @staticmethod
    def _when(v) -> Union[str, float, None]:
        """`since` / `until` as the server takes them: epoch seconds or ISO-8601."""
        if v is None or v == "":
            return None
        if isinstance(v, _dt.datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=_dt.timezone.utc)
            return v.timestamp()
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        if isinstance(v, str):
            return v
        raise TypeError(f"a time is epoch seconds, ISO-8601 or a datetime, not {type(v).__name__}")

    def _boot_params(self, lane, image, gpu, data_center, since, until, resumed) -> dict:
        if resumed is not None:
            resumed = {True: "1", False: "0"}.get(resumed, str(resumed))
        return {"lane": lane, "image": image, "gpu": gpu, "data_center": data_center,
                "since": self._when(since), "until": self._when(until), "resumed": resumed}

    def boot_stats(self, *, lane: str = "", image: str = "", gpu: str = "",
                   data_center: str = "", since=None, until=None,
                   resumed: Union[str, bool, None] = None, group_by: str = "") -> dict:
        """`GET /v1/stats/boots`: `{"since", "until", "group_by", "resumed", "groups":
        [{key, jobs, n, unplaced, ready, failed, ended_before_ready, booting, success_rate,
        placement_s, walk_s, pull_s, boot_total_s, stages, failures, cost_usd}], "basis"}`.
        `group_by` is gpu, image, data_center, lane or pool ("" = one group, "all");
        `resumed` is "0" (the server's default), "1" or "all" (True/False map to 1/0)."""
        params = self._boot_params(lane, image, gpu, data_center, since, until, resumed)
        params["group_by"] = group_by
        return self._req("GET", "/v1/stats/boots", params=params)

    def boot_stats_raw(self, *, lane: str = "", image: str = "", gpu: str = "",
                       data_center: str = "", since=None, until=None,
                       resumed: Union[str, bool, None] = None,
                       limit: Optional[int] = None) -> list:
        """`GET /v1/stats/boots/raw`: the timelines themselves (oldest first), parsed from
        the server's JSON lines. `resumed` defaults to "all" on the server; `limit` ≤ 20000
        (server default 1000)."""
        params = self._boot_params(lane, image, gpu, data_center, since, until, resumed)
        params["limit"] = limit
        text = self._req("GET", "/v1/stats/boots/raw", params=params, raw=True)
        out = []
        for n, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                continue
            try:
                out.append(_json.loads(line))
            except ValueError:
                raise AppAPIError(200, f"line {n} of the raw timelines is not JSON: {line[:200]}",
                                  method="GET", path="/v1/stats/boots/raw") from None
        return out

    def task_stats(self, *, lane: str = "", pool: str = "", kind: str = "", since=None,
                   until=None, group_by: str = "") -> dict:
        """`GET /v1/stats/tasks`: `{"since", "until", "group_by", "groups": [{key, n,
        outcomes, queued_s, held_s, run_s, fetch_s, upload_s, wall_s, cost_usd: {total,
        per_task, p50, p90}, affinity_hit_rate}]}`. `group_by` is kind (the server's
        default) or gpu."""
        return self._req("GET", "/v1/stats/tasks",
                         params={"lane": lane, "pool": pool, "kind": kind,
                                 "since": self._when(since), "until": self._when(until),
                                 "group_by": group_by})

    def spend_stats(self, *, lane: str = "", since=None, until=None,
                    group_by: str = "") -> dict:
        """`GET /v1/stats/spend`: `{"since", "until", "group_by", "rows": [{key, usd,
        run_usd, park_usd, pod_hours}], "total_usd", "basis"}`. `group_by` is day (UTC; the
        server's default), lane, gpu or pool. cost_hr x observed time, not an invoice."""
        return self._req("GET", "/v1/stats/spend",
                         params={"lane": lane, "since": self._when(since),
                                 "until": self._when(until), "group_by": group_by})

    # -- pod logs (APP-API §9) -----------------------------------------------------------

    def request_logs(self, job_id: str, names: Optional[Iterable[str]] = None,
                     lines: int = 200) -> dict:
        """Ask the job's pod for its logs (pod-control never connects into a pod: the
        request rides the pod's next heartbeat reply, and waits at most 10 min).
        `names` None = every log the pod keeps; `lines` 1-2000. Replaces any request
        still pending. `{"job_id", "req_id", "names", "lines", "expires_at"}`; Conflict for
        a job that has ended."""
        body: dict = {"lines": int(lines)}
        if names is not None:
            if isinstance(names, str):
                names = [names]
            body["names"] = list(names)
        return self._req("POST", f"/v1/jobs/{self._id(job_id)}/logs/request", json=body)

    def logs(self, job_id: str, name: Optional[str] = None) -> dict:
        """The latest tail the pod pushed, per log name: `{"job_id", "logs": [{"name",
        "at", "lines", "truncated"}], "pending"}` (`pending`: a request is still waiting
        for the pod)."""
        return self._req("GET", f"/v1/jobs/{self._id(job_id)}/logs",
                         params={"name": name})

    def log_download_url(self, job_id: str, name: str) -> str:
        """A presigned GET for a full log the pod uploaded at its end (gzip). NotFound
        when it never uploaded that name, or no log store is configured."""
        if not name:
            raise ValueError("name required")
        out = self._req("GET", f"/v1/jobs/{self._id(job_id)}/logs/"
                               f"{quote(name, safe='')}/download")
        return out["url"]
