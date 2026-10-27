"""Client for the pod-control app API (docs/APP-API.md r3, §2).

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
from typing import Any, Iterable, Optional, Union
from urllib.parse import quote

try:                                     # the published httpx, or its renamed successor
    import httpx
except ImportError:                      # pragma: no cover - depends on the environment
    import httpx2 as httpx               # type: ignore[no-redef]

from ..base_loader import pinned_ssl_context
from .spec import spec_dict

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

    def addon_export(self, lane: str, name: str) -> str:
        """The set as a file to commit into the app's repo (the server's text, verbatim)."""
        return self._req("GET", self._set_path(lane, name) + "/export", raw=True)
