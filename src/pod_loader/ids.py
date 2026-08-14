"""Job identity. One scheme, sortable, collision-free.

## What was wrong

`f"job{int(time.time())}"` — a second-granularity timestamp. Two launches in the same
second produce the SAME id, and that id is also used as the idempotency key, so the second
launch would be treated as a retry of the first and silently return the first job instead
of running. A fleet makes that likely rather than theoretical.

It also read badly next to everything else. Code revisions are addressed by git sha,
because code is content and a sha is what content-addressing means. Jobs are events, and an
event wants an identifier that sorts by when it happened. Mixing a raw unix integer into
that gave three different-looking id schemes across one system with no principle behind the
difference.

## What this is

A ULID: 48 bits of millisecond timestamp followed by 80 bits of randomness, Crockford
base32, 26 characters.

    job_01K8YQ9F3M7V2XW4B6N8P0RTQZ
        └ lexicographic order == chronological order

That property is the point. Listings, log directories and object keys sort into run order
without a separate timestamp field, which is exactly what a flat object store cannot give
you any other way. 80 random bits make a same-millisecond collision not worth reasoning
about.

Prefixed `job_` so an id is self-describing in a log line — the previous scheme produced
pod names like `job-job1786673987`, where the prefix was applied to something that already
started with "job".
"""
from __future__ import annotations

import os
import time

#: Crockford base32: no I, L, O or U, so an id cannot be misread aloud or in a ticket.
_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _b32(value: int, length: int) -> str:
    out = []
    for _ in range(length):
        out.append(_ALPHABET[value & 0x1F])
        value >>= 5
    return "".join(reversed(out))


def ulid() -> str:
    """A new sortable id. 26 chars, no separators."""
    ms = int(time.time() * 1000)
    return _b32(ms, 10) + _b32(int.from_bytes(os.urandom(10), "big"), 16)


def job_id() -> str:
    """The identifier for one run."""
    return f"job_{ulid()}"


def pod_name(job: str) -> str:
    """What the provider should call the pod.

    Just the job id. It already says what it is, and prefixing it produced
    `job-job1786673987` — which is the kind of thing that makes a console listing harder
    to read than no naming convention at all.

    Providers vary in what they accept, so this stays alphanumeric plus underscore and
    hyphen, and bounded in length.
    """
    safe = "".join(c if (c.isalnum() or c in "-_") else "-" for c in job)
    return safe[:60]


def work_key(spec: dict, code_rev: str = "", image: str = "") -> str:
    """A stable fingerprint of the work being asked for.

    This is the idempotency key, and it has to be derived from CONTENT to mean anything.
    It used to default to the job id — `cfg.idempotency_key or job_id` — which is minted
    fresh on every launch, so the unique index it fed could never collide and the dedupe
    never once fired. The field existed, the index existed, and the feature did not.

    What it protects against is real: an agent that times out and retries, or two operators
    launching the same job, spending two pod-hours to compute one answer.

    Deliberately NOT the git sha alone. The same code against a different spec is different
    work; the same spec on a different image is different work. All three go in.
    """
    import hashlib
    import json as _json

    payload = _json.dumps(
        {"spec": spec, "code": code_rev, "image": image},
        sort_keys=True, separators=(",", ":"), default=str)
    return "wk_" + hashlib.sha256(payload.encode()).hexdigest()[:24]
