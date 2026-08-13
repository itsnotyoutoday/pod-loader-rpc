"""RunPod network volumes — deliberately named for the one provider that has them.

## Why this is not called PODH_VOLUME

Every other storage concept in this codebase is portable: an S3-compatible store is an
S3-compatible store whether it is R2, MinIO or AWS, so it gets a neutral name and a
neutral interface. A RunPod network volume is not that. Nothing else offers it, it cannot
be emulated, and attaching one PINS compute to the volume's datacenter — which is the
failure that blocked every job the day US-NC-1 filled up.

Naming it `PODH_VOLUME_ID` would have implied a portable concept and quietly invited
generic code to depend on it. So the environment variable carries the provider in its
name:

    RUNPOD_VOLUME=/path/to/runpod-volume.key

A reader who sees `RUNPOD_VOLUME` in a launcher knows immediately that this launch is
RunPod-only. That is the whole point of the name.

The same reasoning corrects an earlier mistake. RunPod's object endpoint was configured
through `PODH_S3_*` as though it were S3, and it is not: no presigned URLs, no batch
delete, `head_object` returns 403 on large objects. Calling it S3 made code assume
capabilities it does not have. Real S3-compatible stores (R2, AWS, MinIO) keep the neutral
name; RunPod's partial implementation is flagged for what it is.

## Why the file holds an id and not a description

The file needs only:

    volume_id = <volume-id>

Everything else — datacenter, size, name, current usage — is asked of RunPod, because
RunPod is the authority on its own volume. A file that also recorded `datacenter = US-NC-1`
would be a second source of truth, and the copy that goes stale is always the one you
read. Optional overrides are accepted for offline use, but the API wins when reachable.

## Why a file rather than a plain id in the environment

Consistency with every other credential in this system: `s3-cloudfare.key`, `runpod.key`.
They are gitignored by pattern (`*.key`), so a volume reference cannot be committed by
someone following the obvious path. An inline `RUNPOD_VOLUME=<volume-id>` is also accepted,
because refusing it would only push people toward hardcoding — but the file is the
documented form.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

#: Searched upward from the launch directory to $HOME, the same walk the credential
#: loaders use. Fixed relative paths broke once when the repos moved one level deeper,
#: and the reaper reported "no pods running" whether or not pods existed — failing open,
#: which is the worst way for a cost control to fail.
DEFAULT_FILES = ("runpod-volume.key", "runpod_volume.key")


class VolumeError(RuntimeError):
    """Raised loudly. A launch that silently loses its volume runs against an empty
    /workspace and reports success over no data."""


@dataclass
class RunPodVolume:
    """A RunPod network volume. Provider-scoped by construction."""

    volume_id: str
    mount_path: str = "/workspace"
    #: Filled from the API when reachable; None offline. Never read from the file in
    #: preference to the API — see the module docstring.
    datacenter: str | None = None
    size_gb: int | None = None
    name: str | None = None
    source: str = "?"

    provider = "runpod"          # class-level: this type is only ever RunPod's

    def require_provider(self, provider: str) -> None:
        """Refuse to be used by a provider that has no such concept.

        Without this, moving a job to another backend would silently drop the volume and
        run against an empty workspace — the job would 'succeed' and produce nothing,
        which is precisely the failure `verify_outputs()` exists to catch. Better to fail
        at launch, where the message can say what to do.
        """
        if provider != self.provider:
            raise VolumeError(
                f"RUNPOD_VOLUME is set, but the target provider is {provider!r}, which has "
                f"no network volumes. Network volumes are RunPod-specific and cannot be "
                f"emulated elsewhere.\n"
                f"  For a portable run, use an object store instead: set the mount kind to "
                f"'object' and point the job at your S3-compatible bucket.")

    def describe(self) -> dict:
        return {"provider": self.provider, "volume_id": self.volume_id,
                "mount_path": self.mount_path, "datacenter": self.datacenter,
                "size_gb": self.size_gb, "name": self.name, "source": self.source}

    def create_kwargs(self) -> dict:
        """The RunPod pod-create fields. Kept here so callers never handcraft them."""
        return {"network_volume_id": self.volume_id,
                "volume_mount_path": self.mount_path}


def _parse(text: str) -> dict:
    """key = value, with # comments. The same forgiving format as the other .key files,
    including the tolerance for a typo'd key name that cost an afternoon once."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip().lower()] = v.strip().strip("'\"")
    return out


def _search_up(names: tuple[str, ...], start: Path | None = None) -> Path | None:
    d = (start or Path.cwd()).resolve()
    home = Path.home().resolve()
    while True:
        for n in names:
            p = d / n
            if p.is_file():
                return p
        if d == home or d.parent == d:
            return None
        d = d.parent


def load(spec_value: str | None = None, *, enrich: bool = True) -> RunPodVolume | None:
    """Resolve the volume, or None when this launch does not use one.

    Precedence: an explicit argument, then `RUNPOD_VOLUME`, then a `runpod-volume.key`
    found by walking up to $HOME. Returning None is a legitimate outcome — a job reading
    straight from object storage needs no volume, and that is the portable path.
    """
    raw = (spec_value or os.environ.get("RUNPOD_VOLUME", "")).strip()
    if raw.startswith("~"):
        raw = str(Path(raw).expanduser())   # launch files are written by humans
    src = "argument" if spec_value else ("RUNPOD_VOLUME" if raw else "")

    if raw and not Path(raw).exists() and re.fullmatch(r"[a-z0-9]{6,32}", raw):
        vol = RunPodVolume(volume_id=raw, source=f"{src} (inline id)")
        return _enrich(vol) if enrich else vol

    path = Path(raw).expanduser() if raw else _search_up(DEFAULT_FILES)
    if not path or not path.is_file():
        if raw:
            raise VolumeError(
                f"RUNPOD_VOLUME={raw!r} is neither a readable file nor a volume id.\n"
                f"  Expected a file containing: volume_id = <id>")
        return None

    cfg = _parse(path.read_text())
    vid = cfg.get("volume_id") or cfg.get("volume") or cfg.get("id")
    if not vid:
        raise VolumeError(
            f"{path} has no volume_id.\n"
            f"  Expected a line: volume_id = <your volume id>\n"
            f"  Found keys: {sorted(cfg) or 'none'}")

    vol = RunPodVolume(
        volume_id=vid,
        mount_path=cfg.get("mount_path") or cfg.get("mount") or "/workspace",
        datacenter=cfg.get("datacenter") or None,
        size_gb=int(cfg["size_gb"]) if cfg.get("size_gb", "").isdigit() else None,
        name=cfg.get("name") or None,
        source=str(path))
    return _enrich(vol) if enrich else vol


def _enrich(vol: RunPodVolume) -> RunPodVolume:
    """Ask RunPod about the volume. Best-effort: offline is not a launch failure.

    The API is the authority, so what it returns overwrites what the file said. A file
    that disagrees with reality is worse than a file that says nothing.
    """
    try:
        from .runpod_api import RunPodAPI
        for v in RunPodAPI().volumes():
            if v.get("id") == vol.volume_id:
                vol.datacenter = v.get("dataCenterId") or vol.datacenter
                vol.size_gb = v.get("size") or vol.size_gb
                vol.name = v.get("name") or vol.name
                break
        else:
            raise VolumeError(
                f"volume {vol.volume_id} does not exist on this RunPod account "
                f"(from {vol.source}).\n"
                f"  A pod created with a bad volume id starts with an EMPTY /workspace "
                f"and the job runs against no data.")
    except VolumeError:
        raise
    except Exception:
        pass          # offline, or no credentials — the file's values stand
    return vol


def require(spec_value: str | None = None) -> RunPodVolume:
    """Like `load()`, but a missing volume is an error. For jobs that cannot run without."""
    vol = load(spec_value)
    if vol is None:
        raise VolumeError(
            "this job needs a RunPod network volume, and none is configured.\n"
            "  Set RUNPOD_VOLUME=/path/to/runpod-volume.key, or place a runpod-volume.key "
            "containing 'volume_id = <id>' in this directory or any parent.")
    return vol


if __name__ == "__main__":                                    # python -m pod_loader.volume
    import json
    try:
        v = load()
        print(json.dumps(v.describe() if v else {"volume": None,
                                                 "note": "no volume configured — "
                                                         "object-store runs are unaffected"},
                         indent=2))
    except VolumeError as e:
        print(f"error: {e}")
        raise SystemExit(1)
