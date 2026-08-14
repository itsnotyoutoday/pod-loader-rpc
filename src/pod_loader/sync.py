"""Publish a workload's source to object storage, so a pod runs THIS code.

## Why code travels as objects and not as a git clone

Cloning from the pod would put a credential on every pod and make GitHub a runtime
dependency of every launch — two failure modes bought for no benefit. The pod holds no
credentials; it reads files that were already placed where it can see them.

## Layout

    code/<workload>/_blobs/<sha256>        file CONTENT, stored once, immutable
    code/<workload>/<rev>/.manifest.json   path -> sha, i.e. the tree at that revision
    code/<workload>/latest                 pointer to the newest rev

Content-addressed, so publishing an unchanged file costs nothing and a revision costs one
small JSON plus only the blobs that are genuinely new. Before this, every launch re-uploaded
the whole tree — 55 objects — and a one-line change created a full second copy. Revisions
accumulated with nothing ever removed.

This is git's object model with the interesting parts removed: blobs and a flat manifest,
no trees and no commits, because a published revision is a snapshot and never needs to be
diffed or merged.

Blobs are scoped PER WORKLOAD rather than global. Cross-workload dedup would save nothing
real — separate workloads do not share files — and per-workload scoping means garbage
collecting one workload can never reach another's content. Where a delete path is involved,
the boring isolation is worth more than the clever saving.

Reconstruction needs no extra bookkeeping: a job record already names its code_rev, that
rev's manifest names every path and sha, and the blobs are immutable. job -> rev ->
manifest -> content, with no step that can go stale.

    code/<workload>/dev/        mutable — laptop iteration, deliberately

Publishing to a mutable path can change code under a RUNNING job, and Python's lazy
imports would then mix two versions inside one process: a stage imported at minute 1 and
a helper imported at minute 40 need not agree. Immutable revision directories make that
impossible. `dev/` stays mutable because iteration needs it, and jobs record when they
used it so a confusing result can be traced back to a moving target.

I have already been bitten by the mutable-path version of this: `pip install …@main`
served a stale engine from a Docker layer cache while claiming to be current. The fix was
pinning by SHA — the same argument, one layer up.

## Why this replaced the previous version

`sync_code()` walked hardcoded directories — `pipeline`, `runners`, `corpora`, `jobs` —
which were the folders of the pre-split monolith. After the repo split it silently pushed
nothing relevant, and the first pod to need `maintenance` failed with
`ModuleNotFoundError: No module named 'maintenance'`. Generic machinery carrying one
workload's folder names looks harmless right up to the moment there are two workloads.

So this takes a repo path and publishes its `code/` tree, whatever is in it.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import tarfile
import time
from pathlib import Path

#: Source and small metadata only. No corpora, no caches, and never a key file — the
#: pattern exclusion is belt-and-braces on top of `.gitignore`, because this pushes to a
#: bucket the pod can read and a leaked credential there is a leaked credential.
INCLUDE_SUFFIXES = (".py", ".json", ".yaml", ".yml", ".txt", ".md", ".toml", ".cfg")
EXCLUDE_PARTS = ("__pycache__", ".git", ".venv", "node_modules", ".pytest_cache",
                 ".mypy_cache", ".ruff_cache")
EXCLUDE_SUFFIXES = (".key", ".pem", ".env")


class SyncError(RuntimeError):
    pass


def _rev(repo: Path) -> str:
    """The commit sha, honestly, or "" when this is not a git repo.

    It used to return "dev" for a dirty tree, because the rev was an ADDRESS and a dirty
    tree must never publish under a commit sha — the sha would name code that is not what
    ran. That is no longer a risk: content is addressed by its own hash, so the address is
    always exact regardless of what git thinks.

    Which frees the rev to be what it should have been all along: a human breadcrumb. The
    real sha plus a separate dirty flag says strictly more than "dev" did — you learn which
    commit it was NEAR, which is the useful part when a dirty tree is what ran.
    """
    try:
        out = subprocess.run(["git", "-C", str(repo), "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() if out.returncode == 0 else ""
    except Exception:
        return ""


def _dirty(repo: Path) -> bool:
    """Whether the working tree had uncommitted changes when this was published.

    Recorded because a git sha alone cannot tell you — and a job that ran dirty code is
    exactly the one where "which commit was this?" gives a misleading answer.
    """
    try:
        out = subprocess.run(["git", "-C", str(repo), "status", "--porcelain"],
                             capture_output=True, text=True, timeout=10)
        return bool(out.stdout.strip())
    except Exception:
        return False


def _files(code_dir: Path) -> list[Path]:
    out = []
    for p in sorted(code_dir.rglob("*")):
        if not p.is_file():
            continue
        if any(part in EXCLUDE_PARTS for part in p.parts):
            continue
        if p.suffix in EXCLUDE_SUFFIXES or p.name.startswith("."):
            continue
        if p.suffix not in INCLUDE_SUFFIXES:
            continue
        out.append(p)
    return out


def _tree_bytes(files: dict, execs: list) -> bytes:
    """The canonical serialisation of a tree. The sha is taken over exactly these bytes.

    Sorted keys and no whitespace, because two identical trees MUST hash identically or
    deduplication silently never fires.

    Note what is absent: published_at, git rev, who published it. Those describe a publish
    EVENT, not content. Inside the hashed bytes they would make every publish of identical
    code produce a different tree sha, which defeats the entire mechanism. They live in the
    job pointer, where varying is harmless.
    """
    return json.dumps({"files": files, "exec": sorted(execs)},
                      sort_keys=True, separators=(",", ":")).encode()


def publish(repo: str | Path, *, workload: str | None = None, job_id: str,
            store=None, dry_run: bool = False) -> dict:
    """Publish the tree this job will run. Returns what it pinned.

        _trees/<sha>.json     manifest: path -> file sha, plus the exec list
        _packs/<sha>.tgz      every file, in ONE object
        jobs/<job_id>         which tree this job ran, plus publish metadata

    ## Why one packed object rather than one object per file

    Measured against Cloudflare R2 on this tree — 54 files, 572 KB:

        54 loose objects, 32-way    572 KB   1.43 s
        1 packed object             142 KB   0.32 s

    Four and a half times faster and four times smaller, and the gap grows linearly:
    loose costs O(N/concurrency) round trips, a pack costs one. Extrapolated to 50,000
    files the loose path takes ~22 minutes, which a pod pays for at its hourly rate.

    That measurement also retired the per-file dedup layer this replaced. Its whole value
    was uploading only changed bytes — ~18 KB rather than 454 KB — but a gzipped pack of
    the ENTIRE tree is 142 KB, cheaper than the loose upload had ever been. Deduplicating
    across trees would have saved perhaps 14 MB over a hundred revisions. Not worth a layer,
    and layers are what you pay for forever.

    An unchanged tree still costs nothing: same content gives the same tree sha, the pack
    is already there, and only the ~200 byte job pointer is written.
    """
    repo = Path(repo).resolve()
    code_dir = repo / "code"
    if not code_dir.is_dir():
        raise SyncError(
            f"{repo} has no code/ directory.\n"
            f"  A workload repo keeps its importable packages under code/, so that "
            f"code/<pkg>/ becomes an import root when the volume is on sys.path.")

    workload = workload or repo.name
    base = f"code/{workload}"

    paths = _files(code_dir)
    if not paths:
        raise SyncError(f"{code_dir} contains no publishable files — refusing to publish "
                        f"an empty code root, which would fail on the pod as a confusing "
                        f"ModuleNotFoundError instead of here as a clear one")

    files: dict = {}
    execs: list = []
    total = 0
    for p in paths:
        rel = p.relative_to(code_dir).as_posix()
        data = p.read_bytes()
        # Raw bytes, never normalised. A CRLF/LF difference IS a difference — Python reads
        # different bytes — and normalising would mean the file rebuilt on the pod is not
        # the file that was published, which is the one guarantee this exists to provide.
        # It would also corrupt anything binary. Line-ending churn is a .gitattributes
        # problem, not a sync problem.
        files[rel] = hashlib.sha256(data).hexdigest()
        total += len(data)
        if os.access(p, os.X_OK):
            execs.append(rel)      # a bare path->sha map loses chmod +x, which breaks any
                                   # shell script published under code/

    tree_body = _tree_bytes(files, execs)
    tree = hashlib.sha256(tree_body).hexdigest()

    result = {"workload": workload, "job_id": job_id, "tree": tree,
              "job_key": f"{base}/jobs/{job_id}",
              "files": len(paths), "bytes": total, "dry_run": dry_run}
    if dry_run:
        return result

    if store is None:
        from .objectstore import get_storage
        store = get_storage()
    cfg = store.require()

    pack_key = f"{base}/_packs/{tree}.tgz"
    reused = True
    try:
        store.client.head_object(Bucket=cfg.bucket, Key=pack_key)
    except Exception:
        reused = False

    if not reused:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for p in paths:
                rel = p.relative_to(code_dir).as_posix()
                info = tarfile.TarInfo(rel)
                data = p.read_bytes()
                info.size = len(data)
                # Fixed mtime and ownership so the same tree packs to comparable bytes
                # regardless of when or where it was published. Not required for
                # correctness — the pack is named by the TREE hash, not its own — but it
                # keeps two packs of one tree from looking gratuitously different.
                info.mtime = 0
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mode = 0o755 if rel in execs else 0o644
                tf.addfile(info, io.BytesIO(data))
        store.client.put_object(Bucket=cfg.bucket, Key=pack_key, Body=buf.getvalue())
        # Manifest AFTER the pack: it is what a reader validates against, and one that
        # named content not yet uploaded would turn a partial publish into a checksum
        # failure rather than a clean absence.
        store.client.put_object(Bucket=cfg.bucket, Key=f"{base}/_trees/{tree}.json",
                                Body=tree_body)

    # Pointer last of all. Each level only becomes referenceable once everything it names
    # exists, so a reader can never resolve a pointer to a half-written tree.
    pointer = {"tree": tree, "workload": workload, "job_id": job_id,
               "git_rev": _rev(repo), "git_dirty": _dirty(repo),
               "published_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "files": len(paths), "bytes": total}
    store.client.put_object(Bucket=cfg.bucket, Key=f"{base}/jobs/{job_id}",
                            Body=json.dumps(pointer, indent=2).encode())

    result.update({"tree_reused": reused, "uploaded_pack": not reused})
    return result


def gc(workload: str, *, store=None, dry_run: bool = True) -> dict:
    """Report — and optionally delete — content no job points at any more.

    Reachability, never age. Age-based deletion can sever a chain something still depends
    on: the point of this store is answering "what exactly did job X run", and a rule that
    deletes by date destroys that answer for old jobs specifically, which are the ones
    nobody is watching.

    Dry run by default. Code is text and deduplicated — a thousand distinct trees of a
    large codebase is single-digit GB, which is pennies a month. Deleting wrongly costs
    something that cannot be bought back. The default should reflect that asymmetry.
    """
    if store is None:
        from .objectstore import get_storage
        store = get_storage()
    cfg = store.require()
    base = f"code/{workload}"
    pag = store.client.get_paginator("list_objects_v2")

    def _keys(prefix):
        for page in pag.paginate(Bucket=cfg.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                yield o["Key"], o["Size"]

    live_trees, live_jobs = set(), 0
    for key, _ in _keys(f"{base}/jobs/"):
        live_jobs += 1
        try:
            live_trees.add(json.loads(store.client.get_object(
                Bucket=cfg.bucket, Key=key)["Body"].read())["tree"])
        except Exception as e:
            # ABORT. Treating an unreadable pointer as referencing nothing would mark live
            # content as garbage and delete it while reporting success.
            raise SyncError(f"gc aborted: job pointer {key} is unreadable ({e}). "
                            f"Refusing to compute reachability from incomplete "
                            f"information — that is how a cleanup deletes live code.")

    def _sha_of(key):
        return key.rsplit("/", 1)[-1].split(".")[0]

    orphans = [(k, sz) for k, sz in _keys(f"{base}/_trees/")
               if _sha_of(k) not in live_trees]
    orphans += [(k, sz) for k, sz in _keys(f"{base}/_packs/")
                if _sha_of(k) not in live_trees]

    out = {"workload": workload, "dry_run": dry_run,
           "live_trees": len(live_trees), "live_jobs": live_jobs,
           "orphans": len(orphans),
           "reclaimable_bytes": sum(sz for _, sz in orphans)}
    if not dry_run:
        for k, _ in orphans:
            store.client.delete_object(Bucket=cfg.bucket, Key=k)
        out["deleted"] = len(orphans)
    return out


def resolve(workload: str, rev: str = "latest", store=None) -> str:
    """Turn (workload, rev) into a code root, following `latest` when asked."""
    if rev not in ("latest", ""):
        return f"code/{workload}/{rev}"
    if store is None:
        from .objectstore import get_storage
        store = get_storage()
    cfg = store.require()
    try:
        sha = store.client.get_object(
            Bucket=cfg.bucket, Key=f"code/{workload}/latest")["Body"].read().decode().strip()
    except Exception as e:
        raise SyncError(
            f"no published code for workload {workload!r} ({type(e).__name__}).\n"
            f"  Publish it first: python -m pod_loader.sync <repo>") from e
    return f"code/{workload}/{sha}"


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(prog="python -m pod_loader.sync",
                                 description="publish a workload's code/ to object storage")
    ap.add_argument("repo", nargs="?", default=".")
    ap.add_argument("--workload", default=None, help="defaults to the repo directory name")
    ap.add_argument("--rev", default=None, help="defaults to the git sha, or dev if dirty")
    ap.add_argument("--profile", default=None, help="object store profile")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    store = None
    if a.profile:
        from .objectstore import get_storage
        store = get_storage(a.profile)
    try:
        r = publish(a.repo, workload=a.workload, rev=a.rev, store=store,
                    dry_run=a.dry_run)
    except SyncError as e:
        print(f"error: {e}")
        return 1
    print(json.dumps(r, indent=2))
    if r["mutable"] and not a.dry_run:
        print("\nnote: published to a MUTABLE dev/ path because the tree is dirty or is "
              "not a git repo.\n      Commit to publish under an immutable revision.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
