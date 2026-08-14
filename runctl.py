#!/usr/bin/env python3
"""runctl — one control surface for runners, wherever they run.

    python runctl.py create   --provider local|runpod [--host user@ip] [--id NAME]
    python runctl.py status   [--id NAME]           # or omit --id to list all
    python runctl.py mount    --id NAME --kind local|s3
    python runctl.py push     --id NAME --source SOURCE_ID [--dry-run]
    python runctl.py submit   --id NAME --spec jobs/smoke.json
    python runctl.py poll     --id NAME
    python runctl.py fetch    --id NAME [--dest out/]
    python runctl.py shutdown --id NAME
    python runctl.py destroy  --id NAME

The provider behind --provider is the only thing that changes between a laptop and a pod.
Every command above means the same thing on both, which is what makes the local run a
rehearsal rather than a different program that happens to look similar.

`--provider runpod` never invents a pod: provision one in the console, then pass its ssh
address to `create`. Nothing here spends money without an address you supplied.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import warnings
from pathlib import Path

# boto3 warns that it will drop Python 3.9 in 2026. It is about the HOST interpreter, not
# about this pipeline or the images it runs, and it prints on every single S3 call — which
# buries the output that matters. Silence it here rather than in library code.
warnings.filterwarnings("ignore", message=r".*Python 3\.9.*")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="boto3.*")

# src layout: the package lives in src/, so a checkout that has not been pip-installed
# still runs this CLI. `from pod_loader.…` used to appear below — the package name from the
# pre-split monolith — which meant every command died with a bare ModuleNotFoundError
# naming a package that has not existed for some time.
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

try:
    from pod_loader.provider import (BaseProvider, JobSpec, LocalProvider,
                                     RunPodProvider, get_provider)
except ModuleNotFoundError as exc:
    # An import error in a CLI should say what to do. "No module named 'runners'" told
    # nobody anything, least of all which repo or which install step was missing.
    sys.exit(
        f"runctl: cannot import the loader package ({exc.name}).\n"
        f"\n"
        f"  Expected to find it at: {Path(__file__).resolve().parent / 'src' / 'pod_loader'}\n"
        f"  Present: {'yes' if (Path(__file__).resolve().parent / 'src' / 'pod_loader').is_dir() else 'NO'}\n"
        f"\n"
        f"  Run runctl from a checkout of pod-loader-rpc, or install it:\n"
        f"      pip install -e .\n"
        f"\n"
        f"  If {exc.name!r} is a third-party package, install the dependencies:\n"
        f"      pip install boto3 requests")

MARK = {"ready": "✓", "done": "✓", "running": "🔄", "creating": "…",
        "failed": "✗", "stopped": "■", "absent": "?"}


def _resolve(runner_id: str | None, provider: str | None, **kw):
    """Find an existing runner by id, or construct one of the named kind."""
    if runner_id:
        for rec in BaseProvider.list_runners():
            if rec["runner_id"] == runner_id:
                return get_provider(rec["provider"], runner_id, **kw)
        if not provider:
            raise SystemExit(f"no runner {runner_id!r}. `runctl.py status` lists them.")
    return get_provider(provider or "local", runner_id, **kw)


def _show(st) -> None:
    d = st.as_dict()
    print(f"  {MARK.get(d['state'], '?')} {d['runner_id']}  [{d['provider']}]  "
          f"{d['state']}")
    if d.get("job_id"):
        print(f"      job      {d['job_id']}")
    if d.get("progress"):
        p = d["progress"]
        done = len(p.get("completed") or [])
        print(f"      progress {done}/{p.get('total', '?')} stages")
    if d.get("message"):
        for line in str(d["message"]).strip().splitlines()[-4:]:
            print(f"      {line}")


def cmd_create(a) -> int:
    kw = {}
    if a.provider == "runpod":
        kw = {"ssh_host": a.host, "ssh_port": a.port, "ssh_key": a.key}
        if a.image:
            kw["image"] = a.image
    p = _resolve(a.id, a.provider, **kw)
    print(f"\ncreating {a.provider} runner…  (building the image can take minutes)")
    st = p.create(ssh_host=a.host) if a.provider == "runpod" else p.create()
    _show(st)
    if st.state == "ready":
        print(f"\n  next: python runctl.py mount --id {st.runner_id} "
              f"--kind {'s3' if a.provider == 'runpod' else 'local'}")
    return 0 if st.state == "ready" else 1


def cmd_status(a) -> int:
    if a.id:
        _show(_resolve(a.id, None).status())
        return 0
    recs = BaseProvider.list_runners()
    if not recs:
        print("  no runners. `runctl.py create --provider local` makes one.")
        return 0
    print(f"\n  {len(recs)} runner(s):\n")
    for rec in recs:
        print(f"  {MARK.get(rec.get('state'), '?')} {rec['runner_id']:<22} "
              f"{rec['provider']:<8} {rec.get('state'):<9} "
              f"{rec.get('job_id') or '-':<18} {rec.get('updated', '')}")
    return 0


def cmd_mount(a) -> int:
    p = _resolve(a.id, None)
    r = p.mount({"kind": a.kind, "root": a.root})
    print(f"\n  {'✓' if r.get('ok') else '✗'} mount {a.kind}")
    print(json.dumps(r, indent=2)[:900])
    return 0 if r.get("ok") else 1


def _code_store(a):
    """The store a launch would actually use — not get_storage()'s default.

    get_storage() with no profile resolves to whatever key file is found first, which here
    is RunPod S3, while launches pass PODH_S3_PROFILE=cloudflare. Publishing code with the
    default put it in a bucket no pod would ever read, and push+pull both defaulted the
    same way so the round trip passed while proving nothing.

    So the profile comes from the launch file — one definition of "which store", shared by
    the thing that publishes and the thing that runs.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from pod_loader.objectstore import get_storage
    profile = getattr(a, "store", None)
    if not profile:
        try:
            from pod_loader import launchfile
            profile = launchfile.load(getattr(a, "env_file", None)).store or None
        except Exception:
            profile = None
    st = get_storage(profile)
    cfg = st.require()
    print(f"  store    : {profile or '(default)'} → {cfg.bucket}")
    return st


def cmd_promote(a) -> int:
    """Promote a run's profile to assets/profiles/<region>/<run_id>/, and point `current`.

    Promotion happens automatically when the profile stage succeeds, but it should not ONLY
    happen there. "The profile we stand behind" is a judgement, not a side effect of the
    most recent run finishing:

      - a later run can be worse; you may want to keep pointing at an earlier one
      - automatic promotion is deliberately non-fatal, so it can fail while the run succeeds
      - a run that completed before promotion existed still has a perfectly good profile

    Copies server-side — the objects never travel to this machine and back.

    `current` is written LAST, so it can never name a directory that is not complete.
    """
    st = _code_store(a)
    cfg = st.require()
    region = a.region
    base = f"assets/profiles/{region}"

    if a.show:
        cur = ""
        try:
            cur = st.client.get_object(
                Bucket=cfg.bucket, Key=f"{base}/current")["Body"].read().decode().strip()
        except Exception:
            pass
        runs = set()
        for page in st.client.get_paginator("list_objects_v2").paginate(
                Bucket=cfg.bucket, Prefix=base + "/"):
            for o in page.get("Contents", []):
                rest = o["Key"][len(base) + 1:]
                if "/" in rest:
                    runs.add(rest.split("/", 1)[0])
        print(f"\n  region  : {region}")
        print(f"  current : {cur or '(none)'}")
        for r in sorted(runs):
            print(f"    {'→' if r == cur else ' '} {r}")
        if not runs:
            print("    (nothing promoted yet)")
        return 0

    src = f"runs/{a.job}/out/regions/{region}"
    found = []
    for page in st.client.get_paginator("list_objects_v2").paginate(
            Bucket=cfg.bucket, Prefix=src + "/"):
        for o in page.get("Contents", []):
            found.append(o["Key"])
    if not found:
        print(f"\n  ✗ nothing at {src}/")
        print(f"    That run produced no region artifacts, or the region name is wrong.")
        return 1

    keep = [k for k in found
            if not a.only or pathlib.Path(k).name in set(a.only.split(","))]
    print(f"\n  promoting {len(keep)} artifact(s) from {src}/")
    for k in keep:
        dst = f"{base}/{a.job}/{pathlib.Path(k).name}"
        st.client.copy_object(Bucket=cfg.bucket, Key=dst,
                              CopySource={"Bucket": cfg.bucket, "Key": k})
        print(f"    {pathlib.Path(k).name} → {dst}")

    # Last, and only after every artifact is in place.
    st.client.put_object(Bucket=cfg.bucket, Key=f"{base}/current", Body=a.job.encode())
    print(f"  current → {a.job}")
    return 0


def cmd_code_push(a) -> int:
    """Publish a workload's code/ tree without launching anything.

    Exists so the round trip can be exercised on its own. Testing publish only through a
    real launch means every test costs a pod and a wait, which in practice means it is not
    tested — and this is the machinery every job's code arrives through.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from pod_loader import sync
    from pod_loader.ids import job_id as mint

    job = a.job_id or mint()
    r = sync.publish(a.repo, workload=a.workload, job_id=job,
                     store=None if a.dry_run else _code_store(a),
                     dry_run=a.dry_run)
    print(f"\n  workload : {r['workload']}")
    print(f"  job      : {r['job_id']}")
    print(f"  tree     : {r['tree'][:16]}…")
    print(f"  files    : {r['files']}  ({r['bytes']/1e3:.1f} KB)")
    if not a.dry_run:
        print("  pack     : " + ("reused — this exact tree was already published"
                                  if r["tree_reused"] else "uploaded (new tree)"))
        print(f"  pointer  : {r['job_key']}")
    else:
        print("  (dry run — nothing written)")
    return 0


def cmd_code_pull(a) -> int:
    """Rebuild a job's exact code tree locally. The other half of the round trip.

    Uses the same pod_harness.codestore the POD uses, so a green run here means the pod
    path works — rather than testing a laptop reimplementation of it.
    """
    here = Path(__file__).resolve().parent
    sys.path.insert(0, str(here / "src"))
    sys.path.insert(0, str(here.parent / "pod-harness" / "src"))
    from pod_harness import codestore

    key = a.job if "/" in a.job else f"code/{a.workload}/jobs/{a.job}"
    dest = Path(a.dest or f"./_code/{a.job.rsplit('/', 1)[-1]}")
    r = codestore.fetch(key, dest, store=_code_store(a))
    print(f"\n  tree     : {r['tree'][:16]}…")
    print(f"  git      : {r['git_rev']}" + (" (dirty)" if r["git_dirty"] else ""))
    print(f"  files    : {r['files']}  ({r['bytes']/1e3:.1f} KB), {r['verified']} verified")
    print(f"  dest     : {r['dest']}")
    return 0


def cmd_code_gc(a) -> int:
    """Report content no job points at. Deletes only with --delete, never by age."""
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from pod_loader import sync
    r = sync.gc(a.workload, store=_code_store(a), dry_run=not a.delete)
    print(f"\n  workload      : {r['workload']}")
    print(f"  live          : {r['live_jobs']} job(s) → {r['live_trees']} tree(s)")
    print(f"  orphaned      : {r['orphans']} object(s) no job references")
    print(f"  reclaimable   : {r['reclaimable_bytes']/1e3:.1f} KB")
    if r["dry_run"]:
        print("  (dry run — pass --delete to remove them)")
    else:
        print(f"  deleted       : {r['deleted']} object(s)")
    return 0


def cmd_push(a) -> int:
    """Get the corpus where the runner can see it.

    Kept separate from submit because upload is slow and idempotent while a job is fast
    and re-run often — folding them together re-checks gigabytes on every code fix.
    """
    p = _resolve(a.id, None)
    r = p.push(a.source, dry_run=a.dry_run, limit=a.limit, as_source=getattr(a, "as"))
    print(f"\n  {'✓' if r.get('ok') else '✗'} push {a.source}")
    print(json.dumps(r, indent=2)[:900])
    return 0 if r.get("ok") else 1


def cmd_ls(a) -> int:
    """List what is actually in the object store.

    A local runner writes to the bind mount, not S3, so an empty listing after a local run
    is correct rather than a failure — the data never went there. Push it, or run on a
    provider whose mount is S3.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pipeline.core.storage import Storage

    st = Storage()
    if not st.available:
        print("  ✗ no S3 credentials (runpods3.key)")
        return 1
    cfg = st.require()
    pag = st.client.get_paginator("list_objects_v2")
    rows, total = [], 0
    for page in pag.paginate(Bucket=cfg.bucket, Prefix=a.prefix or ""):
        for o in page.get("Contents", []):
            rows.append((o["Key"], o["Size"], o["LastModified"]))
            total += o["Size"]
            if a.limit and len(rows) >= a.limit:
                break
        if a.limit and len(rows) >= a.limit:
            break
    print(f"\n  s3://{cfg.bucket}/{a.prefix or ''}   {len(rows)} object(s)  "
          f"{total/1e6:.2f} MB\n")
    if not rows:
        print("  (empty — a local run writes to ./out via bind mount, never to S3.\n"
              "   Use `push` to upload corpus, or run on a provider mounted to S3.)")
    for k, sz, lm in rows:
        print(f"  {sz:>12,}  {lm:%Y-%m-%d %H:%M}  {k}")
    return 0


def cmd_cat(a) -> int:
    """Print one object's contents without downloading it to a file.

    For inspecting a profile or a run report in the bucket — the common case is "did the
    thing I expect actually land there, and does it say what I think".
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pipeline.core.storage import Storage

    st = Storage()
    if not st.available:
        print("  ✗ no S3 credentials (runpods3.key)")
        return 1
    cfg = st.require()
    try:
        body = st.client.get_object(Bucket=cfg.bucket, Key=a.key)["Body"].read()
    except Exception as exc:
        print(f"  ✗ {a.key} — {str(exc)[:120]}")
        # Show what IS in that folder. The old hint printed a prefix WITHOUT a trailing
        # slash, which matched sibling directories too (`_neutro` also matches
        # `_neutro_s3/`), so a missing file looked present under a neighbouring region.
        # If a job is still running, THAT is the reason the file is absent — say so
        # rather than making the reader correlate two commands.
        try:
            from pod_loader.batch_pod import progress as _prog
            for jid in ("neutro_full",):
                pr = _prog(jid)
                pending = [n for n in pr["order"]
                           if pr["stages"].get(n, {}).get("state") in
                           ("pending", "running")]
                if pending and not pr["done"]:
                    running = [n for n in pr["order"]
                               if pr["stages"].get(n, {}).get("state") == "running"]
                    print(f"\n    job {jid} is still running "
                          f"({'in ' + running[0] if running else 'queued'}); "
                          f"stages left: {', '.join(pending)}")
                    print("    profile.json is written by the `profile` stage, which runs "
                          "last.")
        except Exception:
            pass

        folder = a.key.rsplit("/", 1)[0] + "/" if "/" in a.key else ""
        try:
            r = st.client.list_objects_v2(Bucket=cfg.bucket, Prefix=folder,
                                          Delimiter="/", MaxKeys=50)
            names = [o["Key"][len(folder):] for o in r.get("Contents", [])]
            subs = [p["Prefix"][len(folder):] for p in r.get("CommonPrefixes", [])]
            print(f"\n    {folder or '(bucket root)'} actually contains:")
            for s in subs:
                print(f"      {s}")
            for n in names:
                print(f"      {n}")
            if not names and not subs:
                print("      (nothing — this folder does not exist yet)")
        except Exception:
            pass
        return 1
    print(f"\n  s3://{cfg.bucket}/{a.key}   {len(body):,} bytes\n")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        print(f"  (binary — first 64 bytes: {body[:64]!r})")
        return 0
    if a.key.endswith(".json") and not a.raw:
        try:
            text = json.dumps(json.loads(text), indent=2)
        except Exception:
            pass
    lines = text.splitlines()
    for line in lines[: a.lines]:
        print("  " + line)
    if len(lines) > a.lines:
        print(f"  … {len(lines) - a.lines} more lines (--lines N for more)")
    return 0


def cmd_browse(a) -> int:
    """Browse the bucket in a web browser. Listings are live; nothing downloads until
    you click a file.

    A FUSE mount is not available: rclone cannot authenticate against RunPod's S3 gateway
    (SignatureDoesNotMatch on credentials boto3 accepts), and macFUSE would need a kernel
    extension and a reboot. This gives the same click-through experience over the client
    that works, without copying anything.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pod_loader.browse import serve

    serve(port=a.port, open_browser=not a.no_open)
    return 0


def cmd_watch(a) -> int:
    """Status of a remote batch job: pod state, cost so far, and the live log.

    The log is read off the network volume rather than from the pod, so it works even when
    the RunPod console lags and it keeps working after the pod is gone.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pod_loader.batch_pod import progress, render, tail
    from pod_loader.runpod_api import RunPodAPI

    prog = progress(a.job)
    api = RunPodAPI()
    pods = api.pods() if api.available else []

    state = "FINISHED" if prog["done"] else ("RUNNING" if pods else "NOT RUNNING")
    print(f"\n  job {a.job}   {state}")
    for p in pods:
        up = RunPodAPI.uptime_seconds(p)
        cost = p.get("costPerHr") or 0
        if up:
            print(f"    pod {p['id']}  ${cost}/hr  up {up/60:.0f} min  "
                  f"spent≈${cost * up / 3600:.2f}")
        else:
            print(f"    pod {p['id']}  ${cost}/hr  (start time unavailable)")
    if not pods and not prog["done"]:
        print("    ⚠ no pod running and no DONE marker — the job stopped early")

    print()
    print(render(prog))

    # A stage that prints nothing cannot be observed directly, so derive what we can:
    # time already spent in it = pod uptime minus every completed stage. Without this a
    # silent stage is indistinguishable from a hang, and the only recourse was arithmetic
    # done by hand.
    up = next((RunPodAPI.uptime_seconds(p) for p in pods), None)
    silent = [n for n in prog["order"]
              if prog["stages"].get(n, {}).get("state") == "running"
              and "percent" not in prog["stages"].get(n, {})]
    if up and silent:
        accounted = sum(s.get("seconds", 0) for s in prog["stages"].values()
                        if s.get("state") == "done")
        in_stage = max(0.0, up - accounted)
        # Expected duration for the one stage that reports nothing. Derived from measured
        # local throughput, stated as a RANGE and labelled an estimate — a fabricated
        # percentage would be worse than silence.
        lo, hi = 20.0, 45.0
        mins = in_stage / 60
        pct = f"{min(99, 100 * mins / hi):.0f}–{min(99, 100 * mins / lo):.0f}%"
        print(f"\n  {silent[0]}: ~{mins:.0f} min elapsed of an expected {lo:.0f}–{hi:.0f} "
              f"min  →  roughly {pct} done (ESTIMATE, not measured)")
        print(f"    derived from {up/60:.0f} min uptime − {accounted/60:.0f} min of "
              f"finished stages")
        print(f"    alive={bool(pods)}  DONE={prog['done']}  →  process is "
              f"{'executing' if pods and not prog['done'] else 'not running'}")
        if mins > hi:
            print("    ⚠ past the expected window — if this persists, kill it")

    if a.log:
        t = tail(a.job, lines=a.lines)
        print(f"\n  --- log (last {a.lines} lines, {t.get('bytes', 0)} bytes) ---")
        print(t.get("log") or "    (nothing written yet)")

    if prog["done"]:
        print(f"\n  results: python runctl.py ls --prefix out/regions/")
        print(f"  bring them home: python runctl.py fetch --id <runner> --dest ./out")
        print(f"  ⚠ terminate the pod if still up: python runctl.py kill")
    return 0


def cmd_launch(a) -> int:
    """Publish code, stage the spec, and start the job on the configured TARGET.

    Almost nothing happens here: BaseLoader.launch() runs the sequence that is the same
    for every destination, and the target subclass supplies only preflight/start/stop.
    This function used to hardcode the RunPod path, which meant TARGET was read from the
    launch file and then ignored.
    """
    import json

    from pod_loader import base_loader, launchfile

    cfg = launchfile.load(a.env_file)
    print(f"  launch config: {cfg.source}   target={cfg.target}")
    problems = launchfile.check(cfg)
    if problems:
        print("\n  ✗ launch config is not usable:")
        for pr in problems:
            print(f"      {pr}")
        return 2
    launchfile.apply_to_environ(cfg)

    spec_path = a.spec or cfg.job_spec
    if not spec_path:
        print("  ✗ no job spec: pass --spec, or set JOB_SPEC in the launch file")
        return 2
    spec = json.loads(Path(spec_path).read_text())

    try:
        loader = base_loader.get_loader(cfg.target)
    except ValueError as e:
        print(f"\n  ✗ {e}")
        return 2

    if a.no_sync:
        cfg.workload = ""

    try:
        run = loader.launch(cfg, spec=spec, job_id=a.job_id or "", dry_run=a.dry_run)
    except base_loader.LoaderError as e:
        print(f"\n  ✗ {e}")
        return 2
    if run is None:
        return 0

    print(f"\n  ✓ {run.target}: {run.handle}  job={run.job_id}"
          + (f"  ${run.cost_hr}/hr" if run.cost_hr else "  (free)"))
    for k, v in run.detail.items():
        if v:
            print(f"      {k}: {v}")
    if run.endpoint:
        print(f"  watch:  python runctl.py watch --job {run.job_id}")
        print(f"  api:    {run.endpoint}/v1/jobs/{run.job_id}/summary")
    return 0


def cmd_kill(a) -> int:
    """Terminate pods. Stopping is not enough — a stopped pod still bills for disk."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from pod_loader.batch_pod import teardown

    r = teardown(a.pod)
    print(f"\n  terminated: {r['terminated'] or '(none were running)'}")
    print(f"  remaining : {r['remaining']}")
    return 0


def cmd_submit(a) -> int:
    p = _resolve(a.id, None)
    job = JobSpec.load(a.spec)
    problems = job.validate()
    if problems:
        print("\n  ✗ instruction file is invalid:")
        for x in problems:
            print(f"      {x}")
        return 2
    print(f"\n  submitting {job.job_id}: {' -> '.join(job.stages)}")
    st = p.submit(job)
    _show(st)
    if st.state == "done" and st.detail.get("tail"):
        print("\n" + st.detail["tail"])
    return 0 if st.state in ("done", "running") else 1


def cmd_poll(a) -> int:
    _show(_resolve(a.id, None).poll())
    return 0


def cmd_fetch(a) -> int:
    r = _resolve(a.id, None).fetch(Path(a.dest) if a.dest else None)
    print(f"\n  {'✓' if r.get('ok') else '✗'} {json.dumps(r, indent=2)[:600]}")
    return 0 if r.get("ok") else 1


def cmd_shutdown(a) -> int:
    _show(_resolve(a.id, None).shutdown())
    return 0


def cmd_destroy(a) -> int:
    _show(_resolve(a.id, None).destroy())
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Control pipeline runners on any provider",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("create", help="bring a runner into existence")
    c.add_argument("--provider", choices=["local", "runpod"], default="local")
    c.add_argument("--id"), c.add_argument("--host"), c.add_argument("--key")
    c.add_argument("--port", type=int, default=22), c.add_argument("--image")
    c.set_defaults(fn=cmd_create)

    s = sub.add_parser("status", help="one runner, or all of them")
    s.add_argument("--id"); s.set_defaults(fn=cmd_status)

    m = sub.add_parser("mount", help="attach storage")
    m.add_argument("--id", required=True)
    m.add_argument("--kind", choices=["local", "s3"], default="local")
    m.add_argument("--root", default="corpus/"); m.set_defaults(fn=cmd_mount)

    h = sub.add_parser("push", help="upload corpus to where the runner can reach it")
    h.add_argument("--id", required=True), h.add_argument("--source", required=True)
    h.add_argument("--dry-run", action="store_true")
    h.add_argument("--limit", type=int, help="upload only the first N files")
    h.add_argument("--as", dest="as", help="upload under a different source id")
    h.set_defaults(fn=cmd_push)



    pr = sub.add_parser("promote", help="promote a run's profile and move `current`")
    pr.add_argument("job", nargs="?", default="", help="job id whose profile to promote")
    pr.add_argument("--region", default="_neutro_v4")
    pr.add_argument("--only", help="comma-separated filenames, e.g. profile.json")
    pr.add_argument("--show", action="store_true", help="list what is promoted")
    pr.add_argument("--store"), pr.add_argument("--env-file")
    pr.set_defaults(fn=cmd_promote)

    cp = sub.add_parser("code-push", help="publish a workload's code/ tree (no pod)")
    cp.add_argument("repo"), cp.add_argument("--workload")
    cp.add_argument("--job-id"), cp.add_argument("--dry-run", action="store_true")
    cp.add_argument("--store", help="store profile; defaults to the launch file's")
    cp.add_argument("--env-file", help="launch file to read the store from")
    cp.set_defaults(fn=cmd_code_push)

    cl = sub.add_parser("code-pull", help="rebuild a job's exact code tree (no pod)")
    cl.add_argument("job", help="job id, or a full code/<workload>/jobs/<id> key")
    cl.add_argument("--workload", default="lingua-trainer"), cl.add_argument("--dest")
    cl.add_argument("--store", help="store profile; defaults to the launch file's")
    cl.add_argument("--env-file", help="launch file to read the store from")
    cl.set_defaults(fn=cmd_code_pull)

    cg = sub.add_parser("code-gc", help="report code no job references")
    cg.add_argument("--workload", default="lingua-trainer")
    cg.add_argument("--delete", action="store_true", help="actually remove them")
    cg.add_argument("--store", help="store profile; defaults to the launch file's")
    cg.add_argument("--env-file", help="launch file to read the store from")
    cg.set_defaults(fn=cmd_code_gc)

    l = sub.add_parser("ls", help="list what is actually in the object store")
    l.add_argument("--prefix", default=""), l.add_argument("--limit", type=int, default=40)
    l.set_defaults(fn=cmd_ls)

    t = sub.add_parser("cat", help="print one object's contents from the bucket")
    t.add_argument("--key", required=True), t.add_argument("--lines", type=int, default=60)
    t.add_argument("--raw", action="store_true", help="do not pretty-print JSON")
    t.set_defaults(fn=cmd_cat)


    b = sub.add_parser("browse", help="browse the bucket in a web browser (live, no copy)")
    b.add_argument("--port", type=int, default=8765)
    b.add_argument("--no-open", action="store_true")
    b.set_defaults(fn=cmd_browse)

    w = sub.add_parser("watch", help="status + live log of a remote batch job")
    w.add_argument("--job", required=True), w.add_argument("--lines", type=int, default=20)
    w.add_argument("--log", action="store_true", help="also print the raw log tail")
    w.set_defaults(fn=cmd_watch)

    la = sub.add_parser("launch",
                        help="publish code, stage the spec, provision a pod, run the job")
    la.add_argument("--spec", help="job spec; defaults to JOB_SPEC in .pod.env")
    la.add_argument("--env-file", help="launch file; defaults to .pod.env found upward")
    la.add_argument("--job-id", help="override the generated job id")
    la.add_argument("--no-sync", action="store_true", help="skip publishing the workload")
    la.add_argument("--dry-run", action="store_true",
                    help="validate everything and provision nothing")
    la.set_defaults(fn=cmd_launch)

    k = sub.add_parser("kill", help="terminate pods (stopping alone still bills)")
    k.add_argument("--pod", help="specific pod id; default terminates all")
    k.set_defaults(fn=cmd_kill)

    u = sub.add_parser("submit", help="send an instruction file")
    u.add_argument("--id", required=True), u.add_argument("--spec", required=True)
    u.set_defaults(fn=cmd_submit)

    p = sub.add_parser("poll", help="progress while it works")
    p.add_argument("--id", required=True); p.set_defaults(fn=cmd_poll)

    f = sub.add_parser("fetch", help="retrieve results")
    f.add_argument("--id", required=True), f.add_argument("--dest")
    f.set_defaults(fn=cmd_fetch)

    d = sub.add_parser("shutdown", help="stop it")
    d.add_argument("--id", required=True); d.set_defaults(fn=cmd_shutdown)

    x = sub.add_parser("destroy", help="stop it and remove the record")
    x.add_argument("--id", required=True); x.set_defaults(fn=cmd_destroy)

    a = ap.parse_args()
    return a.fn(a)


if __name__ == "__main__":
    raise SystemExit(main())
