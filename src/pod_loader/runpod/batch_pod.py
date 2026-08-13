"""Batch pods — provision, run one instruction file, exit.

## How the pod is told what to do

Through the network volume, which is the same storage as the S3 bucket:

    laptop                     volume / bucket                pod
    ------                     ---------------                ---
    runctl launch  ──put──▶    jobs/<id>.json      ──▶  /workspace/jobs/<id>.json
                               corpus/raw/...      ──▶  /workspace/corpus/raw/...
                   ◀──get──    logs/<id>.log       ◀──  /workspace/logs/<id>.log
                   ◀──get──    out/regions/...     ◀──  /workspace/out/regions/...

So changing an instruction is a 2 KB upload, not a 2 GB image rebuild. The image holds
CODE; the volume holds DATA and INSTRUCTIONS. Nothing job-specific is ever baked in.

## Why there is no web API

A server would need a port, auth, and something to babysit it — for a process that runs once
and exits. The volume already gives a bidirectional channel that survives the pod dying, and
a spot instance can vanish mid-run without losing a byte. An API earns its place for
on-demand work (RunPod Serverless), not for batch.

## Why the pod must not exit instantly

A pod is only "up" while its process runs. An image whose CMD returns immediately leaves a
pod that bills, reports no runtime and opens no port — it looks broken in the console. The
start command here does the work in the foreground, so the pod lives exactly as long as the
job and stops billing when it finishes.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
STATE_DIR = REPO / "out" / "_reports" / "runners"

# GPU preference order — RunPod takes the first with capacity, so a sold-out model
# degrades to the next instead of failing the launch.
GPUS = ["NVIDIA GeForce RTX 4090", "NVIDIA RTX A5000",
        "NVIDIA RTX A4500", "NVIDIA RTX A4000"]

# The OFFICIAL MFA image, used as-is. RunPod cannot build images — dockerd fails without
# NET_ADMIN and buildah fails without user namespaces, both verified on a pod — so rather
# than fight that, take an image that already carries the one hard-to-install dependency
# and add the rest at startup. MFA is conda-only; everything else is pip and installs
# cleanly on its Python 3.13 (verified: torch 2.8, speechbrain 1.0.3, librosa 0.11).
IMAGE = "mmcauliffe/montreal-forced-aligner:latest"

# Installed at pod start. ~2-3 min, against datacentre bandwidth. torch comes from the CPU
# index FIRST so speechbrain cannot pull the multi-gigabyte CUDA build behind it.
BOOTSTRAP = (
    "python3 -m pip install --no-cache-dir -q "
    "--index-url https://download.pytorch.org/whl/cpu torch torchaudio && "
    "python3 -m pip install --no-cache-dir -q numpy scipy librosa soundfile requests "
    "pypdf boto3 speechbrain scikit-learn pyarrow")


def _storage():
    import sys
    sys.path.insert(0, str(REPO))
    from ..storage import Storage
    return Storage()


@dataclass
class Launch:
    pod_id: str
    job_id: str
    cost_per_hr: float
    gpu: str = ""

    def as_dict(self) -> dict:
        return {"pod_id": self.pod_id, "job_id": self.job_id,
                "cost_per_hr": self.cost_per_hr, "gpu": self.gpu}


def upload_spec(spec_path: str | Path) -> dict:
    """Put the instruction file on the volume where the pod will read it."""
    p = Path(spec_path)
    spec = json.loads(p.read_text())
    job_id = spec.get("job_id") or p.stem
    st = _storage()
    cfg = st.require()
    key = f"jobs/{job_id}.json"
    st.client.put_object(Bucket=cfg.bucket, Key=key, Body=p.read_bytes())
    return {"ok": True, "job_id": job_id, "key": key, "bytes": p.stat().st_size,
            "region": spec.get("region"), "stages": spec.get("stages")}


def start_command(job_id: str) -> str:
    """What the pod runs. Foreground, logged to the volume, exits when done.

    `tee` rather than plain redirection so the log is readable over S3 WHILE the job runs
    instead of only after it finishes — a 12-minute job with no visible progress is
    indistinguishable from a hung one.

    PYTHONPATH puts /workspace/code AHEAD of the baked /app, so code synced to the volume
    overrides the image without rebuilding it. The image then holds only DEPENDENCIES,
    which change rarely; editing a stage is a few-KB upload instead of a 2 GB push.
    """
    return (
        "set -o pipefail; "
        "mkdir -p /workspace/logs /workspace/out; "
        f"echo 'installing deps…'; {BOOTSTRAP} >/dev/null 2>&1 || "
        f"echo 'BOOTSTRAP FAILED'; "
        # RunPod RESTARTS the container when its start command exits. Without this guard a
        # finished job runs again from the top, and the second pass overwrites the first
        # one's results — it destroyed a 132 KB speaker map and a full embedding archive,
        # leaving a checkpoint that claimed 2,507 files were done over an empty npz.
        # A completed job must be idempotent: see the marker, do nothing, idle.
        f"if [ -f /workspace/logs/{job_id}.DONE ]; then "
        f"  echo 'job {job_id} already completed — refusing to re-run and overwrite "
        f"results. Delete /workspace/logs/{job_id}.DONE to force.'; "
        f"  sleep infinity; "
        f"fi; "
        "export PYTHONPATH=/workspace/code:/app; "
        "cd /workspace/code 2>/dev/null || cd /app; "
        "echo \"running from $(pwd)\"; "
        f"python -u -m runners.execute_job --spec /workspace/jobs/{job_id}.json "
        f"2>&1 | tee /workspace/logs/{job_id}.log; "
        f"RC=${{PIPESTATUS[0]}}; "
        f"echo \"EXIT=$RC\" >> /workspace/logs/{job_id}.log; "
        # Only a SUCCESSFUL run may write .DONE. Writing it unconditionally meant a failed
        # run left a marker that the restart guard then honoured, so the next pod refused
        # to run the fixed code and reported the OLD failure — the guard blocking the fix
        # for the bug it was meant to survive.
        f"if [ \"$RC\" -eq 0 ]; then "
        f"  cp /workspace/logs/{job_id}.log /workspace/logs/{job_id}.DONE; "
        f"else "
        f"  cp /workspace/logs/{job_id}.log /workspace/logs/{job_id}.FAILED; "
        f"  echo 'run FAILED — no .DONE written, so a rerun is allowed'; "
        f"fi; "
        # Idle rather than exit, so the restart loop never begins. The pod still bills, so
        # `runctl kill` is what actually ends it — watch reports FINISHED as the cue.
        "echo 'job complete — pod idle; run: python runctl.py kill'; "
        "sleep infinity"
    )


def sync_code() -> dict:
    """Push the pipeline source to the volume so the pod runs THIS code, not the image's.

    Only .py and the small metadata files — no corpus, no keys, no caches. This is what
    makes an image rebuild unnecessary for ordinary changes: the image supplies torch and
    ffmpeg, the volume supplies our source.
    """
    st = _storage()
    cfg = st.require()
    sent, nbytes = 0, 0
    for sub in ("pipeline", "runners", "corpora", "jobs"):
        base = REPO / sub
        if not base.exists():
            continue
        for p in sorted(base.rglob("*")):
            if not p.is_file() or p.suffix not in (".py", ".json", ".md"):
                continue
            if "__pycache__" in str(p) or p.name.endswith(".key"):
                continue
            key = f"code/{p.relative_to(REPO).as_posix()}"
            st.client.put_object(Bucket=cfg.bucket, Key=key, Body=p.read_bytes())
            sent += 1
            nbytes += p.stat().st_size

    # The manifest is what makes the pod able to FETCH rather than receive: it carries
    # every source URL and, critically, the licence gate. Without it `acquire` cannot run
    # on the pod and the corpus has to be uploaded from the laptop instead.
    man = REPO.parent / "corpus_research.json"
    if man.exists():
        st.client.put_object(Bucket=cfg.bucket, Key="code/corpus_research.json",
                             Body=man.read_bytes())
        sent += 1
        nbytes += man.stat().st_size

    return {"ok": True, "files": sent, "kilobytes": round(nbytes / 1e3, 1),
            "prefix": "code/", "manifest": man.exists(),
            "note": "pod prepends /workspace/code to PYTHONPATH — no image rebuild needed"}


def launch(spec_path: str | Path, *, image: str = IMAGE, gpus: list[str] | None = None,
           volume_id: str | None = None, container_disk_gb: int = 30,
           compute: str = "CPU", vcpus: int = 16) -> Launch:
    """Upload the spec and provision a pod to run it. THIS STARTS BILLING.

    `volume_id` defaults to whatever RUNPOD_VOLUME resolves to. It used to default to a
    literal account-specific id, which meant generic launch code carried one person's
    RunPod configuration: nobody else could run it, and moving providers would have
    required editing a function signature. The id now comes from a key file that lives
    outside every repo.
    """
    from .api import RunPodAPI
    from . import volume as _volume

    if volume_id is None:
        vol = _volume.load()
        volume_id = vol.volume_id if vol else None

    up = upload_spec(spec_path)
    job_id = up["job_id"]

    # Delete this job's previous log and markers BEFORE launching. Leaving them meant a
    # poll could match the last run's output and report a failure the new pod had not
    # produced — I killed a working pod that way. Absence of a log now means "not started",
    # unambiguously.
    _st = _storage()
    _cfg = _st.require()
    for suffix in (".log", ".DONE", ".FAILED"):
        try:
            _st.client.delete_object(Bucket=_cfg.bucket, Key=f"logs/{job_id}{suffix}")
        except Exception:
            pass
    print(f"  spec uploaded -> s3://{_storage().require().bucket}/{up['key']}")
    print(f"  region={up['region']}  stages={' -> '.join(up['stages'] or [])}")

    api = RunPodAPI()
    auth_file = STATE_DIR / "_registry_auth.json"
    auth_id = (json.loads(auth_file.read_text())["id"]
               if auth_file.exists() else None)

    pod = api.create(
        name=f"lingua-{job_id}",
        image=image,
        gpu_type_ids=gpus or GPUS,
        compute_type=compute,
        vcpu_count=vcpus,
        network_volume_id=volume_id,
        registry_auth_id=auth_id,
        container_disk_gb=container_disk_gb,
        entrypoint=["/bin/bash", "-lc"],
        start_cmd=[start_command(job_id)],
        # The corpus and results live on the volume, not in the image. The manifest goes
        # with the code so `acquire` can fetch from source ON THE POD — datacentre
        # bandwidth instead of a home upstream, and the raw archives are retained on the
        # volume so a rerun never depends on a mirror still existing. The YODAS
        # post-mortem in data-provenance.md is the argument: 0 of 48 sampled videos
        # survived, and provenance recovery after the fact is impossible.
        env={"LINGUA_CORPUS_ROOT": "/workspace/corpus",
             "PODH_OUT_ROOT": "/workspace/out",
             "LINGUA_MANIFEST": "/workspace/code/corpus_research.json"},
    )
    L = Launch(pod_id=pod["id"], job_id=job_id,
               cost_per_hr=float(pod.get("costPerHr") or 0),
               gpu=(pod.get("machine") or {}).get("gpuTypeId", "") if
               isinstance(pod.get("machine"), dict) else "")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    (STATE_DIR / f"_launch_{job_id}.json").write_text(json.dumps(L.as_dict(), indent=2))
    return L


def tail(job_id: str, *, lines: int = 25) -> dict:
    """Read the running log off the volume. Works mid-run thanks to `tee`."""
    st = _storage()
    cfg = st.require()
    out: dict = {"job_id": job_id, "done": False, "log": ""}
    try:
        keys = [o["Key"] for o in st.client.list_objects_v2(
            Bucket=cfg.bucket, Prefix=f"logs/{job_id}").get("Contents", [])]
        out["done"] = any(k.endswith(".DONE") for k in keys)
        if f"logs/{job_id}.log" in keys:
            body = st.client.get_object(
                Bucket=cfg.bucket, Key=f"logs/{job_id}.log")["Body"].read()
            text = body.decode("utf-8", "replace")
            out["log"] = "\n".join(text.splitlines()[-lines:])
            out["bytes"] = len(body)
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
    return out


def progress(job_id: str, spec_stages: list[str] | None = None) -> dict:
    """Turn the raw log into a stage-by-stage picture.

    A 3,000-line log of progress counters does not answer the only questions that matter:
    which stage am I in, is it moving, and how much is left. This parses the log back into
    the plan, so a stage that has not started reads as `pending` rather than as absence.
    """
    import re

    t = tail(job_id, lines=100000)
    text = t.get("log", "")
    lines = text.splitlines()

    if spec_stages is None:
        try:
            st = _storage()
            cfg = st.require()
            spec = json.loads(st.client.get_object(
                Bucket=cfg.bucket, Key=f"jobs/{job_id}.json")["Body"].read())
            spec_stages = spec.get("stages") or []
        except Exception:
            spec_stages = []

    stages: dict[str, dict] = {s: {"state": "pending", "detail": ""} for s in spec_stages}
    current = None
    for i, line in enumerate(lines):
        m = re.search(r"STAGE\s+\d+\s+·\s+([A-Z]+)", line)
        if m:
            name = m.group(1).lower()
            current = name
            stages.setdefault(name, {"state": "pending", "detail": ""})
            if stages[name]["state"] == "pending":
                stages[name]["state"] = "running"
            continue
        # completion line, e.g. "  ✓ 445.24s  sources=1 files=2507 minutes=600.2"
        m = re.match(r"\s*✓\s+([\d.]+)s\s*(.*)", line)
        if m and current:
            stages[current]["state"] = "done"
            stages[current]["seconds"] = float(m.group(1))
            stages[current]["detail"] = m.group(2).strip()
            current = None
            continue
        m = re.match(r"\s*✗\s+(\w+):?\s*(.*)", line)
        if m and current:
            stages[current]["state"] = "failed"
            stages[current]["detail"] = m.group(2)[:90]
            current = None
            continue
        # live counter, e.g. "  [300/2507]  12.0% · 16.8x realtime · ETA 29 min · 0 failed"
        m = re.match(r"\s*\[(\d+)/(\d+)\]\s+([\d.]+)%\s*·\s*([\d.]+)x\s+realtime"
                     r"(?:\s*·\s*ETA\s+([^·]+))?(?:·\s*(\d+)\s+failed)?", line)
        if m and current:
            stages[current].update(
                state="running", done=int(m.group(1)), total=int(m.group(2)),
                percent=float(m.group(3)), rate=float(m.group(4)),
                eta=(m.group(5) or "").strip(), failed=int(m.group(6) or 0))

    return {"job_id": job_id, "done": t.get("done", False),
            "stages": stages, "order": spec_stages or list(stages),
            "log_bytes": t.get("bytes", 0),
            "tail": "\n".join(lines[-4:])}


def render(prog: dict) -> str:
    """Format `progress()` for a terminal."""
    out = []
    for name in prog["order"]:
        s = prog["stages"].get(name, {"state": "pending"})
        st = s["state"]
        if st == "done":
            secs = s.get("seconds", 0)
            when = f"{secs/60:.1f} min" if secs >= 60 else f"{secs:.1f}s"
            out.append(f"  ✓ {name:<11} {when:<10} {s.get('detail','')[:52]}")
        elif st == "running":
            if "percent" in s:
                bits = f"{s['rate']}x realtime"
                if s.get("eta"):
                    bits += f" · ~{s['eta']} left"
                if s.get("failed"):
                    bits += f" · ⚠ {s['failed']} failed"
                out.append(f"  🔄 {name:<11} {s['percent']:>5.1f}%     {bits}")
            else:
                out.append(f"  🔄 {name:<11} running    (no progress output yet)")
        elif st == "failed":
            out.append(f"  ✗ {name:<11} FAILED     {s.get('detail','')[:52]}")
        else:
            out.append(f"    {name:<11} pending")
    return "\n".join(out)


def wait(job_id: str, *, poll: int = 20, max_minutes: float = 90,
         quiet: bool = False) -> dict:
    """Poll until the job writes its DONE marker, printing progress as it goes."""
    deadline = time.time() + max_minutes * 60
    last = ""
    while time.time() < deadline:
        t = tail(job_id, lines=6)
        if t.get("log") and t["log"] != last and not quiet:
            for line in t["log"].splitlines()[-3:]:
                if line.strip():
                    print(f"    | {line[:110]}", flush=True)
            last = t["log"]
        if t.get("done"):
            return {"ok": True, "job_id": job_id, **tail(job_id, lines=40)}
        time.sleep(poll)
    return {"ok": False, "job_id": job_id, "error": f"no DONE after {max_minutes} min",
            **tail(job_id, lines=40)}


def teardown(pod_id: str | None = None) -> dict:
    """Terminate pods. Stopping is not enough — a stopped pod still bills for disk."""
    from .api import RunPodAPI
    api = RunPodAPI()
    killed = []
    for p in api.pods():
        if pod_id and p.get("id") != pod_id:
            continue
        api.terminate(p["id"])
        killed.append(p["id"])
    return {"ok": True, "terminated": killed, "remaining": len(api.pods())}
