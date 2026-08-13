"""Run a shell command against the network volume, on a pod, on a budget.

    python -m pod_loader.podrun -- 'du -sh /workspace/*'      # volume from RUNPOD_VOLUME

## Why this exists

The volume is one storage seen two ways: an S3 key from outside, a POSIX path from inside a
pod. Bulk work belongs on the POSIX side, and the difference is not marginal.

RunPod's S3 API is a thin facade over a network filesystem. Their docs warn that operations
"may take a long time when used on a directory containing many files (over 10,000)" and that
ListObjects "degrades on very large volumes". Measured here: listing 87,000 objects takes
over a minute, `head_object` returns 403 on a 91 MB object while working on a 347-byte one,
and batch `delete_objects` returns 307 Temporary Redirect — it is simply not implemented.

So deleting 42,000 objects over the API is 42,000 HTTP round-trips and about an hour. The
same deletion as `rm -rf` on the mount is a filesystem unlink: seconds.

**Rule: bulk operations run on a pod against the mount. The S3 API is for small reads,
single writes, and control-plane access.**

## How output comes back

RunPod exposes no logs API, so the command's output is written to a file on the volume and
read back over S3 afterwards — small, one object, exactly the shape the API is good at. The
pod also serves it live over the harness if the image carries one.

## Cost

Runs under `reaper.pod()`, so the pod cannot outlive its budget even if this process is
killed. A `cpu3c` is about $0.06/hr; a cleanup is pennies.
"""
from __future__ import annotations

import argparse
import shlex
import time

from . import reaper

REPORT = "_tmp/podrun_report.txt"


def build_command(cmd: str, report_key: str = REPORT) -> str:
    """Wrap the user's command so its output lands somewhere readable.

    `set -o pipefail` and an explicit exit-code line matter: a command that fails silently
    inside a tee is indistinguishable from one that worked, and the only evidence available
    afterwards is this file.
    """
    return (
        "set -o pipefail; "
        f"mkdir -p /workspace/{report_key.rsplit('/', 1)[0]}; "
        f"REPORT=/workspace/{report_key}; "
        "{ echo \"=== podrun $(date -u +%FT%TZ) ===\"; "
        f"echo \"$ {cmd}\"; echo; "
        f"( {cmd} ) 2>&1; "
        "RC=$?; echo; echo \"=== exit=$RC ===\"; } | tee \"$REPORT\"; "
        "sync"
    )


#: Tried in order. A network volume pins the pod to ITS datacenter, so when that region
#: has no capacity for one flavor there is no option to move — only to ask for a different
#: shape. Observed: cpu3c unavailable in US-NC-1 while others were free.
FLAVORS = ("cpu3c", "cpu3g", "cpu5c", "cpu5g", "cpu3m", "cpu5m")


def run(cmd: str, *, volume_id: str, budget_min: float = 15,
        image: str = "ghcr.io/itsnotyoutoday/lingua-pipeline:latest",
        flavor: str | None = None, disk_gb: int = 20,
        poll_sec: int = 10, timeout_min: float = 12) -> dict:
    """Execute `cmd` on a pod with the volume mounted. Returns the captured output."""
    from ..objectstore import get_storage

    st = get_storage()
    cfg = st.require()

    # Clear any previous report so a stale one cannot be mistaken for this run's.
    try:
        st.client.delete_object(Bucket=cfg.bucket, Key=REPORT)
    except Exception:
        pass

    wrapped = build_command(cmd)
    flavors = [flavor] if flavor else list(FLAVORS)
    create = dict(
        image=image, compute_type="CPU", cpu_flavor_ids=flavors, vcpu_count=2,
        container_disk_gb=disk_gb, cloud_type="SECURE",
        network_volume_id=volume_id, volume_mount_path="/workspace",
        env={"PODH_API_TOKEN": "podrun", "PODH_MOUNT_KIND": "volume",
             "PODH_STATUS_S3": "0", "PODH_MODE": "shell"},
        start_cmd=["bash", "-lc", wrapped],
    )

    out, deadline = None, time.time() + timeout_min * 60
    with reaper.pod(create, budget_min=budget_min) as p:
        print(f"  pod {p['pod_id']} (${p['cost_hr']}/hr) — waiting for the report",
              flush=True)
        while time.time() < deadline:
            time.sleep(poll_sec)
            try:
                body = st.client.get_object(Bucket=cfg.bucket, Key=REPORT)["Body"].read()
            except Exception:
                continue
            text = body.decode("utf-8", "replace")
            if "=== exit=" in text:
                out = text
                break
    if out is None:
        return {"ok": False, "error": f"no report within {timeout_min} min",
                "hint": "the image pull can take 2-3 minutes; raise --timeout-min"}
    rc = 0
    for line in out.splitlines():
        if line.startswith("=== exit="):
            rc = int(line.split("=")[-1].rstrip("= "))
    return {"ok": rc == 0, "returncode": rc, "output": out}


# --------------------------------------------------------------------------------------
# Prepared operations
# --------------------------------------------------------------------------------------

#: Everything except the raw corpus and its manifest. Raw audio is the only thing in the
#: system that cannot be recomputed or re-authored, so it is protected by name rather than
#: by the caller remembering.
CLEAN_KEEPING_CORPUS = r"""
echo '--- before ---'; du -sh /workspace/* 2>/dev/null | sort -h
echo
for d in /workspace/*; do
  case "$(basename "$d")" in
    corpus) ;;                      # handled below, selectively
    *) echo "rm -rf $d"; rm -rf "$d" ;;
  esac
done
for d in /workspace/corpus/*; do
  case "$(basename "$d")" in
    raw|corpus_research.json) echo "keep $d" ;;
    *) echo "rm -rf $d"; rm -rf "$d" ;;
  esac
done
echo
echo '--- after ---'; du -sh /workspace/* /workspace/corpus/* 2>/dev/null | sort -h
echo; echo '--- raw corpus intact? ---'
find /workspace/corpus/raw -type f 2>/dev/null | wc -l | xargs echo 'files in corpus/raw:'
du -sh /workspace/corpus/raw 2>/dev/null
"""

OPS = {
    "clean-keeping-corpus": CLEAN_KEEPING_CORPUS,
    "tree": "du -sh /workspace/* 2>/dev/null | sort -h; echo; find /workspace -maxdepth 2 -type d | head -40",
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--volume", default=None,
                    help="RunPod volume id; defaults to RUNPOD_VOLUME")
    ap.add_argument("--op", choices=sorted(OPS), help="a prepared operation")
    ap.add_argument("--budget-min", type=float, default=15)
    ap.add_argument("--timeout-min", type=float, default=12)
    ap.add_argument("cmd", nargs="*", help="or an arbitrary command after --")
    a = ap.parse_args()

    if a.op:
        cmd = OPS[a.op]
    elif a.cmd:
        cmd = " ".join(shlex.quote(x) if " " in x else x for x in a.cmd)
    else:
        ap.error("give --op or a command after --")

    from . import volume as _volume
    vid = a.volume or (_volume.require().volume_id)
    r = run(cmd, volume_id=vid, budget_min=a.budget_min, timeout_min=a.timeout_min)
    print(r.get("output") or r)
    return 0 if r.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
