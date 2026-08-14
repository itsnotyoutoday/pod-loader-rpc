"""Run the job here — in a container, or directly in this interpreter."""
from __future__ import annotations

from ..ids import pod_name

import os
import signal
import subprocess
import sys

from ..base_loader import BaseLoader, PodInfo, Running


class DockerLoader(BaseLoader):
    """The same image the pod would run, on this machine.

    Same harness, same /v1, same contract — so a job that works here works on a pod and
    the difference is the bill rather than the behaviour. This is the destination for
    reproducing a failure without paying to see it twice.
    """

    name = "docker"

    def preflight(self, cfg) -> list[str]:
        try:
            subprocess.run(["docker", "info"], capture_output=True, timeout=15, check=True)
        except Exception:
            return ["docker is not available or not running"]
        return []

    def plan(self, cfg) -> str:
        return f"docker run {cfg.image} with {cfg.local_workspace} as /workspace"

    def start(self, cfg, *, job_id: str, env: dict, spec_key: str) -> Running:
        ws = os.path.abspath(os.path.expanduser(cfg.local_workspace))
        os.makedirs(ws, exist_ok=True)
        args = ["docker", "run", "-d", "--name", f"podjob-{pod_name(job_id)}", "-p", "8000:8000"]
        for k, v in env.items():
            args += ["-e", f"{k}={v}"]
        args += ["-v", f"{ws}:/workspace", cfg.image]
        r = subprocess.run(args, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"docker run failed: {r.stderr.strip()[:200]}")
        return Running(target=self.name, job_id=job_id, handle=r.stdout.strip()[:12],
                       endpoint="http://localhost:8000", cost_hr=0.0,
                       detail={"workspace": ws})

    def stop(self, handle: str) -> dict:
        subprocess.run(["docker", "rm", "-f", handle], capture_output=True)
        return {"removed": handle}

    def list_pods(self) -> list[PodInfo]:
        """Containers this loader started. Filtered by the podjob- name prefix so a sweep
        can never touch an unrelated container someone else is running."""
        import time
        r = subprocess.run(
            ["docker", "ps", "--filter", "name=podjob-",
             "--format", "{{.ID}}\t{{.Names}}\t{{.CreatedAt}}"],
            capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"docker ps failed: {r.stderr.strip()[:160]}")
        out = []
        for line in r.stdout.strip().splitlines():
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            out.append(PodInfo(pod_id=parts[0], name=parts[1], cost_hr=0.0,
                               age_sec=0.0, target=self.name))
        return out


class DirectLoader(BaseLoader):
    """The harness in this interpreter. No container, no isolation, no /v1.

    For developing stages: a traceback lands in your terminal instead of in a log you have
    to fetch. Deliberately the least faithful option — it uses your Python, your installed
    packages and your filesystem, so "works locally" proves the stage logic and nothing
    whatsoever about the image.
    """

    name = "direct"

    def preflight(self, cfg) -> list[str]:
        try:
            import pod_harness  # noqa: F401
        except ImportError:
            return ["pod_harness is not importable here.\n"
                    "      TARGET=local runs the harness in THIS interpreter, so it must "
                    "be installed:\n"
                    "          pip install "
                    "git+https://github.com/itsnotyoutoday/pod-harness.git"]
        return []

    def plan(self, cfg) -> str:
        return "run pod_harness.execute_job in a subprocess of this shell"

    def start(self, cfg, *, job_id: str, env: dict, spec_key: str) -> Running:
        # A subprocess rather than in-process: a segfault in a native library must not take
        # down the shell, and cancelling needs a process to signal.
        p = subprocess.Popen(
            [sys.executable, "-m", "pod_harness.execute_job", "--spec", env["PODH_JOB_SPEC"]],
            env={**os.environ, **env})
        return Running(target=self.name, job_id=job_id, handle=str(p.pid), cost_hr=0.0,
                       detail={"pid": p.pid, "note": "no /v1 — watch this terminal"})

    def stop(self, handle: str) -> dict:
        try:
            os.kill(int(handle), signal.SIGTERM)
        except (ProcessLookupError, ValueError):
            pass
        return {"signalled": handle}
