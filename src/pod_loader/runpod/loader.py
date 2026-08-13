"""Launch onto a rented RunPod pod. The expensive destination, so it carries the guards."""
from __future__ import annotations

import time

from ..base_loader import BaseLoader, PodInfo, Running


class RunPodLoader(BaseLoader):
    name = "runpod"

    def preflight(self, cfg) -> list[str]:
        problems = []
        try:
            from .api import RunPodAPI
            RunPodAPI()
        except Exception as e:
            problems.append(f"RunPod API unavailable: {str(e)[:90]}")
        if getattr(cfg, "budget_min", 0) <= 0:
            problems.append(
                "BUDGET_MIN must be positive. It is the ONLY hard ceiling: an in-pod "
                "timeout cannot terminate a RunPod pod — runpodctl from inside returns "
                "Unauthorized, so it detects the overrun and keeps billing.")
        return problems

    def plan(self, cfg) -> str:
        from . import reaper
        kw = {"compute": cfg.compute, "clouds": cfg.clouds,
              "fallback_to_gpu": cfg.fallback_to_gpu}
        if cfg.flavors:
            kw["flavors"] = cfg.flavors
        if cfg.gpu_types:
            kw["gpu_types"] = cfg.gpu_types
        n = len(list(reaper.placements(**kw)))
        ceiling = f", refusing anything above ${cfg.max_cost_hr}/hr" if cfg.max_cost_hr else ""
        return f"would try {n} placements{ceiling}"

    def create_kwargs(self, cfg, *, env: dict) -> dict:
        from . import volume
        kw = {"image": cfg.image, "container_disk_gb": cfg.disk_gb,
              "vcpu_count": cfg.vcpu, "ports": ["8000/http"], "env": env}
        vol = volume.load(cfg.runpod_volume or None)
        if vol:
            kw.update(vol.create_kwargs())
        return kw

    def start(self, cfg, *, job_id: str, env: dict, spec_key: str) -> Running:
        from .. import launchfile
        from . import reaper, volume

        create = {"image": cfg.image, "container_disk_gb": cfg.disk_gb,
                  "vcpu_count": cfg.vcpu, "ports": ["8000/http"], "env": env}
        vol = volume.load(cfg.runpod_volume or None)
        if vol:
            vol.require_provider("runpod")
            create.update(vol.create_kwargs())
            self._say(f"volume: {vol.volume_id} in {vol.datacenter} "
                      f"(pins compute to that datacenter)")

        # Not a plain create: walks flavor x cloud x compute type, and refuses a placement
        # above MAX_COST_HR rather than silently taking a 12x pricier slot.
        api = reaper._api()
        pod, used = reaper.create_with_capacity(
            api, create, **launchfile.capacity_kwargs(cfg))
        pod_id = pod["id"]
        cost = float(pod.get("costPerHr") or 0)

        # Journalled BEFORE anything else can fail, so a pod whose launcher dies is still
        # discoverable. A pod nobody journaled is a pod nobody can find.
        reaper.journal(pod_id, f"job-{job_id}", cost,
                       time.time() + cfg.budget_min * 60)
        shape = (used.get("cpu_flavor_ids") or used.get("gpu_type_ids") or ["?"])[0]
        return Running(
            target=self.name, job_id=job_id, handle=pod_id,
            endpoint=f"https://{pod_id}-8000.proxy.runpod.net", cost_hr=cost,
            detail={"placement": f"{used.get('cloud_type','')}/{shape}",
                    "budget_min": cfg.budget_min,
                    "datacenter": vol.datacenter if vol else None})

    def stop(self, handle: str) -> dict:
        from .api import RunPodAPI
        RunPodAPI().terminate(handle)
        return {"terminated": handle}

    def list_pods(self) -> list[PodInfo]:
        from .api import RunPodAPI
        return [PodInfo(pod_id=p["id"], name=p.get("name", ""),
                        cost_hr=float(p.get("costPerHr") or 0),
                        age_sec=_age_sec(p), target=self.name,
                        running=(p.get("desiredStatus") == "RUNNING"))
                for p in RunPodAPI().pods()]


def _age_sec(pod: dict) -> float:
    """Uptime from whichever timestamp RunPod supplies.

    An unparseable date reads as age ZERO, never as old. A watcher protects young pods
    with a grace period, so "unknown" must land on the side that does not terminate
    somebody's running job over a date format.
    """
    import datetime as dt
    for key in ("lastStartedAt", "createdAt"):
        raw = (pod.get(key) or "").strip()
        if not raw:
            continue
        for fmt in ("%Y-%m-%d %H:%M:%S.%f %z %Z", "%Y-%m-%d %H:%M:%S.%f %z",
                    "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                t = dt.datetime.strptime(raw, fmt)
                if t.tzinfo is None:
                    t = t.replace(tzinfo=dt.timezone.utc)
                return max(0.0, (dt.datetime.now(dt.timezone.utc) - t).total_seconds())
            except ValueError:
                continue
    return 0.0
