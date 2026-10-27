"""Where a pod landed, read back from RunPod's pod record; and a caller refusing a host.

RunPod chooses the machine and cannot be told to avoid one, so a caller that knows a host
is bad can only look after placement and give the pod back. No network: the API is a fake.
"""
import sys

sys.path.insert(0, "src")
from pod_loader.runpod import reaper
from pod_loader.runpod.api import FACT_KEYS, RunPodAPI, facts_from_pod

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


RECORD = {"id": "p1", "machineId": "m-123", "costPerHr": 0.69, "gpuCount": 1,
          "machine": {"gpuTypeId": "NVIDIA GeForce RTX 4090", "dataCenterId": "EU-RO-1"},
          "gpu": {"id": "NVIDIA GeForce RTX 4090", "displayName": "RTX 4090", "count": 1}}

f = facts_from_pod(RECORD)
check("machine, GPU, count, datacentre and price are read",
      f["machine_id"] == "m-123" and f["gpu_name"] == "RTX 4090"
      and f["gpu_type_id"] == "NVIDIA GeForce RTX 4090" and f["gpu_count"] == 1
      and f["data_center_id"] == "EU-RO-1" and f["cost_hr"] == 0.69)
check("the host CUDA/driver version is not in the record, so it is None, not a guess",
      f["cuda_version"] is None and f["driver_version"] is None)
empty = facts_from_pod({"id": "p2"})
check("every key is always present", set(empty) == set(FACT_KEYS))
check("missing facts are None", empty["machine_id"] is None and empty["gpu_count"] is None)
check("a CPU pod's record without a gpu block still parses",
      facts_from_pod({"id": "c", "costPerHr": 0.06, "machineId": "m9"})["machine_id"] == "m9")


class SlowAPI:
    """Assigns the machine on the third look."""
    def __init__(s): s.looks = 0
    def pod(s, pid):
        s.looks += 1
        return RECORD if s.looks >= 3 else {"id": pid, "costPerHr": 0.69}


naps = []
api = SlowAPI()
f = reaper.placement_facts(api, {"id": "p1", "costPerHr": 0.69}, wait_sec=30, interval=3,
                           sleep=naps.append)
check("polls until the machine is assigned", f["machine_id"] == "m-123" and api.looks == 3)
check("…sleeping between looks", naps == [3, 3])

class NeverAPI:
    def __init__(s): s.looks = 0
    def pod(s, pid):
        s.looks += 1
        return {"id": pid}


api = NeverAPI()
f = reaper.placement_facts(api, "p1", wait_sec=0.1, interval=0.03)
check("bounded: gives up with the machine unknown", f["machine_id"] is None and 1 <= api.looks <= 5)

api, naps = SlowAPI(), []
f = reaper.placement_facts(api, "p1", wait_sec=0, sleep=naps.append)
check("wait_sec=0 is a single look", api.looks == 1 and naps == [])

f = reaper.placement_facts(SlowAPI(), RECORD, sleep=naps.append)
check("a create response that already names the machine needs no GET", f["machine_id"] == "m-123")


class Broken:
    def pod(s, pid): raise RuntimeError("HTTP 500")


check("a failing GET never raises", reaper.placement_facts(Broken(), "p", wait_sec=0)["pod_id"] == "p")

# -- vet: refuse a placement after the fact ------------------------------------------------


class API:
    def __init__(s):
        s.made, s.killed = [], []
    def create(s, **kw):
        s.made.append(kw)
        return {"id": f"p{len(s.made)}", "costPerHr": 0.3, "machineId": f"m{len(s.made)}"}
    def terminate(s, pid):
        s.killed.append(pid)


api, seen = API(), []
pod, used = reaper.create_with_capacity(
    api, {"name": "n", "image": "i"}, compute="GPU", gpu_types=("A", "B"), clouds=("SECURE",),
    verbose=False, vet=lambda p: "host m1 is blocked" if p["machineId"] == "m1" else None,
    on_attempt=lambda *a: seen.append((a[0], a[1], a[3])))
check("a refused host is terminated at once", api.killed == ["p1"])
check("the walk moves on and places the next shape", pod["id"] == "p2")
check("the refusal is reported with its reason",
      seen[0][1] == "refused" and "blocked" in seen[0][2] and seen[1][1] == "placed")

api = API()
try:
    reaper.create_with_capacity(api, {"name": "n", "image": "i"}, compute="GPU",
                                gpu_types=("A", "B", "C", "D"), clouds=("SECURE",),
                                verbose=False, vet=lambda p: "bad host", max_refusals=2)
    check("refusals are bounded", False)
except RuntimeError as e:
    check("refusals are bounded: two creates, then a waitable no-placement",
          len(api.made) == 2 and api.killed == ["p1", "p2"]
          and "no acceptable placement" in str(e) and "refused" in str(e))


def boom(p):
    raise ValueError("vet bug")


pod, _ = reaper.create_with_capacity(API(), {"name": "n", "image": "i"}, compute="GPU",
                                     gpu_types=("A",), clouds=("SECURE",), verbose=False,
                                     vet=boom)
check("a vet that raises accepts the pod", pod["id"] == "p1")

rp = RunPodAPI(key="k")
rp._call = lambda method, path, body=None, timeout=60: RECORD
check("RunPodAPI.placement_facts reads the record", rp.placement_facts("p1")["machine_id"] == "m-123")

print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
