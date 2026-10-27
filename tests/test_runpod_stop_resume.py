"""RunPodAPI.stop_pod / resume_pod: the REST paths, the record they return, and a resume
that comes back short of GPUs. No network: _call is replaced with a fake HTTP layer."""
import sys

sys.path.insert(0, "src")
from pod_loader.runpod.api import RunPodAPI

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


def api_with(replies):
    """An API whose HTTP layer answers from `replies` keyed by (method, path)."""
    api = RunPodAPI(key="k")
    calls = []

    def fake(method, path, body=None, timeout=60):
        calls.append((method, path, body))
        r = replies[(method, path)]
        return r() if callable(r) else r

    api._call = fake
    return api, calls


api, calls = api_with({("POST", "/pods/p1/stop"): {"id": "p1", "desiredStatus": "EXITED"}})
rec = api.stop_pod("p1")
check("stop is POST /pods/{id}/stop with no body", calls == [("POST", "/pods/p1/stop", None)])
check("stop returns the pod record", rec["desiredStatus"] == "EXITED")

api, calls = api_with({("POST", "/pods/p1/stop"): {},
                       ("GET", "/pods/p1"): {"id": "p1", "desiredStatus": "EXITED"}})
rec = api.stop_pod("p1")
check("an empty stop reply is followed by a GET", [c[:2] for c in calls] ==
      [("POST", "/pods/p1/stop"), ("GET", "/pods/p1")] and rec["id"] == "p1")

api, calls = api_with({("POST", "/pods/p1/start"): {"id": "p1", "gpuCount": 2,
                                                     "desiredStatus": "RUNNING"}})
rec = api.resume_pod("p1")
check("resume is POST /pods/{id}/start with no body",
      calls == [("POST", "/pods/p1/start", None)])
check("resume returns the pod record", rec["desiredStatus"] == "RUNNING")
check("a pod with more GPUs than the minimum is fine", api.resume_pod("p1", gpu_count=2)["id"] == "p1")

api, _ = api_with({("POST", "/pods/p1/start"): {"id": "p1", "gpuCount": 0}})
try:
    api.resume_pod("p1", gpu_count=1)
    check("a resume with no GPU raises", False)
except RuntimeError as e:
    check("a resume with no GPU raises, naming the pod and the bill",
          "p1" in str(e) and "billing" in str(e))
check("gpu_count=0 accepts a GPU-less resume", api.resume_pod("p1", gpu_count=0)["id"] == "p1")

api, _ = api_with({("POST", "/pods/p1/start"): {"id": "p1"}})
check("a record that does not state its GPUs is not refused", api.resume_pod("p1")["id"] == "p1")

try:
    RunPodAPI(key="k").stop_pod("")
    check("an empty pod id is refused", False)
except ValueError:
    check("an empty pod id is refused before any call", True)

print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
