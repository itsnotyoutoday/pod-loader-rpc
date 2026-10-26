"""Which RunPod errors mean "this shape is full, try the next" — by the exact bodies seen.

A miss is not soft: the first full shape raises out of the walk and the job fails as not
retryable. A false hit is the other way to lose: a real error gets walked past as if it
were a full rack.
"""
import sys

sys.path.insert(0, "src")
from pod_loader.runpod import reaper

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


full = [
    'HTTP 500 POST /pods: {"error":"create pod: There are no instances currently available","status":500}',
    "HTTP 500 POST /pods: There are no longer any instances available with the requested specifications",
    "HTTP 500 POST /pods: no instances available",
    "NO INSTANCES AVAILABLE",
    'HTTP 500 POST /pods: {"error":"create pod: This machine does not have the resources to deploy your pod. Please try a different machine","status":500}',
]
real = [
    'HTTP 400 POST /pods: {"error":"invalid imageName"}',
    "HTTP 401 POST /pods: Unauthorized",
    'HTTP 500 POST /pods: {"error":"internal error"}',
    "HTTP 403 GET /pods: error code: 1010",
]
for m in full:
    check(f"full: {m[22:80]}", reaper._is_capacity_error(RuntimeError(m)))
for m in real:
    check(f"not capacity: {m[:60]}", not reaper._is_capacity_error(RuntimeError(m)))


class API:
    """First shape full with today's wording, second places."""
    calls = 0

    def create(self, **kw):
        API.calls += 1
        if API.calls == 1:
            raise RuntimeError(full[0])
        return {"id": "p1", "costPerHr": 0.3}

    def terminate(self, pod_id):
        pass


seen = []
pod, used = reaper.create_with_capacity(API(), {"name": "n", "image": "i"}, compute="GPU",
                                        gpu_types=("A", "B"), clouds=("SECURE",), verbose=False,
                                        on_attempt=lambda *a: seen.append(a[1]))
check("today's wording walks on to the next shape", pod["id"] == "p1" and API.calls == 2)
check("the full shape is recorded as a placement", seen[:1] == ["full"])

print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
