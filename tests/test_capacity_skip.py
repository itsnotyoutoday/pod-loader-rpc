"""A shape on cooldown is passed over: RunPod gives the same dead host back for the same
shape, so skipping the shape is the only way to land somewhere else."""
import sys

sys.path.insert(0, "src")
from pod_loader.runpod import reaper

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


class API:
    def __init__(self):
        self.made = []

    def create(self, **kw):
        self.made.append(kw.get("gpuTypeIds") or kw.get("gpu_type_ids"))
        return {"id": f"p{len(self.made)}", "costPerHr": 0.3}

    def terminate(self, pod_id):
        pass


labels = [p["_label"] for p in reaper.placements(compute="GPU", gpu_types=("A", "B"),
                                                 clouds=("SECURE",))]
api, seen = API(), []
pod, used = reaper.create_with_capacity(api, {"name": "n", "image": "i"}, compute="GPU",
                                        gpu_types=("A", "B"), clouds=("SECURE",),
                                        verbose=False, skip=[labels[0]],
                                        on_attempt=lambda *a: seen.append((a[0], a[1])))
check("the cooled shape is never created", len(api.made) == 1)
check("the next shape is placed", pod["id"] == "p1")
check("the skip is recorded", seen[0] == (labels[0], "skipped"))

try:
    reaper.create_with_capacity(API(), {"name": "n", "image": "i"}, compute="GPU",
                                gpu_types=("A",), clouds=("SECURE",), verbose=False,
                                skip=labels[:1])
    check("everything skipped raises", False)
except RuntimeError as e:
    check("everything skipped is a waitable 'no acceptable placement'",
          "no acceptable placement" in str(e) and "cooldown" in str(e))

print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
