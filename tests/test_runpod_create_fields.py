"""RunPodAPI.create maps its kwargs onto RunPod's REST body, and refuses what cannot work.

No network: _call is replaced with a recorder. Field names are RunPod's PodCreateInput
(rest.runpod.io/v1/openapi.json).
"""
import sys

sys.path.insert(0, "src")
from pod_loader.runpod.api import RunPodAPI, USER_AGENT

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


api = RunPodAPI(key="k")
sent = {}
api._call = lambda method, path, body=None, timeout=60: sent.update(body=body) or {"id": "p"}

api.create(name="n", image="i", gpu_type_ids=["NVIDIA GeForce RTX 4090"],
           allowed_cuda_versions=["12.8", "12.9"], min_ram_per_gpu=48, min_vcpu_per_gpu=8,
           data_center_ids=["US-KS-2"])
b = sent["body"]
check("CUDA floor reaches RunPod", b.get("allowedCudaVersions") == ["12.8", "12.9"])
check("RAM and vCPU per GPU reach RunPod", b.get("minRAMPerGPU") == 48 and b.get("minVCPUPerGPU") == 8)
check("datacentres reach RunPod", b.get("dataCenterIds") == ["US-KS-2"])

api.create(name="n", image="i", compute_type="CPU", allowed_cuda_versions=["12.8"], min_ram_per_gpu=48)
b = sent["body"]
check("GPU host filters are not sent for a CPU pod",
      "allowedCudaVersions" not in b and "minRAMPerGPU" not in b)

api.create(name="n", image="i")
b = sent["body"]
check("nothing is sent when not asked for",
      not ({"allowedCudaVersions", "minRAMPerGPU", "minVCPUPerGPU", "dataCenterIds"} & set(b)))

try:
    api.create(name="n", image="i", network_volume_id="v", data_center_ids=["US-KS-2"])
    check("a volume plus datacentres is refused", False)
except ValueError:
    check("a volume plus datacentres is refused", True)

try:
    api.create(name="n", image="i", dataCenterIds=["US-KS-2"])
    check("an unknown key still fails", False)
except TypeError:
    check("an unknown key still fails", True)

api.create(name="n", image="i", volume_in_gb=100)
b = sent["body"]
check("pod volume is sized when asked", b.get("volumeInGb") == 100 and b.get("volumeMountPath") == "/workspace")
api.create(name="n", image="i", network_volume_id="v", volume_in_gb=100)
b = sent["body"]
check("a network volume wins over a pod volume size", "volumeInGb" not in b and b.get("networkVolumeId") == "v")

check("REST calls carry a named User-Agent", USER_AGENT and "urllib" not in USER_AGENT.lower())

print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
