"""The Task API client methods send exactly the requests TASK-API.md §2/§8/§9 and the
server's routes (pod-control taskapi.py) take: method, path, query and body. A recording
transport answers each call; nothing touches the network."""
import json
import sys

sys.path.insert(0, "src")
from pod_loader.app import (Client, Conflict, Gpu, Invalid, JobSpec, Needs, NotFound,
                            TaskSpec, task_dict)
from pod_loader.app.client import httpx

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


def raises(fn, exc):
    try:
        fn()
    except exc as e:
        return e
    except Exception as e:
        print(f"     raised {type(e).__name__}: {e}")
        return None
    return None


seen = []
replies = {}


def handler(request):
    body = json.loads(request.content) if request.content else None
    seen.append((request.method, request.url.path, dict(request.url.params), body,
                 request.url.raw_path.decode()))
    status, out = replies.pop((request.method, request.url.path), (200, {}))
    return httpx.Response(status, json=out)


pc = Client("http://control.test", "tok", transport=httpx.MockTransport(handler))


def last():
    return seen[-1]


# -- TaskSpec / Needs --------------------------------------------------------------------
t = TaskSpec(pool="video", kind="comfy", lane="media",
             needs=Needs(min_vram_gb=24, models=["a.safetensors"], loads=["ckpt:a"]),
             payload={"x": 1}, labels={"shot": "14"}, extra={"future": True})
d = t.to_dict()
check("TaskSpec serialises the API's field names", d["pool"] == "video" and d["kind"] == "comfy"
      and d["needs"] == {"min_vram_gb": 24, "models": ["a.safetensors"], "loads": ["ckpt:a"]})
check("unset task fields are left out (server defaults apply)",
      not {"priority", "deadline_s", "lease_s", "timeout_s", "max_attempts"} & set(d))
check("extra passes through", d["future"] is True)
check("Needs.extra passes through", Needs(extra={"gpu_class": "x"}).to_dict() == {"gpu_class": "x"})
check("task_dict takes a dict", task_dict({"pool": "p", "kind": "k"}) == {"pool": "p", "kind": "k"})
check("task_dict refuses anything else", raises(lambda: task_dict(3), TypeError) is not None)

# -- pools -------------------------------------------------------------------------------
replies[("PUT", "/v1/pools/media/video")] = (200, {"lane": "media", "name": "video", "version": 3})
v = pc.pool_put("media", "video",
                JobSpec(lane="media", image="img:1", gpu=Gpu(min_vram_gb=24), queue_deadline_s=None),
                scale={"max_pods": 2, "launch_when": {"queued_tasks": 1}},
                match={"affinity_wait_s": 30}, cost_share={"media.encode": 0})
m, path, q, body, _ = last()
check("pool_put: PUT /v1/pools/{lane}/{name} → version", (m, path, v) == ("PUT", "/v1/pools/media/video", 3))
check("pool_put: job is the JobSpec as JSON", body["job"]["image"] == "img:1"
      and body["job"]["gpu"]["min_vram_gb"] == 24)
check("pool_put: cost_share folds into scale", body["scale"] == {
    "max_pods": 2, "launch_when": {"queued_tasks": 1}, "cost_share": {"media.encode": 0}})
check("pool_put: match passes", body["match"] == {"affinity_wait_s": 30})
replies[("PUT", "/v1/pools/media/enc")] = (200, {"version": 1})
pc.pool_put("media", "enc", {"image": "i"})
check("pool_put: no scale/match keys when not given", set(last()[3]) == {"job"})

replies[("GET", "/v1/pools")] = (200, {"pools": [{"lane": "media", "name": "video", "version": 3}]})
check("pools(): the list", pc.pools()[0]["name"] == "video")
replies[("GET", "/v1/pools/media/video")] = (200, {"queued": 2})
check("pool_get", pc.pool_get("media", "video")["queued"] == 2 and last()[1] == "/v1/pools/media/video")
pc.pool_expect("media", "video", in_s=120, tasks=3, needs=Needs(min_vram_gb=24))
check("pool_expect: POST .../expect {in_s, tasks, needs}",
      last()[:2] == ("POST", "/v1/pools/media/video/expect")
      and last()[3] == {"in_s": 120, "tasks": 3, "needs": {"min_vram_gb": 24}})
pc.pool_stats("media", "video", kind="comfy", since=1_800_000_000)
check("pool_stats: GET .../stats?kind=&since=", last()[1] == "/v1/pools/media/video/stats"
      and last()[2] == {"kind": "comfy", "since": "1800000000"})
pc.pool_stats("media", "video")
check("pool_stats: no params when unset", last()[2] == {})
check("pool paths need lane and name", raises(lambda: pc.pool_get("", "x"), ValueError) is not None)
pc.pool_get("a b", "c/d")
check("pool path parts are quoted", seen[-1][4] == "/v1/pools/a%20b/c%2Fd")

# -- tasks -------------------------------------------------------------------------------
replies[("POST", "/v1/tasks")] = (202, {"task_id": "t1", "state": "queued", "lane": "media",
                                        "pool": "video", "existing": False})
check("submit_task → task id", pc.submit_task(t) == "t1" and last()[3]["needs"]["loads"] == ["ckpt:a"])
replies[("POST", "/v1/tasks")] = (202, {"task_id": "t1", "state": "running", "existing": True})
check("submit_task_reply → the whole reply", pc.submit_task_reply({"pool": "p", "kind": "k"})["existing"])

replies[("GET", "/v1/tasks/t1")] = (200, {"task_id": "t1", "state": "queued"})
check("task(id)", pc.task("t1")["state"] == "queued")
replies[("GET", "/v1/tasks")] = (200, {"tasks": [{"task_id": "a", "labels": {"shot": "1", "step": "i2v"}},
                                                 {"task_id": "b", "labels": {"shot": "1", "step": "enc"}}]})
rows = pc.tasks(lane="media", pool="video", states=["queued", "held"], labels={"shot": "1", "step": "i2v"})
check("tasks(): lane, pool, state=a,b, first label to the server",
      last()[2] == {"lane": "media", "pool": "video", "state": "queued,held", "label": "shot:1"})
check("tasks(): further labels matched client-side", [r["task_id"] for r in rows] == ["a"])
pc.tasks(labels="shot:2")
check("tasks(): a 'k:v' label passes as is", last()[2] == {"label": "shot:2"})
check("a label without ':' is refused", raises(lambda: pc.tasks(labels="shot"), ValueError) is not None)

pc.cancel_task("t1", reason="user")
check("cancel_task: DELETE /v1/tasks/{id}?reason=", last()[:3] == ("DELETE", "/v1/tasks/t1", {"reason": "user"}))
replies[("DELETE", "/v1/tasks")] = (200, {"cancelled": 4})
n = pc.cancel_tasks(lane="media", label={"shot": "14"}, state=["queued"], reason="reshoot")
check("cancel_tasks: DELETE /v1/tasks?lane&label&state → count",
      n == 4 and last()[2] == {"lane": "media", "label": "shot:14", "state": "queued", "reason": "reshoot"})
replies[("POST", "/v1/tasks/cancel")] = (200, {"cancelled": 2})
check("cancel_tasks(ids=): POST /v1/tasks/cancel {ids}",
      pc.cancel_tasks(ids=["a", "b"]) == 2 and last()[3] == {"ids": ["a", "b"]})
check("cancel_tasks without ids needs lane and label",
      raises(lambda: pc.cancel_tasks(lane="media"), ValueError) is not None)
check("cancel_tasks: one label only", raises(
    lambda: pc.cancel_tasks(lane="m", label={"a": 1, "b": 2}), ValueError) is not None)

replies[("POST", "/v1/tasks/t1/retry")] = (200, {"task_id": "t1", "state": "queued", "attempt": 1})
r = pc.retry_task("t1", payload={"seed": 2}, patch={"12.inputs.seed": 2})
check("retry_task: POST .../retry {payload, patch}", r["attempt"] == 1
      and last()[3] == {"payload": {"seed": 2}, "patch": {"12.inputs.seed": 2}})
pc.retry_task("t1")
check("retry_task: empty body when nothing replaced", last()[3] == {})
pc.set_priority("t1", 5)
check("set_priority: PATCH /v1/tasks/{id} {priority}", last()[:2] == ("PATCH", "/v1/tasks/t1")
      and last()[3] == {"priority": 5})

# -- typed errors, reused ------------------------------------------------------------------
replies[("PATCH", "/v1/tasks/t1")] = (409, {"detail": {"error": "task t1 is running",
                                                       "hint": "priority can be changed only while queued"}})
e = raises(lambda: pc.set_priority("t1", 1), Conflict)
check("409 → Conflict with the server's words and hint", e and "running" in e.error and "queued" in e.hint)
replies[("POST", "/v1/tasks")] = (422, {"detail": {"error": "no pool media/x"}})
check("422 → Invalid", raises(lambda: pc.submit_task({"pool": "x", "kind": "k"}), Invalid) is not None)
replies[("GET", "/v1/tasks/nope")] = (404, {"detail": {"error": "no task nope"}})
check("404 → NotFound", raises(lambda: pc.task("nope"), NotFound) is not None)

print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
