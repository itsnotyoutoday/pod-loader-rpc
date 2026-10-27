"""The Task API client methods against FakeControl, and the state rules the fake follows
(TASK-API.md §1–§9): cancel is final across lease loss and release, retry re-rolls under
the same id, fallback tasks end `returned`, priority and position, bulk cancel, the
autoscaler's launches, and task events in the shared event log."""
import sys

sys.path.insert(0, "src")
from pod_loader.app import Conflict, Invalid, JobSpec, Needs, NotFound, TaskSpec, Unauthorized
from pod_loader.app.testing import FakeControl, FakeError

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


TEMPLATE = {"image": "registry.example/worker:1", "gpu": {"count": 1, "min_vram_gb": 24},
            "boot": {"models": {"profile": "models", "paths": []}, "workload": {"kind": "comfy"}},
            "policy": {"idle_stop_s": 300}}


def setup(**scale):
    fake = FakeControl()
    pc = fake.client()
    pc.pool_put("media", "video", TEMPLATE, scale={"max_pods": 2, **scale},
                cost_share={"media.encode": 0})
    return fake, pc


def kinds(fake, task_id):
    return [e["kind"] for e in fake.events_of(job_id=task_id)]


# -- pools ---------------------------------------------------------------------------------
fake, pc = setup()
check("pool_put → version 1, then 2", pc.pool_put("media", "video", TEMPLATE,
                                                   scale={"max_pods": 2}) == 2)
pg = pc.pool_get("media", "video")
check("pool_get: server defaults applied to the template",
      pg["job"]["boot"]["app"]["serve"] == ["podh-worker"] and pg["job"]["policy"]["idle_stop_s"] == 300)
check("pool_get: scale defaults", pg["scale"]["tasks_per_pod"] == 1 and pg["scale"]["min_pods"] == 0
      and pg["scale"]["launch_when"] == {"queued_s": 0, "queued_tasks": 1})
check("pool_get: counts and pods", (pg["queued"], pg["running"], pg["pods"]) == (0, 0, []))
check("pools(): the list", [(p["lane"], p["name"], p["version"]) for p in pc.pools()]
      == [("media", "video", 2)])
check("pool_put: a JobSpec template works",
      pc.pool_put("media", "enc", JobSpec(lane="media", image="img:2"),
                  scale={"never_launch": True}, match={"accepts_from": ["video"]}) == 1)
check("pool_put: job_id in the template is refused", raises(
    lambda: pc.pool_put("media", "x", {**TEMPLATE, "job_id": "j"}), Invalid) is not None)
check("pool_put: another lane in the template is refused", raises(
    lambda: pc.pool_put("media", "x", {**TEMPLATE, "lane": "other"}), Invalid) is not None)
check("pool_put: min_pods > max_pods refused", raises(
    lambda: pc.pool_put("media", "x", TEMPLATE, scale={"min_pods": 3, "max_pods": 1}), Invalid))
check("pool_get of a missing pool → NotFound", raises(lambda: pc.pool_get("media", "nope"), NotFound))
lane_tok = FakeControl(tokens={"other-tok": ["other"]})
lane_tok.client().pool_put("media", "video", TEMPLATE)
oc = lane_tok.client(token="other-tok")
check("a lane-confined token cannot write another lane's pool",
      raises(lambda: oc.pool_put("media", "video", TEMPLATE), Unauthorized) is not None)
check("…nor see it", raises(lambda: oc.pool_get("media", "video"), NotFound) is not None
      and oc.pools() == [])

# -- submit / read -------------------------------------------------------------------------
fake, pc = setup()
spec = TaskSpec(pool="video", kind="comfy", lane="media", task_id="shot-14-i2v",
                needs=Needs(min_vram_gb=24, models=["diffusion_models/a.safetensors"],
                            loads=["ckpt:a"]),
                workflow={"ref": "workflows/i2v.json", "patch": {"12.inputs.seed": 1}},
                payload={"shot": 14}, labels={"shot": "14", "step": "i2v"})
r = pc.submit_task_reply(spec)
check("submit_task_reply: {task_id, state, lane, pool, existing}",
      r == {"task_id": "shot-14-i2v", "state": "queued", "lane": "media", "pool": "video",
            "existing": False})
t = pc.task("shot-14-i2v")
check("task(): server defaults", (t["attempt"], t["max_attempts"], t["priority"], t["timeout_s"])
      == (1, 2, 0, 1800) and t["position"] == 1 and t["deadline_at"] == fake.now + 86400)
check("task(): needs normalised with the pool's store",
      t["needs"]["models"] == [{"path": "diffusion_models/a.safetensors"}]
      and t["needs"]["store"] == "models")
check("task.queued emitted under the task id", kinds(fake, "shot-14-i2v") == ["task.queued"]
      and fake.events_of("task.queued")[0]["detail"]["pool"] == "video")
k = TaskSpec(pool="video", kind="comfy", lane="media", idempotency_key="k-1")
a = pc.submit_task(k)
check("idempotency key → the existing task", pc.submit_task_reply(k) == {
    "task_id": a, "state": "queued", "lane": "media", "pool": "video", "existing": True})
check("a duplicate task_id → Conflict", raises(lambda: pc.submit_task(spec), Conflict) is not None)
check("an unknown pool → Invalid", raises(
    lambda: pc.submit_task({"lane": "media", "pool": "nope", "kind": "comfy"}), Invalid) is not None)
check("unknown keys → Invalid", raises(
    lambda: pc.submit_task({"lane": "media", "pool": "video", "kind": "comfy", "x": 1}), Invalid))
check("needs.addons must match the template", raises(lambda: pc.submit_task(TaskSpec(
    pool="video", kind="comfy", lane="media", needs=Needs(addons={"set": "s", "version": 1}))),
    Invalid) is not None)
check("fallback_after_s needs fallback", raises(lambda: pc.submit_task(
    {"lane": "media", "pool": "video", "kind": "comfy", "fallback_after_s": 5}), Invalid))
check("task() of a missing id → NotFound", raises(lambda: pc.task("nope"), NotFound) is not None)

rows = pc.tasks(lane="media", pool="video")
check("tasks(): newest first", [x["task_id"] for x in rows] == [a, "shot-14-i2v"])
check("tasks(labels=…): server label filter", [x["task_id"] for x in pc.tasks(labels={"shot": "14"})]
      == ["shot-14-i2v"])
check("tasks(labels=two): rest matched client-side",
      pc.tasks(labels={"shot": "14", "step": "enc"}) == [])
check("tasks(states=…)", pc.tasks(states=["running"]) == [] and len(pc.tasks(states="queued")) == 2)

# -- the pod side: claim → start → progress → event → artifact → done ------------------------
fake, pc = setup()
tid = pc.submit_task(spec)
check("a queued task makes the autoscaler ask for a pod",
      [x["reason"] for x in fake.pool_launches("media", "video")] == ["demand"]
      and fake.events_of("pool.scaled")[0]["detail"]["action"] == "launched")
pod = fake.pool_launches("media", "video")[0]["job_id"]
check("…a real fake job labelled with its pool", fake.job(pod)["labels"] == {"pool": "video",
                                                                              "lane": "media"})
fake.place(pod)
fake.ready(pod)
c = fake.claim_task("media/video", have_loaded=["ckpt:a"])
check("claim_task: held while its models fetch (pod_fetch)", c["task_id"] == tid
      and c["state"] == "held" and c["held_reason"] == "pod_fetch"
      and c["missing_models"] == ["diffusion_models/a.safetensors"])
check("claim: the pod's job gets the model as a mid-life addition",
      fake.job(pod)["models"]["diffusion_models/a.safetensors"] == "fetching")
check("claim: the pod holding a task is busy", fake.state(pod) == "busy")
check("claim view carries payload, workflow and lease",
      c["payload"] == {"shot": 14} and c["workflow"]["patch"] == {"12.inputs.seed": 1}
      and c["lease_s"] == 180)
check("task.claimed then task.held", kinds(fake, tid)[-2:] == ["task.claimed", "task.held"])
check("task(): pod facts and affinity", pc.task(tid)["pod"]["job_id"] == pod)
fake.advance(20)
fake.model_state(pod, "diffusion_models/a.safetensors", "present")     # the worker fetched it
check("task_start: held → running", fake.task_start(tid)["state"] == "running"
      and pc.task(tid)["timings"]["held_s"] == 20)
check("task_progress: lease renewed, cancel False",
      fake.task_progress(tid, 0.5, "half")["cancel"] is False and pc.task(tid)["progress"] == 0.5)
fake.task_progress(tid, 0.6)
check("task.progress rate-limited to one per 5 s",
      len(fake.events_of("task.progress", job_id=tid)) == 1)
fake.advance(5)
fake.task_progress(tid, 0.7)
check("…and the next one after 5 s", len(fake.events_of("task.progress", job_id=tid)) == 2)
check("task_event emits task.event", fake.task_event(tid, {"segment": 1})["emitted"]
      and fake.events_of("task.event")[-1]["detail"]["data"] == {"segment": 1})
check("task_event: a second within 1 s is dropped",
      fake.task_event(tid, {"segment": 2})["emitted"] is False)
fake.task_artifact(tid, "seg1.mp4", "https://cdn.example/seg1.mp4", size=10)
check("task_artifact: shows at once, with task.artifact",
      pc.task(tid)["artifacts"] == [{"name": "seg1.mp4", "url": "https://cdn.example/seg1.mp4",
                                     "size": 10}]
      and fake.events_of("task.artifact")[-1]["detail"]["artifact"]["name"] == "seg1.mp4")
fake.advance(60)
r = fake.task_done(tid, True, result={"frames": 81},
                   artifacts=[{"name": "out.mp4", "url": "https://cdn.example/out.mp4"}],
                   usage={"vram_peak_mb": 20000, "phases": {"run_s": 50}})
t = pc.task(tid)
check("task_done(ok) → succeeded with result and artifacts", r["state"] == "succeeded"
      and t["result"] == {"frames": 81} and [x["name"] for x in t["artifacts"]] == ["seg1.mp4", "out.mp4"])
check("usage: the pod's $/h over the task's wall", t["usage"]["cost_usd"] == round(0.5 * 85 / 3600, 6)
      and t["usage"]["vram_peak_mb"] == 20000 and t["usage"]["phases"] == {"run_s": 50.0}
      and t["usage"]["affinity_hit"] is True)
check("task.succeeded carries result", fake.events_of("task.succeeded")[-1]["detail"]["result"]
      == {"frames": 81})
check("attempts[]: one, succeeded, with the payload hash", len(t["attempts"]) == 1
      and t["attempts"][0]["outcome"] == "succeeded" and len(t["attempts"][0]["payload_sha"]) == 64)
check("the pod is idle once it holds nothing", fake.state(pod) == "idle")
ev = pc.events(job_id=tid)["events"]
check("apps polling events see the task.* stream", [e["kind"] for e in ev][0] == "task.queued"
      and ev[-1]["kind"] == "task.succeeded")
st = pc.pool_stats("media", "video")
check("pool_stats: per-kind counts and walls", st["kinds"]["comfy"]["counts"] == {"succeeded": 1}
      and st["kinds"]["comfy"]["run_s"]["p50"] == 65.0 and st["kinds"]["comfy"]["held_s"]["n"] == 1)
check("pool_stats(kind=other) is empty", pc.pool_stats("media", "video", kind="x")["kinds"] == {})
fake.advance(300)
check("the idle pool pod stops per its policy", fake.state(pod) == "done"
      and fake.events_of("pool.scaled")[-1]["detail"]["action"] == "stopped")

# -- cancel is final ---------------------------------------------------------------------------
fake, pc = setup()
tid = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
c = fake.claim_task("video")
fake.task_start(tid)
r = pc.cancel_task(tid, reason="user")
check("cancel_task on a running task", r["cancelled"] and r["was"] == "running" and "note" in r)
check("the pod hears cancel in its beat reply", fake.task_progress(tid, 0.9)["cancel"] is True)
check("lose_lease does not requeue a cancelled task", fake.lose_lease(tid) == "cancelled")
check("release does not requeue it", fake.task_release(tid, "draining") == {
    "released": False, "state": "cancelled"})
check("done is not accepted", fake.task_done(tid, True) == {
    "accepted": False, "state": "cancelled", "cancel": True})
check("start → the server's 409 body", fake.task_start(tid)["cancel"] is True)
fake.advance(3600)
check("the lease timer leaves it cancelled", pc.task(tid)["state"] == "cancelled"
      and pc.task(tid)["cancel_reason"] == "user")
check("cancel again: idempotent 200", pc.cancel_task(tid)["already"] is True)
check("no task.requeued ever", "task.requeued" not in kinds(fake, tid))
t2 = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
fake.claim_task("video")
fake.task_done(t2, True)
check("cancel a succeeded task → Conflict", raises(lambda: pc.cancel_task(t2), Conflict) is not None)

# -- lease loss, attempts, release ----------------------------------------------------------
fake, pc = setup()
tid = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", lease_s=60))
fake.claim_task("video", pod="pod-a")
check("lose_lease → queued at attempt 2", fake.lose_lease(tid) == "queued"
      and pc.task(tid)["attempt"] == 2)
check("task.requeued counted", fake.events_of("task.requeued")[-1]["detail"]["counted"] is True)
fake.claim_task("video", pod="pod-a")
fake.advance(61)
check("a lapsed lease on the clock at max_attempts → failed", pc.task(tid)["state"] == "failed"
      and "max_attempts" in pc.task(tid)["error"]["reason"])
check("attempts[] keeps both", [x["outcome"] for x in pc.task(tid)["attempts"]] == ["lost", "lost"])
tid = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
fake.claim_task("video", pod="pod-a")
check("release → queued, attempt not counted", fake.task_release(tid, "draining")["state"] == "queued"
      and pc.task(tid)["attempt"] == 1)
check("…not offered to that pod again", fake.claim_task("video", pod="pod-a") is None)
check("…another pod takes it", fake.claim_task("video", pod="pod-b")["task_id"] == tid)
fake.task_start(tid)
check("done(ok=False, retryable) → requeued at attempt 2",
      fake.task_done(tid, False, error={"reason": "oom"}, retryable=True)["state"] == "queued"
      and pc.task(tid)["attempt"] == 2)
fake.claim_task("video", pod="pod-c")
r = fake.task_done(tid, False, error={"stage": "run", "reason": "bad graph", "log_tail": "x"})
check("done(ok=False) → failed with the error", r["state"] == "failed"
      and pc.task(tid)["error"] == {"stage": "run", "reason": "bad graph", "log_tail": "x"})
tid = pc.submit_task(TaskSpec(pool="video", kind="media.nope", lane="media", max_releases=2))
fake.claim_task("video", pod="p1")
fake.task_release(tid, "no handler")
fake.claim_task("video", pod="p2")
check("'no handler' at max_releases → failed (stage handler)",
      fake.task_release(tid, "no handler")["state"] == "failed"
      and pc.task(tid)["error"]["stage"] == "handler")
fake2, pc2 = setup()
tid = pc2.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
pod = fake2.pool_launches("media", "video")[0]["job_id"]
fake2.place(pod)
fake2.ready(pod)
fake2.claim_task("video")
pc2.cancel(pod)
check("a pod that ends requeues its task (attempt + 1)", pc2.task(tid)["state"] == "queued"
      and pc2.task(tid)["attempt"] == 2)

# -- retry: same id, re-roll ---------------------------------------------------------------
fake, pc = setup()
tid = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", payload={"seed": 1},
                              workflow={"patch": {"3.inputs.seed": 1}}))
fake.claim_task("video")
fake.task_done(tid, False, error={"reason": "ugly"})
check("retry_task on running → Conflict before it fails",
      raises(lambda: pc.retry_task(pc.submit_task(TaskSpec(pool="video", kind="comfy",
                                                           lane="media"))), Conflict) is not None)
r = pc.retry_task(tid, payload={"seed": 2}, patch={"3.inputs.seed": 2})
check("retry_task → queued, same id, attempt 1", r == {"task_id": tid, "state": "queued",
                                                      "attempt": 1})
c = fake.claim_task("video", pod="p9")
while c and c["task_id"] != tid:
    c = fake.claim_task("video", pod="p9")
check("the re-roll hands the pod the new payload and patch",
      c["payload"] == {"seed": 2} and c["workflow"]["patch"] == {"3.inputs.seed": 2})
at = pc.task(tid)["attempts"]
check("attempts[] keeps the old payload hash beside the new",
      len(at) == 2 and at[0]["payload_sha"] != at[1]["payload_sha"] and at[0]["outcome"] == "failed")
x = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", payload={"p": 1}))
pc.cancel_task(x)
pc.retry_task(x)
check("a cancelled task can be retried (the app's explicit act)",
      pc.task(x)["state"] == "queued" and pc.task(x)["cancel_reason"] is None)
check("…with its payload unchanged", fake.claim_tasks("video", pod="pz", capacity=5)[-1]["payload"]
      == {"p": 1})

# -- priority and position -----------------------------------------------------------------
fake, pc = setup(never_launch=True)
ids = [pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media")) for _ in range(3)]
check("positions follow submit order", [pc.task(i)["position"] for i in ids] == [1, 2, 3])
r = pc.set_priority(ids[2], 5)
check("set_priority → {priority, position}", r == {"task_id": ids[2], "priority": 5, "position": 1})
check("…the others move down", [pc.task(i)["position"] for i in ids] == [2, 3, 1])
check("claim takes the highest priority first", fake.claim_task("video")["task_id"] == ids[2])
check("set_priority on a claimed task → Conflict", raises(lambda: pc.set_priority(ids[2], 1), Conflict))
check("the claimed task has no position", pc.task(ids[2])["position"] is None)
check("a priority out of range → Invalid", raises(lambda: pc.set_priority(ids[0], 5000), Invalid))

# -- bulk cancel -----------------------------------------------------------------------------
fake, pc = setup(never_launch=True)
shot = [pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", labels={"shot": "7"}))
        for _ in range(3)]
other = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", labels={"shot": "8"}))
fake.claim_task("video")                        # shot[0] is now on a pod
check("cancel_tasks(state=queued) leaves the claimed one",
      pc.cancel_tasks(lane="media", label={"shot": "7"}, state="queued") == 2
      and pc.task(shot[0])["state"] == "claimed")
check("cancel_tasks by label (all open states)", pc.cancel_tasks(lane="media", label="shot:7") == 1
      and pc.task(shot[0])["state"] == "cancelled" and pc.task(other)["state"] == "queued")
check("cancel_tasks(ids=…) counts only what it cancelled",
      pc.cancel_tasks(ids=[other, shot[1], "nope"]) == 1)

# -- fallback "app" → returned, never_launch -------------------------------------------------
fake, pc = setup()
pc.pool_put("media", "encode", {"image": "enc:1"}, scale={"never_launch": True},
            match={"accepts_from": ["video"]})
fb = pc.submit_task({"lane": "media", "pool": "encode", "kind": "media.encode",
                     "fallback": "app", "fallback_after_s": 60})
check("a never_launch pool asks for no pod", fake.pool_launches("media", "encode") == [])
fake.advance(61)
check("unclaimed past fallback_after_s → returned (final)", pc.task(fb)["state"] == "returned"
      and kinds(fake, fb)[-1] == "task.returned")
fb2 = pc.submit_task({"lane": "media", "pool": "video", "kind": "media.encode", "fallback": "app"})
check("a fallback task never causes a launch", fake.pool_launches("media", "video") == [])
fake.claim_task("video", pod="v1")
check("released → returned", fake.task_release(fb2, "busy")["state"] == "returned")
fb3 = pc.submit_task({"lane": "media", "pool": "encode", "kind": "media.encode", "fallback": "app"})
c = fake.claim_task("video", pod="v2")
check("an idle pod of an accepts_from pool claims it", c and c["task_id"] == fb3)
check("any pod-side failure returns it", fake.task_done(fb3, False, error={"reason": "x"})["state"]
      == "returned")
check("retry accepts a returned task", pc.retry_task(fb3)["state"] == "queued")

# -- deadlines, dependencies, timeouts ------------------------------------------------------------
fake, pc = setup(never_launch=True)
d = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", deadline_s=100))
fake.advance(101)
check("queued past deadline_s → expired", pc.task(d)["state"] == "expired"
      and kinds(fake, d)[-1] == "task.expired")
a = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
b = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", after=[a],
                            inputs={"clip": {"task": a, "artifact": "out.mp4"},
                                    "n": {"task": a, "result": "frames"}}))
check("a task whose `after` is open is not claimed", fake.claim_task("video", pod="d1")["task_id"] == a
      and fake.claim_task("video", pod="d2") is None)
fake.task_done(a, True, result={"frames": 81},
               artifacts=[{"name": "out.mp4", "url": "https://cdn.example/a.mp4"}])
c = fake.claim_task("video", pod="d2")
check("…then claimed, inputs resolved from the dependency", c["task_id"] == b
      and c["inputs"]["clip"]["url"] == "https://cdn.example/a.mp4" and c["inputs"]["n"] == {"value": 81})
h = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", inputs={
    "lora": {"ref": "store://art/sealed/l.seal", "sha256": "AB" * 32, "size": 5},
    "ext": {"ref": "https://cdn.example/v.wav"}}))
c = fake.claim_task("video", pod="d2")
check("a {ref, sha256, size} input: the claim carries url, ref, sha256 and size", c["task_id"] == h
      and c["inputs"]["lora"]["ref"] == "store://art/sealed/l.seal" and c["inputs"]["lora"]["url"]
      and c["inputs"]["lora"]["sha256"] == "ab" * 32 and c["inputs"]["lora"]["size"] == 5
      and c["inputs"]["ext"] == {"url": fake._presign("https://cdn.example/v.wav")})
fake.task_done(h, True)
for bad in ({"ref": "store://art/x", "sha256": "zz"}, {"ref": "store://art/x", "md5": "1"},
            {"ref": "http://x/a"}, {"ref": "store://art/x", "size": -1}):
    try:
        pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", inputs={"a": bad}))
        refused = False
    except Exception as e:
        refused = "422" in str(e) or getattr(e, "status", None) == 422
    check(f"a bad {{ref}} input is refused 422: {sorted(bad)}", refused)
x = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
y = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", after=[x]))
pc.cancel_task(x)
check("a dependency that ends cancelled fails the dependant (stage after)",
      pc.task(y)["state"] == "failed" and pc.task(y)["error"]["stage"] == "after")
tt = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", timeout_s=100, lease_s=600))
fake.claim_tasks("video", pod="d3", capacity=5)
fake.task_start(tt)
fake.advance(101)
check("running past timeout_s → failed (stage timeout)", pc.task(tt)["error"]["stage"] == "timeout")

# -- seeding ------------------------------------------------------------------------------------
fake, pc = setup(never_launch=True)
s = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", needs=Needs(models=[
    {"path": "loras/new.safetensors", "source": "hf://org/repo/new.safetensors"}])))
check("a model with a source the store lacks → held (seeding)",
      pc.task(s)["held_reason"] == "seeding" and pc.task(s)["seeding"][0]["state"] == "pending")
check("a seeding task is not claimed", fake.claim_task("video") is None)
fake.seed_done("loras/new.safetensors")
check("seed done → queued", pc.task(s)["state"] == "queued")
s2 = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media", needs=Needs(models=[
    {"path": "loras/bad.safetensors", "source": "https://example.com/bad"}])))
fake.seed_fail("loras/bad.safetensors", "404")
check("seed failed → failed (stage seeding)", pc.task(s2)["error"]["stage"] == "seeding")
pc.retry_task(s2)
check("retry re-arms the seed", pc.task(s2)["held_reason"] == "seeding"
      and fake.seeds["loras/bad.safetensors"]["state"] == "pending")

# -- autoscaler bookkeeping, expect hints ---------------------------------------------------------
fake, pc = setup(launch_when={"queued_s": 120, "queued_tasks": 3}, prelaunch=True)
pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
check("launch_when not met → no launch", fake.pool_launches("media", "video") == [])
fake.advance(120)
check("oldest waited queued_s → launch", len(fake.pool_launches("media", "video")) == 1)
for _ in range(4):
    pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
check("more demand → up to max_pods", len(fake.pool_launches("media", "video")) == 2)
check("pool_get lists its pods", [p["state"] for p in pc.pool_get("media", "video")["pods"]]
      == ["queued", "queued"])
fake, pc = setup(prelaunch=True)
r = pc.pool_expect("media", "video", in_s=60, tasks=1)
check("pool_expect: accepted, and a pod is launched for it", r["accepted"]
      and [x["reason"] for x in fake.pool_launches("media", "video")] == ["expect"])
fake.advance(661)
r = pc.pool_expect("media", "video", in_s=60, tasks=1)
check("a wrong hint → cooldown (accepted False, no error)", r["accepted"] is False
      and r["cooldown_until"] == fake.now + 900)
fake3, pc3 = setup()
check("expect on a pool without prelaunch → Conflict",
      raises(lambda: pc3.pool_expect("media", "video", in_s=1), Conflict) is not None)
fake, pc = setup(min_pods=1)
check("min_pods keeps one pod asked for", [x["reason"] for x in fake.pool_launches("media", "video")]
      == ["min_pods"])

# -- cost_share ------------------------------------------------------------------------------------
fake, pc = setup(never_launch=True)
v = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
e = pc.submit_task(TaskSpec(pool="video", kind="media.encode", lane="media"))
fake.claim_tasks("video", pod="shared", capacity=2, cost_hr=3.6)
fake.advance(100)
fake.task_done(e, True)
fake.task_done(v, True)
check("cost_share: a weight-0 kind rides free, the other pays the whole wall",
      pc.task(e)["usage"]["cost_usd"] == 0.0 and pc.task(v)["usage"]["cost_usd"] == 0.1)
check("the second claimed task counts as prefetched", pc.task(e)["usage"]["prefetched"] is True)

# -- hooks refuse what the server could not do -------------------------------------------------------
check("claim from an unknown pool → FakeError", raises(lambda: fake.claim_task("nope"), FakeError))
na = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media"))
check("an artifact for a task not on a pod is answered cancel, not raised",
      fake.task_artifact(na, "x.png", "https://cdn.example/x.png")["cancel"] is True)
fake.claim_tasks("video", pod="art", capacity=5)
check("an artifact with no store prefix and no https url → FakeError (the server 422s)",
      raises(lambda: fake.task_artifact(na, "x.png"), FakeError) is not None)
so = pc.submit_task(TaskSpec(pool="video", kind="comfy", lane="media",
                             outputs={"to": "store://art/shots/1", "names": ["a.png"]}))
c = fake.claim_tasks("video", pod="art2", capacity=5)[-1]
fake.task_artifact(so, "a.png", "ignored")
check("with outputs.to the artifact ref is <to><name>; claim carries PUT links",
      pc.task(so)["artifacts"][0]["ref"] == "store://art/shots/1/a.png"
      and set(c["outputs"]["put"]) == {"a.png"})

print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
