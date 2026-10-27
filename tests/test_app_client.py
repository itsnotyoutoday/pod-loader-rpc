"""pod_loader.app.Client against FakeControl: every endpoint, the request it sends, and
the errors it raises. No network: the fake is served through httpx's MockTransport."""
import datetime as dt
import json
import sys

sys.path.insert(0, "src")
from pod_loader.app import (Boot, Client, Conflict, Gpu, Invalid, JobSpec, NotFound, Policy,
                            Unauthorized, Unreachable, spec_dict)
from pod_loader.app.client import httpx
from pod_loader.app.testing import FakeControl

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


def raises(fn, exc):
    try:
        fn()
    except exc as e:
        return e
    except Exception as e:                     # wrong type is a failure, with its name
        print(f"     raised {type(e).__name__}: {e}")
        return None
    return None


SHA = "0123456789abcdef0123456789abcdef01234567"


def spec(**kw):
    base = dict(lane="media", image="registry.example/app:1",
                gpu=Gpu(count=1, min_vram_gb=24, clouds=["SECURE"], datacenter_only=True),
                ports=["8188/http"], env={"SECRET": "x"},
                boot=Boot(models=Boot.models_from("models", ["a/x.safetensors",
                                                             {"path": "b/y.safetensors",
                                                              "sha256": "00", "size": 3}])),
                policy=Policy(idle_stop_s=300, park_ttl_s=3600, max_life_s=14400,
                              boot_stall_s=300, boot_timeout_s=2700),
                labels={"affinity": "a"})
    base.update(kw)
    return JobSpec(**base)


# -- spec serialisation ------------------------------------------------------------------
d = spec().to_dict()
check("JobSpec serialises the spec's field names", d["gpu"]["min_vram_gb"] == 24
      and d["policy"]["idle_stop_s"] == 300 and d["queue_deadline_s"] == 900)
check("unset fields are left out, so server defaults apply",
      "types" not in d["gpu"] and "reuse" not in d and "keep_warm_until" not in d["policy"])
check("boot models keep path strings and {path, sha256, size}",
      d["boot"]["models"]["paths"][1] == {"path": "b/y.safetensors", "sha256": "00", "size": 3})
check("extra fields merge in unchanged",
      JobSpec(lane="l", image="i", extra={"future": 1}).to_dict()["future"] == 1)
check("a dict spec passes through", spec_dict({"lane": "l", "image": "i"}) == {"lane": "l", "image": "i"})
check("a non-spec is refused", raises(lambda: spec_dict(42), TypeError) is not None)

# -- request shape: header, path, body ---------------------------------------------------
seen = []


def recorder(req):
    seen.append(req)
    return httpx.Response(202, json={"job_id": "j1", "state": "queued", "lane": "media",
                                     "resumed_from": None})


rc = Client("http://control.test", "tok-1", transport=httpx.MockTransport(recorder))
check("submit returns the job id", rc.submit(spec(job_id="j1")) == "j1")
check("the token rides in X-Podh-Token", seen[0].headers["x-podh-token"] == "tok-1")
check("submit is POST /v1/jobs with the spec as JSON",
      seen[0].method == "POST" and seen[0].url.path == "/v1/jobs"
      and json.loads(seen[0].content)["job_id"] == "j1")

# -- against the fake ----------------------------------------------------------------------
fake = FakeControl()
pc = fake.client()

r = pc.submit_reply(spec(job_id="j-a", idempotency_key="k-a"))
check("submit reply: queued, lane, resumed_from", r == {"job_id": "j-a", "state": "queued",
                                                       "lane": "media", "resumed_from": None})
again = pc.submit_reply(spec(job_id="j-a2", idempotency_key="k-a"))
check("an idempotent retry returns the first job", again["job_id"] == "j-a" and again["existing"])
e = raises(lambda: pc.submit(spec(job_id="j-a")), Conflict)
check("a reused job_id is a Conflict with the server's text", e is not None
      and "already exists" in e.error and e.status == 409 and "idempotency_key" in e.hint)
e = raises(lambda: pc.submit({"lane": "media", "queue_deadline_s": 60}), Invalid)
check("a spec without an image is Invalid (422)", e is not None and "image" in str(e))
e = raises(lambda: pc.submit(spec(boot=Boot(addons={"set": "s"}))), Invalid)
check("an addon set without a version is Invalid", e is not None and "version" in e.error)

g = pc.get("j-a")
check("get returns the job", g["state"] == "queued" and g["labels"] == {"affinity": "a"}
      and g["pod"] is None)
check("env is never read back", "env" not in json.dumps(g) or "SECRET" not in json.dumps(g))
check("get on an unknown job is NotFound", raises(lambda: pc.get("nope"), NotFound) is not None)

fake.place("j-a", gpu="CARD-X", cost_hr=0.9)
g = pc.get("j-a")
check("placed: booting, pod facts, urls from ports",
      g["state"] == "booting" and g["pod"]["gpu"] == "CARD-X"
      and g["urls"]["8188"].startswith("https://"))
fake.boot_line("j-a", "gpu ok", stage="gpu_check")
fake.ready("j-a")
check("ready: boot models present", pc.get("j-a")["models"] ==
      {"a/x.safetensors": "present", "b/y.safetensors": "present"})
check("boot lines are on the job", pc.get("j-a")["boot"][0]["line"] == "gpu ok")

pc.submit(spec(job_id="j-b", lane="media"))
pc.submit(spec(job_id="j-c", lane="other"))
check("list filters by lane", {j["job_id"] for j in pc.list("media")} == {"j-a", "j-b"})
check("list filters by states (list or csv)",
      [j["job_id"] for j in pc.list("media", ["ready", "busy"])] == ["j-a"]
      and [j["job_id"] for j in pc.list(states="queued,parked")] == ["j-c", "j-b"])

# warm
w = pc.warm("j-a", delta_s=600)
t0 = dt.datetime.fromtimestamp(fake.now, dt.timezone.utc)
check("warm delta_s from now when nothing is held",
      w["keep_warm_until"] == (t0 + dt.timedelta(seconds=600)).isoformat())
w = pc.warm("j-a", delta_s=-300)
check("warm delta_s moves the current warm-until, negative too",
      w["keep_warm_until"] == (t0 + dt.timedelta(seconds=300)).isoformat())
w = pc.warm("j-a", until=t0 + dt.timedelta(seconds=999))
check("warm until accepts a datetime", w["keep_warm_until"] == (t0 + dt.timedelta(seconds=999)).isoformat())
check("warm needs exactly one of until/delta_s", raises(lambda: pc.warm("j-a"), ValueError)
      and raises(lambda: pc.warm("j-a", until=1, delta_s=1), ValueError))
check("warm on an unknown job is NotFound", raises(lambda: pc.warm("zz", delta_s=1), NotFound))

# models
m = pc.add_models("j-a", ["c/z.safetensors", {"path": "d/w.safetensors", "sha256": "1", "size": 2}])
check("add_models bumps models_rev and marks fetching", m["models_rev"] == 1
      and pc.get("j-a")["models"]["c/z.safetensors"] == "fetching")
check("add_models with nothing is refused locally", raises(lambda: pc.add_models("j-a", []), ValueError))
fake.model_state("j-a", "c/z.safetensors", "present")
fake.model_state("j-a", "d/w.safetensors", "failed", error="403 from bucket")
g = pc.get("j-a")
check("a mid-life model that fails is reported and the pod lives",
      g["models"]["d/w.safetensors"] == "failed" and g["state"] == "ready"
      and g["model_errors"]["d/w.safetensors"] == "403 from bucket")

# park / resume
p = pc.park("j-a", ttl_s=120)
check("park returns parked with park_until", p["state"] == "parked" and p["park_until"])
check("resume of a parked job boots it", pc.resume("j-a")["state"] == "booting")
e = raises(lambda: pc.resume("j-a"), Conflict)
check("resume of a job that is not parked is a Conflict", e is not None and "booting" in e.error)
fake.ready("j-a")

# drain
dr = pc.drain("j-a")
check("drain answers draining", dr["draining"] is True and pc.get("j-a")["pod"]["draining"])
check("drain of a queued job is a Conflict", raises(lambda: pc.drain("j-b"), Conflict))

# cancel
c = pc.cancel("j-b", reason="no longer needed")
check("cancel from queued", c["cancelled"] and c["was"] == "queued"
      and pc.get("j-b")["state"] == "cancelled")
check("cancel twice is a Conflict", raises(lambda: pc.cancel("j-b"), Conflict))
check("the reason reaches the server", pc.get("j-b")["reason"] == "cancelled: no longer needed")

# spend
fake.set_lane_cap("media", daily_usd=50, max_pods=2)
s = pc.spend("media")
check("spend has the r3 fields", set(s) >= {"today_usd", "rolling_24h_usd", "running_usd",
                                             "parked_usd", "cap_usd", "max_pods", "breached",
                                             "live_pods"})
check("spend carries the lane cap", s["cap_usd"] == 50 and s["max_pods"] == 2
      and s["breached"] is False and s["live_pods"] == 1)

# events
ev = pc.events()
check("events from 0, oldest first, with cursors", ev["events"][0]["seq"] == 1
      and ev["next_seq"] == ev["last_seq"] == len(fake.event_log))
ev2 = pc.events(since_seq=ev["next_seq"], wait=5)
check("events after the cursor: none yet, and wait does not block the fake",
      ev2["events"] == [] and ev2["next_seq"] == ev["next_seq"])
kinds = [x["kind"] for x in pc.events(job_id="j-a")["events"]]
check("events filter by job", kinds[:3] == ["job.queued", "job.running", "pod.boot"]
      and "pod.parked" in kinds and "pod.resumed" in kinds and "pod.draining" in kinds)
check("events filter by kind", {x["kind"] for x in pc.events(kind="pod.ready")["events"]}
      == {"pod.ready"})

# addons
item = {"name": "Node-Pack", "repo": "https://github.com/owner/repo", "commit": SHA,
        "pip": ["pkg==1.0"], "links": [{"file": "models/x/", "to": "comfy:models/x"}],
        "probe": "import x"}
check("addon_put returns version 1, then 2", pc.addon_put("media", "comfy", [item]) == 1
      and pc.addon_put("media", "comfy", [item], note="again") == 2)
check("addon_get latest and by version", pc.addon_get("media", "comfy")["version"] == 2
      and pc.addon_get("media", "comfy", 1)["note"] == "")
check("addons lists sets", pc.addons()["sets"] == [{"lane": "media", "name": "comfy", "version": 2}])
exp = pc.addon_export("media", "comfy")
check("addon_export returns the file text", json.loads(exp)["items"][0]["commit"] == SHA)
e = raises(lambda: pc.addon_put("media", "comfy", [{**item, "repo": "https://gitlab.com/a/b"}]), Invalid)
check("a non-GitHub repo is refused naming the item", e is not None and "Node-Pack" in e.error)
fake.known_commits["https://github.com/owner/repo"] = [SHA, "0123456fff" + "0" * 30]
e = raises(lambda: pc.addon_put("media", "comfy", [{**item, "commit": "0123456"}]), Invalid)
check("an ambiguous short pin is a 422 naming the item", e is not None and "ambiguous" in e.error)
v = pc.addon_put("media", "comfy", [{**item, "commit": "0123456789"}])
check("a short pin resolves to the full SHA", pc.addon_get("media", "comfy", v)["items"][0]["commit"] == SHA)
e = raises(lambda: pc.addon_put("media", "comfy", [{**item, "links": [{"file": "../x", "to": "y"}]}]), Invalid)
check("a link outside its root is refused", e is not None and "leaves its root" in e.error)
check("an unknown set is NotFound", raises(lambda: pc.addon_get("media", "nope"), NotFound))

# auth
bad = fake.client(token="wrong")
e = raises(lambda: bad.get("j-a"), Unauthorized)
check("a bad token is Unauthorized with the server's text", e is not None and e.status == 401
      and "X-Podh-Token" in e.error)
fake.tokens["confined"] = ["other"]
cc = fake.client(token="confined")
check("a confined token cannot see another lane's job", raises(lambda: cc.get("j-a"), NotFound))
check("a confined token cannot submit to another lane",
      raises(lambda: cc.submit(spec(lane="media")), Unauthorized))
check("a confined token cannot read another lane's spend", raises(lambda: cc.spend("media"), Unauthorized))
check("a confined token cannot write another lane's addons",
      raises(lambda: cc.addon_put("media", "comfy", [item]), Unauthorized))
check("a confined token defaults to its own lane",
      cc.submit_reply(JobSpec(lane="", image="i"))["lane"] == "other")
check("a confined token lists only its lanes", {j["lane"] for j in cc.list()} == {"other"})


# unreachable
def boom(req):
    raise httpx.ConnectError("refused", request=req)


e = raises(lambda: Client("http://x", "t", transport=httpx.MockTransport(boom)).get("j"), Unreachable)
check("a transport failure is Unreachable, status None", e is not None and e.status is None)


def plain(req):
    return httpx.Response(500, text="upstream exploded")


e = raises(lambda: Client("http://x", "t", transport=httpx.MockTransport(plain)).get("j"), Exception)
check("a non-JSON error keeps the server's text", e is not None and "upstream exploded" in str(e))
check("job ids are path-quoted", raises(lambda: pc.get("a/b"), NotFound) is not None)
check("an empty job id is refused locally", raises(lambda: pc.get(""), ValueError))

with fake.client() as c2:
    check("Client is a context manager", c2.get("j-a")["job_id"] == "j-a")

# -- GET /v1/gpus ------------------------------------------------------------------------
_gc = FakeControl().client()
_rows = _gc.gpus(min_vram_gb=40, datacenter_only=True, max_cost_hr=0.5)
check("gpus: VRAM floor and data-centre only, in walk order",
      [r["gpu"] for r in _rows] == ["NVIDIA A40", "NVIDIA L40S"])
check("gpus: over max_cost_hr is flagged, not dropped",
      _rows[0]["over_max_cost"] is False and _rows[1]["over_max_cost"] is True)
check("gpus: consumer cards listed unless datacenter_only",
      [r["gpu"] for r in _gc.gpus(min_vram_gb=24)][:2] == ["NVIDIA RTX A5000", "NVIDIA GeForce RTX 4090"])

print(f"\n  {sum(ok)}/{len(ok)} passed")

sys.exit(0 if all(ok) else 1)

