"""Timelines, statistics and pod logs: the client sends exactly the requests pod-control's
routes take (statsapi.py, podlogs.py, GET /v1/jobs/{id}), and FakeControl answers them
from its in-memory jobs and tasks. No network."""
import datetime as dt
import json
import sys

sys.path.insert(0, "src")
from pod_loader.app import (AppAPIError, Client, Conflict, Gpu, Invalid, JobSpec, NotFound,
                            Policy, TaskSpec)
from pod_loader.app.client import httpx
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


# == the requests ============================================================================

seen = []
replies = {}


def handler(request):
    body = json.loads(request.content) if request.content else None
    seen.append((request.method, request.url.path, dict(request.url.params), body))
    status, out = replies.pop((request.method, request.url.path), (200, {}))
    if isinstance(out, str):
        return httpx.Response(status, text=out, headers={"content-type": "application/x-ndjson"})
    return httpx.Response(status, json=out)


pc = Client("http://control.test", "tok", transport=httpx.MockTransport(handler))


def last():
    return seen[-1]


print("requests: timelines and stats")
replies[("GET", "/v1/jobs/j 1")] = (200, {"job_id": "j 1", "timeline": {"job_id": "j 1", "pull_s": 3}})
check("job_timeline reads GET /v1/jobs/{id} → timeline",
      pc.job_timeline("j 1") == {"job_id": "j 1", "pull_s": 3}
      and last()[:2] == ("GET", "/v1/jobs/j 1"))
replies[("GET", "/v1/jobs/j2")] = (200, {"job_id": "j2"})
check("job_timeline of an older server's reply is {}", pc.job_timeline("j2") == {})

replies[("GET", "/v1/stats/boots")] = (200, {"groups": [{"key": "L40"}]})
out = pc.boot_stats(lane="media", gpu="L40", since=1000, until=dt.datetime(2026, 1, 1),
                    group_by="gpu")
check("boot_stats: GET /v1/stats/boots with the filters, epoch since/until, group_by",
      out == {"groups": [{"key": "L40"}]} and last()[:2] == ("GET", "/v1/stats/boots")
      and last()[2] == {"lane": "media", "gpu": "L40", "since": "1000.0",
                        "until": str(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp()),
                        "group_by": "gpu"})
pc.boot_stats(image="img:1", data_center="EU-RO-1", resumed=True, since="2026-10-01T00:00:00Z")
check("boot_stats: image, data_center, resumed True → 1, ISO since passed through",
      last()[2] == {"image": "img:1", "data_center": "EU-RO-1", "resumed": "1",
                    "since": "2026-10-01T00:00:00Z"})
pc.boot_stats(resumed="all")
check("boot_stats: nothing set sends only what was given", last()[2] == {"resumed": "all"})
check("a bad since type is refused locally", raises(lambda: pc.boot_stats(since=[1]), TypeError))

replies[("GET", "/v1/stats/boots/raw")] = (200, '{"job_id":"a","pull_s":1}\n\n{"job_id":"b"}\n')
rows = pc.boot_stats_raw(lane="media", limit=50, resumed="0")
check("boot_stats_raw parses the JSON lines (blank lines skipped)",
      rows == [{"job_id": "a", "pull_s": 1}, {"job_id": "b"}]
      and last()[:3] == ("GET", "/v1/stats/boots/raw", {"lane": "media", "resumed": "0", "limit": "50"}))
replies[("GET", "/v1/stats/boots/raw")] = (200, '{"job_id":"a"}\nnot json\n')
e = raises(lambda: pc.boot_stats_raw(), AppAPIError)
check("a line that is not JSON raises AppAPIError naming the line", e and "line 2" in str(e))

pc.task_stats(lane="media", pool="video", kind="comfy", since=5, group_by="gpu")
check("task_stats: GET /v1/stats/tasks with lane, pool, kind, since, group_by",
      last()[:3] == ("GET", "/v1/stats/tasks", {"lane": "media", "pool": "video", "kind": "comfy",
                                                "since": "5.0", "group_by": "gpu"}))
pc.spend_stats(lane="media", until=10, group_by="pool")
check("spend_stats: GET /v1/stats/spend with lane, until, group_by",
      last()[:3] == ("GET", "/v1/stats/spend", {"lane": "media", "until": "10.0", "group_by": "pool"}))

print("\nrequests: pod logs")
replies[("POST", "/v1/jobs/j1/logs/request")] = (202, {"req_id": "r1"})
check("request_logs: POST /v1/jobs/{id}/logs/request {lines: 200} by default",
      pc.request_logs("j1") == {"req_id": "r1"}
      and last()[:2] == ("POST", "/v1/jobs/j1/logs/request") and last()[3] == {"lines": 200})
pc.request_logs("j1", names=["workload", "boot"], lines=50)
check("request_logs with names and lines", last()[3] == {"lines": 50, "names": ["workload", "boot"]})
pc.request_logs("j1", names="workload")
check("a single name string becomes a list", last()[3] == {"lines": 200, "names": ["workload"]})
pc.request_logs("j1", names=[])
check("names=[] is sent (every log)", last()[3] == {"lines": 200, "names": []})
replies[("GET", "/v1/jobs/j1/logs")] = (200, {"logs": [], "pending": True})
check("logs: GET /v1/jobs/{id}/logs, no name", pc.logs("j1") == {"logs": [], "pending": True}
      and last()[:3] == ("GET", "/v1/jobs/j1/logs", {}))
pc.logs("j1", name="workload")
check("logs with ?name=", last()[2] == {"name": "workload"})
replies[("GET", "/v1/jobs/j1/logs/app_serve/download")] = (200, {"url": "https://s/x?sig"})
check("log_download_url: GET …/logs/{name}/download → the url",
      pc.log_download_url("j1", "app_serve") == "https://s/x?sig"
      and last()[:2] == ("GET", "/v1/jobs/j1/logs/app_serve/download"))
replies[("GET", "/v1/jobs/j1/logs/x/download")] = (404, {"error": "no log store configured"})
e = raises(lambda: pc.log_download_url("j1", "x"), NotFound)
check("the server's top-level 404 {error} becomes NotFound with its words",
      e and e.error == "no log store configured")
replies[("POST", "/v1/jobs/j1/logs/request")] = (409, {"detail": {"error": "job j1 is done"}})
check("409 → Conflict", raises(lambda: pc.request_logs("j1"), Conflict))
check("an empty log name is refused locally", raises(lambda: pc.log_download_url("j1", ""), ValueError))

# == FakeControl ==============================================================================

fake = FakeControl(tokens={"app-tok": ["media"], "other-tok": ["other"]})
pc = fake.client()
app = fake.client(token="app-tok")
other = fake.client(token="other-tok")


def spec(job_id, lane="media", **pol):
    return JobSpec(lane=lane, image="registry.example/app:1", job_id=job_id,
                   gpu=Gpu(count=1, min_vram_gb=24), policy=Policy(idle_stop_s=300, **pol),
                   queue_deadline_s=900)


print("\nfake: logs")
pc.submit(spec("j1"))
check("asking before placement is allowed (the pod answers once it heartbeats)",
      app.request_logs("j1")["lines"] == 200 and fake.log_request("j1")["names"] == [])
fake.place("j1", gpu="NVIDIA L40", cost_hr=0.9, data_center="EU-RO-1")
r = app.request_logs("j1", names=["workload"], lines=100)
check("a second request replaces the first; log_request shows the alive reply's logs",
      fake.log_request("j1") == {"names": ["workload"], "lines": 100, "req_id": r["req_id"]})
check("pending before the pod answers", app.logs("j1") == {"job_id": "j1", "logs": [], "pending": True})
for bad, why in (({"lines": 0}, "lines 0"), ({"lines": 2001}, "lines 2001"),
                 ({"names": ["../x"]}, "a bad name"),
                 ({"names": [f"n{i}" for i in range(17)]}, "17 names")):
    check(f"fake refuses {why} (Invalid)",
          raises(lambda: app.request_logs("j1", **bad), Invalid))
check("another lane's token gets NotFound", raises(lambda: other.logs("j1"), NotFound))
fake.push_logs("j1", "workload", ["a", "b"], req_id="stale")
check("a push with a stale req_id leaves the request pending", app.logs("j1")["pending"] is True)
out = fake.push_logs("j1", "workload", ["c", "d"], req_id=r["req_id"])
got = app.logs("j1")
check("the matching push clears it, and the tail is replaced",
      out["cleared"] and got["pending"] is False and got["logs"][0]["lines"] == ["c", "d"]
      and got["logs"][0]["truncated"] is False and got["logs"][0]["at"] == fake.now)
fake.push_logs("j1", "boot", ["x"], truncated=True)
check("names filter with name=", [x["name"] for x in app.logs("j1", name="boot")["logs"]] == ["boot"]
      and app.logs("j1", name="boot")["logs"][0]["truncated"] is True)
fake.push_logs("j1", "big", ["z" * 1000] * 400)
big = app.logs("j1", name="big")["logs"][0]
check("a tail over 256 KB keeps the newest lines and is truncated",
      big["truncated"] and len(big["lines"]) < 400)
check("pod.logs events name the logs only",
      all(set(e["detail"]) <= {"names", "req_id"} for e in fake.events_of("pod.logs")))
app.request_logs("j1")
fake.advance(601)
check("a request expires after 10 min of fake clock",
      fake.log_request("j1") is None and app.logs("j1")["pending"] is False)
check("push_logs with a bad name raises FakeError",
      raises(lambda: fake.push_logs("j1", "a/b", []), FakeError))
e = raises(lambda: app.log_download_url("j1", "workload"), NotFound)
check("download with no log store: NotFound 'no log store configured'",
      e and e.error == "no log store configured")
check("upload_logs with no log store raises FakeError",
      raises(lambda: fake.upload_logs("j1", ["workload"]), FakeError))
fake.log_store = "logs"
refs = fake.upload_logs("j1", ["workload", "boot"], sizes={"boot": 12})
check("upload_logs records refs under logs/<lane>/<job_id>/<name>.log.gz",
      refs["workload"] == "store://logs/logs/media/j1/workload.log.gz")
check("download gives a fake presigned url",
      app.log_download_url("j1", "workload").startswith("https://fake-store.test/logs/media/j1/workload.log.gz"))
check("download of a name never uploaded: NotFound",
      raises(lambda: app.log_download_url("j1", "nope"), NotFound))

print("\nfake: timeline")
fake.boot_line("j1", "pulling done", stage="binaries")
fake.advance(20)
fake.boot_line("j1", "models", stage="models")
fake.advance(40)
fake.ready("j1")
t = app.job_timeline("j1")
check("timeline: placed, pull_s, ready, boot_total_s, gpu without 'NVIDIA '",
      t["placed_at"] is not None and t["pull_s"] is not None and t["ready_at"] == fake.now
      and t["boot_total_s"] == round(t["ready_at"] - t["placed_at"], 3) and t["gpu"] == "L40"
      and t["data_center"] == "EU-RO-1" and t["placement"]["outcomes"] == {"placed": 1})
check("timeline stages: binaries 20 s ok, models 40 s ok",
      [(s["stage"], s["s"], s["outcome"]) for s in t["stages"]]
      == [("binaries", 20.0, "ok"), ("models", 40.0, "ok")])
check("timeline.logs lists the uploads (size when given)",
      t["logs"] == [{"name": "boot", "ref": "store://logs/logs/media/j1/boot.log.gz", "size": 12},
                    {"name": "workload", "ref": "store://logs/logs/media/j1/workload.log.gz"}])
fake.advance(3600)
check("cost_usd is the job's spend so far", t["cost_usd"] < app.job_timeline("j1")["cost_usd"])
check("the idle stop ended it", fake.state("j1") == "done")
check("asking an ended job: Conflict", raises(lambda: app.request_logs("j1"), Conflict))

print("\nfake: boot stats")
pc.submit(spec("j2"))
fake.place("j2", gpu="A40", cost_hr=0.4)
fake.boot_line("j2", "gpu", stage="gpu_check")
fake.advance(30)
fake.fail_boot("j2", "gpu_check", "CUDA unknown error")
pc.submit(spec("j3"))                                   # never placed
pc.submit(spec("o1", lane="other"))
fake.place("o1")
fake.ready("o1")
s = pc.boot_stats(group_by="gpu")
by = {g["key"]: g for g in s["groups"]}
check("boot_stats groups by gpu: L40 ready, A40 failed at gpu_check, unplaced under unknown",
      by["L40"]["ready"] == 1 and by["L40"]["success_rate"] == 1.0
      and by["A40"]["failed"] == 1 and by["A40"]["success_rate"] == 0.0
      and by["A40"]["failures"] == [{"stage": "gpu_check", "reason": "CUDA unknown error", "n": 1}]
      and by["unknown"]["unplaced"] == 1 and by["unknown"]["n"] == 0)
check("stage percentiles from completed stages", by["L40"]["stages"]["models"]["p50"] == 40.0)
one = pc.boot_stats()
check("no group_by: one group 'all' with every job",
      [g["key"] for g in one["groups"]] == ["all"] and one["groups"][0]["jobs"] == 4)
conf = app.boot_stats()
check("a confined token sees only its lanes", conf["groups"][0]["jobs"] == 3)
check("naming another lane: NotFound", raises(lambda: app.boot_stats(lane="other"), NotFound))
check("bad group_by: Invalid", raises(lambda: pc.boot_stats(group_by="color"), Invalid))
check("bad resumed: Invalid", raises(lambda: pc.boot_stats(resumed="2"), Invalid))
check("until before since: Invalid", raises(lambda: pc.boot_stats(since=10, until=5), Invalid))
check("a window that excludes them: no groups",
      pc.boot_stats(since=fake.now + 10, until=fake.now + 20)["groups"] == [])
raw = pc.boot_stats_raw(lane="media", limit=2)
check("boot_stats_raw: timelines oldest first, limited",
      [r["job_id"] for r in raw] == ["j1", "j2"] and "stages" in raw[0])
check("boot_stats_raw filters by gpu", [r["job_id"] for r in pc.boot_stats_raw(gpu="NVIDIA A40")] == ["j2"])

print("\nfake: task stats")
pc.pool_put("media", "video", {"image": "img:1", "gpu": {"min_vram_gb": 24},
                               "queue_deadline_s": 900}, scale={"max_pods": 1})
tid = pc.submit_task(TaskSpec(lane="media", pool="video", kind="comfy"))
tid2 = pc.submit_task(TaskSpec(lane="media", pool="video", kind="other.kind"))
pod = fake.pool_launches("media", "video")[0]["job_id"]
fake.place(pod)
fake.ready(pod)
fake.claim_task("media/video")
fake.advance(5)
fake.task_start(tid)
fake.advance(10)
fake.task_done(tid, True, result={})
ts = pc.task_stats(lane="media")
byk = {g["key"]: g for g in ts["groups"]}
check("task_stats by kind: outcomes, run_s, queued_s",
      ts["group_by"] == "kind" and byk["comfy"]["outcomes"] == {"succeeded": 1}
      and byk["comfy"]["run_s"]["p50"] == 10.0 and byk["comfy"]["queued_s"]["n"] == 1
      and byk["other.kind"]["outcomes"] == {"queued": 1})
check("task_stats kind= filters", [g["key"] for g in pc.task_stats(kind="comfy")["groups"]] == ["comfy"])
check("task_stats group_by=gpu", pc.task_stats(group_by="gpu")["group_by"] == "gpu")
check("task_stats bad group_by: Invalid", raises(lambda: pc.task_stats(group_by="pool"), Invalid))

print("\nfake: spend stats")
sp = pc.spend_stats(group_by="lane")
lanes = {r["key"]: r for r in sp["rows"]}
check("spend by lane: both lanes, totals add up",
      set(lanes) == {"media", "other"} and lanes["media"]["usd"] > 0
      and abs(sp["total_usd"] - sum(r["usd"] for r in sp["rows"])) < 1e-6)
check("spend by gpu names the cards", {"L40", "A40"} <= {r["key"] for r in pc.spend_stats(group_by="gpu")["rows"]})
check("spend by pool: the pool pod under video, the rest under none",
      {r["key"] for r in pc.spend_stats(group_by="pool")["rows"]} == {"video", "none"})
days = pc.spend_stats()
check("spend by day (default): UTC day keys", days["group_by"] == "day"
      and all(len(r["key"]) == 10 for r in days["rows"]))
check("a confined token's spend is its lanes only",
      {r["key"] for r in app.spend_stats(group_by="lane")["rows"]} == {"media"})
check("spend_stats bad group_by: Invalid", raises(lambda: pc.spend_stats(group_by="x"), Invalid))

print(f"\n{sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
