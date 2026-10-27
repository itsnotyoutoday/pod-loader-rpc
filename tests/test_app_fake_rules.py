"""FakeControl follows the state rules of docs/APP-API.md r3: the timers on a manual clock,
idle only when not busy and nothing fetching, drain, warm, park/resume, cancel while
placing, reuse parked-first, spend and cap, lane backoff. Driven through the Client."""
import sys

sys.path.insert(0, "src")
from pod_loader.app import Boot, Gpu, JobSpec, Policy
from pod_loader.app.testing import FakeControl, FakeError, FakeHeld

ok = []


def check(name, cond):
    ok.append(bool(cond))
    print(f"  {'✅' if cond else '❌'} {name}")


def spec(job_id, lane="media", queue_deadline_s=900, **pol):
    policy = dict(idle_stop_s=300, park_ttl_s=3600, max_life_s=14400, boot_stall_s=300,
                  boot_timeout_s=2700)
    policy.update(pol)
    return JobSpec(lane=lane, image="registry.example/app:1", job_id=job_id,
                   gpu=Gpu(count=1, min_vram_gb=24),
                   boot=Boot(models=Boot.models_from("p", ["m/a"]),
                             addons={"set": "comfy", "version": 3}),
                   policy=Policy(**policy), queue_deadline_s=queue_deadline_s)


def up(fake, pc, job_id, **kw):
    """Submit, place and make ready."""
    pc.submit(spec(job_id, **kw))
    fake.place(job_id)
    fake.ready(job_id)


def kinds(fake, job_id):
    return [e["kind"] for e in fake.events_of(job_id=job_id)]


# -- queue deadline ----------------------------------------------------------------------
fake = FakeControl(); pc = fake.client()
pc.submit(spec("q1"))
fake.advance(899)
check("queued until the deadline", pc.get("q1")["state"] == "queued")
fake.advance(1)
check("expired at queue_deadline_s", pc.get("q1")["state"] == "expired"
      and "job.expired" in kinds(fake, "q1"))

# -- boot stall and boot timeout -----------------------------------------------------------
fake = FakeControl(); pc = fake.client()
pc.submit(spec("b1")); fake.place("b1")
fake.advance(200); fake.boot_line("b1", "models 40%", stage="models")
fake.advance(200)
check("a boot line resets the stall clock", pc.get("b1")["state"] == "booting")
fake.advance(100)
g = pc.get("b1")
check("no boot line for boot_stall_s → failed at the last stage",
      g["state"] == "failed" and g["reason"] == "boot_stall" and g["stage"] == "models")
check("the stall is reported as pod.unhealthy", "pod.unhealthy" in kinds(fake, "b1"))

pc.submit(spec("b2", boot_stall_s=10_000, boot_timeout_s=1000)); fake.place("b2")
for _ in range(9):
    fake.advance(100); fake.boot_line("b2", "still going")
fake.advance(100)
check("boot_timeout_s caps the boot however much progress",
      pc.get("b2")["reason"] == "boot_timeout")

# -- boot events are rate-limited ----------------------------------------------------------
fake = FakeControl(); pc = fake.client()
pc.submit(spec("e1")); fake.place("e1")
fake.boot_line("e1", "a", stage="gpu_check")
fake.boot_line("e1", "b")
fake.advance(3); fake.boot_line("e1", "c", stage="models")
fake.advance(3); fake.boot_line("e1", "d")
fake.advance(10); fake.boot_line("e1", "e")
boots = [e["detail"]["line"] for e in fake.events_of("pod.boot", "e1")]
check("pod.boot: one per 10 s per job, or on a stage change", boots == ["a", "c", "e"])
check("every line is kept on the job", [b["line"] for b in pc.get("e1")["boot"]] == list("abcde"))

# -- never idle while booting; idle clock; park; park TTL ------------------------------------
fake = FakeControl(); pc = fake.client()
pc.submit(spec("i1")); fake.place("i1")
fake.set_busy("i1", False)
fake.advance(299)
check("never idle while booting (an idle heartbeat is ignored)", pc.get("i1")["state"] == "booting")
fake.ready("i1")
check("ready: the idle clock starts at once", pc.get("i1")["idle_since"] is not None)
fake.set_busy("i1", True)
check("busy from the heartbeat", pc.get("i1")["state"] == "busy" and pc.get("i1")["idle_since"] is None)
fake.advance(1000)
check("a busy pod never idle-stops", pc.get("i1")["state"] == "busy")
fake.set_busy("i1", False)
check("idle when the heartbeat says not busy", pc.get("i1")["state"] == "idle")
fake.advance(299)
check("still idle before idle_stop_s", pc.get("i1")["state"] == "idle")
fake.advance(1)
g = pc.get("i1")
check("idle_stop_s with park_ttl_s → parked", g["state"] == "parked" and g["park_until"])
fake.advance(3600)
g = pc.get("i1")
check("park TTL runs out → done", g["state"] == "done" and g["reason"] == "park_ttl")
check("events in order", [k for k in kinds(fake, "i1") if k.startswith("pod.")
                          and k != "pod.boot"] ==
      ["pod.ready", "pod.busy", "pod.idle", "pod.parked"])

fake = FakeControl(); pc = fake.client()
up(fake, pc, "i2", park_ttl_s=0)
fake.advance(300)
check("park_ttl_s 0 → terminated on idle", pc.get("i2")["state"] == "done"
      and pc.get("i2")["reason"] == "idle_stop")

fake = FakeControl(); pc = fake.client()
pc.submit(JobSpec(lane="media", image="i", job_id="spot", gpu=Gpu(interruptible=True),
                  policy=Policy(idle_stop_s=60, park_ttl_s=600)))
fake.place("spot"); fake.ready("spot")
fake.advance(60)
check("a spot pod is never parked: idle stop terminates it", pc.get("spot")["state"] == "done")

# -- the idle clock waits for fetching models -------------------------------------------------
fake = FakeControl(); pc = fake.client()
up(fake, pc, "f1")
fake.advance(200)
pc.add_models("f1", ["m/late"])
g = pc.get("f1")
check("a mid-life fetch stops the idle clock", g["idle_since"] is None and g["models_rev"] == 1)
fake.advance(1000)
check("never idle-stopped while a model is fetching", pc.get("f1")["state"] == "ready")
fake.model_state("f1", "m/late", "present")
check("pod.models when the addition lands", fake.events_of("pod.models", "f1")[-1]["detail"]
      == {"path": "m/late", "state": "present", "error": None})
fake.advance(299)
check("the idle clock restarts when the fetch ends", pc.get("f1")["state"] == "ready")
fake.advance(1)
check("…and runs out idle_stop_s later", pc.get("f1")["state"] == "parked")

fake = FakeControl(); pc = fake.client()
up(fake, pc, "f2"); fake.set_busy("f2", True); fake.set_busy("f2", False)
pc.add_models("f2", ["m/x"])
check("an idle pod that starts fetching is no longer idle", pc.get("f2")["state"] == "ready")
again = pc.add_models("f2", ["m/x", "m/a"])
check("adding what is fetching or present changes nothing", again["added"] == []
      and again["models_rev"] == 1)

# -- warm -----------------------------------------------------------------------------------
fake = FakeControl(); pc = fake.client()
up(fake, pc, "w1")
pc.warm("w1", delta_s=900)
fake.advance(600)
check("held warm past idle_stop_s", pc.get("w1")["state"] == "ready")
pc.warm("w1", delta_s=-600)          # warm-until now = start + 300, already passed
fake.advance(0)
check("a negative delta that ends the hold lets the idle stop fire", pc.get("w1")["state"] == "parked")

fake = FakeControl(); pc = fake.client()
up(fake, pc, "w2", max_life_s=1000)
w = pc.warm("w2", delta_s=50_000)
check("warm is capped by max_life_s", w["keep_warm_until"] == fake._iso(fake.now + 1000))

# -- drain ----------------------------------------------------------------------------------
fake = FakeControl(); pc = fake.client()
up(fake, pc, "d1"); fake.set_busy("d1", True)
pc.drain("d1")
g = pc.get("d1")
check("drain on a busy pod: draining at once, visible on the pod",
      g["state"] == "draining" and g["pod"]["draining"] and "pod.draining" in kinds(fake, "d1"))
fake.set_busy("d1", False)
check("then idles when the work ends", pc.get("d1")["state"] == "idle")
fake.advance(300)
check("then parks per policy", pc.get("d1")["state"] == "parked")

fake = FakeControl(); pc = fake.client()
up(fake, pc, "d2"); pc.warm("d2", delta_s=10_000); pc.drain("d2")
fake.advance(300)
check("drain clears a warm hold", pc.get("d2")["state"] == "parked")

fake = FakeControl(); pc = fake.client()
pc.submit(spec("d3")); fake.place("d3"); pc.drain("d3")
check("drain while booting keeps booting", pc.get("d3")["state"] == "booting")
fake.ready("d3")
check("…and the pod goes straight to idle at ready", pc.get("d3")["state"] == "idle")

# -- max life ---------------------------------------------------------------------------------
fake = FakeControl(); pc = fake.client()
up(fake, pc, "l1", max_life_s=1000); fake.set_busy("l1", True)
fake.advance(1000)
check("max_life_s drains a busy pod", pc.get("l1")["state"] == "draining")
fake.set_busy("l1", False)
g = pc.get("l1")
check("…then stops (not parks) as soon as it is not busy",
      g["state"] == "done" and g["reason"] == "max_life")

# -- cancel ---------------------------------------------------------------------------------
fake = FakeControl(); pc = fake.client()
pc.submit(spec("c1")); fake.placing("c1")
check("placing is visible", pc.get("c1")["state"] == "placing")
r = pc.cancel("c1", reason="changed my mind")
check("cancel while placing", r["was"] == "placing" and pc.get("c1")["state"] == "cancelled")
pod = fake.place("c1")
check("the pod that lands after is terminated on arrival",
      pod["id"] in fake.terminated_pods and pc.get("c1")["state"] == "cancelled")
up(fake, pc, "c2"); pc.park("c2")
pc.cancel("c2")
check("cancel a parked job terminates its pod", pc.get("c2")["state"] == "cancelled"
      and fake.jobs["c2"].pod["id"] in fake.terminated_pods)

# -- preempt ---------------------------------------------------------------------------------
fake = FakeControl(); pc = fake.client()
up(fake, pc, "p1"); fake.preempt("p1")
check("preempted is terminal and evented", pc.get("p1")["state"] == "preempted"
      and "pod.preempted" in kinds(fake, "p1"))

# -- park/resume rules ----------------------------------------------------------------------
fake = FakeControl(); pc = fake.client()
pc.submit(JobSpec(lane="media", image="i", job_id="s1", gpu=Gpu(interruptible=True)))
fake.place("s1"); fake.ready("s1")
from pod_loader.app import Conflict  # noqa: E402
try:
    pc.park("s1"); check("a spot pod cannot be parked", False)
except Conflict as e:
    check("a spot pod cannot be parked", "spot" in e.error)
up(fake, pc, "r1"); pc.park("r1", ttl_s=100)
pc.resume("r1")
check("resume: booting again, gpu_check reruns", pc.get("r1")["state"] == "booting"
      and "pod.resumed" in kinds(fake, "r1"))
fake.resume_without_gpu("r1")
check("a resume with no GPU falls back to a fresh placement", pc.get("r1")["state"] == "queued"
      and pc.get("r1")["pod"] is None)

# -- reuse parked-first ---------------------------------------------------------------------
fake = FakeControl(); pc = fake.client()
up(fake, pc, "old")
pc.add_models("old", ["m/b"]); fake.model_state("old", "m/b", "present")
pc.park("old")
s = spec("new"); s.reuse = "parked-first"
s.boot.models = Boot.models_from("p", ["m/a", "m/c"])
r = pc.submit_reply(s)
g = pc.get("new")
check("parked-first resumes a matching parked pod", r["resumed_from"] == "old"
      and g["state"] == "booting" and g["pod"]["id"] == fake.jobs["old"].pod["id"])
check("the parked job ends, naming who took it", pc.get("old")["state"] == "done"
      and pc.get("old")["reason"] == "reused_by:new")
check("models are not matched: what the pod lacks is added mid-life",
      g["models"] == {"m/a": "present", "m/b": "present", "m/c": "fetching"}
      and g["models_rev"] == 1)

fake = FakeControl(); pc = fake.client()
up(fake, pc, "old2"); pc.park("old2")
for name, change in [("other image", dict(image="registry.example/app:2")),
                     ("other addon version", dict(addons={"set": "comfy", "version": 4})),
                     ("more VRAM than the card has", dict(gpu=Gpu(min_vram_gb=80))),
                     ("other lane", dict(lane="else"))]:
    s = spec(f"n-{name}"); s.reuse = "parked-first"
    if "image" in change:
        s.image = change["image"]
    if "addons" in change:
        s.boot.addons = change["addons"]
    if "gpu" in change:
        s.gpu = change["gpu"]
    if "lane" in change:
        s.lane = change["lane"]
    r = pc.submit_reply(s)
    check(f"no reuse with {name}", r["resumed_from"] is None and r["state"] == "queued")
s = spec("n-never")
check("reuse never (the default) ignores parked pods", pc.submit_reply(s)["resumed_from"] is None)

# -- spend and cap ---------------------------------------------------------------------------
fake = FakeControl(park_cost_hr=0.1); pc = fake.client()
pc.submit(spec("$1")); fake.place("$1", cost_hr=2.0); fake.ready("$1"); fake.set_busy("$1", True)
fake.advance(3600)
s = pc.spend("media")
check("running pods accrue cost_hr", abs(s["running_usd"] - 2.0) < 1e-6 and s["live_pods"] == 1)
check("cost_so_far on the job", abs(pc.get("$1")["cost_so_far"] - 2.0) < 1e-6)
fake.set_busy("$1", False); pc.park("$1")
fake.advance(3600)
s = pc.spend("media")
check("parked pods accrue storage, shown apart", abs(s["parked_usd"] - 0.1) < 1e-6
      and abs(s["rolling_24h_usd"] - 2.1) < 1e-6 and s["live_pods"] == 0)
fake.advance(86400)
check("the rolling 24 h window forgets old spend",
      abs(pc.spend("media")["running_usd"]) < 1e-6)

fake = FakeControl(lane_caps={"media": {"daily_usd": 1.0}}); pc = fake.client()
pc.submit(spec("cap1")); fake.place("cap1", cost_hr=1.0); fake.ready("cap1")
fake.set_busy("cap1", True)
fake.advance(3600)
s = pc.spend("media")
check("breached at the cap", s["breached"] and s["cap_usd"] == 1.0)
check("a breach is evented once", len(fake.events_of("budget.breach")) == 1)
check("…and drains the lane's live pods", pc.get("cap1")["state"] == "draining")
pc.submit(spec("cap2"))
try:
    fake.place("cap2"); check("a breached lane holds new jobs", False)
except FakeHeld as e:
    check("a breached lane holds new jobs", "cap" in str(e))

fake = FakeControl(lane_caps={"media": {"max_pods": 1}}); pc = fake.client()
up(fake, pc, "mp1"); pc.submit(spec("mp2"))
try:
    fake.place("mp2"); check("max_pods holds the next job", False)
except FakeHeld:
    check("max_pods holds the next job", True)

# -- lane backoff ------------------------------------------------------------------------------
fake = FakeControl(); pc = fake.client()
for i in range(3):
    pc.submit(spec(f"bf{i}")); fake.place(f"bf{i}")
    fake.fail_boot(f"bf{i}", "gpu_check" if i == 0 else "workload", "CUDA unknown error",
                   log_tail="...")
check("gpu_check failure blocks the host", len(fake.blocked_hosts) == 1
      and fake.events_of("host.blocked"))
check("3 pre-ready failures → lane held 5 min", fake.lane_backoff("media") == fake.now + 300
      and fake.events_of("lane.backoff")[-1]["detail"]["hold_s"] == 300)
check("the failure carries reason, stage, log_tail", pc.get("bf1")["log_tail"] == "..."
      and pc.get("bf1")["stage"] == "workload")
pc.submit(spec("bf3"))
try:
    fake.place("bf3"); check("a held lane refuses placement", False)
except FakeHeld:
    check("a held lane refuses placement", True)
check("another lane is not held", fake.lane_backoff("other") is None)
fake.advance(300)
fake.place("bf3"); fake.fail_boot("bf3", "models", "stall")
check("the next failure holds 15 min", fake.lane_backoff("media") == fake.now + 900)
fake.advance(900)
pc.submit(spec("bf4")); fake.place("bf4"); fake.fail_boot("bf4", "models", "x")
check("then 30 min", fake.lane_backoff("media") == fake.now + 1800)
fake.advance(1800)
pc.submit(spec("bf5")); fake.place("bf5"); fake.ready("bf5")
check("the first pod to reach ready resets it", fake._fail_streak["media"] == 0)
up(fake, pc, "bf6"); fake.fail_boot("bf6", "workload", "died")
check("a failure after ready does not count", fake._fail_streak["media"] == 0)
pc.submit(spec("bf7", queue_deadline_s=60))
for i in range(8, 11):
    pc.submit(spec(f"bf{i}")); fake.place(f"bf{i}"); fake.fail_boot(f"bf{i}", "models", "x")
fake.advance(61)
check("a held job still expires at its own queue deadline", pc.get("bf7")["state"] == "expired")

# -- hooks refuse what cannot happen -----------------------------------------------------------
fake = FakeControl(); pc = fake.client()
pc.submit(spec("h1"))
for name, fn in [("ready before placement", lambda: fake.ready("h1")),
                 ("busy before ready", lambda: fake.set_busy("h1", True)),
                 ("boot line before placement", lambda: fake.boot_line("h1", "x")),
                 ("unknown model", lambda: fake.model_state("h1", "nope", "present")),
                 ("clock backwards", lambda: fake.advance(-1))]:
    try:
        fn(); check(f"hook refuses {name}", False)
    except FakeError:
        check(f"hook refuses {name}", True)

print(f"\n  {sum(ok)}/{len(ok)} passed")
sys.exit(0 if all(ok) else 1)
