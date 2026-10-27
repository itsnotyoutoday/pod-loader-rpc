# pod-loader-rpc

Build a job, push it onto rented compute, watch it run, and be certain it dies when it
should.

The client side of [`pod-harness`](https://github.com/itsnotyoutoday/pod-harness). It shares
no code with it — the two agree on a versioned contract and talk over HTTP.

---

## 1. What this is for

You have work that needs a bigger machine than your laptop for an hour or two. Renting one
is easy; doing it *safely and repeatably* is three separate problems:

1. **Getting the job there.** Your code, your spec and your data have to reach a machine
   that did not exist a minute ago — without putting credentials or a git token on it.
2. **Knowing what happened.** The provider offers no logs API. A pod is either watched live
   or it is a black box, and "the script exited 0" is not the same as "it produced
   something".
3. **Making sure it stops.** This is the one that costs money. An in-pod timeout **cannot
   terminate a RunPod pod** — `runpodctl` from inside returns `Unauthorized`. It detects
   the overrun, tries to self-delete, is refused, logs the refusal, and keeps billing.

This handles all three: it publishes your code and spec to object storage, provisions a pod
pointed at them, drives it over `/v1`, and guarantees termination from outside the pod,
where termination actually works.

**Three rules it enforces, because each one has already gone wrong**

- **The pod holds no credentials.** Code arrives as objects the pod can already read. No
  git access, no API key, nothing to leak.
- **The loader assigns; workers never discover.** A worker that scans a queue can race
  another worker, and object storage cannot arbitrate. Every pod is told what it owns.
- **This package is never installed in a pod.** It can provision compute; a pod must not
  be able to. `pod-harness` fails its own build if any of these modules appear inside it.

---

## 2. Building a job

A job is a **workload repo** with two things in it: stage code, and a spec naming which
stages to run.

### Directory layout

```
my-workload/
  code/
    mywork/
      __init__.py
      stages.py         the stage registry — the only required file
  jobs/
    my-job.json         one spec per job you want to run
  .pod.env              how it launches (§3b)
```

`code/` is the convention that matters: **only that directory is published**, and it
becomes an import root on the pod. So `code/mywork/stages.py` is importable as
`mywork.stages`.

### The stage registry

```python
# code/mywork/stages.py
from pod_harness.framework import Stage, Verification   # convenience; duck typing works too

class ExtractStage(Stage):
    name, number = "extract", 1
    produces = ("rows",)

    def execute(self, ctx):
        rows = read(ctx.params["input"])
        for i, r in enumerate(rows):
            ctx.progress(i, len(rows))      # sub-stage progress, throttled to ~1/sec
        ctx.put("rows", rows)
        return {"count": len(rows)}

    def verify_outputs(self, ctx):          # optional, strongly encouraged
        rows = ctx.get("rows")
        return Verification(ok=bool(rows), checks={"rows": len(rows or [])})

STAGES = {"extract": ExtractStage, "transform": TransformStage}
```

`verify_outputs` is what makes the framework worth using. Without it, a stage that reports
success while producing nothing is indistinguishable from one that worked — a bug that
occurred three times in the project this grew out of, once as a glob matching `*.wav`
against a FLAC corpus and cheerfully reporting "40 files" that were zero files.

> **Do not vendor `pod-harness` into `code/`** (no git submodules). The code root is
> prepended to `PYTHONPATH` on the pod, so a vendored copy would shadow the image's and you
> would have two harnesses resolved by import order. Depend on it for *development* only —
> the image supplies it at runtime:
> ```toml
> dev = ["pod-harness @ git+https://github.com/itsnotyoutoday/pod-harness.git@<sha>"]
> ```

Full stage contract: [`pod-harness` README](https://github.com/itsnotyoutoday/pod-harness#integration).

---

## 3. How to run

### a) The job spec

`jobs/my-job.json` says which stages to run and hands the workload its parameters.

```json
{
  "spec_version": 2,
  "pipeline": {
    "stages_from": "mywork.stages:STAGES",
    "stages": ["extract", "transform"]
  },
  "mount":  {"kind": "volume"},
  "resume": {"enabled": true, "from": "auto"},
  "params": {"input": "data/batch-7", "workers": 8}
}
```

| field | meaning |
|---|---|
| `stages_from` | `module:attribute` — where your registry lives |
| `stages` | which to run, in order. A name not in the registry is rejected before launch |
| `resume.from` | `auto` re-runs `verify_outputs()` per completed stage and restarts at the first that fails. Never marker-driven: a `.DONE` file once survived a wiped volume and reported "done: 24" over no outputs |
| `params` | yours. Opaque to the framework |

Check it without spending anything:

```bash
python -m pod_loader.contract jobs/my-job.json
```

### b) The launch file

`.pod.env` sits in the directory you launch from, found by walking upward to `$HOME`. A
launch has a dozen inputs; passing them as flags means retyping them, and retyping means
getting one wrong on the attempt that matters.

```bash
python -m pod_loader.launchfile --template > .pod.env
python -m pod_loader.launchfile              # show what resolved, and check it
```

```bash
# ---- destination -------------------------------------------------------------
TARGET            = runpod                 # runpod | docker | direct
IMAGE             = ghcr.io/itsnotyoutoday/pod-harness:latest

# ---- capacity ----------------------------------------------------------------
COMPUTE           = CPU                    # generic: every target understands it
MAX_COST_HR       = 0.20                   # generic

# PODH_RUNPOD_* = "this line stops meaning anything if you change TARGET".
# Network volumes, CPU flavours and cloud tiers are RunPod's concepts, not the
# framework's.
PODH_RUNPOD_CPU_FLAVORS = cpu3c,cpu3g,cpu5c,cpu5g,cpu3m,cpu5m
PODH_RUNPOD_GPU_TYPES   = NVIDIA RTX A4000,NVIDIA RTX A4500,NVIDIA RTX A5000
PODH_RUNPOD_CLOUD       = SECURE,COMMUNITY
PODH_RUNPOD_FALLBACK_TO_GPU = false

# ---- cost --------------------------------------------------------------------
BUDGET_MIN        = 60                     # hard kill, enforced outside the pod
QUEUE_DEADLINE_MIN= 240

# ---- storage -----------------------------------------------------------------
STORE             = runpod                 # a PROFILE, never a credential
PODH_RUNPOD_VOLUME = ~/runpod-volume.key   # RunPod only; PINS the datacentre
CACHE             = persistent             # persistent | ephemeral | off

# ---- the job -----------------------------------------------------------------
JOB_SPEC          = jobs/my-job.json
WORKLOAD          = .
AUTORUN           = true

# ---- forwarded to the pod with ENV_ stripped ---------------------------------
# ENV_MY_SETTING  = value
```

**Never put a secret in it.** `STORE` names a profile; credentials live in `*.key` files
outside the repo. The loader *refuses* a value under a key ending
`SECRET`/`PASSWORD`/`API_KEY`/`ACCESS_KEY`/`TOKEN` that is not a path to a real file —
because this file sits beside the job, which means it gets committed.

One-time credential setup:

```bash
cat > ~/runpod.key        <<< 'api_key = <your runpod key>'
cat > ~/runpod-volume.key <<< 'volume_id = <your network volume id>'
```

### c) Launch

```bash
cd my-workload
python runctl.py launch --dry-run     # validate everything, provision nothing
python runctl.py launch               # for real
```

`--dry-run` is free and does everything short of spending money: reads `.pod.env`,
publishes the code, validates the spec against the harness contract, resolves the volume,
and reports how many placements it would try. Run it first.

```
launch config: /path/my-workload/.pod.env
code: 4 files → code/my-workload/93303c4
spec: runs/job1786655065/spec.json
volume: v0lexample in US-NC-1 (pins compute to that datacenter)
✓ pod abc123  job=job1786655065
```

Code publishes to `code/<workload>/<git-sha>/` — or `dev` if your tree is dirty. That
distinction matters: publishing to a mutable path can change code under a **running** job,
and Python's lazy imports would then mix two versions inside one process.

### d) Check on it

```bash
python runctl.py watch --job <job_id>      # status + live log
python runctl.py poll  --job <job_id>      # progress only
python runctl.py fetch --job <job_id>      # pull artifacts back
python runctl.py kill                      # terminate now
```

Or over HTTP — what a web app or an agent would use:

```bash
curl -H "X-Podh-Token: $TOKEN" https://<pod>-8000.proxy.runpod.net/v1/jobs/$JOB/summary
curl -H "X-Podh-Token: $TOKEN" https://<pod>-8000.proxy.runpod.net/v1/jobs/$JOB/stages
```

`/summary` is a ~500-token digest — start there. `/stages` gives per-stage verification
detail. The status key is **`job_state`**, not `state`.

Status is mirrored to object storage as it goes, so it survives the pod. Afterwards, read
`runs/<job_id>/status.json` from the bucket.

---

## 4. Advanced

### All `runctl` commands

```bash
python runctl.py create   --provider runpod   # bare runner, no job
python runctl.py status                       # one runner, or all
python runctl.py mount                        # attach storage
python runctl.py push                         # upload data where a runner can reach it
python runctl.py ls | cat | browse            # inspect the object store; browse opens a UI
python runctl.py submit  --spec …             # send a job to a runner already up
python runctl.py shutdown | destroy           # stop vs terminate — stopping still bills
```

### Guaranteed termination

```python
from pod_loader import reaper

with reaper.pod(create_kwargs, budget_min=60) as pod:
    ...
```

| layer | covers |
|---|---|
| context manager | normal exit, exceptions, Ctrl-C, SIGTERM |
| deadline thread | wall clock exceeded while the main thread is blocked on a hung call |
| `reaper.sweep()` | pods orphaned by a process that died before its `finally` could run |

Every launch is journaled to disk **before** the API call returns, so a pod whose creating
process vanished is still discoverable. A pod nobody journaled is a pod nobody can find.

> **Known gap.** All three layers need a process alive on your machine. If your laptop
> sleeps with a pod up, nothing reaps it. A long-lived reaper service is the fix and is not
> built yet.

### Capacity is a priority list, not a value

RunPod takes the first shape with capacity, so an ordered list degrades instead of failing.
The walk covers **three** dimensions:

```
flavor   cpu3c → cpu3g → cpu5c → cpu5g → cpu3m → cpu5m
cloud    SECURE → COMMUNITY
type     CPU → GPU                    ← opt-in, ~12× the price
```

A list within one compute type is not enough: on 2026-08-13 all six CPU flavours were
exhausted on **both** clouds while GPU had capacity — and a network volume pins you to one
datacentre, so there is no other region to fall back to.

That is why `FALLBACK_TO_GPU` requires `MAX_COST_HR`: $0.06/hr → $0.74/hr. A four-minute
benchmark does not care; a six-hour build is $0.36 against $4.44. Cost and capacity resolve
into **one** decision — if the only free slot is too expensive, that is a reason to wait,
not a placement. An over-budget pod that does get created is terminated immediately,
because it bills from the instant it exists.

### Where it landed, and giving a host back

A create asks for a list of GPU types; RunPod picks one, and a machine, and a price. What it
picked is read back from the pod record:

```python
from pod_loader.runpod import reaper
facts = reaper.placement_facts(reaper._api(), pod, wait_sec=30)
# {"machine_id", "gpu_type_id", "gpu_name", "gpu_count", "data_center_id", "cost_hr",
#  "cuda_version", "driver_version"}   — None where unknown, never guessed
```

It waits (bounded) for the machine to be assigned and never raises. `cuda_version` and
`driver_version` are always None: RunPod's REST pod object does not report them, so only
the pod itself can. `RunPodLoader.placement_facts(pod)` is the same through the loader.

RunPod cannot be told to avoid a machine, so `create_with_capacity(..., vet=fn,
max_refusals=n)` checks after placement: `vet(pod)` returning a reason gives the pod back at
once (outcome `refused`) and the walk moves on, at most `n` times before the usual waitable
"no acceptable placement".

Every pod's env carries `PODH_GPU_COUNT` — set by `RunPodAPI.create` from the compute type
it actually requested, `0` for CPU — so a pod can skip its GPU check when it has none.

### Storage profiles

```bash
python -m pod_loader.volume                    # resolve and describe the RunPod volume
python -m pod_loader.sync ../workload --dry-run
```

| profile | true S3? | notes |
|---|---|---|
| `runpod` | **no** | no presigned URLs, `delete_objects` returns 307, `head_object` 403s on large objects |
| `cloudflare` | yes | verified including presigned URLs; signing region must be `auto` |
| `aws`, `minio` | yes | |

RunPod's endpoint is `RUNPOD_STORE_*` rather than `*_S3_*` deliberately. Calling it S3
invited code to assume S3 semantics, and all three limitations above were found the
expensive way. Ask instead of assuming:

```python
from pod_loader.capabilities import flavor_for
flavor_for(endpoint).require("presigned", "batch_delete")   # raises at plan time
```

A named profile that cannot be resolved **raises** rather than falling back to the default
store. It used to fall back, so `get_storage("cloudflare")` silently returned the RunPod
store — writes intended for one bucket landing in another, and looking successful.

### Using it as a library

```python
from pod_loader import contract, launchfile, reaper, sync, volume

cfg  = launchfile.load()
root = sync.publish(cfg.workload)["root"]
env  = launchfile.pod_env(cfg, job_id=jid, spec_key=key, code_root=root)

assert not contract.check_env(env)                              # required vars present
contract.require_valid(spec, contract.from_image(pod_url))      # against the live image

with reaper.pod(create, budget_min=cfg.budget_min,
                capacity=launchfile.capacity_kwargs(cfg)) as pod:
    ...
```

Read the required env set from the contract rather than hardcoding it: when the harness
starts needing a new root, launches fail while composing instead of the pod refusing to
boot.

### App client

`pod_loader.app` is a typed client for pod-control's app API (`POST /v1/jobs` with a boot
spec and policy, then read, drain, warm, park, resume, add models, spend, events, addon
sets) and its Task API (pools and tasks, below). Install with `pip install "pod-loader-rpc[app]"` (adds httpx). Times are seconds
everywhere; errors raise `AppAPIError` subclasses carrying the server's own text.

```python
from pod_loader.app import Boot, Client, Gpu, JobSpec, Policy

spec = JobSpec(
    lane="render", image="registry.example/worker:1", ports=["8188/http"],
    gpu=Gpu(count=1, min_vram_gb=24, datacenter_only=True),
    boot=Boot(models=Boot.models_from("my-models", ["checkpoints/a.safetensors"]),
              addons={"set": "nodes", "version": 3}),
    policy=Policy(idle_stop_s=300, park_ttl_s=3600, max_life_s=4 * 3600),
    reuse="parked-first", idempotency_key="render-batch-42")

with Client("https://203.0.113.5:8443", token, fingerprint=cert_sha256) as pc:
    job = pc.submit(spec)
    pc.get(job)["state"]                       # queued → placing → booting → ready …
    pc.add_models(job, ["loras/b.safetensors"])
    pc.warm(job, delta_s=900)                  # hold it warm 15 min longer
    pc.drain(job)
```

`fingerprint` pins pod-control's certificate and is checked inside the TLS handshake, so
the token is never sent to anything else.

For an app's own tests, `pod_loader.app.testing.FakeControl` runs the whole API in memory
with a manual clock and hooks that play the pod and the dispatcher:

```python
from pod_loader.app.testing import FakeControl

fake = FakeControl()
pc = fake.client()                             # Client(..., transport=fake.transport)
job = pc.submit(spec)
fake.place(job); fake.ready(job)
fake.set_busy(job, True); fake.set_busy(job, False)
fake.advance(300)                              # idle_stop_s passes
assert pc.get(job)["state"] == "parked"
```

It is a test double: its docstring lists what it simplifies (placement is manual,
`wait` never blocks, flat parking rate, …).

#### Tasks

The Task API (pod-control `docs/TASK-API.md`) has the app submit **tasks** to a **pool**
and lets pod-control launch, feed and stop the pods. A pool is a job template plus scaling
limits; a task names its pool, a `kind` (`comfy` or an app handler) and what it `needs`.

```python
from pod_loader.app import Needs, TaskSpec

pc.pool_put("render", "gpu", {"image": "registry.example/worker:1",
                              "gpu": {"min_vram_gb": 24},
                              "boot": {"models": {"profile": "my-models", "paths": []}}},
            scale={"max_pods": 2, "launch_when": {"queued_tasks": 1}})
task = pc.submit_task(TaskSpec(
    pool="gpu", kind="comfy", lane="render", idempotency_key="frame-14",
    needs=Needs(min_vram_gb=24, models=["checkpoints/a.safetensors"], loads=["ckpt:a"]),
    workflow={"ref": "workflows/i2v.json", "patch": {"12.inputs.seed": 7}},
    labels={"batch": "42"}))
pc.task(task)["state"]                         # queued → claimed/held → running → succeeded
pc.set_priority(task, 10)                      # while queued
pc.retry_task(task, patch={"12.inputs.seed": 8})   # after a failure: same id, a re-roll
pc.cancel_tasks(lane="render", label={"batch": "42"})   # final: nothing requeues it
pc.addon_put_if_changed("render", "nodes", items)       # a new version only if it differs
```

In tests, the fake plays the pod's worker:

```python
pod = fake.pool_launches("render", "gpu")[0]["job_id"]   # the autoscaler asked for one
fake.place(pod); fake.ready(pod)
claimed = fake.claim_task("render/gpu", have_loaded=["ckpt:a"])
fake.task_start(task); fake.task_progress(task, 0.5)
fake.task_done(task, True, result={"frames": 81})
assert pc.task(task)["state"] == "succeeded"
```

#### Timelines, statistics and pod logs

Every job carries a timeline (the placement walk, image pull, each boot stage, ready,
busy/idle, the end, cost), and pod-control aggregates them. Times are epoch seconds,
ISO-8601 or a `datetime`; the server's window defaults to the last 7 days.

```python
pc.job_timeline(job)["boot_total_s"]           # GET /v1/jobs/{id} → timeline
pc.boot_stats(lane="render", group_by="gpu")   # success rate, p50/p90 per stage, failures
pc.boot_stats_raw(since=week_ago, limit=500)   # the timelines themselves (parsed JSON lines)
pc.task_stats(pool="gpu", group_by="kind")     # outcomes, queued/run percentiles, cost per task
pc.spend_stats(group_by="day")                 # {rows: [{key, usd, run_usd, park_usd, pod_hours}]}
```

pod-control never connects into a pod, so logs are asked for: the request rides the pod's
next heartbeat reply (and lapses after 10 min), the pod pushes its tails, and the app
reads the latest tail per log name. Full logs uploaded at the pod's end (when the server
has a log store) come back as presigned links.

```python
pc.request_logs(job, names=["workload"], lines=200)
pc.logs(job)                                   # {"logs": [{name, at, lines, truncated}], "pending"}
pc.log_download_url(job, "workload")           # presigned GET of workload.log.gz
```

In tests: `fake.log_request(job)` (what the pod would be told), `fake.push_logs(job,
"workload", lines)` (the pod answers), `fake.log_store = "logs"` with
`fake.upload_logs(job, ["workload"])`. The fake's stats aggregate its own jobs, tasks and
spend; its docstring lists what its timelines leave out (no busy/idle split, no MB/s).

### What reaches a pod, and what never does

| | goes to the pod? |
|---|---|
| workload code, job spec | yes — as objects it reads |
| `PODH_API_TOKEN` | yes — minted per job, for its own `/v1` only |
| `PODH_CONTROL_TOKEN` | yes — a **per-pod** token, good only for "I am alive" / "I am done" |
| `PODH_GPU_COUNT` | yes — GPUs the pod was rented with, `0` for CPU |
| store credentials | only for profiles named in `FORWARD_STORES` |
| the master control token | **never** |
| any `*.key` file | **never** |
| a git credential | **never** |

The master control token can terminate any pod, submit work and read the whole queue.
Putting it in a machine rented by the minute would make one compromised pod a full
control-plane compromise. Each pod gets its own random token instead, registered against
that pod and individually revocable.

Store credentials are resolved here and the **values** are forwarded — the pod never
receives a key file, and only for stores the job explicitly names.

### Targets, and adding one

`TARGET` in the launch file picks a class, not a branch:

| target | what it is | cost |
|---|---|---|
| `runpod` | a rented pod, with the capacity walk and cost ceiling | billed |
| `docker` | a container on this machine, from the same image | free |
| `direct` | the harness in your interpreter — no container, no `/v1` | free |

```
src/pod_loader/
  base_loader.py       the sequence every launch shares
  runpod/              api, volume, reaper, capacity, RunPodLoader
  local/               DockerLoader, DirectLoader
```

`BaseLoader.launch()` publishes the code, stages the spec, composes the environment and
validates both against the harness contract. A subclass supplies only three things:
`preflight()`, `start()`, `stop()`. So a new backend cannot accidentally skip contract
validation — it never writes that part.

RunPod is one vendor among several that rent containers by the hour. Vast.ai, Lambda,
Paperspace, CoreWeave or a Kubernetes cluster you already own are each a subclass in their
own package, registered in `base_loader._registry()`. Nothing else in the launch path
changes, because every destination runs the same image and speaks the same `/v1`.

### Storage layout

`paths.py` and `store.py` own the object-key layout, and the harness deliberately does not.
That knowledge used to live in both and the copies drifted three times in one day. One
definition here; the harness is told, and is granted only the prefixes it may write to.

---

## Development

```bash
pip install -e ".[dev]"
python -m pod_loader.contract jobs/*.json     # validate specs offline
python -m pod_loader.launchfile               # resolve and check .pod.env
```

## Licence

MIT.
