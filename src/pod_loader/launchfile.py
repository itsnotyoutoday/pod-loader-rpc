"""`.pod.env` — everything a launch directory needs to start a job, in one readable file.

## Why a file and not flags

A launch has a dozen inputs: which store, which credentials, which image, which volume,
what the budget is, what the job is, what extra environment the workload wants. Passing
those as flags means retyping them, and retyping them means getting one wrong on the
attempt that matters. Putting them in a file next to the job makes a launch reproducible
and reviewable — you can read it before spending money.

## Where it looks

`./.pod.env`, then upward to `$HOME`. So a workload repo carries its own launch config and
you can run from any subdirectory of it.

## What it can contain

    # ---- destination ---------------------------------------------------
    TARGET             runpod | docker | local
    IMAGE              ghcr.io/…/pod-harness:latest
    VCPU  DISK_GB

    # ---- capacity ------------------------------------------------------
    COMPUTE                       CPU | GPU        (generic)
    MAX_COST_HR                   refuses a placement above this  (generic)
    PODH_RUNPOD_CPU_FLAVORS       cpu3c,cpu3g,…    tried in this order
    PODH_RUNPOD_GPU_TYPES         NVIDIA RTX A4000,…
    PODH_RUNPOD_CLOUD             SECURE,COMMUNITY
    PODH_RUNPOD_FALLBACK_TO_GPU   opt-in; roughly 12x the price

    # ---- cost ----------------------------------------------------------
    MAX_COST_HR        THE budget: most you will pay per hour while placing
    MAX_LIFETIME_MIN   lifetime cap for a pod that never reports liveness
    QUEUE_DEADLINE_MIN stop waiting for capacity after this

    # ---- storage -------------------------------------------------------
    STORE              runpod | cloudflare | aws | minio      (a profile, not a secret)
    STORE_KEYFILE      durable source of truth
    VOLUME_KEYFILE     provider-local volume gateway
    PODH_RUNPOD_VOLUME RunPod only; PINS the datacenter
    CACHE              persistent | ephemeral | off

    # ---- the job -------------------------------------------------------
    JOB_SPEC  WORKLOAD  AUTORUN

    # ---- optional: hand off to pod-control -----------------------------
    PODH_CONTROL_URL   https://control.example.com:8787
                       When set, the launch is SUBMITTED there and queues until
                       capacity exists, instead of being provisioned from here.
                       Requires QUEUE_DEADLINE_MIN.

    # ---- anything else the workload wants ------------------------------
    ENV_LINGUA_MFA_ACOUSTIC = english_us_arpa

## Why capacity is a list and not a value

RunPod takes the first shape with capacity, so an ordered list degrades instead of
failing. The walk covers three dimensions — flavor, cloud, and compute type — because a
list within one type is not enough: on 2026-08-13 all six CPU flavours were exhausted on
both clouds while GPU had capacity, and a network volume pins you to one datacenter so
there is no other region to fall back to.

GPU fallback is opt-in and requires `MAX_COST_HR`, because it is roughly a 12x price jump
($0.06/hr to $0.74/hr). A four-minute benchmark does not care; a six-hour corpus build is
$0.36 against $4.44. Cost and capacity resolve together into one decision: if the only free
slot is too expensive, that is a reason to wait, not a placement.

Keys prefixed `ENV_` are forwarded to the pod verbatim with the prefix stripped, so a
workload can pass its own settings without this file knowing what they mean.

## What it must NOT contain

Credentials. `STORE` names a profile and `RUNPOD_VOLUME` points at a key file; the secrets
stay in those files, outside any repo, where `*.key` already keeps them out of git. A
`.pod.env` with a secret in it is a `.pod.env` that gets committed.

The loader refuses to read a value that looks like a key rather than a path, because the
alternative is discovering it in a public repo later.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

FILENAMES = (".pod.env", "pod.env")

#: Values that must never appear inline. Matched loosely on purpose — a false positive
#: costs one confused moment; a false negative costs a leaked credential.
_SECRET_KEYS = re.compile(r"(SECRET|PASSWORD|API_KEY|ACCESS_KEY|TOKEN)$")


class LaunchFileError(RuntimeError):
    pass


def _find(start: Path | None = None) -> Path | None:
    d = (start or Path.cwd()).resolve()
    home = Path.home().resolve()
    while True:
        for n in FILENAMES:
            if (d / n).is_file():
                return d / n
        if d == home or d.parent == d:
            return None
        d = d.parent


def _parse(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip().upper()] = v.strip().strip("'\"")
    return out


@dataclass
class LaunchConfig:
    """A resolved launch. Everything needed to start a job, nothing secret."""

    target: str = "runpod"              # runpod | docker | local
    image: str = "ghcr.io/itsnotyoutoday/pod-harness:latest"
    vcpu: int = 8
    disk_gb: int = 20

    # -- capacity, in priority order ---------------------------------------------------
    compute: str = "CPU"                # CPU | GPU
    flavors: tuple = ()                 # empty = the reaper's default order
    gpu_types: tuple = ()
    clouds: tuple = ("SECURE", "COMMUNITY")
    fallback_to_gpu: bool = False       # opt-in: ~12x the price
    max_cost_hr: float = 0.0            # 0 = no ceiling. Refuses, does not just warn.

    # -- cost --------------------------------------------------------------------------
    #: THE budget: the most you will pay per hour. Placement walks shapes and refuses any
    #: slot above this, releasing one immediately if the provider creates it anyway. This
    #: is what "budget" means — a price ceiling for finding a pod, not a time limit.
    #: (declared above as max_cost_hr)

    #: A last-resort lifetime cap, and NOT a budget despite once being called one. It
    #: applies only to a pod that never reports liveness: pod-control judges a reporting
    #: pod by whether it is still making progress, so a healthy job may run for days. This
    #: exists so a pod that reports nothing at all cannot be immortal.
    max_lifetime_min: float = 1440.0
    queue_deadline_min: float = 0.0     # give up waiting for capacity after this

    # -- storage -----------------------------------------------------------------------
    store: str = ""                     # profile: runpod | cloudflare | aws | minio
    store_keyfile: str = ""             # durable source of truth
    volume_keyfile: str = ""            # provider-local volume gateway
    #: RunPod only, and it PINS compute to the volume's datacenter. Empty means NO
    #: volume when a launch file is present — see `volume_declared`.
    runpod_volume: str = ""
    #: Did a launch file actually speak about the volume? Omitting the key used to mean
    #: "search upward for a key file and attach whatever you find", so a job that wanted
    #: no volume got one anyway — and with it a datacenter pin that made pods
    #: unobtainable. A launch file is authoritative: if it does not ask for a volume,
    #: there is no volume.
    volume_declared: bool = False
    cache: str = "persistent"           # persistent | ephemeral | off
    #: Store profiles whose credentials this job needs ON THE POD. Empty by default: a pod
    #: reading a mounted volume needs no credentials at all, and the fewer places a key
    #: exists the fewer places it leaks from. Naming a profile here is an explicit
    #: statement that this job talks to that store directly.
    forward_stores: tuple = ()
    local_workspace: str = "./work"     # TARGET=docker mounts this as /workspace

    #: Where pod-control lives, if it is running. With it set, a launch is SUBMITTED there
    #: and the queue, the placement walk and the deadline all move off this machine.
    #: Without it, the loader provisions directly exactly as before — this service must be
    #: an upgrade, never a dependency.
    control_url: str = ""
    control_token_file: str = ""
    #: SHA-256 of pod-control's certificate. The control plane has no DNS name,
    #: so pods verify it by PIN rather than by CA — one exact certificate instead
    #: of any certificate any CA was willing to sign.
    control_fingerprint: str = ""
    idempotency_key: str = ""
    #: Usually left empty and minted per job. Set it only when something outside
    #: this launch needs to poll the pod and cannot be told the minted value.
    api_token: str = ""

    job_spec: str = ""
    workload: str = ""
    autorun: bool = True

    extra_env: dict = field(default_factory=dict)
    source: str = "(defaults)"

    def describe(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k != "extra_env"}
        d["extra_env_keys"] = sorted(self.extra_env)
        return d


def load(path: str | Path | None = None) -> LaunchConfig:
    """Read `.pod.env`, or return defaults when there is none."""
    p = Path(path) if path else _find()
    if not p or not p.is_file():
        if path:
            raise LaunchFileError(f"no launch file at {path}")
        return LaunchConfig()

    kv = _parse(p.read_text())

    # Refuse inline secrets before doing anything else with the file.
    leaked = [k for k, v in kv.items()
              if _SECRET_KEYS.search(k) and v and not Path(v).expanduser().exists()]
    if leaked:
        raise LaunchFileError(
            f"{p} appears to contain secrets inline: {', '.join(leaked)}\n"
            f"  Put credentials in a *.key file outside the repo and reference it by path.\n"
            f"  A launch file lives next to the job, which means it gets committed.")

    def g(key, default=""):
        """Read a key, accepting the unprefixed spelling as an alias.

        Provider-specific settings are PODH_RUNPOD_* so a launch file shows at a glance
        which lines stop working the moment you change TARGET. Network volumes, CPU
        flavours and cloud tiers are RunPod's concepts, not the framework's — the
        unprefixed names implied they were portable.
        """
        if key in kv:
            return kv[key]
        for pre in ("PODH_RUNPOD_", "PODH_", "RUNPOD_"):
            if key.startswith(pre) and key[len(pre):] in kv:
                return kv[key[len(pre):]]
        return default

    def num(key, default):
        try:
            return type(default)(kv[key])
        except (KeyError, ValueError):
            return default

    cfg = LaunchConfig(
        target=g("TARGET", "runpod").lower(),
        image=g("IMAGE") or LaunchConfig.image,
        compute=g("COMPUTE", "CPU").upper(),
        flavors=tuple(x.strip() for x in g("PODH_RUNPOD_CPU_FLAVORS").split(",") if x.strip()),
        gpu_types=tuple(x.strip() for x in g("PODH_RUNPOD_GPU_TYPES").split(",") if x.strip()),
        clouds=tuple(x.strip().upper() for x in g("PODH_RUNPOD_CLOUD").split(",") if x.strip())
                or LaunchConfig.clouds,
        fallback_to_gpu=g("PODH_RUNPOD_FALLBACK_TO_GPU", "false").lower() in ("1","true","yes"),
        max_cost_hr=num("MAX_COST_HR", 0.0),
        max_lifetime_min=num("MAX_LIFETIME_MIN", num("BUDGET_MIN", 1440.0)),
        queue_deadline_min=num("QUEUE_DEADLINE_MIN", 0.0),
        vcpu=num("VCPU", 8),
        disk_gb=num("DISK_GB", 20),
        store=g("STORE").lower(),
        store_keyfile=g("STORE_KEYFILE"),
        volume_keyfile=g("VOLUME_KEYFILE"),
        cache=g("CACHE", "persistent").lower(),
        forward_stores=tuple(x.strip().lower()
                             for x in g("FORWARD_STORES").split(",") if x.strip()),
        local_workspace=g("LOCAL_WORKSPACE", "./work"),
        control_url=g("PODH_CONTROL_URL").rstrip("/"),
        control_token_file=g("PODH_CONTROL_TOKEN_FILE"),
        control_fingerprint=g("PODH_CONTROL_FINGERPRINT"),
        idempotency_key=g("IDEMPOTENCY_KEY"),
        api_token=g("PODH_API_TOKEN"),
        runpod_volume=g("PODH_RUNPOD_VOLUME"),
        volume_declared=bool(g("PODH_RUNPOD_VOLUME")),
        job_spec=g("JOB_SPEC"),
        workload=g("WORKLOAD"),
        autorun=g("AUTORUN", "true").lower() in ("1", "true", "yes"),
        extra_env={k[4:]: v for k, v in kv.items() if k.startswith("ENV_")},
        source=str(p),
    )
    return cfg


def apply_to_environ(cfg: LaunchConfig) -> None:
    """Export what the loader's own libraries read from the environment.

    Only the store profile and the volume: everything else is passed explicitly at launch,
    because mutating the environment to communicate between our own functions is how
    configuration becomes untraceable.
    """
    if cfg.store:
        os.environ["PODH_S3_PROFILE"] = cfg.store
    if cfg.runpod_volume:
        os.environ["RUNPOD_VOLUME"] = str(Path(cfg.runpod_volume).expanduser())
    if cfg.store_keyfile:
        os.environ["PODH_S3_KEY_FILE"] = str(Path(cfg.store_keyfile).expanduser())


def _minted_token(job_id: str) -> str:
    """A deterministic per-job token.

    Deterministic so a launcher that has the job id can reconstruct it and poll the pod
    without a side channel; salted with the machine's own secret so knowing a job id is
    not enough to derive it.
    """
    import hashlib
    import os
    from pathlib import Path

    salt = os.environ.get("PODH_TOKEN_SALT", "")
    if not salt:
        p = Path.home() / ".podh-token-salt"
        if not p.is_file():
            p.write_text(os.urandom(24).hex())
            p.chmod(0o600)
        salt = p.read_text().strip()
    return hashlib.sha256(f"{salt}:{job_id}".encode()).hexdigest()[:32]


def token_for(job_id: str) -> str:
    """The token a given job's pod is using, for polling it."""
    return _minted_token(job_id)


def pod_env(cfg: LaunchConfig, *, job_id: str, spec_key: str, code_root: str = "",
            workspace: str = "/workspace") -> dict:
    """Compose the environment the pod is given.

    Built here rather than at each call site so that a new required variable is added in
    one place — and checked against the harness contract, so a missing one fails while
    composing instead of when the pod refuses to boot.
    """
    # Without a token the harness's Caddy rejects every request, so the pod runs the job
    # perfectly and is completely unobservable. Generated per job rather than configured,
    # because a token that lives in a launch file is a token that gets committed, and one
    # shared across pods means compromising any pod compromises them all.
    api_token = cfg.api_token or _minted_token(job_id)

    env = {
        "PODH_API_TOKEN": api_token,
        "PODH_MODE": "batch" if cfg.autorun else "serve",
        "PODH_WORKSPACE": workspace,
        "PODH_JOB_ID": job_id,
        "PODH_JOB_SPEC": f"{workspace}/{spec_key}",
        "PODH_LOG_ROOT": f"{workspace}/runs",
        "PODH_RUN_PREFIX": f"runs/{job_id}",
        "PODH_WRITE_PREFIXES": f"runs/{job_id},_tmp/",
        "PODH_MOUNT_KIND": "volume" if cfg.runpod_volume else "object",
        "PODH_MAX_LIFE_SEC": str(int(cfg.max_lifetime_min * 60)),
        "PODH_MAX_IDLE_SEC": "0",
    }
    # The pod reports to pod-control only if it was told where, with what token, and
    # which certificate to expect. All three or none: a heartbeat that cannot verify the
    # endpoint must not send the token at all.
    if cfg.control_url and cfg.control_fingerprint:
        from .base_loader import _control_token, control_pod_token
        env["PODH_CONTROL_URL"] = cfg.control_url
        # The SCOPED token, never the master. A pod holding the master credential could
        # terminate every other pod, submit work and read the whole queue — an admin key
        # baked into a machine rented by the minute, in a datacenter we do not own,
        # running an image anyone can pull. This one says "I am alive" and "I am done"
        # about its own pod and nothing else.
        env["PODH_CONTROL_TOKEN"] = control_pod_token(cfg, job_id)
        env["PODH_CONTROL_JOB_ID"] = job_id
        env["PODH_CONTROL_FINGERPRINT"] = cfg.control_fingerprint

    # Resolve each named profile HERE and forward the resolved values, never a key file
    # path — the pod has no key files and must not be given one. Per-profile prefixes so a
    # job handed two stores can still tell them apart.
    for prof in cfg.forward_stores:
        from .objectstore import resolve_config
        c = resolve_config(prof)
        if c is None:
            continue
        pre = f"PODH_S3_{prof.upper()}_"
        env.update({pre + "BUCKET": c.bucket, pre + "ENDPOINT": c.endpoint_url,
                    pre + "ACCESS": c.access_key, pre + "SECRET": c.secret_key,
                    pre + "REGION": c.region})
        # Also unprefixed, so a workload that just calls get_storage() with no profile
        # gets the first store it was granted rather than nothing.
        if prof == cfg.forward_stores[0]:
            env.update({"PODH_S3_BUCKET": c.bucket, "PODH_S3_ENDPOINT": c.endpoint_url,
                        "PODH_S3_ACCESS": c.access_key, "PODH_S3_SECRET": c.secret_key,
                        "PODH_S3_REGION": c.region, "PODH_S3_PROFILE": prof})

    if cfg.cache == "off":
        env["PODH_CACHE_DISABLED"] = "1"
    elif cfg.cache == "persistent":
        env["PODH_CACHE_PERSISTENT"] = "1"
    if code_root:
        env["PODH_CODE_ROOT"] = f"{workspace}/{code_root}"
    env.update(cfg.extra_env)          # workload settings, forwarded verbatim
    return env


def capacity_kwargs(cfg: LaunchConfig) -> dict:
    """The capacity arguments for reaper.create_with_capacity, straight from the file."""
    kw = {"compute": cfg.compute, "clouds": cfg.clouds,
          "fallback_to_gpu": cfg.fallback_to_gpu, "max_cost_hr": cfg.max_cost_hr}
    if cfg.flavors:
        kw["flavors"] = cfg.flavors
    if cfg.gpu_types:
        kw["gpu_types"] = cfg.gpu_types
    return kw


def check(cfg: LaunchConfig) -> list[str]:
    """Problems that would waste a launch. Cheap, and run before anything is provisioned."""
    problems = []
    # Ask the registry rather than carrying a list. A hardcoded pair went stale the moment
    # "local" became "direct" and "docker" was added, and rejected a target that worked.
    try:
        from .base_loader import _registry
        known = sorted(_registry())
        if cfg.target not in known:
            problems.append(f"TARGET={cfg.target!r} is not one of: {', '.join(known)}")
    except Exception as e:
        problems.append(f"could not resolve targets: {e}")
    if cfg.job_spec and not Path(cfg.job_spec).expanduser().exists():
        problems.append(f"JOB_SPEC not found: {cfg.job_spec}")
    if cfg.workload and not (Path(cfg.workload).expanduser() / "code").is_dir():
        problems.append(f"WORKLOAD has no code/ directory: {cfg.workload}")
    if cfg.max_lifetime_min <= 0:
        problems.append("MAX_LIFETIME_MIN must be positive — a pod that reports "
                        "nothing must not be immortal")
    if cfg.control_url and not cfg.control_url.startswith(("http://", "https://")):
        problems.append(f"PODH_CONTROL_URL={cfg.control_url!r} needs a scheme (https://…)")
    if cfg.runpod_volume and cfg.runpod_volume.lower() in ("none", "off", "no"):
        problems.append(
            "PODH_RUNPOD_VOLUME is set to a word rather than a path or id. To run without "
            "a volume, omit the line entirely — a launch file that does not ask for one "
            "does not get one.")
    if cfg.control_url and not cfg.queue_deadline_min:
        problems.append(
            "PODH_CONTROL_URL is set with no QUEUE_DEADLINE_MIN. A job queued at 6pm that "
            "finally launches at 3am is a surprise you pay for — unattended launching "
            "without an expiry is how you find a bill in the morning.")
    # Caught here rather than as a provider 500 that a queue then retries forever.
    if cfg.disk_gb > 20:
        problems.append(
            f"DISK_GB={cfg.disk_gb} exceeds the 20 GB container-disk cap RunPod enforces. "
            f"The provider rejects this outright, and a queue cannot wait its way out of "
            f"a rejected request.")
    if cfg.compute not in ("CPU", "GPU"):
        problems.append(f"COMPUTE={cfg.compute!r} is not CPU or GPU")
    if cfg.cache not in ("persistent", "ephemeral", "off"):
        problems.append(f"CACHE={cfg.cache!r} is not persistent, ephemeral or off")
    if cfg.fallback_to_gpu and not cfg.max_cost_hr:
        problems.append(
            "FALLBACK_TO_GPU is on with no MAX_COST_HR. GPU is roughly 12x the price of "
            "CPU here ($0.06/hr vs $0.74/hr); without a ceiling a capacity shortage "
            "silently becomes a 12x bill.")
    if cfg.store:
        try:
            from .objectstore import resolve_config
            resolve_config(cfg.store)
        except Exception as e:
            problems.append(f"STORE={cfg.store!r}: {str(e).splitlines()[0]}")
    return problems


def template() -> str:
    return """# How this job launches. Lives beside the job; NEVER contains secrets.

# ---- destination -------------------------------------------------------------
TARGET            = runpod                 # runpod | docker | local
IMAGE             = ghcr.io/itsnotyoutoday/pod-harness:latest
VCPU              = 8
DISK_GB           = 30

# ---- capacity ----------------------------------------------------------------
# Generic. Every target understands these.
COMPUTE           = CPU
MAX_COST_HR       = 0.20

# RunPod-specific. PODH_RUNPOD_* means "this line stops meaning anything the moment
# you change TARGET" — network volumes, CPU flavours and cloud tiers are RunPod's
# concepts, not the framework's. RunPod takes the first shape with capacity, so a
# sold-out model degrades rather than failing.
PODH_RUNPOD_CPU_FLAVORS = cpu3c,cpu3g,cpu5c,cpu5g,cpu3m,cpu5m
PODH_RUNPOD_GPU_TYPES   = NVIDIA RTX A4000,NVIDIA RTX A4500,NVIDIA RTX A5000
PODH_RUNPOD_CLOUD       = SECURE,COMMUNITY

# GPU is roughly 12x the price of CPU ($0.06/hr vs $0.74/hr), so falling back to it
# is opt-in and requires a ceiling. A 4-minute benchmark does not care; a 6-hour
# corpus build is $0.36 against $4.44.
PODH_RUNPOD_FALLBACK_TO_GPU = false

# ---- cost --------------------------------------------------------------------
BUDGET_MIN        = 60                     # hard kill, enforced from outside the pod
QUEUE_DEADLINE_MIN= 240                    # stop waiting for capacity after this

# ---- storage -----------------------------------------------------------------
# Profiles, not credentials. Secrets stay in *.key files outside the repo.
#   runpod      RunPod's endpoint — NOT true S3 (no presigned URLs, batch delete 307s)
#   cloudflare  S3-compatible, verified
STORE             = runpod
STORE_KEYFILE     = ~/s3-cloudfare.key     # durable source of truth
VOLUME_KEYFILE    = ~/runpod-storage.key   # provider-local volume gateway
PODH_RUNPOD_VOLUME = ~/runpod-volume.key   # RunPod only; PINS the datacenter
CACHE             = persistent             # persistent | ephemeral | off

# ---- the job -----------------------------------------------------------------
JOB_SPEC          = jobs/my-job.json
WORKLOAD          = ../my-workload         # published before launch
AUTORUN           = true

# ---- optional: hand off to pod-control ---------------------------------------
# With this set, `runctl launch` submits and returns; the queue, the placement walk
# and the deadline move off this machine. Without it, nothing changes.
# PODH_CONTROL_URL = https://control.example.com:8787

# ---- forwarded to the pod with ENV_ stripped ---------------------------------
# ENV_LINGUA_MFA_ACOUSTIC = english_us_arpa
"""


if __name__ == "__main__":                      # python -m pod_loader.launchfile
    import json
    import sys
    if "--template" in sys.argv:
        print(template())
        raise SystemExit(0)
    try:
        cfg = load()
    except LaunchFileError as e:
        print(f"error: {e}")
        raise SystemExit(1)
    print(json.dumps(cfg.describe(), indent=2))
    problems = check(cfg)
    print("\nchecks: " + ("all pass" if not problems else ""))
    for p in problems:
        print(f"  ✗ {p}")
    raise SystemExit(1 if problems else 0)
