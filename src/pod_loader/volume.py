"""Moved to pod_loader.runpod.volume — RunPod-specific code now lives in its own package.

Kept as a re-export so existing imports do not break; the real module is one level down.
"""
from .runpod.volume import *          # noqa: F401,F403
from .runpod import volume as _m
import sys as _sys
_sys.modules[__name__].__dict__.update(
    {k: v for k, v in _m.__dict__.items() if not k.startswith("__")})
