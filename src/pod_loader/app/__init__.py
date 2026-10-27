"""App-side client for pod-control's pod API (docs/APP-API.md r3).

    from pod_loader.app import Client, JobSpec, Gpu, Boot, Policy

`pod_loader.app.testing.FakeControl` is an in-memory server for an app's own tests.
Needs httpx (`pip install "pod-loader-rpc[app]"`).
"""
from .client import (AppAPIError, Client, Conflict, Invalid, NotFound, TOKEN_HEADER,
                     Unauthorized, Unreachable)
from .spec import Boot, Gpu, JobSpec, Policy, spec_dict

__all__ = ["Client", "JobSpec", "Gpu", "Boot", "Policy", "spec_dict", "TOKEN_HEADER",
           "AppAPIError", "Unauthorized", "NotFound", "Conflict", "Invalid", "Unreachable"]
