"""Destinations on this machine.

    docker   a container from the same image a pod would run — the faithful one
    direct   the harness in your interpreter — the fast one, and the least faithful

Both cost nothing, which makes them the right place to reproduce a failure you would
otherwise pay to see twice.
"""
from .loader import DirectLoader, DockerLoader          # noqa: F401
