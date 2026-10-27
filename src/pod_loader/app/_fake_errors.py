"""Errors shared by FakeControl and its task half (testing.py, _fake_tasks.py)."""


class FakeError(RuntimeError):
    """A test hook was used in a way the real system could not produce."""


class FakeHeld(FakeError):
    """`place()` on a job the real dispatcher would leave queued."""


class _HTTPError(Exception):
    def __init__(self, status: int, error: str, hint: str = ""):
        self.status, self.error, self.hint = status, error, hint
