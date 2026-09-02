from __future__ import annotations

import contextlib
import fcntl
import os
from pathlib import Path


class ReconcilerBusy(RuntimeError):
    """Another reconciler already holds the exclusive lock for this farm."""


@contextlib.contextmanager
def single_reconciler(runtime_dir: Path):
    """Enforce INV-6 (serialized reconciliation) with an OS advisory lock.

    A non-blocking exclusive flock on <runtime>/reconcile.lock guarantees at most
    one reconciler mutates a given farm at a time, so two `farm reconcile`
    processes cannot race on the same READY task. Advisory + local-fs only, which
    is sufficient for a single-host farm; a distributed farm would need a fenced
    lease service instead (deferred).
    """
    runtime_dir = Path(runtime_dir)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    lock_path = runtime_dir / "reconcile.lock"
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ReconcilerBusy(
                f"another reconciler holds {lock_path}; refusing to run concurrently"
            ) from exc
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
