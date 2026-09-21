from __future__ import annotations

import contextlib
from pathlib import Path

from .adapters.filesystem import FileLockBusy, exclusive_lock


class ReconcilerBusy(RuntimeError):
    """Another reconciler already holds the exclusive lock for this farm."""


@contextlib.contextmanager
def task_mutation_lock(runtime_dir: Path):
    """Short, cooperative transaction lock shared by masters and reconciler.

    Separate from the daemon-lifetime lock. All writers must run this version;
    this is not a cross-host fencing service and cannot fence legacy JSON edits.
    """
    with exclusive_lock(runtime_dir / "task-mutation.lock", blocking=True):
        yield


@contextlib.contextmanager
def single_reconciler(runtime_dir: Path):
    """Enforce INV-6 (serialized reconciliation) with an OS advisory lock.

    A non-blocking exclusive flock on <runtime>/reconcile.lock guarantees at most
    one reconciler mutates a given farm at a time, so two `farm reconcile`
    processes cannot race on the same READY task. Advisory + local-fs only, which
    is sufficient for a single-host farm; a distributed farm would need a fenced
    lease service instead (deferred).
    """
    try:
        with exclusive_lock(Path(runtime_dir) / "reconcile.lock", blocking=False, record_pid=True):
            yield
    except FileLockBusy as exc:
        raise ReconcilerBusy(str(exc)) from exc
