from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..models import Lease, Receipt, Task


@dataclass(frozen=True)
class LaunchHandle:
    """Opaque handle to a launched worker process/session.

    session_handle is executor-specific (a pid, a tmux "session:window", a codex
    rollout id). The reconciler treats it as opaque and only stores it on the
    observed Worker record.
    """

    worker_id: str
    session_handle: str


@dataclass(frozen=True)
class WorkerObservation:
    """What the executor can observe about a worker it launched.

    alive: is the underlying process/session still running?
    receipt: the worker's latest structured receipt, if it has written one.
    The reconciler fences the receipt against Task.lease itself; the executor is
    only responsible for surfacing it.
    """

    worker_id: str
    alive: bool
    receipt: Receipt | None = None


class WorkerExecutor(Protocol):
    """Boundary for disposable worker backends (codex/tmux, local process, fake).

    Contract:
    - launch(task, lease): start a NEW worker to execute `task` under `lease`.
      The worker is told its worker_id, task_id, and lease_id, and is told where
      to write its receipt; it must echo lease_id in every receipt (fencing).
    - resume(task, worker_id, lease): re-wake an existing worker for the same
      lease (idempotent; cheap no-op if already awake).
    - poll(worker_id): observe liveness + latest receipt. Never mutates.
    - stop(worker_id): best-effort terminate; used when fencing off a superseded
      or failed worker. Must be safe to call on an already-dead worker.
    """

    def launch(self, task: Task, lease: Lease) -> LaunchHandle: ...

    def resume(self, task: Task, worker_id: str, lease: Lease) -> None: ...

    def poll(self, worker_id: str) -> WorkerObservation: ...

    def stop(self, worker_id: str) -> None: ...


class ComputeObserver(Protocol):
    """Future boundary for external compute substrates such as SLURM."""

    def observe_job(self, job_id: str) -> str | None: ...
