from __future__ import annotations

from dataclasses import replace
from typing import Any

from .models import Lease, Task, TaskState


class TransitionError(ValueError):
    pass


LEGAL_TRANSITIONS: dict[TaskState, set[TaskState]] = {
    TaskState.READY: {TaskState.RUNNING, TaskState.FAILED},
    TaskState.RUNNING: {
        TaskState.WAITING,
        TaskState.SUBMITTED,
        TaskState.BLOCKED,
        TaskState.FAILED,
    },
    TaskState.WAITING: {
        TaskState.RUNNING,
        TaskState.BLOCKED,
        TaskState.FAILED,
    },
    TaskState.SUBMITTED: {
        TaskState.RUNNING,
        TaskState.DONE,
        TaskState.FAILED,
    },
    TaskState.BLOCKED: {
        TaskState.READY,
        TaskState.RUNNING,
        TaskState.FAILED,
    },
    TaskState.DONE: set(),
    TaskState.FAILED: set(),
}


def validate_transition(
    task: Task,
    target: TaskState,
    *,
    lease_id: str | None = None,
    acceptance_recorded: bool = False,
) -> None:
    if target not in LEGAL_TRANSITIONS[task.state]:
        raise TransitionError(f"illegal task transition: {task.state} -> {target}")

    execution_scoped = task.state in {TaskState.RUNNING, TaskState.WAITING}
    if execution_scoped and task.lease is not None:
        if lease_id != task.lease.lease_id:
            raise TransitionError("stale or missing lease id")

    if target is TaskState.DONE and not acceptance_recorded:
        raise TransitionError("DONE requires recorded acceptance")


def transition_task(
    task: Task,
    target: TaskState,
    *,
    lease_id: str | None = None,
    acceptance_recorded: bool = False,
    metadata_patch: dict[str, Any] | None = None,
    new_lease: Lease | None | object = ...,
) -> Task:
    """Pure transition function; storage must call this before authoritative writes."""
    validate_transition(
        task,
        target,
        lease_id=lease_id,
        acceptance_recorded=acceptance_recorded,
    )
    metadata = dict(task.metadata)
    if metadata_patch:
        metadata.update(metadata_patch)

    lease = task.lease if new_lease is ... else new_lease
    return replace(task, state=target, metadata=metadata, lease=lease)  # type: ignore[arg-type]


class LeaseError(ValueError):
    pass


def acquire_lease(task: Task, new_lease: Lease) -> Task:
    """Grant execution ownership to a task that has none (adoption / first start).

    A lease is granted only when the task currently holds no lease. This is the
    normal READY->RUNNING actuation path and the re-actuation path after a lease
    has been released by rotation off a dead worker.
    """
    if task.lease is not None:
        raise LeaseError(
            f"{task.id} already leased to {task.lease.worker_id}; "
            "rotate off the dead holder before re-acquiring"
        )
    return replace(task, lease=new_lease)


def rotate_lease(task: Task, *, dead_worker_id: str) -> Task:
    """Release a lease held by a worker the reconciler has PROVEN dead/absent.

    This is the crash-adoption primitive: it makes "task state is durable,
    workers are disposable" real. It is a lease mutation, not a state
    transition, so INV-7 (transitions are the only state mutation) is untouched;
    the task's state is preserved. INV-5 (single valid executor) is preserved
    because the caller must pass the id of the CURRENT lease holder and must have
    already established that that worker is dead/absent — a lease can never be
    rotated away from a live worker, so ownership is never double-granted.

    Beyond frozen V2_DESIGN v0.2 (which stopped before actuation); intended for
    ratification into v0.3 once canary evidence confirms it.
    """
    if task.lease is None:
        raise LeaseError(f"{task.id} holds no lease to rotate")
    if task.lease.worker_id != dead_worker_id:
        raise LeaseError(
            f"refusing to rotate lease: current holder is {task.lease.worker_id}, "
            f"not {dead_worker_id} (never rotate away from a possibly-live worker)"
        )
    return replace(task, lease=None)
