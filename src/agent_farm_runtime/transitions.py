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


# Lease ownership is authoritative Task state. acquire/rotate/release below are
# authoritative Task mutations, NOT exempt from INV-7: like state transitions,
# each returns a new Task that the caller MUST commit through the single durable
# store boundary (see Reconciler._commit), paired with an event. They are kept as
# separate pure validators only because the legality rules differ from the state
# graph's; the persistence boundary is the same one transitions go through.


def with_metadata(task: Task, patch: dict[str, Any]) -> Task:
    """Authoritative metadata-only mutation (e.g. launched_ts). Commit like any other."""
    meta = dict(task.metadata)
    meta.update(patch)
    return replace(task, metadata=meta)


def acquire_lease(task: Task, new_lease: Lease, *, metadata_patch: dict[str, Any] | None = None) -> Task:
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
    meta = dict(task.metadata)
    if metadata_patch:
        meta.update(metadata_patch)
    return replace(task, lease=new_lease, metadata=meta)


def rotate_lease(task: Task, *, dead_worker_id: str, reason: str) -> Task:
    """Release a lease from a worker the reconciler has established is DEAD.

    The crash-adoption primitive that makes "task state is durable, workers are
    disposable" real. INV-5 (single valid executor) is preserved because the
    caller must pass the id of the CURRENT lease holder AND must supply a `reason`
    documenting how death was established (e.g. no heartbeat within the grace
    window) — a lease must never be rotated on a single transient liveness miss,
    which would revoke a live worker's ownership. See the reconciler's grace
    policy for the death criterion.
    """
    if not reason:
        raise LeaseError("rotate_lease requires a reason establishing death")
    if task.lease is None:
        raise LeaseError(f"{task.id} holds no lease to rotate")
    if task.lease.worker_id != dead_worker_id:
        raise LeaseError(
            f"refusing to rotate lease: current holder is {task.lease.worker_id}, "
            f"not {dead_worker_id} (never rotate away from a possibly-live worker)"
        )
    return replace(task, lease=None)
