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
