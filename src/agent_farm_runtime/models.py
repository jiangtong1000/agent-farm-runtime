from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class TaskState(StrEnum):
    READY = "READY"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    SUBMITTED = "SUBMITTED"
    DONE = "DONE"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"


class WorkerState(StrEnum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    DEAD = "DEAD"


@dataclass(frozen=True)
class Lease:
    worker_id: str
    lease_id: str
    expiry: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Lease":
        return cls(**data)


@dataclass
class Task:
    id: str
    objective: str
    deliverable: str
    acceptance: str
    state: TaskState = TaskState.READY
    lease: Lease | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["state"] = self.state.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Task":
        data = dict(data)
        data["state"] = TaskState(data["state"])
        if data.get("lease") is not None:
            data["lease"] = Lease.from_dict(data["lease"])
        return cls(**data)


@dataclass
class Worker:
    id: str
    session_handle: str | None = None
    heartbeat: str | None = None
    lease: dict[str, Any] | None = None  # observed only; Task.lease is authoritative
    state: WorkerState = WorkerState.IDLE

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["state"] = self.state.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Worker":
        data = dict(data)
        data["state"] = WorkerState(data["state"])
        return cls(**data)


@dataclass(frozen=True)
class Event:
    id: str
    task_id: str
    type: str
    actor: str
    payload: dict[str, Any]
    ts: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ReceiptStatus(StrEnum):
    """What a worker reports about the unit of work it was leased to do.

    This is the structured receipt primitive (V2_DESIGN deferred item, minimal
    form). A worker never reports DONE: DONE requires recorded acceptance, which
    is a master/harness judgment, not a worker claim (Layer-1/Layer-2 boundary).
    """

    RUNNING = "RUNNING"      # heartbeat; still executing -> task stays RUNNING
    AWAITING = "AWAITING"    # reached a named wait boundary -> RUNNING->WAITING
    SUBMITTED = "SUBMITTED"  # produced the final deliverable -> RUNNING->SUBMITTED
    FAILED = "FAILED"        # unrecoverable -> RUNNING->FAILED


@dataclass(frozen=True)
class Receipt:
    """A worker's structured report, fenced by lease_id.

    The reconciler MUST ignore any receipt whose lease_id does not match the
    task's authoritative lease (a receipt from a superseded worker generation).
    """

    worker_id: str
    task_id: str
    lease_id: str
    status: ReceiptStatus
    ts: str
    note: str = ""
    waiting_on: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Receipt":
        data = dict(data)
        data["status"] = ReceiptStatus(data["status"])
        return cls(**data)
