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
