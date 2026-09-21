from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from .adapters.filesystem import atomic_write_json, require_writable_filesystem, sync_directory
from .events import EventLog
from .locking import task_mutation_lock
from .models import Event, Task, TaskState, Worker
from .transitions import transition_task


class StoreError(RuntimeError):
    pass


class StoreConflict(StoreError):
    """Stale snapshot or occupied workspace: reread, never blindly retry a write."""


RESERVED_STATES = {TaskState.READY, TaskState.RUNNING, TaskState.WAITING, TaskState.SUBMITTED}


def require_no_pending_recovery(paths: FarmPaths) -> None:
    if (paths.runtime / "pending-recovery.json").exists():
        raise StoreError("offline recovery pending: finish the recorded recovery before ordinary writes")


def workspace_key(task: Task) -> str | None:
    value = task.metadata.get("workspace")
    return str(Path(value).resolve()) if value else None


class FarmPaths:
    def __init__(self, root: Path):
        self.root = root
        self.tasks = root / "tasks"
        self.workers = root / "workers"
        self.events = root / "events"
        self.decisions = root / "decisions"
        self.runtime = root / "runtime"

    def ensure(self) -> None:
        require_writable_filesystem()
        for path in (self.tasks, self.workers, self.events, self.decisions, self.runtime):
            path.mkdir(parents=True, exist_ok=True)


class TaskStore:
    """Authoritative task store. Mutations should be routed through transition logic."""

    def __init__(self, paths: FarmPaths):
        self.paths = paths

    def path_for(self, task_id: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", task_id):
            raise StoreError(f"invalid task id: {task_id!r}")
        return self.paths.tasks / f"{task_id}.json"

    def create(self, task: Task) -> None:
        self.commit(task, expected=None, event_type="TASK_CREATED", actor="master")

    def get(self, task_id: str) -> Task:
        path = self.path_for(task_id)
        if not path.exists():
            raise StoreError(f"task not found: {task_id}")
        return Task.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def put_authoritative(self, task: Task, *, expected: Task) -> Task:
        """Compatibility API; a pre-edit snapshot is now mandatory. Prefer CLI."""
        return self.commit(task, expected=expected, event_type="TASK_UPDATED", actor="api")

    def _recover_locked(self) -> None:
        pending = self.paths.runtime / "pending-task-commit.json"
        if not pending.exists():
            return
        txn = json.loads(pending.read_text(encoding="utf-8"))
        after = Task.from_dict(txn["after"])
        path = self.path_for(after.id)
        current = self.get(after.id).to_dict() if path.exists() else None
        if current not in (txn["before"], txn["after"]):
            raise StoreConflict("pending transaction conflicts with disk; stop writers and inspect")
        log = EventLog(self.paths.events / "log.ndjson")
        # Parse before writing: a damaged audit log is a fail-closed repair case.
        ids = log.ids()
        # A visible after-image may still need its failed directory fsync retried.
        atomic_write_json(path, txn["after"])
        if txn["event"]["id"] not in ids:
            log.append(Event(**txn["event"]))
        else:
            # A prior append may have become visible but failed file/directory
            # fsync. Re-establish durability before removing the recovery journal.
            log.sync()
        pending.unlink()
        sync_directory(pending.parent)

    def recover(self) -> None:
        """Finish a prepared commit before reconciliation; never used by readers."""
        with task_mutation_lock(self.paths.runtime):
            from .turnover import require_task_writer
            require_task_writer(self.paths)
            self._recover_locked()

    def commit(self, task: Task, *, expected: Task | None, event_type: str,
               actor: str, payload: dict | None = None, ts: str | None = None,
               dispatch: Callable[[Task], None] | None = None, execution_epoch: str | None = None) -> Task:
        """CAS + workspace reservation + recoverable state/audit write.

        The journal is a single pending transaction, not a second task store.
        A process crash at any completed write is replayed idempotently. Corrupt
        JSON/torn audit records fail closed and require operator inspection.
        """
        with task_mutation_lock(self.paths.runtime):
            from .turnover import require_task_writer
            require_task_writer(self.paths, dispatch=dispatch is not None, epoch=execution_epoch)
            saved = self._commit_locked(task, expected=expected, event_type=event_type,
                                        actor=actor, payload=payload, ts=ts)
            # Serialize admission with drain, INCLUDING the external dispatch.
            # Never replay this callback from the journal: an uncertain effect
            # keeps its persisted lease and requires positive observation.
            if dispatch is not None:
                dispatch(saved)
            return saved

    def _commit_locked(self, task: Task, *, expected: Task | None, event_type: str,
                       actor: str, payload: dict | None = None, ts: str | None = None) -> Task:
        """Same commit boundary; offline recovery already holds task_mutation_lock."""
        self._recover_locked()
        path = self.path_for(task.id)
        before = expected.to_dict() if expected is not None else None
        if expected is not None and expected.id != task.id:
            raise StoreError("cannot change task id")
        current = self.get(task.id).to_dict() if path.exists() else None
        if current != before:
            raise StoreConflict(f"{task.id}: stale snapshot or task already exists; reread task-show")
        if expected is not None and task.state != expected.state:
            transition_task(expected, task.state,
                            lease_id=expected.lease.lease_id if expected.lease else None,
                            acceptance_recorded=bool(task.metadata.get("acceptance_receipt")),
                            metadata_patch=task.metadata, new_lease=task.lease)
        ws = workspace_key(task)
        if ws and task.state in RESERVED_STATES:
            for other in self.list():
                if other.id != task.id and other.state in RESERVED_STATES and workspace_key(other) == ws:
                    raise StoreConflict(f"{task.id}: workspace {ws} reserved by {other.id} ({other.state})")
        revision = int((expected.metadata if expected else {}).get("revision", 0)) + 1
        saved = replace(task, metadata={**task.metadata, "revision": revision})
        event = Event(id=uuid.uuid4().hex, task_id=task.id, type=event_type,
                      actor=actor, payload={**(payload or {}), "revision": revision},
                      ts=ts or datetime.now(timezone.utc).isoformat())
        atomic_write_json(self.paths.runtime / "pending-task-commit.json", {
            "before": before, "after": saved.to_dict(), "event": event.to_dict(),
        })
        self._recover_locked()
        return saved

    def list(self) -> list[Task]:
        return [
            Task.from_dict(json.loads(path.read_text(encoding="utf-8")))
            for path in sorted(self.paths.tasks.glob("*.json"))
        ]


class WorkerRegistry:
    """Observed worker state; never authoritative over Task.lease."""

    def __init__(self, paths: FarmPaths):
        self.paths = paths

    def path_for(self, worker_id: str) -> Path:
        return self.paths.workers / f"{worker_id}.json"

    def put_observed(self, worker: Worker) -> None:
        atomic_write_json(self.path_for(worker.id), worker.to_dict())

    def list(self) -> list[Worker]:
        out: list[Worker] = []
        for path in sorted(self.paths.workers.glob("*.json")):
            out.append(Worker.from_dict(json.loads(path.read_text(encoding="utf-8"))))
        return out
