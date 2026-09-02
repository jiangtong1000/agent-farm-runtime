from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .models import Task, Worker


class StoreError(RuntimeError):
    pass


class FarmPaths:
    def __init__(self, root: Path):
        self.root = root
        self.tasks = root / "tasks"
        self.workers = root / "workers"
        self.events = root / "events"
        self.decisions = root / "decisions"
        self.runtime = root / "runtime"

    def ensure(self) -> None:
        for path in (self.tasks, self.workers, self.events, self.decisions, self.runtime):
            path.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


class TaskStore:
    """Authoritative task store. Mutations should be routed through transition logic."""

    def __init__(self, paths: FarmPaths):
        self.paths = paths

    def path_for(self, task_id: str) -> Path:
        return self.paths.tasks / f"{task_id}.json"

    def create(self, task: Task) -> None:
        path = self.path_for(task.id)
        if path.exists():
            raise StoreError(f"task already exists: {task.id}")
        atomic_write_json(path, task.to_dict())

    def get(self, task_id: str) -> Task:
        path = self.path_for(task_id)
        if not path.exists():
            raise StoreError(f"task not found: {task_id}")
        return Task.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def put_authoritative(self, task: Task) -> None:
        """Low-level write path. Callers must validate via transitions first."""
        if not self.path_for(task.id).exists():
            raise StoreError(f"task not found: {task.id}")
        atomic_write_json(self.path_for(task.id), task.to_dict())

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
