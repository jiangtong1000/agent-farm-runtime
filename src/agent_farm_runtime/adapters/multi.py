"""Per-task executor selection inside one farm (D6, first batch).

The reconciler keeps talking to a single WorkerExecutor; this one routes each call
to the backend named by `task.metadata["executor"]` (default: the farm's default).
Worker-scoped calls (poll/stop) are routed through the task that owns the worker,
looked up in the Task Store, so no second registry of ownership is introduced.
"""
from __future__ import annotations

from collections.abc import Callable

from ..models import Lease, Task
from ..store import TaskStore
from .base import ExecutorUnavailable, LaunchHandle, WorkerExecutor, WorkerObservation


class MultiExecutor:
    def __init__(self, default: str, factories: dict[str, Callable[[], WorkerExecutor]], store: TaskStore):
        if default not in factories:
            raise ValueError(f"default executor {default!r} is not among {sorted(factories)}")
        self.default = default
        self._factories = factories
        self._instances: dict[str, WorkerExecutor] = {}
        self._worker_backend: dict[str, str] = {}
        self.store = store

    # -- resolution -------------------------------------------------------------
    def name_for_task(self, task: Task) -> str:
        name = task.metadata.get("executor") or self.default
        if name not in self._factories:
            raise ValueError(f"task {task.id} names unknown executor {name!r}; known: {sorted(self._factories)}")
        return name

    def backend(self, name: str) -> WorkerExecutor:
        if name not in self._instances:
            self._instances[name] = self._factories[name]()
        return self._instances[name]

    def for_task(self, task: Task) -> WorkerExecutor:
        return self.backend(self.name_for_task(task))

    def for_worker(self, worker_id: str) -> WorkerExecutor:
        name = self._worker_backend.get(worker_id)
        if name is None:
            for task in self.store.list():
                issued = (task.metadata.get("dispatches") or {}).get("workers", [])
                if (task.lease and task.lease.worker_id == worker_id) or worker_id in issued:
                    name = self.name_for_task(task)
                    break
            else:
                name = self.default  # unknown worker: the default backend answers UNKNOWN/no-op
            self._worker_backend[worker_id] = name
        return self.backend(name)

    # -- WorkerExecutor protocol ------------------------------------------------
    def validate_task(self, task: Task) -> None:
        backend = self.for_task(task)  # raises ValueError for an unknown executor name
        validate = getattr(backend, "validate_task", None)
        if validate is not None:
            validate(task)

    def launch(self, task: Task, lease: Lease) -> LaunchHandle:
        self._worker_backend[lease.worker_id] = self.name_for_task(task)
        return self.for_task(task).launch(task, lease)

    def resume(self, task: Task, worker_id: str, lease: Lease) -> None:
        self._worker_backend[worker_id] = self.name_for_task(task)
        self.for_task(task).resume(task, worker_id, lease)

    def poll(self, worker_id: str) -> WorkerObservation:
        return self.for_worker(worker_id).poll(worker_id)

    def stop(self, worker_id: str) -> None:
        self.for_worker(worker_id).stop(worker_id)
