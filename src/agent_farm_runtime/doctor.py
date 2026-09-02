from __future__ import annotations

from dataclasses import dataclass

from .models import TaskState
from .store import FarmPaths, TaskStore, WorkerRegistry


@dataclass(frozen=True)
class Check:
    level: str
    message: str


def run_doctor(paths: FarmPaths) -> list[Check]:
    checks: list[Check] = []
    store = TaskStore(paths)
    registry = WorkerRegistry(paths)
    tasks = store.list()
    workers = {w.id: w for w in registry.list()}

    lease_ids: set[str] = set()
    worker_to_task: dict[str, str] = {}
    active_workspace: dict[str, str] = {}

    for task in tasks:
        # A workspace is one agent's mutable state (LEDGER, .session_id, MASTER
        # notes); two concurrently-active tasks must never share one, or their
        # workers would corrupt each other. Forbid it as a hard invariant.
        if task.state in {TaskState.RUNNING, TaskState.WAITING}:
            ws = task.metadata.get("workspace")
            if ws:
                if ws in active_workspace:
                    checks.append(Check("FAIL", f"workspace {ws} shared by active tasks "
                                                 f"{active_workspace[ws]} and {task.id}"))
                else:
                    active_workspace[ws] = task.id

        if task.lease:
            if task.lease.lease_id in lease_ids:
                checks.append(Check("FAIL", f"duplicate lease id {task.lease.lease_id}"))
            lease_ids.add(task.lease.lease_id)
            if task.lease.worker_id in worker_to_task:
                checks.append(Check("FAIL", f"worker {task.lease.worker_id} authoritatively leased to multiple tasks"))
            worker_to_task[task.lease.worker_id] = task.id

        if task.state is TaskState.RUNNING and task.lease is None:
            checks.append(Check("FAIL", f"{task.id}: RUNNING without authoritative lease"))
        if task.state is TaskState.WAITING and not task.metadata.get("waiting_on"):
            checks.append(Check("FAIL", f"{task.id}: WAITING without metadata.waiting_on"))
        if task.state is TaskState.DONE and not task.metadata.get("acceptance_receipt"):
            checks.append(Check("FAIL", f"{task.id}: DONE without acceptance_receipt"))

    for worker_id, task_id in worker_to_task.items():
        worker = workers.get(worker_id)
        if worker is None:
            checks.append(Check("WARN", f"{task_id}: leased worker {worker_id} absent from observed registry"))
            continue
        observed_task = (worker.lease or {}).get("task_id")
        if observed_task not in (None, task_id):
            checks.append(Check("WARN", f"worker registry mismatch for {worker_id}: observed={observed_task}, authoritative={task_id}"))

    if not any(c.level == "FAIL" for c in checks):
        checks.append(Check("PASS", f"{len(tasks)} task(s): no invariant violations detected"))
    return checks
