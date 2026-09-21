from __future__ import annotations

from dataclasses import dataclass
import json

from .models import TaskState
from .store import FarmPaths, RESERVED_STATES, TaskStore, WorkerRegistry, workspace_key
from .observers import waiting_on_error
from .provenance import runtime_identity


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
        if task.metadata.get("recovery_hold"):
            checks.append(Check("WARN", f"{task.id}: recovery hold; master ruling required before new execution"))
        if task.metadata.get("runtime_error"):
            checks.append(Check("WARN", f"{task.id}: unresolved runtime_error; inspect task-show --summary"))
        if task.metadata.get("restart_limit_reached"):
            checks.append(Check("WARN", f"{task.id}: restart budget exhausted; explicit ruling required"))
        # A workspace is one agent's mutable state (LEDGER, .session_id, MASTER
        # notes); two concurrently-active tasks must never share one, or their
        # workers would corrupt each other. Forbid it as a hard invariant.
        if task.state in RESERVED_STATES:
            ws = workspace_key(task)
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
        if task.state is TaskState.WAITING and task.lease is None:
            from .turnover import clean_surrender
            if not clean_surrender(task):
                checks.append(Check("FAIL", f"{task.id}: WAITING without lease or certified clean surrender"))
        if task.state is TaskState.WAITING and not task.metadata.get("waiting_on"):
            checks.append(Check("FAIL", f"{task.id}: WAITING without metadata.waiting_on"))
        elif task.state is TaskState.WAITING:
            error = waiting_on_error(task.metadata.get("waiting_on"))
            if error:
                checks.append(Check("FAIL", f"{task.id}: invalid waiting_on: {error}"))
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

    if (paths.runtime / "pending-task-commit.json").exists():
        checks.append(Check("FAIL", "pending task commit: next upgraded writer must recover it; do not hand-edit"))
    if (paths.runtime / "pending-recovery.json").exists():
        checks.append(Check("FAIL", "offline recovery pending: ordinary writes disabled; rerun recover with its recorded plan"))
    deployment = paths.runtime / "deployment.json"
    if not deployment.exists():
        checks.append(Check("WARN", "no daemon deployment manifest: running code version is unverified"))
    else:
        try:
            recorded = json.loads(deployment.read_text())
            current = runtime_identity()
            if recorded.get("pending_event"):
                checks.append(Check("FAIL", "pending deployment audit; retry the recorded handoff/startup"))
            if recorded.get("handoff", {}).get("phase") in {"draining", "released"}:
                checks.append(Check("WARN", f"handoff {recorded['handoff']['phase']}; dispatch disabled"))
            if recorded.get("recovery_id") and recorded.get("started_at") is None:
                checks.append(Check("WARN", "deployment recovered but daemon not started; recovery does not actuate"))
            if recorded.get("host") != current["host"] and any(task.lease for task in tasks):
                checks.append(Check("WARN", "active leases on a different deployment host; no automatic takeover"))
            if any(recorded.get(k) != current[k] for k in ("protocol_version", "source_sha256")):
                checks.append(Check("WARN", "daemon startup code differs from this CLI; controlled daemon restart required"))
            else:
                checks.append(Check("PASS", "daemon startup source matches CLI (not a daemon liveness check)"))
        except (ValueError, OSError):
            checks.append(Check("FAIL", "unreadable daemon deployment manifest"))
    if not any(c.level == "FAIL" for c in checks):
        checks.append(Check("PASS", f"{len(tasks)} task(s): no invariant violations detected"))
    return checks
