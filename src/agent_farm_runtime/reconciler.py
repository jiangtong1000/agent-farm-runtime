from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .adapters.base import WorkerExecutor
from .events import EventLog
from .models import (
    Event,
    Lease,
    Receipt,
    ReceiptStatus,
    Task,
    TaskState,
    Worker,
    WorkerState,
)
from .store import FarmPaths, TaskStore, WorkerRegistry
from .transitions import acquire_lease, rotate_lease, transition_task

# States from which a worker's structured receipt drives the next state.
_RECEIPT_TARGET = {
    ReceiptStatus.AWAITING: TaskState.WAITING,
    ReceiptStatus.SUBMITTED: TaskState.SUBMITTED,
    ReceiptStatus.FAILED: TaskState.FAILED,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ReconcileReport:
    launched: list[str] = field(default_factory=list)       # task ids newly actuated
    adopted: list[str] = field(default_factory=list)        # task ids re-actuated after crash
    advanced: list[tuple[str, str]] = field(default_factory=list)  # (task, new state)
    resumed: list[str] = field(default_factory=list)        # WAITING->RUNNING
    heartbeats: list[str] = field(default_factory=list)     # worker ids seen alive
    ignored_stale: list[str] = field(default_factory=list)  # fenced-out receipts (worker ids)


class Reconciler:
    """Serialized, idempotent control loop (INV-6).

    One `reconcile_once` call is a single pass over authoritative task state; it
    is the only thing that actuates workers. It:
      1. actuates READY tasks (mint lease, launch worker, READY->RUNNING);
      2. applies fenced receipts from live workers (RUNNING->WAITING/SUBMITTED/FAILED);
      3. adopts crashed RUNNING tasks (rotate lease off the dead worker, relaunch);
      4. resumes WAITING tasks whose unblock predicate fires;
      5. repairs the observed Worker Registry to match Task.lease + liveness.
    All state mutation goes through transitions/lease helpers; the registry is
    observed-only and never authoritative.
    """

    def __init__(
        self,
        paths: FarmPaths,
        executor: WorkerExecutor,
        *,
        actor: str = "reconciler",
        clock: Callable[[], str] = _now,
        unblock: Callable[[Task], bool] | None = None,
    ):
        self.paths = paths
        self.executor = executor
        self.actor = actor
        self.clock = clock
        self.unblock = unblock
        self.tasks = TaskStore(paths)
        self.registry = WorkerRegistry(paths)
        self.events = EventLog(paths.events / "log.ndjson")

    # -- helpers ---------------------------------------------------------------

    def _emit(self, task_id: str, type_: str, payload: dict) -> None:
        self.events.append(
            Event(
                id=uuid.uuid4().hex,
                task_id=task_id,
                type=type_,
                actor=self.actor,
                payload=payload,
                ts=self.clock(),
            )
        )

    def _mint_lease(self, worker_id: str) -> Lease:
        return Lease(worker_id=worker_id, lease_id=uuid.uuid4().hex)

    def _observe_worker(self, worker_id: str, handle: str | None, task_id: str, lease_id: str, alive: bool) -> None:
        self.registry.put_observed(
            Worker(
                id=worker_id,
                session_handle=handle,
                heartbeat=self.clock(),
                lease={"task_id": task_id, "lease_id": lease_id},
                state=WorkerState.BUSY if alive else WorkerState.DEAD,
            )
        )

    # -- individual task handlers ---------------------------------------------

    def _actuate_ready(self, task: Task, report: ReconcileReport) -> None:
        worker_id = f"W-{task.id}-{uuid.uuid4().hex[:6]}"
        lease = self._mint_lease(worker_id)
        leased = acquire_lease(task, lease)
        handle = self.executor.launch(leased, lease)
        started = transition_task(
            leased, TaskState.RUNNING, lease_id=lease.lease_id, new_lease=lease
        )
        self.tasks.put_authoritative(started)
        self._observe_worker(worker_id, handle.session_handle, task.id, lease.lease_id, alive=True)
        self._emit(task.id, "WORKER_LAUNCHED", {"worker_id": worker_id, "lease_id": lease.lease_id})
        report.launched.append(task.id)

    def _apply_receipt(self, task: Task, receipt: Receipt, report: ReconcileReport) -> bool:
        """Apply a fenced receipt to a RUNNING task. Returns True if state changed."""
        if receipt.status is ReceiptStatus.RUNNING:
            self._observe_worker(
                receipt.worker_id, None, task.id, task.lease.lease_id, alive=True
            )
            report.heartbeats.append(receipt.worker_id)
            return False

        target = _RECEIPT_TARGET[receipt.status]
        patch: dict = {"last_receipt_note": receipt.note}
        if target is TaskState.WAITING:
            patch["waiting_on"] = receipt.waiting_on or "unspecified"
        # SUBMITTED/FAILED/WAITING: keep the lease on WAITING (same worker may
        # resume); release it on SUBMITTED/FAILED so no worker lingers as owner.
        new_lease: object = task.lease if target is TaskState.WAITING else None
        advanced = transition_task(
            task,
            target,
            lease_id=task.lease.lease_id,
            metadata_patch=patch,
            new_lease=new_lease,
        )
        self.tasks.put_authoritative(advanced)
        if target is not TaskState.WAITING:
            self.executor.stop(receipt.worker_id)
            self._observe_worker(
                receipt.worker_id, None, task.id, task.lease.lease_id, alive=False
            )
        else:
            self._observe_worker(
                receipt.worker_id, None, task.id, task.lease.lease_id, alive=True
            )
        self._emit(
            task.id,
            "RECEIPT_APPLIED",
            {"worker_id": receipt.worker_id, "status": receipt.status.value, "to": target.value},
        )
        report.advanced.append((task.id, target.value))
        return True

    def _adopt_crashed(self, task: Task, report: ReconcileReport) -> None:
        dead = task.lease.worker_id
        self.executor.stop(dead)  # best-effort fence of the dead generation
        released = rotate_lease(task, dead_worker_id=dead)
        self.tasks.put_authoritative(released)
        self._emit(task.id, "LEASE_ROTATED", {"dead_worker_id": dead})
        # re-actuate onto a fresh worker; state is preserved (still RUNNING)
        worker_id = f"W-{task.id}-{uuid.uuid4().hex[:6]}"
        lease = self._mint_lease(worker_id)
        released = self.tasks.get(task.id)
        leased = acquire_lease(released, lease)
        handle = self.executor.launch(leased, lease)
        self.tasks.put_authoritative(leased)
        self._observe_worker(worker_id, handle.session_handle, task.id, lease.lease_id, alive=True)
        self._emit(task.id, "WORKER_ADOPTED", {"worker_id": worker_id, "lease_id": lease.lease_id})
        report.adopted.append(task.id)

    def _resume_waiting(self, task: Task, report: ReconcileReport) -> None:
        lease = task.lease
        resumed = transition_task(task, TaskState.RUNNING, lease_id=lease.lease_id, new_lease=lease)
        self.executor.resume(resumed, lease.worker_id, lease)
        self.tasks.put_authoritative(resumed)
        self._observe_worker(lease.worker_id, None, task.id, lease.lease_id, alive=True)
        self._emit(task.id, "RESUMED", {"worker_id": lease.worker_id})
        report.resumed.append(task.id)

    # -- main pass -------------------------------------------------------------

    def reconcile_once(self) -> ReconcileReport:
        report = ReconcileReport()
        for task in self.tasks.list():
            if task.state is TaskState.READY:
                self._actuate_ready(task, report)
                continue

            if task.state is TaskState.RUNNING and task.lease is not None:
                obs = self.executor.poll(task.lease.worker_id)
                # Fence: only a receipt echoing the authoritative lease counts.
                if obs.receipt is not None and obs.receipt.lease_id != task.lease.lease_id:
                    report.ignored_stale.append(obs.receipt.worker_id)
                    obs = obs.__class__(worker_id=obs.worker_id, alive=obs.alive, receipt=None)
                if obs.receipt is not None:
                    self._apply_receipt(task, obs.receipt, report)
                elif not obs.alive:
                    self._adopt_crashed(task, report)
                else:
                    self._observe_worker(
                        task.lease.worker_id, None, task.id, task.lease.lease_id, alive=True
                    )
                    report.heartbeats.append(task.lease.worker_id)
                continue

            if task.state is TaskState.WAITING and task.lease is not None:
                if self.unblock is not None and self.unblock(task):
                    self._resume_waiting(task, report)
                continue

        return report
