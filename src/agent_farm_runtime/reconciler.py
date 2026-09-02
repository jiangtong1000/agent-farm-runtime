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

_RECEIPT_TARGET = {
    ReceiptStatus.AWAITING: TaskState.WAITING,
    ReceiptStatus.SUBMITTED: TaskState.SUBMITTED,
    ReceiptStatus.FAILED: TaskState.FAILED,
}


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _parse(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


@dataclass
class ReconcileReport:
    launched: list[str] = field(default_factory=list)
    adopted: list[str] = field(default_factory=list)
    advanced: list[tuple[str, str]] = field(default_factory=list)
    resumed: list[str] = field(default_factory=list)
    heartbeats: list[str] = field(default_factory=list)
    ignored_stale: list[str] = field(default_factory=list)
    waiting_grace: list[str] = field(default_factory=list)  # dead-looking but within grace


class Reconciler:
    """Serialized, idempotent control loop (INV-6).

    Crash-consistency contract (per review):
      * Desired authoritative state (RUNNING + lease) is PERSISTED BEFORE any
        external actuation. If the reconciler dies between persist and launch,
        the next pass sees a RUNNING task whose worker is absent and re-actuates
        it after the grace window -- it never double-launches, because launch is
        never re-issued for a lease that is only *transiently* unobserved.
      * A lease is rotated off a worker ONLY when that worker is DURABLY dead:
        not observed alive AND no valid receipt AND no heartbeat within
        `grace_seconds`. A single transient liveness miss never revokes a live
        worker's lease (INV-5).
      * Every authoritative mutation -- state transition AND lease acquire/
        rotate/release -- goes through the one `_commit` boundary (INV-7): a
        durable Task Store write paired with an event.
    Concurrency is the caller's responsibility (see locking.single_reconciler);
    two reconcilers must not run on one farm.
    """

    def __init__(
        self,
        paths: FarmPaths,
        executor: WorkerExecutor,
        *,
        actor: str = "reconciler",
        clock: Callable[[], datetime] = _default_clock,
        unblock: Callable[[Task], bool] | None = None,
        grace_seconds: float = 60.0,
    ):
        self.paths = paths
        self.executor = executor
        self.actor = actor
        self.clock = clock
        self.unblock = unblock
        self.grace_seconds = grace_seconds
        self.tasks = TaskStore(paths)
        self.registry = WorkerRegistry(paths)
        self.events = EventLog(paths.events / "log.ndjson")

    # -- boundaries ------------------------------------------------------------

    def _iso(self) -> str:
        return self.clock().isoformat()

    def _emit(self, task_id: str, type_: str, payload: dict) -> None:
        self.events.append(Event(
            id=uuid.uuid4().hex, task_id=task_id, type=type_,
            actor=self.actor, payload=payload, ts=self._iso(),
        ))

    def _commit(self, new_task: Task, event_type: str, payload: dict) -> None:
        """The single authoritative-mutation boundary: durable write + event.
        Every state transition and every lease change is applied through here."""
        self.tasks.put_authoritative(new_task)
        self._emit(new_task.id, event_type, payload)

    def _mint(self, task_id: str) -> tuple[str, Lease]:
        worker_id = f"W-{task_id}-{uuid.uuid4().hex[:6]}"
        return worker_id, Lease(worker_id=worker_id, lease_id=uuid.uuid4().hex)

    def _observe(self, worker_id: str, handle: str | None, task_id: str,
                 lease_id: str, *, alive: bool) -> None:
        self.registry.put_observed(Worker(
            id=worker_id, session_handle=handle, heartbeat=self._iso(),
            lease={"task_id": task_id, "lease_id": lease_id},
            state=WorkerState.BUSY if alive else WorkerState.DEAD,
        ))

    # -- actuation (persist BEFORE launch) ------------------------------------

    def _start(self, task: Task, report: ReconcileReport, *, adopting_from: str | None = None) -> None:
        """Compose lease acquisition + RUNNING into ONE persisted mutation, then
        actuate. Used for first start (READY->RUNNING) and adoption (rotate off a
        dead worker + re-acquire), so no RUNNING-without-lease state is ever
        persisted, even across a crash mid-adoption."""
        base = task
        if adopting_from is not None:
            base = rotate_lease(task, dead_worker_id=adopting_from,
                                reason=f"no heartbeat within {self.grace_seconds}s")
        worker_id, lease = self._mint(task.id)
        leased = acquire_lease(base, lease, metadata_patch={"launched_ts": self._iso()})
        if base.state is TaskState.READY:
            leased = transition_task(leased, TaskState.RUNNING,
                                     lease_id=lease.lease_id, new_lease=lease)
            event, key = "WORKER_LAUNCHED", "launched"
        else:  # already RUNNING (adoption / leaseless repair)
            event, key = ("WORKER_ADOPTED", "adopted") if adopting_from else ("WORKER_RELAUNCHED", "launched")
        payload = {"worker_id": worker_id, "lease_id": lease.lease_id}
        if adopting_from:
            payload["dead_worker_id"] = adopting_from
        # 1. commit the NEW authoritative ownership FIRST, so a crash never leaves
        #    the stale generation retired with no durable successor recorded;
        self._commit(leased, event, payload)
        # 2. only then retire the stale generation (fenced already by the new lease);
        if adopting_from:
            self.executor.stop(adopting_from)
        # 3. then actuate the new worker (idempotent per lease).
        handle = self.executor.launch(leased, lease)
        self._observe(worker_id, handle.session_handle, task.id, lease.lease_id, alive=True)
        getattr(report, key).append(task.id)

    def _apply_receipt(self, task: Task, receipt: Receipt, report: ReconcileReport) -> None:
        if receipt.status is ReceiptStatus.RUNNING:
            self._observe(receipt.worker_id, None, task.id, task.lease.lease_id, alive=True)
            report.heartbeats.append(receipt.worker_id)
            return
        target = _RECEIPT_TARGET[receipt.status]
        patch: dict = {"last_receipt_note": receipt.note}
        if target is TaskState.WAITING:
            patch["waiting_on"] = receipt.waiting_on or "unspecified"
        new_lease: object = task.lease if target is TaskState.WAITING else None
        advanced = transition_task(task, target, lease_id=task.lease.lease_id,
                                   metadata_patch=patch, new_lease=new_lease)
        self._commit(advanced, "RECEIPT_APPLIED",
                     {"worker_id": receipt.worker_id, "status": receipt.status.value,
                      "to": target.value})
        # the codex worker ends its run at a wait/submit boundary: mark not-alive
        if target is not TaskState.WAITING:
            self.executor.stop(receipt.worker_id)
        self._observe(receipt.worker_id, None, task.id, task.lease.lease_id, alive=False)
        report.advanced.append((task.id, target.value))

    def _resume(self, task: Task, report: ReconcileReport) -> None:
        lease = task.lease
        resumed = transition_task(task, TaskState.RUNNING, lease_id=lease.lease_id,
                                  new_lease=lease, metadata_patch={"launched_ts": self._iso()})
        self._commit(resumed, "RESUMED", {"worker_id": lease.worker_id})  # persist first
        self.executor.resume(resumed, lease.worker_id, lease)             # then actuate
        self._observe(lease.worker_id, None, task.id, lease.lease_id, alive=True)
        report.resumed.append(task.id)

    # -- death detection (grace) ----------------------------------------------

    def _durably_dead(self, task: Task, workers: dict[str, Worker]) -> bool:
        """True only if the leased worker has been unobserved past the grace
        window. Baseline = last heartbeat (observed) or launch time (authoritative
        floor). A single transient miss returns False."""
        now = self.clock()
        w = workers.get(task.lease.worker_id)
        baseline = _parse(w.heartbeat if w else None) or _parse(task.metadata.get("launched_ts"))
        if baseline is None:
            return False  # unknown age -> conservative: do not revoke
        return (now - baseline).total_seconds() >= self.grace_seconds

    # -- main pass -------------------------------------------------------------

    def reconcile_once(self) -> ReconcileReport:
        report = ReconcileReport()
        workers = {w.id: w for w in self.registry.list()}
        for task in self.tasks.list():
            if task.state is TaskState.READY:
                self._start(task, report)
                continue

            if task.state is TaskState.RUNNING:
                if task.lease is None:
                    # persisted RUNNING must carry a lease; repair by re-actuating
                    self._start(task, report)
                    continue
                obs = self.executor.poll(task.lease.worker_id)
                if obs.receipt is not None and obs.receipt.lease_id != task.lease.lease_id:
                    report.ignored_stale.append(obs.receipt.worker_id)
                    obs = obs.__class__(worker_id=obs.worker_id, alive=obs.alive, receipt=None)
                if obs.receipt is not None:
                    self._apply_receipt(task, obs.receipt, report)
                elif obs.alive:
                    self._observe(task.lease.worker_id, None, task.id, task.lease.lease_id, alive=True)
                    report.heartbeats.append(task.lease.worker_id)
                elif self._durably_dead(task, workers):
                    self._start(task, report, adopting_from=task.lease.worker_id)
                else:
                    report.waiting_grace.append(task.id)  # transient miss: wait, do not revoke
                continue

            if task.state is TaskState.WAITING and task.lease is not None:
                if self.unblock is not None and self.unblock(task):
                    self._resume(task, report)
                continue

        return report
