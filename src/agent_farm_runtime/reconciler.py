from __future__ import annotations

import uuid
import math
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

from .adapters.base import ExecutorUnavailable, WorkerExecutor
from .observers import master_note_digest
from .models import (
    Lease,
    Receipt,
    ReceiptStatus,
    Task,
    TaskState,
    Worker,
    WorkerState,
)
from .store import FarmPaths, StoreConflict, StoreError, TaskStore, WorkerRegistry
from .transitions import acquire_lease, rotate_lease, transition_task
from .turnover import clean_surrender, deployment, execution_epoch, request_rotation

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
    conflicts: list[str] = field(default_factory=list)
    preflight_failures: list[str] = field(default_factory=list)
    observation_errors: list[str] = field(default_factory=list)
    executor_errors: list[str] = field(default_factory=list)


class Reconciler:
    """Serialized, idempotent control loop (INV-6).

    Crash-consistency contract (per review):
      * Desired authoritative state (RUNNING + lease) is PERSISTED BEFORE any
        external actuation. If the reconciler dies between persist and launch,
        missing/uncertain executor identity does not establish death: keep the
        assigned lease and inspect rather than risk a second dispatch.
      * A lease is rotated off a worker ONLY when that worker is DURABLY dead:
        positively observed dead AND no usable receipt AND no heartbeat within
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
        max_auto_restarts: int = 3,
    ):
        self.paths = paths
        self.executor = executor
        self.actor = actor
        self.clock = clock
        self.unblock = unblock
        self.grace_seconds = grace_seconds
        if not math.isfinite(grace_seconds) or grace_seconds < 0:
            raise ValueError("grace must be finite and nonnegative")
        if not isinstance(max_auto_restarts, int) or max_auto_restarts < 0:
            raise ValueError("max_auto_restarts must be a nonnegative integer")
        self.max_auto_restarts = max_auto_restarts
        self.tasks = TaskStore(paths)
        self.registry = WorkerRegistry(paths)
        self.execution_epoch = execution_epoch(deployment(paths))

    # -- boundaries ------------------------------------------------------------

    def _iso(self) -> str:
        return self.clock().isoformat()

    def _commit(self, new_task: Task, event_type: str, payload: dict, *, expected: Task,
                dispatch: Callable[[Task], None] | None = None) -> None:
        """The single authoritative-mutation boundary: durable write + event.
        Every state transition and every lease change is applied through here."""
        self.tasks.commit(new_task, expected=expected, event_type=event_type,
                          actor=self.actor, payload=payload, ts=self._iso(),
                          dispatch=dispatch, execution_epoch=self.execution_epoch)

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
        if deployment(self.paths).get("handoff", {}).get("phase") in {"draining", "released"}:
            return
        if adopting_from and task.metadata.get("automatic_restarts", 0) >= self.max_auto_restarts:
            paused = transition_task(task, TaskState.WAITING, lease_id=task.lease.lease_id,
                                    new_lease=task.lease, metadata_patch={
                "waiting_on": "ruling:runtime-restart-limit", "restart_limit_reached": True,
            })
            self._commit(paused, "RESTART_LIMIT_REACHED", {"limit": self.max_auto_restarts}, expected=task)
            report.executor_errors.append(f"{task.id}: automatic restart limit reached; explicit ruling required")
            return
        if not self._preflight(task, report):
            return
        base = task
        if adopting_from is not None:
            base = rotate_lease(task, dead_worker_id=adopting_from,
                                reason=f"no heartbeat within {self.grace_seconds}s")
        worker_id, lease = self._mint(task.id)
        launch_patch = {"launched_ts": self._iso(), "runtime_error": None, "worker_checkpoint": None}
        prior = task.metadata.get("dispatches", {})
        issued = prior.get("workers", []) if prior.get("epoch") == self.execution_epoch else []
        launch_patch["dispatches"] = {"epoch": self.execution_epoch, "workers": [*issued, worker_id]}
        if clean_surrender(task):
            launch_patch.update(rotation_request=None, resume_requested=None)
            if task.metadata.get("restart_limit_reached") and task.metadata.get("resume_requested"):
                launch_patch.update(restart_limit_reached=False, automatic_restarts=0)
        if adopting_from:
            launch_patch["automatic_restarts"] = task.metadata.get("automatic_restarts", 0) + 1
        if task.metadata.get("rework_requested"):
            launch_patch["rework_requested"] = None
            launch_patch["automatic_restarts"] = 0
        digest = master_note_digest(task.metadata.get("workspace"))
        if digest is not None:
            launch_patch["master_notes_digest"] = digest
        leased = acquire_lease(base, lease, metadata_patch=launch_patch)
        if base.state in {TaskState.READY, TaskState.SUBMITTED, TaskState.WAITING}:
            leased = transition_task(leased, TaskState.RUNNING,
                                     lease_id=lease.lease_id, new_lease=lease)
            event, key = ("REWORK_STARTED" if base.state is TaskState.SUBMITTED else "WORKER_LAUNCHED"), "launched"
        else:  # already RUNNING (adoption / leaseless repair)
            event, key = ("WORKER_ADOPTED", "adopted") if adopting_from else ("WORKER_RELAUNCHED", "launched")
        payload = {"worker_id": worker_id, "lease_id": lease.lease_id}
        if adopting_from:
            payload["dead_worker_id"] = adopting_from
        # 1. commit the NEW authoritative ownership FIRST, so a crash never leaves
        #    the stale generation retired with no durable successor recorded;
        def dispatch(saved):
            if adopting_from:
                self.executor.stop(adopting_from)
            handle = self.executor.launch(saved, lease)
            self._observe(worker_id, handle.session_handle, task.id, lease.lease_id, alive=True)
        self._commit(leased, event, payload, expected=task, dispatch=dispatch)
        getattr(report, key).append(task.id)

    def _apply_receipt(self, task: Task, receipt: Receipt, report: ReconcileReport,
                       *, alive: bool | None) -> None:
        target = _RECEIPT_TARGET[receipt.status]
        patch: dict = {"last_receipt_note": receipt.note, "runtime_error": None}
        if target is not TaskState.WAITING:
            patch["rotation_request"] = None
        if target is TaskState.WAITING:
            patch["worker_checkpoint"] = None
            request = task.metadata.get("rotation_request")
            if receipt.rotation_id and (not request or receipt.rotation_id != request["id"]):
                report.ignored_stale.append(receipt.worker_id)
                return
            patch["waiting_on"] = receipt.waiting_on or (
                f"rotation:{receipt.rotation_id}" if receipt.rotation_id else "unspecified")
            if receipt.checkpoint:
                from .master import snapshot_file
                from pathlib import Path
                try:
                    if not isinstance(receipt.checkpoint, str) or not Path(receipt.checkpoint).is_absolute():
                        raise StoreError("receipt checkpoint must be an absolute path")
                    checkpoint = snapshot_file(receipt.checkpoint)
                except (StoreError, ValueError, OSError) as exc:
                    report.preflight_failures.append(f"{task.id}: checkpoint: {exc}")
                    return
                patch["worker_checkpoint"] = {
                    **checkpoint, "worker_id": receipt.worker_id,
                    "lease_id": receipt.lease_id, "rotation_id": receipt.rotation_id,
                }
        new_lease: object = task.lease if target is TaskState.WAITING else None
        advanced = transition_task(task, target, lease_id=task.lease.lease_id,
                                   metadata_patch=patch, new_lease=new_lease)
        self._commit(advanced, "RECEIPT_APPLIED",
                     {"worker_id": receipt.worker_id, "status": receipt.status.value,
                      "to": target.value}, expected=task)
        # A receipt is intent, not proof that the process has already exited.
        if alive is not None:
            self._observe(receipt.worker_id, None, task.id, task.lease.lease_id, alive=alive)
        report.advanced.append((task.id, target.value))
        if target is not TaskState.WAITING:
            self.executor.stop(receipt.worker_id)

    def _resume(self, task: Task, report: ReconcileReport) -> None:
        if deployment(self.paths).get("handoff", {}).get("phase") in {"draining", "released"}:
            return
        if not self._preflight(task, report):
            return
        lease = task.lease
        obs = self.executor.poll(lease.worker_id)
        if obs.alive is None:
            report.observation_errors.append(f"{task.id}: {obs.detail or 'worker identity unknown'}; resume withheld")
            return
        if obs.alive:
            report.waiting_grace.append(task.id)  # receipt can precede process exit
            return
        resume_patch = {"launched_ts": self._iso(), "resume_requested": None,
                        "runtime_error": None, "worker_checkpoint": None}
        if task.metadata.get("restart_limit_reached"):
            resume_patch.update(restart_limit_reached=False, automatic_restarts=0)
        digest = master_note_digest(task.metadata.get("workspace"))
        if digest is not None:
            resume_patch["master_notes_digest"] = digest
        resumed = transition_task(task, TaskState.RUNNING, lease_id=lease.lease_id,
                                  new_lease=lease, metadata_patch=resume_patch)
        def dispatch(saved):
            self.executor.resume(saved, lease.worker_id, lease)
            self._observe(lease.worker_id, None, task.id, lease.lease_id, alive=True)
        self._commit(resumed, "RESUMED", {"worker_id": lease.worker_id}, expected=task, dispatch=dispatch)
        report.resumed.append(task.id)

    def _surrender(self, task: Task, report: ReconcileReport) -> None:
        request = task.metadata["rotation_request"]
        lease = task.lease
        if request["worker_id"] != lease.worker_id or request["lease_id"] != lease.lease_id:
            report.observation_errors.append(f"{task.id}: stale rotation identity")
            return
        checkpoint = request.get("checkpoint")
        if not checkpoint:
            candidate = task.metadata.get("worker_checkpoint") or {}
            if (candidate.get("worker_id") == lease.worker_id and candidate.get("lease_id") == lease.lease_id
                    and (request["requested_state"] == "WAITING" or candidate.get("rotation_id") == request["id"])):
                checkpoint = candidate
        if not checkpoint:
            report.observation_errors.append(f"{task.id}: rotation needs a checkpoint; do not wake a held task to get one")
            return
        obs = self.executor.poll(lease.worker_id)
        if obs.alive is not False:
            report.observation_errors.append(f"{task.id}: rotation awaits positively observed exit")
            return
        record = {"id": request["id"], "worker_id": lease.worker_id, "lease_id": lease.lease_id,
                  "checkpoint": checkpoint, "waiting_on": task.metadata.get("waiting_on"), "ts": self._iso()}
        retired = rotate_lease(task, dead_worker_id=lease.worker_id,
                               reason=f"clean checkpoint and confirmed exit: {request['id']}")
        retired = replace(retired, metadata={**retired.metadata, "clean_surrender": record,
                                            "rotation_request": None, "runtime_error": None})
        self._commit(retired, "WORKER_SURRENDERED", record, expected=task)
        report.advanced.append((task.id, TaskState.WAITING.value))

    def _preflight(self, task: Task, report: ReconcileReport) -> bool:
        validate = getattr(self.executor, "validate_task", None)
        if validate is not None:
            try:
                validate(task)
            except (ValueError, OSError, StoreError) as exc:
                report.preflight_failures.append(f"{task.id}: {exc}")
                return False
        return True

    # -- death detection (grace) ----------------------------------------------

    def _durably_dead(self, task: Task, workers: dict[str, Worker]) -> bool:
        """Age gate only, called AFTER a positive dead observation.

        Age/heartbeat silence alone never establishes process death.
        """
        now = self.clock()
        w = workers.get(task.lease.worker_id)
        baseline = _parse(w.heartbeat if w else None) or _parse(task.metadata.get("launched_ts"))
        if baseline is None:
            return False  # unknown age -> conservative: do not revoke
        return (now - baseline).total_seconds() >= self.grace_seconds

    # -- main pass -------------------------------------------------------------

    def reconcile_once(self) -> ReconcileReport:
        self.tasks.recover()
        report = ReconcileReport()
        workers = {w.id: w for w in self.registry.list()}
        for task in self.tasks.list():
            try:
                self._reconcile_task(task, workers, report)
            except StoreConflict as exc:
                # No actuation follows a rejected commit. Other work can proceed.
                report.conflicts.append(str(exc))
            except (ExecutorUnavailable, OSError) as exc:
                # An external effect may already have occurred. Do not roll back
                # ownership, call stop(), or retry dispatch in this error handler.
                report.executor_errors.append(f"{task.id}: {exc}")
                current = self.tasks.get(task.id)
                error = {"worker_id": current.lease.worker_id if current.lease else None,
                         "message": str(exc)[:1000]}
                if current.metadata.get("runtime_error") != error:
                    updated = replace(current, metadata={**current.metadata, "runtime_error": error})
                    try:
                        self._commit(updated, "EXECUTOR_ERROR", error, expected=current)
                    except StoreConflict as conflict:
                        report.conflicts.append(str(conflict))
        return report

    def _reconcile_task(self, task: Task, workers: dict[str, Worker], report: ReconcileReport) -> None:
        control = deployment(self.paths).get("handoff", {})
        if control.get("phase") == "draining" and task.lease and not task.metadata.get("rotation_request"):
            task = request_rotation(self.tasks, task.id, expected_revision=task.metadata.get("revision", 0),
                                    request_id=control["id"], actor=self.actor)
        if task.state is TaskState.READY:
            self._start(task, report)
            return

        if task.state is TaskState.SUBMITTED and task.metadata.get("rework_requested"):
            self._start(task, report)
            return

        if task.state is TaskState.RUNNING:
            if task.lease is None:
                # persisted RUNNING must carry a lease; repair by re-actuating
                self._start(task, report)
                return
            obs = self.executor.poll(task.lease.worker_id)
            if obs.receipt is not None and (
                obs.receipt.lease_id != task.lease.lease_id
                or obs.receipt.worker_id != task.lease.worker_id
                or obs.receipt.task_id != task.id
            ):
                report.ignored_stale.append(obs.receipt.worker_id)
                obs = obs.__class__(worker_id=obs.worker_id, alive=obs.alive, receipt=None,
                                    detail=obs.detail)
            if obs.receipt is not None and obs.receipt.status is not ReceiptStatus.RUNNING:
                self._apply_receipt(task, obs.receipt, report, alive=obs.alive)
            elif obs.alive is True:
                if task.metadata.get("runtime_error"):
                    cleared = replace(task, metadata={**task.metadata, "runtime_error": None})
                    self._commit(cleared, "EXECUTOR_VERIFIED", {}, expected=task)
                self._observe(task.lease.worker_id, None, task.id, task.lease.lease_id, alive=True)
                report.heartbeats.append(task.lease.worker_id)
            elif obs.alive is None:
                report.observation_errors.append(f"{task.id}: {obs.detail or 'worker identity unknown'}; ownership unchanged")
            elif task.metadata.get("runtime_error"):
                report.executor_errors.append(f"{task.id}: unresolved executor error; automatic restart withheld")
            elif task.metadata.get("rotation_request"):
                report.observation_errors.append(f"{task.id}: exited without a clean rotation receipt; inspect checkpoint")
            elif self._durably_dead(task, workers):
                self._start(task, report, adopting_from=task.lease.worker_id)
            else:
                report.waiting_grace.append(task.id)  # transient miss: wait, do not revoke
            return

        if task.state is TaskState.WAITING:
            if task.lease and task.metadata.get("rotation_request"):
                self._surrender(task, report)
                return
            if task.metadata.get("restart_limit_reached") and not task.metadata.get("resume_requested"):
                return  # a file change cannot reset a restart budget
            if clean_surrender(task):
                if self.unblock is not None and (
                    task.metadata.get("waiting_on") == f"rotation:{task.metadata['clean_surrender']['id']}"
                    or self.unblock(task)
                ):
                    self._start(task, report)
            elif task.lease and self.unblock is not None and self.unblock(task):
                self._resume(task, report)
