from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import pytest

from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.adapters.local_process import LocalProcessExecutor
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.locking import ReconcilerBusy, single_reconciler
from agent_farm_runtime.models import Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.procutil import pid_identity_alive, proc_starttime
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.store import FarmPaths, TaskStore
from agent_farm_runtime.transitions import (
    LeaseError,
    Lease,
    acquire_lease,
    rotate_lease,
)


class _Clock:
    """Manual clock for deterministic grace-window tests."""

    def __init__(self, start: float = 1_000.0):
        self.t = start

    def __call__(self) -> datetime:
        return datetime.fromtimestamp(self.t, tz=timezone.utc)

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _events(paths: FarmPaths) -> list[dict]:
    log = paths.events / "log.ndjson"
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]


def _paths(tmp_path: Path) -> FarmPaths:
    paths = FarmPaths(tmp_path / ".farm")
    paths.ensure()
    return paths


def _ready_task(tid: str = "T-1", **meta) -> Task:
    return Task(
        id=tid,
        objective="obj",
        deliverable="del",
        acceptance="acc",
        metadata=dict(meta),
    )


def _no_fail(paths: FarmPaths) -> None:
    assert not any(c.level == "FAIL" for c in run_doctor(paths))


# --- lease helper guards (the crash-adoption primitive) ---------------------


def test_acquire_lease_rejects_double_grant():
    task = _ready_task()
    leased = acquire_lease(task, Lease("W1", "L1"))
    with pytest.raises(LeaseError):
        acquire_lease(leased, Lease("W2", "L2"))


def test_rotate_lease_refuses_wrong_holder():
    task = acquire_lease(_ready_task(), Lease("W1", "L1"))
    # never rotate a lease off anyone but the current (proven-dead) holder
    with pytest.raises(LeaseError):
        rotate_lease(task, dead_worker_id="W2", reason="dead")
    # and a rotation must document how death was established
    with pytest.raises(LeaseError):
        rotate_lease(task, dead_worker_id="W1", reason="")
    released = rotate_lease(task, dead_worker_id="W1", reason="no heartbeat within grace")
    assert released.lease is None


# --- full actuation loop with the fake executor -----------------------------


def test_actuates_ready_task(tmp_path):
    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready_task())
    ex = FakeExecutor()
    rep = Reconciler(paths, ex).reconcile_once()

    assert rep.launched == ["T-1"]
    task = TaskStore(paths).get("T-1")
    assert task.state is TaskState.RUNNING
    assert task.lease is not None
    assert ex.launched and ex.launched[0][1] == "T-1"
    _no_fail(paths)


def test_happy_path_running_waiting_resume_submitted(tmp_path):
    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready_task())
    ex = FakeExecutor()

    unblocked = {"go": False}
    rec = Reconciler(paths, ex, unblock=lambda t: unblocked["go"])
    rec.reconcile_once()  # -> RUNNING
    lease = TaskStore(paths).get("T-1").lease
    wid = lease.worker_id

    # worker reports it hit a wait boundary
    ex.set_receipt(Receipt(wid, "T-1", lease.lease_id, ReceiptStatus.AWAITING,
                           ts="t", waiting_on="job:12345"))
    rec.reconcile_once()
    t = TaskStore(paths).get("T-1")
    assert t.state is TaskState.WAITING
    assert t.metadata["waiting_on"] == "job:12345"
    assert t.lease is not None  # lease kept across WAITING
    _no_fail(paths)

    # still blocked -> reconcile is a no-op
    ex.clear_receipt(wid)
    rec.reconcile_once()
    assert TaskStore(paths).get("T-1").state is TaskState.WAITING

    # unblock fires -> resumed to RUNNING, same lease/worker
    unblocked["go"] = True
    rep = rec.reconcile_once()
    assert rep.resumed == ["T-1"]
    t = TaskStore(paths).get("T-1")
    assert t.state is TaskState.RUNNING and t.lease.lease_id == lease.lease_id
    assert wid in ex.resumed

    # worker submits the final deliverable
    ex.set_receipt(Receipt(wid, "T-1", lease.lease_id, ReceiptStatus.SUBMITTED, ts="t"))
    rec.reconcile_once()
    t = TaskStore(paths).get("T-1")
    assert t.state is TaskState.SUBMITTED
    assert t.lease is None  # ownership released on submission
    assert wid in ex.stopped
    _no_fail(paths)


def test_stale_receipt_is_fenced_out(tmp_path):
    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready_task())
    ex = FakeExecutor()
    rec = Reconciler(paths, ex)
    rec.reconcile_once()
    lease = TaskStore(paths).get("T-1").lease

    # a superseded worker generation emits a SUBMITTED with the WRONG lease id
    ex.set_receipt(Receipt(lease.worker_id, "T-1", "STALE-LEASE",
                           ReceiptStatus.SUBMITTED, ts="t"))
    rep = rec.reconcile_once()

    assert rep.ignored_stale == [lease.worker_id]
    assert TaskStore(paths).get("T-1").state is TaskState.RUNNING  # not advanced
    _no_fail(paths)


def test_adopts_crashed_worker(tmp_path):
    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready_task())
    ex = FakeExecutor()
    rec = Reconciler(paths, ex, grace_seconds=0)  # adopt as soon as death is seen
    rec.reconcile_once()
    first = TaskStore(paths).get("T-1").lease
    assert first is not None

    ex.kill(first.worker_id)  # worker dies mid-flight, no receipt
    rep = rec.reconcile_once()

    assert rep.adopted == ["T-1"]
    t = TaskStore(paths).get("T-1")
    assert t.state is TaskState.RUNNING            # state survived the crash
    assert t.lease is not None
    assert t.lease.worker_id != first.worker_id    # new worker generation
    assert t.lease.lease_id != first.lease_id
    assert len(ex.launched) == 2                   # relaunched
    assert first.worker_id in ex.stopped           # dead gen fenced
    _no_fail(paths)                                 # exactly one authoritative lease


# --- master acceptance is NOT a worker/reconciler action --------------------


def test_submitted_requires_master_acceptance_for_done(tmp_path):
    from agent_farm_runtime.transitions import transition_task

    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready_task())
    ex = FakeExecutor()
    rec = Reconciler(paths, ex)
    rec.reconcile_once()
    lease = TaskStore(paths).get("T-1").lease
    ex.set_receipt(Receipt(lease.worker_id, "T-1", lease.lease_id,
                           ReceiptStatus.SUBMITTED, ts="t"))
    rec.reconcile_once()
    submitted = TaskStore(paths).get("T-1")

    # a worker can never drive DONE: DONE needs recorded acceptance
    from agent_farm_runtime.transitions import TransitionError
    with pytest.raises(TransitionError):
        transition_task(submitted, TaskState.DONE, acceptance_recorded=False)

    accepted = transition_task(
        submitted, TaskState.DONE, acceptance_recorded=True,
        metadata_patch={"acceptance_receipt": "sha256:abc"},
    )
    TaskStore(paths).put_authoritative(accepted)
    assert TaskStore(paths).get("T-1").state is TaskState.DONE
    _no_fail(paths)


# --- real OS subprocess end-to-end (non-fake executor) ----------------------


def _wait(pred, timeout=15.0, dt=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(dt)
    return False


def test_local_process_executor_submits(tmp_path):
    paths = _paths(tmp_path)
    worker_py = tmp_path / "worker.py"
    worker_py.write_text(dedent("""
        import json, os
        r = {"worker_id": os.environ["FARM_WORKER_ID"],
             "task_id": os.environ["FARM_TASK_ID"],
             "lease_id": os.environ["FARM_LEASE_ID"],
             "status": "SUBMITTED", "ts": "t", "note": "real process done",
             "waiting_on": None}
        with open(os.environ["FARM_RECEIPT_PATH"], "w") as fh:
            json.dump(r, fh)
    """))
    TaskStore(paths).create(_ready_task(command=f"{sys.executable} {worker_py}"))
    ex = LocalProcessExecutor(paths.runtime)
    rec = Reconciler(paths, ex)

    rec.reconcile_once()  # launches a real subprocess
    wid = TaskStore(paths).get("T-1").lease.worker_id
    assert _wait(lambda: ex.poll(wid).receipt is not None), "worker never wrote receipt"

    rec.reconcile_once()  # picks up the fenced SUBMITTED receipt
    assert TaskStore(paths).get("T-1").state is TaskState.SUBMITTED
    _no_fail(paths)


def test_local_process_executor_adopts_dead_process(tmp_path):
    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready_task(command="exit 7"))  # dies, writes no receipt
    ex = LocalProcessExecutor(paths.runtime)
    rec = Reconciler(paths, ex, grace_seconds=0)

    rec.reconcile_once()
    first = TaskStore(paths).get("T-1").lease
    assert _wait(lambda: ex.poll(first.worker_id).alive is False), "process never exited"

    rep = rec.reconcile_once()  # observes dead process, no receipt -> adopt
    assert rep.adopted == ["T-1"]
    t = TaskStore(paths).get("T-1")
    assert t.state is TaskState.RUNNING
    assert t.lease.worker_id != first.worker_id
    _no_fail(paths)


# --- crash-consistency fixes (PR review) ------------------------------------


def test_persists_running_and_lease_before_external_launch(tmp_path):
    """INV: desired authoritative state is committed BEFORE actuation, so a crash
    between persist and launch cannot double-actuate."""
    paths = _paths(tmp_path)
    store = TaskStore(paths)
    store.create(_ready_task())

    seen = {}

    class PeekExecutor(FakeExecutor):
        def launch(self, task, lease):
            t = store.get(task.id)                 # what is durably persisted at launch time?
            seen["state"] = t.state
            seen["lease_id"] = t.lease.lease_id if t.lease else None
            return super().launch(task, lease)

    Reconciler(paths, PeekExecutor()).reconcile_once()
    assert seen["state"] is TaskState.RUNNING
    assert seen["lease_id"] == store.get("T-1").lease.lease_id


def test_grace_prevents_premature_adoption(tmp_path):
    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready_task())
    ex = FakeExecutor()
    clk = _Clock()
    rec = Reconciler(paths, ex, clock=clk, grace_seconds=30)
    rec.reconcile_once()                              # launch at t0
    lease = TaskStore(paths).get("T-1").lease
    ex.kill(lease.worker_id)                          # looks dead...

    clk.advance(10)                                   # ...but only 10s < 30s grace
    rep = rec.reconcile_once()
    assert rep.adopted == [] and rep.waiting_grace == ["T-1"]
    still = TaskStore(paths).get("T-1")
    assert still.state is TaskState.RUNNING
    assert still.lease.worker_id == lease.worker_id   # live worker's lease NOT revoked

    clk.advance(25)                                   # now 35s > grace -> durably dead
    rep = rec.reconcile_once()
    assert rep.adopted == ["T-1"]
    assert TaskStore(paths).get("T-1").lease.worker_id != lease.worker_id
    _no_fail(paths)


def test_adoption_is_atomic_and_audited(tmp_path):
    """Lease rotation goes through the authoritative boundary: WORKER_ADOPTED
    names the dead worker, and no RUNNING-without-lease is ever persisted."""
    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready_task())
    ex = FakeExecutor()
    rec = Reconciler(paths, ex, grace_seconds=0)
    rec.reconcile_once()
    first = TaskStore(paths).get("T-1").lease
    ex.kill(first.worker_id)
    rec.reconcile_once()

    adopted = [e for e in _events(paths) if e["type"] == "WORKER_ADOPTED"]
    assert adopted and adopted[0]["payload"]["dead_worker_id"] == first.worker_id
    _no_fail(paths)  # doctor never saw RUNNING without an authoritative lease


def test_local_process_launch_is_idempotent_per_lease(tmp_path):
    paths = _paths(tmp_path)
    ex = LocalProcessExecutor(paths.runtime)
    task = _ready_task(command="sleep 5")
    lease = Lease("W-idem", "L-idem")
    h1 = ex.launch(task, lease)
    h2 = ex.launch(task, lease)                       # already alive -> no second process
    assert h1.session_handle == h2.session_handle
    ex.stop("W-idem")


def test_single_reconciler_lock_is_exclusive(tmp_path):
    rt = tmp_path / "rt"
    rt.mkdir()
    with single_reconciler(rt):
        with pytest.raises(ReconcilerBusy):
            with single_reconciler(rt):
                pass
    # released -> acquirable again
    with single_reconciler(rt):
        pass


def _active_task(tid: str, ws: str) -> Task:
    return Task(id=tid, objective="o", deliverable="d", acceptance="a",
                state=TaskState.RUNNING, lease=Lease(f"W-{tid}", f"L-{tid}"),
                metadata={"workspace": ws})


def test_shared_workspace_among_active_tasks_is_rejected(tmp_path):
    """Two concurrently-active tasks must never share a workspace (their workers
    would corrupt each other's LEDGER/.session_id). doctor flags it FAIL."""
    paths = _paths(tmp_path)
    store = TaskStore(paths)
    store.create(_active_task("A", "/lab/ws/shared"))
    store.create(_active_task("B", "/lab/ws/shared"))
    checks = run_doctor(paths)
    assert any(c.level == "FAIL" and "shared" in c.message for c in checks)
    # distinct workspaces are fine
    store2 = TaskStore(_paths(tmp_path / "other"))
    store2.create(_active_task("A", "/lab/ws/a"))
    store2.create(_active_task("B", "/lab/ws/b"))
    assert not any(c.level == "FAIL" for c in run_doctor(store2.paths))


def test_new_ownership_committed_before_stale_worker_stopped(tmp_path):
    """During adoption the NEW lease must be durably committed BEFORE the stale
    generation is stopped, so a crash never orphans the task."""
    paths = _paths(tmp_path)
    store = TaskStore(paths)
    store.create(_ready_task())
    order: list[tuple[str, str]] = []

    class OrderExecutor(FakeExecutor):
        def stop(self, worker_id):
            t = store.get("T-1")  # what is durably committed at stop time?
            order.append(("stop", t.lease.lease_id if t.lease else None))
            super().stop(worker_id)

        def launch(self, task, lease):
            order.append(("launch", lease.lease_id))
            return super().launch(task, lease)

    ex = OrderExecutor()
    rec = Reconciler(paths, ex, grace_seconds=0)
    rec.reconcile_once()                      # start gen1
    first = store.get("T-1").lease
    ex.kill(first.worker_id)
    rec.reconcile_once()                      # adopt: commit new -> stop old -> launch new

    stops = [o for o in order if o[0] == "stop"]
    assert stops, "adoption must stop the stale generation"
    # the lease committed at stop time is the NEW one, not the dead generation's
    assert stops[-1][1] == store.get("T-1").lease.lease_id
    assert stops[-1][1] != first.lease_id


def test_resumed_worker_survives_grace_without_adoption(tmp_path):
    """After a WAITING->RUNNING resume, the resumed worker's refreshed liveness
    must keep it alive past the grace window -- no false adoption."""
    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready_task())
    ex = FakeExecutor()
    clk = _Clock()
    rec = Reconciler(paths, ex, clock=clk, grace_seconds=30,
                     unblock=lambda t: t.metadata.get("waiting_on") == "job:1")
    rec.reconcile_once()                                   # launch
    lease = TaskStore(paths).get("T-1").lease
    wid = lease.worker_id

    # worker reaches a wait boundary and its process ENDS
    ex.set_receipt(Receipt(wid, "T-1", lease.lease_id, ReceiptStatus.AWAITING,
                           ts="t", waiting_on="job:1"))
    rec.reconcile_once()
    assert TaskStore(paths).get("T-1").state is TaskState.WAITING
    ex.clear_receipt(wid)
    ex.kill(wid)                                           # the awaiting worker exited

    # unblock fires -> resume must refresh liveness (the bug: it didn't)
    rep = rec.reconcile_once()
    assert rep.resumed == ["T-1"]
    assert TaskStore(paths).get("T-1").state is TaskState.RUNNING

    clk.advance(10_000)                                    # far past grace
    rep = rec.reconcile_once()
    assert rep.adopted == []                               # NOT falsely adopted
    assert wid in rep.heartbeats
    assert TaskStore(paths).get("T-1").lease.worker_id == wid
    _no_fail(paths)


def test_pid_identity_rejects_recycled_pid(tmp_path):
    me = os.getpid()
    st = proc_starttime(me)
    assert st is not None
    assert pid_identity_alive(me, st) is True
    assert pid_identity_alive(me, st + 987654) is False   # same pid, different process
    assert pid_identity_alive(2147480000, None) is False  # nonexistent pid


def test_local_process_worker_is_reaped_not_zombied(tmp_path):
    paths = _paths(tmp_path)
    ex = LocalProcessExecutor(paths.runtime)
    ex.launch(_ready_task(command="true"), Lease("W-z", "L-z"))  # exits immediately
    assert _wait(lambda: ex.poll("W-z").alive is False), "worker never exited"
    pid, _ = ex._identity("W-z")
    # poll()/stop() reap children, so the exited worker is gone, not a zombie
    if pid is not None and os.path.exists(f"/proc/{pid}"):
        stat = Path(f"/proc/{pid}/stat").read_text()
        state = stat[stat.rindex(")") + 2]
        assert state != "Z", "exited worker left as a zombie"
