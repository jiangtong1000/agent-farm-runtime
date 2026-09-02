from __future__ import annotations

import sys
import time
from pathlib import Path
from textwrap import dedent

import pytest

from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.adapters.local_process import LocalProcessExecutor
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.models import Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.store import FarmPaths, TaskStore
from agent_farm_runtime.transitions import (
    LeaseError,
    Lease,
    acquire_lease,
    rotate_lease,
)


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
        rotate_lease(task, dead_worker_id="W2")
    released = rotate_lease(task, dead_worker_id="W1")
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
    rec = Reconciler(paths, ex)
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
    rec = Reconciler(paths, ex)

    rec.reconcile_once()
    first = TaskStore(paths).get("T-1").lease
    assert _wait(lambda: ex.poll(first.worker_id).alive is False), "process never exited"

    rep = rec.reconcile_once()  # observes dead process, no receipt -> adopt
    assert rep.adopted == ["T-1"]
    t = TaskStore(paths).get("T-1")
    assert t.state is TaskState.RUNNING
    assert t.lease.worker_id != first.worker_id
    _no_fail(paths)
