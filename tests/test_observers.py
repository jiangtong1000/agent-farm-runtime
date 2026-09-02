from __future__ import annotations

from pathlib import Path

from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.models import Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.observers import evaluate_waiting_on, make_unblock
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.store import FarmPaths, TaskStore


def _paths(tmp: Path) -> FarmPaths:
    p = FarmPaths(tmp / ".farm")
    p.ensure()
    return p


def _ready(tid="T-1", **meta) -> Task:
    return Task(id=tid, objective="o", deliverable="d", acceptance="a", metadata=dict(meta))


def test_job_end_unblocks_when_job_leaves_queue(tmp_path):
    store = TaskStore(_paths(tmp_path))
    # still queued -> blocked; gone -> unblocked
    assert evaluate_waiting_on("job:123", store=store, slurm=lambda j: "RUNNING") is False
    assert evaluate_waiting_on("job:123", store=store, slurm=lambda j: None) is True


def test_job_start_unblocks_when_running(tmp_path):
    store = TaskStore(_paths(tmp_path))
    assert evaluate_waiting_on("job:123:start", store=store, slurm=lambda j: None) is False
    assert evaluate_waiting_on("job:123:start", store=store, slurm=lambda j: "RUNNING") is True


def test_artifact_unblocks_when_path_exists(tmp_path):
    store = TaskStore(_paths(tmp_path))
    art = tmp_path / "out.txt"
    assert evaluate_waiting_on(f"artifact:{art}", store=store) is False
    art.write_text("done")
    assert evaluate_waiting_on(f"artifact:{art}", store=store) is True


def test_task_dep_unblocks_when_done(tmp_path):
    paths = _paths(tmp_path)
    store = TaskStore(paths)
    dep = _ready("T-dep")
    dep.state = TaskState.SUBMITTED
    store.create(dep)
    assert evaluate_waiting_on("task:T-dep#deliverable", store=store) is False
    dep.state = TaskState.DONE
    dep.metadata["acceptance_receipt"] = "x"
    store.put_authoritative(dep)
    assert evaluate_waiting_on("task:T-dep", store=store) is True


def test_ruling_is_never_auto_unblocked(tmp_path):
    store = TaskStore(_paths(tmp_path))
    assert evaluate_waiting_on("ruling:MR_009", store=store) is False
    assert evaluate_waiting_on(None, store=store) is False


def test_reconciler_auto_resumes_on_job_end(tmp_path):
    paths = _paths(tmp_path)
    TaskStore(paths).create(_ready())
    ex = FakeExecutor()
    queued = {"v": True}
    unblock = make_unblock(paths, slurm=lambda j: "RUNNING" if queued["v"] else None)
    rec = Reconciler(paths, ex, unblock=unblock)

    rec.reconcile_once()  # launch
    lease = TaskStore(paths).get("T-1").lease
    ex.set_receipt(Receipt(lease.worker_id, "T-1", lease.lease_id,
                           ReceiptStatus.AWAITING, ts="t", waiting_on="job:999"))
    rec.reconcile_once()  # -> WAITING(job:999)
    assert TaskStore(paths).get("T-1").state is TaskState.WAITING

    ex.clear_receipt(lease.worker_id)
    rec.reconcile_once()  # job still queued -> stays WAITING
    assert TaskStore(paths).get("T-1").state is TaskState.WAITING

    queued["v"] = False   # job ended
    rep = rec.reconcile_once()
    assert rep.resumed == ["T-1"]
    assert TaskStore(paths).get("T-1").state is TaskState.RUNNING
