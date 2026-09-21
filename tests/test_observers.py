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
    # Unknown is not evidence of completion; require a terminal observation.
    assert evaluate_waiting_on("job:123", store=store, slurm=lambda j: "RUNNING") is False
    assert evaluate_waiting_on("job:123", store=store, slurm=lambda j: None) is False
    assert evaluate_waiting_on("job:123", store=store, slurm=lambda j: "COMPLETED") is True


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
    before = store.get(dep.id)
    dep = store.get(dep.id)
    dep.state = TaskState.DONE
    dep.metadata["acceptance_receipt"] = "x"
    store.put_authoritative(dep, expected=before)
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
    unblock = make_unblock(paths, slurm=lambda j: "RUNNING" if queued["v"] else "COMPLETED")
    rec = Reconciler(paths, ex, unblock=unblock)

    rec.reconcile_once()  # launch
    lease = TaskStore(paths).get("T-1").lease
    ex.set_receipt(Receipt(lease.worker_id, "T-1", lease.lease_id,
                           ReceiptStatus.AWAITING, ts="t", waiting_on="job:999"))
    rec.reconcile_once()  # -> WAITING(job:999)
    assert TaskStore(paths).get("T-1").state is TaskState.WAITING

    ex.clear_receipt(lease.worker_id)
    ex.kill(lease.worker_id)  # the worker exited at the wait boundary
    rec.reconcile_once()  # job still queued -> stays WAITING
    assert TaskStore(paths).get("T-1").state is TaskState.WAITING

    queued["v"] = False   # job ended
    rep = rec.reconcile_once()
    assert rep.resumed == ["T-1"]
    assert TaskStore(paths).get("T-1").state is TaskState.RUNNING


# ---- runtime defect 001 regression -------------------------------------------

def test_job_end_unblocks_on_terminal_state_not_only_on_none(tmp_path):
    """A finished job reports its sacct TERMINAL state, never None."""
    store = TaskStore(_paths(tmp_path))
    for live in ("PENDING", "RUNNING", "COMPLETING", "SUSPENDED", "CONFIGURING", "RESIZING"):
        assert evaluate_waiting_on("job:123", store=store, slurm=lambda j, s=live: s) is False, live
    for done in ("COMPLETED", "FAILED", "TIMEOUT", "OUT_OF_MEMORY",
                 "NODE_FAIL", "CANCELLED by 64004", "COMPLETED+"):
        assert evaluate_waiting_on("job:123", store=store, slurm=lambda j, s=done: s) is True, done
    # Unknown id / scheduler failure / purged accounting stays parked.
    assert evaluate_waiting_on("job:123", store=store, slurm=lambda j: None) is False
    # :start is unaffected
    assert evaluate_waiting_on("job:123:start", store=store, slurm=lambda j: "COMPLETED") is False
    assert evaluate_waiting_on("job:123:start", store=store, slurm=lambda j: "RUNNING") is True


# ---- runtime defect 002 regression -------------------------------------------

def test_new_master_note_unblocks_a_parked_task(tmp_path):
    from agent_farm_runtime.observers import master_note_digest
    from agent_farm_runtime.models import Lease
    paths = _paths(tmp_path)
    ws = tmp_path / "ws"; ws.mkdir()
    unblock = make_unblock(paths, slurm=lambda j: "RUNNING")
    never = f"artifact:{tmp_path / 'never_appears'}"
    t = Task(id="T-x", objective="", deliverable="", acceptance="",
             state=TaskState.WAITING, lease=Lease("W-1", "L-1"),
             metadata={"waiting_on": never, "workspace": str(ws)})

    assert unblock(t) is False                       # nothing to read yet
    (ws / "MASTER_REPLY_001.md").write_text("ruling")
    assert unblock(t) is True                        # the master has spoken
    t.metadata["master_notes_digest"] = master_note_digest(str(ws))
    assert unblock(t) is False                       # delivered, no re-wake
    (ws / "MASTER_REPLY_002.md").write_text("second ruling")
    assert unblock(t) is True                        # a further ruling wakes again
    t.metadata["master_notes_digest"] = master_note_digest(str(ws))
    assert unblock(t) is False
    (ws / "MASTER_REPLY_002.md").write_text("edited in place")
    assert unblock(t) is True                        # content-addressed: edits count
    # a task with no workspace, or a workspace with no notes, is unaffected
    t2 = Task(id="T-y", objective="", deliverable="", acceptance="",
              state=TaskState.WAITING, lease=Lease("W-2", "L-2"),
              metadata={"waiting_on": never})
    assert unblock(t2) is False
    empty = tmp_path / "empty"; empty.mkdir()
    t3 = Task(id="T-z", objective="", deliverable="", acceptance="",
              state=TaskState.WAITING, lease=Lease("W-3", "L-3"),
              metadata={"waiting_on": never, "workspace": str(empty)})
    assert unblock(t3) is False


def test_master_note_does_not_override_a_ruling_that_was_already_seen(tmp_path):
    """ruling: waits are still master-decided, but a NEW note releases them."""
    from agent_farm_runtime.observers import master_note_digest
    from agent_farm_runtime.models import Lease
    paths = _paths(tmp_path)
    ws = tmp_path / "ws2"; ws.mkdir()
    (ws / "MASTER_NOTE_001.md").write_text("earlier ruling")
    unblock = make_unblock(paths, slurm=lambda j: None)
    t = Task(id="T-r", objective="", deliverable="", acceptance="",
             state=TaskState.WAITING, lease=Lease("W-1", "L-1"),
             metadata={"waiting_on": "ruling:some-decision", "workspace": str(ws),
                       "master_notes_digest": master_note_digest(str(ws))})
    assert unblock(t) is False                       # ruling: still waits for the master
    (ws / "MASTER_NOTE_002.md").write_text("the decision")
    assert unblock(t) is True                        # writing the ruling IS the release
