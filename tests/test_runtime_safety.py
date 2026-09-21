from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import time

import pytest

from agent_farm_runtime.adapters.base import ExecutorUnavailable, WorkerObservation
from agent_farm_runtime.adapters.codex import CodexClusterConfig, CodexTmuxExecutor, _default_run
from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.adapters.filesystem import FileLockBusy, exclusive_lock
from agent_farm_runtime.cli import build_parser, task_summary
from agent_farm_runtime.master import record_decision
from agent_farm_runtime.models import Lease, Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.procutil import host_identity, observe_pidfile, proc_starttime
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.store import FarmPaths, TaskStore
from test_codex_tmux import TmuxRecorder


def _setup_farm(tmp_path, executor=None, **kwargs):
    ws = tmp_path / "workspace"
    ws.mkdir()
    paths = FarmPaths(tmp_path / ".farm")
    store = TaskStore(paths)
    store.create(Task("T1", "objective", "artifact", "check", metadata={"workspace": str(ws), "brief": "do work"}))
    ex = executor or FakeExecutor()
    return store, ex, Reconciler(paths, ex, **kwargs)


def write_identity(ex, worker, *, alive=False):
    state = ex._load_state(worker)
    pid = os.getpid() if alive else 2147480000
    start = proc_starttime(pid) if alive else 1
    Path(state["pid_file"]).write_text(f"{pid} {start} {state['attempt_id']}")


def test_unknown_observation_never_rotates_lease_or_signals(tmp_path):
    class Unknown(FakeExecutor):
        def poll(self, wid):
            return WorkerObservation(wid, None, detail="cannot reach process namespace")

    store, ex, rec = _setup_farm(tmp_path, Unknown(), grace_seconds=0)
    rec.reconcile_once()
    original = store.get("T1").to_dict()
    for _ in range(4):
        rep = rec.reconcile_once()
        assert rep.observation_errors and not rep.adopted
    assert store.get("T1").to_dict() == original
    assert len(ex.launched) == 1 and not ex.stopped


@pytest.mark.parametrize("alive", [True, None])
def test_wait_receipt_does_not_authorize_duplicate_resume(tmp_path, alive):
    store, ex, rec = _setup_farm(tmp_path, unblock=lambda _: True)
    rec.reconcile_once()
    lease = store.get("T1").lease
    ex.set_receipt(Receipt(lease.worker_id, "T1", lease.lease_id, ReceiptStatus.AWAITING,
                           ts="t", waiting_on="ruling:review"))
    rec.reconcile_once()
    ex._alive[lease.worker_id] = alive
    rec.reconcile_once()
    assert not ex.resumed and store.get("T1").state is TaskState.WAITING
    ex.clear_receipt(lease.worker_id)
    ex.kill(lease.worker_id)
    rec.reconcile_once()
    assert ex.resumed == [lease.worker_id]


@pytest.mark.parametrize("operation", ["has-session", "new-session", "list-windows", "new-window", "send-keys"])
def test_tmux_timeout_survives_reconciler_restart_without_redispatch(tmp_path, operation):
    recorder = TmuxRecorder()
    if operation in {"list-windows", "new-window"}:
        recorder._session = True

    def runner(cmd):
        if cmd[1] == operation:
            recorder.calls.append(cmd)
            raise subprocess.TimeoutExpired(cmd, 0.01)
        return recorder(cmd)

    ex = CodexTmuxExecutor(tmp_path / ".farm" / "runtime", run=runner)
    store, _, rec = _setup_farm(tmp_path, ex, grace_seconds=0)
    rep = rec.reconcile_once()
    assert rep.executor_errors
    original = store.get("T1")
    assert original.state is TaskState.RUNNING and original.metadata["runtime_error"]
    calls = len(recorder.calls)
    restarted = Reconciler(store.paths, CodexTmuxExecutor(store.paths.runtime, run=runner), grace_seconds=0)
    for _ in range(3):
        assert restarted.reconcile_once().observation_errors
    assert len(recorder.calls) == calls
    assert store.get("T1").lease == original.lease


@pytest.mark.parametrize("code,stderr", [(1, "permission denied"), (1, ""), (2, "server exited unexpectedly")])
def test_probe_failure_is_not_session_absence(tmp_path, code, stderr):
    calls = []
    def runner(cmd):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, code, "", stderr)
    ex = CodexTmuxExecutor(tmp_path / "runtime", run=runner)
    with pytest.raises(ExecutorUnavailable):
        ex._ensure_window("worker", str(tmp_path))
    assert len(calls) == 1 and calls[0][1] == "has-session"


def test_default_transport_uses_finite_timeout(monkeypatch):
    def run(cmd, **kwargs):
        assert kwargs["timeout"] == 0.25
        assert kwargs["env"]["LC_ALL"] == "C"
        return subprocess.CompletedProcess(cmd, 0, "", "")
    monkeypatch.setattr(subprocess, "run", run)
    _default_run(["tmux", "-V"], timeout=0.25)
    for value in (0, -1, float("inf"), float("nan")):
        with pytest.raises(ValueError):
            CodexClusterConfig(command_timeout_seconds=value)


def test_resume_excludes_old_receipt_and_old_pid(tmp_path):
    recorder = TmuxRecorder()
    ex = CodexTmuxExecutor(tmp_path / ".farm" / "runtime", run=recorder)
    store, _, rec = _setup_farm(tmp_path, ex, grace_seconds=0, unblock=lambda _: True)
    rec.reconcile_once()
    lease = store.get("T1").lease
    state = ex._load_state(lease.worker_id)
    write_identity(ex, lease.worker_id)
    Path(state["receipt_path"]).write_text(json.dumps(Receipt(
        lease.worker_id, "T1", lease.lease_id, ReceiptStatus.AWAITING, ts="t", waiting_on="job:1").to_dict()))
    rec.reconcile_once()
    assert store.get("T1").state is TaskState.WAITING
    rec.reconcile_once()  # send resume, but simulate delayed execution/no fresh pid
    assert store.get("T1").state is TaskState.RUNNING
    obs = ex.poll(lease.worker_id)
    assert obs.alive is None and obs.receipt is None
    calls = len(recorder.calls)
    for _ in range(3):
        assert rec.reconcile_once().observation_errors
    assert len(recorder.calls) == calls and store.get("T1").lease == lease
    with pytest.raises(ExecutorUnavailable):
        ex.resume(store.get("T1"), lease.worker_id, lease)


def test_process_observation_distinguishes_missing_identity_from_death(tmp_path, monkeypatch):
    path = tmp_path / "pid"
    identity = host_identity()
    assert observe_pidfile(str(path), identity, "attempt") is None
    path.write_text(f"{os.getpid()} {proc_starttime(os.getpid())} attempt")
    assert observe_pidfile(str(path), identity, "attempt") is True
    assert observe_pidfile(str(path), identity, "different-attempt") is None
    assert observe_pidfile(str(path), {}, "attempt") is None
    assert observe_pidfile(str(path), {**identity, "host": "another-host"}, "attempt") is None
    original = Path.read_text
    def read(p, *args, **kwargs):
        if str(p) == f"/proc/{os.getpid()}/stat":
            raise PermissionError("not observable")
        return original(p, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_text", read)
        assert observe_pidfile(str(path), identity, "attempt") is None
    path.write_text("2147480000 1 attempt")
    assert observe_pidfile(str(path), identity, "attempt") is False


def test_foreign_host_never_signals_or_resumes(tmp_path):
    recorder = TmuxRecorder()
    ex = CodexTmuxExecutor(tmp_path / ".farm" / "runtime", run=recorder)
    store, _, rec = _setup_farm(tmp_path, ex)
    rec.reconcile_once()
    lease = store.get("T1").lease
    state = ex._load_state(lease.worker_id)
    ex._save_state(lease.worker_id, {**state, "host": "different-host"})
    calls = len(recorder.calls)
    for operation in (lambda: ex.stop(lease.worker_id),
                      lambda: ex.resume(store.get("T1"), lease.worker_id, lease)):
        with pytest.raises(ExecutorUnavailable):
            operation()
    assert len(recorder.calls) == calls and ex.poll(lease.worker_id).alive is None


def test_automatic_restart_budget_needs_explicit_ruling(tmp_path):
    store, ex, rec = _setup_farm(tmp_path, grace_seconds=0, max_auto_restarts=2, unblock=lambda _: True)
    rec.reconcile_once()
    for _ in range(3):
        ex.kill(store.get("T1").lease.worker_id)
        rec.reconcile_once()
    task = store.get("T1")
    assert len(ex.launched) == 3 and task.state is TaskState.WAITING
    assert task.metadata["restart_limit_reached"]
    rec.reconcile_once()  # even a satisfied generic unblock cannot reset the cap
    assert not ex.resumed
    note = tmp_path / "ruling.md"
    note.write_text("Underlying failure checked; authorize another bounded attempt.")
    record_decision(store, "T1", expected_revision=task.metadata["revision"], action="ruling",
                    actor="reviewer", evidence_file=str(note))
    rec.reconcile_once()
    assert len(ex.resumed) == 1 and store.get("T1").metadata["automatic_restarts"] == 0


def test_contended_lock_times_out_without_replacing_inode(tmp_path):
    path = tmp_path / "writer.lock"
    with exclusive_lock(path, blocking=False):
        inode = path.stat().st_ino
        start = time.monotonic()
        with pytest.raises(FileLockBusy):
            with exclusive_lock(path, blocking=True, timeout_seconds=0.02):
                pytest.fail("lock bypassed")
        assert time.monotonic() - start < 1
        assert path.stat().st_ino == inode
    with exclusive_lock(path, blocking=False):
        assert path.stat().st_ino == inode


def test_summary_is_small_read_only_and_full_output_unchanged(tmp_path, capsys):
    paths = FarmPaths(tmp_path / ".farm")
    store = TaskStore(paths)
    body = "PRIVATE_CONTEXT_BODY " * 1500
    store.create(Task("T1", "objective", "result", "verify", metadata={
        "brief": body, "context_manifest": [{"path": "/methods/check.md", "sha256": "abc", "text": body}],
        "latest_master_instruction": {"text": "Check inputs first", "path": "/review/decision.md"},
    }))
    before = store.path_for("T1").read_bytes()
    args = build_parser().parse_args(["--project", str(tmp_path), "task-show", "T1", "--summary"])
    assert args.func(args) == 0
    short = capsys.readouterr().out
    assert "PRIVATE_CONTEXT_BODY" not in short and "/methods/check.md" in short
    assert len(short) < len(before) / 10
    assert json.loads(short)["revision"] == 1
    args = build_parser().parse_args(["--project", str(tmp_path), "task-show", "T1"])
    assert args.func(args) == 0
    assert json.loads(capsys.readouterr().out) == store.get("T1").to_dict()
    assert store.path_for("T1").read_bytes() == before
    assert task_summary(store.get("T1"))["full_text_omitted"] is True


def test_startup_cannot_claim_foreign_active_leases(tmp_path):
    from agent_farm_runtime.provenance import require_local_executor_host, runtime_identity
    from agent_farm_runtime.store import StoreError, atomic_write_json
    store, ex, rec = _setup_farm(tmp_path)
    rec.reconcile_once()
    with pytest.raises(StoreError, match="prior deployment"):
        require_local_executor_host(store.paths)
    manifest = store.paths.runtime / "deployment.json"
    atomic_write_json(manifest, {**runtime_identity(), "host": "foreign-host"})
    before = manifest.read_bytes()
    with pytest.raises(StoreError, match="cross-host"):
        require_local_executor_host(store.paths)
    assert manifest.read_bytes() == before
    atomic_write_json(manifest, runtime_identity())
    require_local_executor_host(store.paths)


def test_corrupt_worker_mapping_cannot_target_another_pane(tmp_path):
    recorder = TmuxRecorder()
    ex = CodexTmuxExecutor(tmp_path / ".farm" / "runtime", run=recorder)
    store, _, rec = _setup_farm(tmp_path, ex)
    rec.reconcile_once()
    lease = store.get("T1").lease
    state = ex._load_state(lease.worker_id)
    ex._save_state(lease.worker_id, {**state, "window": "unrelated-pane"})
    calls = len(recorder.calls)
    with pytest.raises(ExecutorUnavailable):
        ex.stop(lease.worker_id)
    assert len(recorder.calls) == calls


def test_failure_of_one_task_does_not_block_independent_task(tmp_path):
    class Failure(FakeExecutor):
        def launch(self, task, lease):
            if task.id == "T1":
                raise ExecutorUnavailable("dispatch outcome unknown")
            return super().launch(task, lease)
        def poll(self, worker):
            if worker.startswith("W-T1-"):
                return WorkerObservation(worker, None)
            return super().poll(worker)
    store, ex, rec = _setup_farm(tmp_path, Failure())
    store.create(Task("T2", "independent", "file", "verify"))
    report = rec.reconcile_once()
    assert report.executor_errors and report.launched == ["T2"]
    assert store.get("T1").lease is not None
    assert store.get("T2").state is TaskState.RUNNING


@pytest.mark.parametrize("interval", ["0", "-1", "nan", "inf"])
def test_invalid_loop_interval_fails_before_creating_farm(tmp_path, interval):
    project = tmp_path / "absent"
    args = build_parser().parse_args(["--project", str(project), "reconcile", "--interval", interval])
    with pytest.raises(ValueError, match="interval"):
        args.func(args)
    assert not project.exists()
