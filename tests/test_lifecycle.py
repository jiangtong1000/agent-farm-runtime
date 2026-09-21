"""farm stop --drain / --now and farm restart --to (D5, D31, D36, D39)."""
from __future__ import annotations

import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.adapters.filesystem import FileLockBusy, atomic_write_json, exclusive_lock
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.lifecycle import protocol_digest, restart_plan, stop_drain, stop_now
from agent_farm_runtime.models import Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.provenance import runtime_identity
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.store import FarmPaths, StoreError, TaskStore
from agent_farm_runtime.turnover import claim_farm, deployment, release_farm

SRC = Path(runtime_identity()["source_root"]).parent


def _farm(tmp_path, **manifest_extra):
    paths = FarmPaths(tmp_path / ".farm"); paths.ensure()
    atomic_write_json(paths.runtime / "deployment.json", {
        **runtime_identity(), "started_at": "2026-09-19T00:00:00+00:00", "executor": "local-process",
        "session": "farm2", "tmux_socket": None, "interval": 30.0, "grace_seconds": 60.0,
        "max_auto_restarts": 3, "auto_unblock": True, "loop": True, "writer_policy": "pinned-host",
        **manifest_extra})
    return paths, TaskStore(paths)


def test_stop_drain_withholds_dispatch_and_lets_workers_check_out(tmp_path):
    paths, store = _farm(tmp_path)
    ex = FakeExecutor()
    rec = Reconciler(paths, ex, grace_seconds=0)
    store.create(Task("T-run", "o", "d", "a"))
    rec.reconcile_once()                                  # T-run RUNNING with a lease
    lease = store.get("T-run").lease
    store.create(Task("T-new", "o", "d", "a"))            # READY, must never launch after stop
    control = stop_drain(paths, actor="owner")
    assert control["kind"] == "stop" and control["phase"] == "draining"
    assert stop_drain(paths, actor="owner")["id"] == control["id"]   # idempotent
    rep = rec.reconcile_once()
    assert rep.launched == [] and store.get("T-new").state is TaskState.READY
    assert store.get("T-run").metadata["rotation_request"]["id"] == control["id"]   # worker asked to check out
    # worker checks out cleanly
    ckpt = tmp_path / "CHECKPOINT.md"; ckpt.write_text("state: round 1 submitted\nnext: wait job 1\n")
    ex.set_receipt(Receipt(lease.worker_id, "T-run", lease.lease_id, ReceiptStatus.AWAITING, ts="t",
                           waiting_on="job:1", rotation_id=control["id"], checkpoint=str(ckpt)))
    rec.reconcile_once()
    ex.kill(lease.worker_id); ex.clear_receipt(lease.worker_id)
    rec.reconcile_once()
    task = store.get("T-run")
    assert task.state is TaskState.WAITING and task.lease is None and task.metadata["clean_surrender"]
    assert not any(t.lease for t in store.list())        # the --loop exit condition is now true
    assert any("draining" in c.message for c in run_doctor(paths))
    # a stop is not a node handoff
    with pytest.raises(StoreError):
        release_farm(paths, ex, request_id=control["id"], actor="owner")
    with pytest.raises(StoreError):
        claim_farm(paths, request_id=control["id"], actor="owner")


def test_stop_now_terminates_recorded_pid_between_commits(tmp_path):
    from agent_farm_runtime.procutil import proc_starttime
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        paths, _ = _farm(tmp_path, pid=proc.pid)              # legacy manifest: pid without identity
        refused = stop_now(paths, actor="owner", wait_seconds=1)
        assert refused["stopped"] is False and "pid_starttime" in refused["reason"] and proc.poll() is None
        paths, _ = _farm(tmp_path, pid=proc.pid, pid_starttime=proc_starttime(proc.pid))
        result = stop_now(paths, actor="owner", wait_seconds=10)
        assert result["stopped"] is True and result["pid"] == proc.pid
        assert proc.wait(timeout=5) is not None
    finally:
        if proc.poll() is None:
            proc.kill()


def test_stop_now_waits_for_the_mutation_lock(tmp_path):
    paths, _ = _farm(tmp_path, pid=2**22 + 999)
    release = threading.Event()

    def hold():
        with exclusive_lock(paths.runtime / "task-mutation.lock", blocking=False):
            release.wait(5)

    t = threading.Thread(target=hold); t.start(); time.sleep(0.2)
    try:
        with pytest.raises(FileLockBusy):
            stop_now(paths, actor="owner", lock_timeout=0.3)
    finally:
        release.set(); t.join()
    # once the lock is free a dead pid is reported honestly, not killed blindly
    assert stop_now(paths, actor="owner")["stopped"] is False


def test_stop_without_running_daemon_is_honest(tmp_path):
    paths, _ = _farm(tmp_path, pid=None)                   # started_at set but no pid
    assert stop_now(paths, actor="owner")["stopped"] is False


def test_stop_never_signals_the_cli_itself(tmp_path):
    # runtime_identity() carries the calling pid; a manifest written by a CLI (not a daemon)
    # must not turn `stop --now` into self-termination.
    paths, _ = _farm(tmp_path)
    with pytest.raises(StoreError, match="this CLI"):
        stop_now(paths, actor="owner")


def _candidate(tmp_path, name, mutate):
    dst = tmp_path / name
    shutil.copytree(SRC, dst, ignore=shutil.ignore_patterns("__pycache__"))
    mutate(dst / "agent_farm_runtime")
    return dst


def test_restart_refuses_protocol_change_and_accepts_same_protocol(tmp_path):
    paths, _ = _farm(tmp_path)
    same = _candidate(tmp_path, "same", lambda p: (p / "reconciler.py").open("a").write("\n# release note\n"))
    plan = restart_plan(paths, target_src=same, actor="owner")
    assert plan["protocol_dir_unchanged"] and plan["same_protocol_version"]
    assert "--upgrade-from-source" in plan["next_command"]
    assert plan["next_command"][plan["next_command"].index("--upgrade-from-source") + 1] == runtime_identity()["source_sha256"]
    assert "--executor" in plan["next_command"] and "--loop" in plan["next_command"]
    changed = _candidate(tmp_path, "changed", lambda p: (p / "protocol" / "__init__.py").open("a").write("\nNEW_FIELD = 1\n"))
    assert protocol_digest(changed) != protocol_digest(SRC)
    with pytest.raises(StoreError, match="protocol/ changed"):
        restart_plan(paths, target_src=changed, actor="owner")
    with pytest.raises(StoreError, match="nothing to restart"):
        restart_plan(paths, target_src=SRC, actor="owner")


def test_cli_stop_and_restart_plan_only(tmp_path, capsys):
    paths, _ = _farm(tmp_path)
    args = build_parser().parse_args(["--project", str(tmp_path), "--writer-policy", "pinned-host",
                                      "stop", "--drain", "--actor", "owner"])
    assert args.func(args) == 0 and deployment(paths)["handoff"]["kind"] == "stop"
    capsys.readouterr()
    same = _candidate(tmp_path, "same", lambda p: (p / "cli.py").open("a").write("\n# note\n"))
    args = build_parser().parse_args(["--project", str(tmp_path), "--writer-policy", "pinned-host",
                                      "restart", "--to", str(same), "--actor", "owner", "--plan-only"])
    assert args.func(args) == 0 and "next_command" in capsys.readouterr().out


def test_stop_control_is_cleared_when_the_daemon_starts_again(tmp_path, monkeypatch):
    """Canary 2026-09-20: after stop --drain the control stayed 'draining', so a restarted
    daemon ran one pass and exited, and restart --to would have been refused."""
    from agent_farm_runtime.events import EventLog
    from agent_farm_runtime.lifecycle import clear_stop_control
    paths, store = _farm(tmp_path, pid=None)
    control = stop_drain(paths, actor="owner")
    assert deployment(paths)["handoff"]["kind"] == "stop"
    assert clear_stop_control(paths, actor="reconciler") == control
    assert "handoff" not in deployment(paths)
    assert f"{control['id']}-FARM_STOP_CLEARED" in EventLog(paths.events / "log.ndjson").ids()
    assert clear_stop_control(paths, actor="reconciler") is None          # idempotent
    # a real node handoff is not a stop and must never be cleared this way
    from agent_farm_runtime.turnover import drain_farm
    drain_farm(paths, request_id="move-1", target_host="other", actor="owner")
    assert clear_stop_control(paths, actor="reconciler") is None
    assert deployment(paths)["handoff"]["id"] == "move-1"


def test_reconcile_startup_clears_a_stop_and_runs(tmp_path, monkeypatch, capsys):
    from agent_farm_runtime.reconciler import ReconcileReport
    paths, store = _farm(tmp_path, pid=None)
    stop_drain(paths, actor="owner")
    monkeypatch.setattr(Reconciler, "reconcile_once", lambda _: ReconcileReport())
    args = build_parser().parse_args(["--project", str(tmp_path), "--writer-policy", "pinned-host", "reconcile"])
    assert args.func(args) == 0
    assert "handoff" not in deployment(paths)
    assert (paths.runtime / "last_tick.json").exists()
