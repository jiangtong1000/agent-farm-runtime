"""Durable recovery and behavior boundaries retained by the reduction pass."""
from dataclasses import replace
import json

import pytest

from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.events import EventLog
from agent_farm_runtime.models import Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.reconciler import Reconciler, ReconcileReport
from agent_farm_runtime.store import FarmPaths, TaskStore


@pytest.mark.parametrize("operation", ["create", "update"])
def test_task_directory_sync_failure_is_retried_before_clearing_journal(tmp_path, monkeypatch, operation):
    from agent_farm_runtime.adapters import posix_filesystem
    paths = FarmPaths(tmp_path / ".farm")
    paths.ensure()
    store = TaskStore(paths)
    task = Task("T1", "new objective", "output", "check")
    before = None
    if operation == "update":
        store.create(task)
        before = store.get(task.id)
        task = replace(before, objective="updated objective")
    log = EventLog(paths.events / "log.ndjson")
    original_ids = log.ids()
    pending = paths.runtime / "pending-task-commit.json"
    real_sync = posix_filesystem.sync_directory
    attempts = []

    def fail_task_directory(directory):
        if directory == paths.tasks:
            attempts.append(directory)
            raise OSError("injected task directory sync failure")
        real_sync(directory)

    with monkeypatch.context() as scoped:
        scoped.setattr(posix_filesystem, "sync_directory", fail_task_directory)
        with pytest.raises(OSError, match="task directory sync"):
            if before is None:
                store.create(task)
            else:
                store.commit(task, expected=before, actor="test", event_type="TEST")
        prepared = pending.read_bytes()
        # Rename made the new state visible, but visibility is not durability.
        assert store.get(task.id).objective == task.objective
        for _ in range(2):
            with pytest.raises(OSError, match="task directory sync"):
                store.recover()
            assert pending.read_bytes() == prepared
            assert log.ids() == original_ids
    assert len(attempts) == 3
    store.recover()
    store.recover()  # completed recovery is a no-op, not a duplicate commit
    assert not pending.exists()
    assert store.get(task.id).to_dict() == json.loads(prepared)["after"]
    assert len(log.ids() - original_ids) == 1


@pytest.mark.parametrize("alive", [True, None, False])
def test_running_receipt_does_not_override_process_observation(tmp_path, alive):
    store = TaskStore(FarmPaths(tmp_path / ".farm"))
    store.create(Task("T1", "objective", "output", "check"))
    executor = FakeExecutor()
    reconciler = Reconciler(store.paths, executor, grace_seconds=0)
    reconciler.reconcile_once()
    before = store.get("T1")
    lease = before.lease
    executor.set_receipt(Receipt(lease.worker_id, "T1", lease.lease_id, ReceiptStatus.RUNNING, ts="t"))
    executor._alive[lease.worker_id] = alive
    report = reconciler.reconcile_once()
    after = store.get("T1")
    assert after.state is TaskState.RUNNING and report.advanced == []
    if alive is False:
        assert report.adopted == ["T1"] and after.lease != lease
        assert executor.stopped == [lease.worker_id]
    else:
        assert after.to_dict() == before.to_dict()
        assert report.adopted == [] and executor.stopped == []
        assert report.heartbeats == ([lease.worker_id] if alive is True else [])
        assert bool(report.observation_errors) is (alive is None)


@pytest.mark.parametrize("populated", [False, True])
def test_reconcile_report_json_contract_is_unchanged(tmp_path, monkeypatch, capsys, populated):
    # Keep the existing public keys explicit: a new report field must be reviewed.
    payload = {key: [] for key in (
        "launched", "adopted", "advanced", "resumed", "heartbeats", "ignored_stale",
        "waiting_grace", "conflicts", "preflight_failures", "observation_errors", "executor_errors",
    )}
    if populated:
        for key in payload:
            payload[key].append(("T1", "WAITING") if key == "advanced" else "T1")
    report = ReconcileReport(**payload)
    monkeypatch.setattr(Reconciler, "reconcile_once", lambda _: report)
    monkeypatch.setattr("agent_farm_runtime.adapters.local_process.LocalProcessExecutor", lambda _: FakeExecutor())
    args = build_parser().parse_args(["--project", str(tmp_path), "reconcile"])
    assert args.func(args) == 0
    assert capsys.readouterr().out == json.dumps(payload, sort_keys=True) + "\n"
