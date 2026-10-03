"""Operator-visible state regressions from GitHub #7 and #30."""
from dataclasses import asdict
from datetime import datetime, timezone
import json

import pytest

from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.cli import build_parser, task_summary
from agent_farm_runtime.models import Lease, Task, TaskState
from agent_farm_runtime.observers import make_unblock
from agent_farm_runtime.provenance import runtime_identity
from agent_farm_runtime.reconciler import ReconcileReport, Reconciler
from agent_farm_runtime.status import farm_status, write_last_tick
from agent_farm_runtime.store import FarmPaths, TaskStore, atomic_write_json
from farmkit.watch import ruling_parks


@pytest.mark.parametrize("state", list(TaskState))
def test_wait_views_distinguish_current_and_historical_conditions(tmp_path, state):
    paths = FarmPaths(tmp_path / ".farm")
    task = Task("T1", "objective", "result", "check", state=state,
                metadata={"waiting_on": "job:123"})
    TaskStore(paths).create(task)
    before = (paths.tasks / "T1.json").read_bytes()
    summary = task_summary(task)
    status = farm_status(paths, slurm=lambda _: "COMPLETED")["tasks"][0]
    for view in (summary, status):
        assert view["waiting_on"] == ("job:123" if state is TaskState.WAITING else None)
        assert view["last_wait"] == (None if state is TaskState.WAITING else "job:123")
    assert (paths.tasks / "T1.json").read_bytes() == before


@pytest.mark.parametrize("state", list(TaskState))
def test_watch_treats_legacy_ruling_metadata_as_active_only_while_waiting(state):
    # An upgraded farmkit may still inspect an older runtime's status response.
    parks = ruling_parks({"tasks": [{"id": "T1", "state": state.value,
                                    "revision": 4, "waiting_on": "ruling:T1-code-fix"}]})
    assert bool(parks) == (state is TaskState.WAITING)


def test_claim_reports_completed_jobs_without_resuming_them(tmp_path, monkeypatch, capsys):
    paths = FarmPaths(tmp_path / ".farm")
    store = TaskStore(paths)
    store.create(Task("T1", "objective", "result", "check", state=TaskState.WAITING,
                      lease=Lease("W1", "L1"), metadata={"waiting_on": "job:123"}))
    atomic_write_json(paths.runtime / "deployment.json", {**runtime_identity(), "interval": 30})
    write_last_tick(paths, asdict(ReconcileReport()), observed_jobs={"123": "RUNNING"})
    before = store.get("T1").to_dict()
    monkeypatch.setattr("agent_farm_runtime.turnover.claim_farm", lambda *a, **k: {"phase": "claimed"})
    monkeypatch.setattr("agent_farm_runtime.adapters.slurm.slurm_state", lambda _: "COMPLETED")
    args = build_parser().parse_args(["--project", str(tmp_path), "handoff", "claim",
                                     "--request-id", "move-1", "--actor", "master"])
    assert args.func(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["catch_up"] == [{"job_id": "123", "task": "T1", "state": "COMPLETED",
                                   "terminal": True, "source": "query"}]
    assert store.get("T1").to_dict() == before
    executor = FakeExecutor()
    rec = Reconciler(paths, executor, unblock=make_unblock(paths, slurm=lambda _: "COMPLETED"))
    assert rec.reconcile_once().resumed == ["T1"]
    assert rec.reconcile_once().resumed == []
    assert executor.resumed == ["W1"]


def test_status_rejects_job_observations_from_previous_epoch(tmp_path):
    paths = FarmPaths(tmp_path / ".farm")
    TaskStore(paths).create(Task("T1", "objective", "result", "check", state=TaskState.WAITING,
                                metadata={"waiting_on": "job:123"}))
    old = {**runtime_identity(), "execution_epoch": "old", "interval": 30}
    write_last_tick(paths, asdict(ReconcileReport()), observed_jobs={"123": "RUNNING"}, deployment=old)
    atomic_write_json(paths.runtime / "deployment.json", {**old, "execution_epoch": "new"})
    jobs = farm_status(paths, slurm=lambda _: "COMPLETED")["observed_jobs"]
    assert jobs[0]["state"] == "COMPLETED" and jobs[0]["source"] == "query"


@pytest.mark.parametrize("tick_patch", [
    {"ts": 123}, {"ts": "2026-01-01T00:00:00"},
    {"observed_jobs": ["123"]}, {"observed_jobs": {"123": {"state": "COMPLETED"}}},
])
def test_status_queries_past_malformed_advisory_tick(tmp_path, tick_patch):
    paths = FarmPaths(tmp_path / ".farm")
    TaskStore(paths).create(Task("T1", "objective", "result", "check", state=TaskState.WAITING,
                                metadata={"waiting_on": "job:123"}))
    tick = {"ts": datetime.now(timezone.utc).isoformat(), "observed_jobs": {"123": "RUNNING"}, **tick_patch}
    atomic_write_json(paths.runtime / "last_tick.json", tick)
    before = (paths.runtime / "last_tick.json").read_bytes()
    status = farm_status(paths, slurm=lambda _: "COMPLETED")
    assert status["observed_jobs"][0]["source"] == "query"
    assert status["observed_jobs"][0]["state"] == "COMPLETED"
    assert (paths.runtime / "last_tick.json").read_bytes() == before


@pytest.mark.parametrize("wait", ["job:123:unexpected", {"job": "123"}])
def test_status_does_not_query_invalid_job_references(tmp_path, wait):
    paths = FarmPaths(tmp_path / ".farm")
    TaskStore(paths).create(Task("T1", "objective", "result", "check", state=TaskState.WAITING,
                                metadata={"waiting_on": wait}))
    def unexpected_query(job_id):
        pytest.fail("invalid reference must not be converted into a different valid job")
    assert farm_status(paths, slurm=unexpected_query)["observed_jobs"] == []
