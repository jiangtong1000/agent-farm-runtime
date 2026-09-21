"""Read-only structured status, event cursor, last-tick heartbeat, quiet loop log (D15, D33, D38)."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import pytest

from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.adapters.filesystem import atomic_write_json
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.models import Task, TaskState
from agent_farm_runtime.provenance import runtime_identity
from agent_farm_runtime.reconciler import ReconcileReport, Reconciler
from agent_farm_runtime.status import events_after, farm_status, write_last_tick
from agent_farm_runtime.store import FarmPaths, TaskStore


def _farm(tmp_path):
    paths = FarmPaths(tmp_path / ".farm"); paths.ensure()
    return paths, TaskStore(paths)


def test_status_json_without_deployment_is_honest(tmp_path):
    paths, store = _farm(tmp_path)
    store.create(Task("T-1", "o", "d", "a"))
    status = farm_status(paths, slurm=lambda j: None)
    assert status["deployment"] is None and status["source_matches"] is False
    assert status["pid_alive"] is None and status["last_tick"] is None
    assert status["task_counts"]["READY"] == 1 and len(status["tasks"]) == 1
    assert not status["pending_task_commit"] and not status["pending_recovery"] and not status["pending_deployment_event"]


def test_status_json_reports_daemon_identity_and_tick_age(tmp_path):
    paths, _ = _farm(tmp_path)
    identity = runtime_identity()
    atomic_write_json(paths.runtime / "deployment.json", {**identity, "pid": 2**22 + 12345,  # surely absent
                      "pid_starttime": 1, "started_at": "2026-09-19T00:00:00+00:00", "interval": 30.0, "loop": True,
                      "executor": "codex-tmux", "session": "x", "handoff": {"id": "h1", "phase": "draining"}})
    write_last_tick(paths, asdict(ReconcileReport()))
    later = datetime.now(timezone.utc) + timedelta(seconds=95)
    status = farm_status(paths, now=later, slurm=lambda j: None)
    assert status["source_matches"] is True
    assert status["pid_alive"] is False                       # same host, identity recorded, pid absent: positive observation
    legacy = {**identity, "pid": 2**22 + 12345, "started_at": "2026-09-19T00:00:00+00:00"}   # no pid_starttime
    atomic_write_json(paths.runtime / "deployment.json", legacy)
    assert farm_status(paths, now=later, slurm=lambda j: None)["pid_alive"] is None    # a bare pid is not an identity
    assert 90 <= status["last_tick_age_s"] <= 100
    assert abs(status["last_tick"]["epoch"] - datetime.fromisoformat(status["last_tick"]["ts"]).timestamp()) < 1e-6
    assert status["handoff"] == {"phase": "draining", "id": "h1", "kind": None}


def test_status_json_on_foreign_host_is_unknown(tmp_path):
    paths, _ = _farm(tmp_path)
    atomic_write_json(paths.runtime / "deployment.json", {**runtime_identity(), "host": "elsewhere", "pid": 1})
    assert farm_status(paths, slurm=lambda j: None)["pid_alive"] is None


def test_status_lists_observed_jobs_for_job_waits(tmp_path):
    paths, store = _farm(tmp_path)
    task = Task("T-w", "o", "d", "a", metadata={"waiting_on": "job:4711"})
    store.create(task)
    from dataclasses import replace
    from agent_farm_runtime.models import Lease
    waiting = replace(store.get("T-w"), state=TaskState.WAITING, lease=Lease("W", "L"))
    # go through the legal path: READY->RUNNING->WAITING via the store's transition validation
    running = replace(store.get("T-w"), state=TaskState.RUNNING, lease=Lease("W", "L"))
    running = store.commit(running, expected=store.get("T-w"), event_type="X", actor="t")
    store.commit(replace(running, state=TaskState.WAITING), expected=running, event_type="Y", actor="t")
    status = farm_status(paths, slurm=lambda j: "COMPLETED" if j == "4711" else None)
    assert status["observed_jobs"] == [{"job_id": "4711", "task": "T-w", "state": "COMPLETED", "terminal": True, "source": "query"}]


def test_events_cursor_is_idempotent_and_skips_partial_tail(tmp_path):
    paths, store = _farm(tmp_path)
    store.create(Task("T-1", "o", "d", "a"))
    store.create(Task("T-2", "o", "d", "a"))
    first = events_after(paths, 0)
    assert [e["type"] for e in first["events"]] == ["TASK_CREATED", "TASK_CREATED"]
    assert events_after(paths, first["cursor"])["events"] == []
    # an append in progress: partial line without newline is not a record yet
    log = paths.events / "log.ndjson"
    with log.open("ab") as fh:
        fh.write(b'{"id": "partial"')
    again = events_after(paths, first["cursor"])
    assert again["events"] == [] and again["cursor"] == first["cursor"]
    with log.open("ab") as fh:
        fh.write(b', "task_id": "T-3", "type": "X", "actor": "t", "payload": {}, "ts": "now"}\n')
    done = events_after(paths, first["cursor"])
    assert [e["id"] for e in done["events"]] == ["partial"]
    assert done["cursor"] == log.stat().st_size
    with pytest.raises(ValueError):
        events_after(paths, done["cursor"] + 10)


def test_events_limit_reports_truncation(tmp_path):
    paths, store = _farm(tmp_path)
    for i in range(3):
        store.create(Task(f"T-{i}", "o", "d", "a"))
    page = events_after(paths, 0, limit=2)
    assert len(page["events"]) == 2 and page["truncated"] is True
    rest = events_after(paths, page["cursor"], limit=2)
    assert len(rest["events"]) == 1 and rest["truncated"] is False


def test_single_pass_prints_full_report_but_loop_is_quiet(tmp_path, monkeypatch, capsys):
    paths, _ = _farm(tmp_path)
    monkeypatch.setattr(Reconciler, "reconcile_once", lambda _: ReconcileReport())
    monkeypatch.setattr("agent_farm_runtime.adapters.local_process.LocalProcessExecutor", lambda _: FakeExecutor())
    args = build_parser().parse_args(["--project", str(tmp_path), "reconcile"])
    assert args.func(args) == 0
    out = capsys.readouterr().out
    assert json.loads(out)["launched"] == []                 # single pass: full report
    assert (paths.runtime / "last_tick.json").exists()       # heartbeat file written (D33)
    tick = json.loads((paths.runtime / "last_tick.json").read_text())
    assert set(tick) == {"ts", "counts", "observed_jobs"} and tick["counts"]["launched"] == 0


def test_status_cli_flag(tmp_path, capsys):
    paths, store = _farm(tmp_path)
    store.create(Task("T-1", "o", "d", "a"))
    args = build_parser().parse_args(["--project", str(tmp_path), "status", "--json"])
    assert args.func(args) == 0
    assert json.loads(capsys.readouterr().out)["task_counts"]["READY"] == 1
    args = build_parser().parse_args(["--project", str(tmp_path), "events", "--after", "0"])
    assert args.func(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["events"][0]["type"] == "TASK_CREATED" and isinstance(payload["cursor"], str)


def test_quiet_loop_ignores_routine_worker_heartbeats():
    from agent_farm_runtime.cli import tick_acted
    idle = asdict(ReconcileReport())
    assert tick_acted(idle) is False
    assert tick_acted({**idle, "heartbeats": ["W-1"]}) is False        # observation only: stays quiet
    assert tick_acted({**idle, "launched": ["T-1"]}) is True
    assert tick_acted({**idle, "heartbeats": ["W-1"], "advanced": [["T-1", "WAITING"]]}) is True


def test_status_prefers_the_daemons_own_job_observations(tmp_path):
    """One observer (D28/D29): a fresh last_tick carries the states the daemon saw; status
    queries the scheduler only for jobs the daemon did not report, or when it is stale."""
    from datetime import datetime, timedelta, timezone
    from agent_farm_runtime.status import write_last_tick
    from dataclasses import replace
    from agent_farm_runtime.models import Lease
    paths, store = _farm(tmp_path)
    atomic_write_json(paths.runtime / "deployment.json", {**runtime_identity(), "pid": None, "interval": 30.0,
                                                          "started_at": "2026-09-19T00:00:00+00:00"})
    for tid, job in (("T-a", "1"), ("T-b", "2")):
        store.create(Task(tid, "o", "d", "a", metadata={"waiting_on": f"job:{job}"}))
        running = store.commit(replace(store.get(tid), state=TaskState.RUNNING, lease=Lease("W", "L")),
                               expected=store.get(tid), event_type="X", actor="t")
        store.commit(replace(running, state=TaskState.WAITING), expected=running, event_type="Y", actor="t")
    write_last_tick(paths, asdict(ReconcileReport()), observed_jobs={"1": "RUNNING"})
    queried = []
    status = farm_status(paths, slurm=lambda j: queried.append(j) or "COMPLETED")
    by_job = {j["job_id"]: j for j in status["observed_jobs"]}
    assert by_job["1"]["state"] == "RUNNING" and by_job["1"]["source"] == "daemon"
    assert by_job["2"]["state"] == "COMPLETED" and by_job["2"]["source"] == "query" and queried == ["2"]
    stale = datetime.now(timezone.utc) + timedelta(hours=1)
    status = farm_status(paths, now=stale, slurm=lambda j: "COMPLETED")
    assert all(j["source"] == "query" for j in status["observed_jobs"])      # stale daemon: do not trust it


def test_events_last_reads_from_the_end(tmp_path, capsys):
    from agent_farm_runtime.status import events_tail
    paths, store = _farm(tmp_path)
    for i in range(30):
        store.create(Task(f"T-{i}", "o", "d", "a"))
    tail = events_tail(paths, 5)
    assert [e["task_id"] for e in tail["events"]] == [f"T-{i}" for i in range(25, 30)]
    assert events_tail(paths, 1000)["events"][0]["task_id"] == "T-0"        # asking for more than exists
    args = build_parser().parse_args(["--project", str(tmp_path), "events", "--last", "2"])
    assert args.func(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [e["task_id"] for e in payload["events"]] == ["T-28", "T-29"]
