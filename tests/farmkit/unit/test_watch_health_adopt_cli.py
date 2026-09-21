import json
import subprocess
import sys

import pytest

from farmkit import adopt, brief, cli, health, watch
from farmkit.ledger import Ledger
from farmkit.runtime_reader import CliRuntimeReader, RuntimeReaderError


class FakeReader:
    """A live fake daemon ticks with the test clock; an explicit status is returned verbatim."""

    def __init__(self, status=None):
        self.events = []
        self._status = status
        self.now = lambda: 1000.0

    def push(self, etype, task, **payload):
        self.events.append({"id": f"e{len(self.events)}", "type": etype, "task_id": task, "payload": payload})

    def status(self):
        if self._status is not None:
            return self._status
        return {"pid_alive": True, "interval": 30, "last_tick": {"epoch": self.now()}}

    def events_after(self, cursor):
        start = int(cursor or 0)
        return self.events[start:], str(len(self.events))


def run_watch(reader, cursor_file, **kw):
    clock = [1000.0]

    def tick():
        return clock[0]

    def sleep(s):
        clock[0] += s
    reader.now = tick
    kw.setdefault("timeout_s", 100)
    return watch.watch(reader, cursor_file, clock=tick, sleep=sleep, poll_s=10, **kw)


def test_watch_hits_on_ruling_and_submitted_and_replays_until_acked(tmp_path):
    reader = FakeReader()
    cursor_file = tmp_path / "cursor"
    reader.push("RECEIPT_APPLIED", "T-1", status="AWAITING", to="WAITING", waiting_on="job:12")
    reader.push("RECEIPT_APPLIED", "T-1", status="AWAITING", to="WAITING", waiting_on="ruling:T-1-code-train-lam0-abcd1234")
    reader.push("RECEIPT_APPLIED", "T-2", status="SUBMITTED", to="SUBMITTED")
    r = run_watch(reader, cursor_file)
    assert [h["reason"] for h in r["hits"]] == ["ruling", "submitted"]
    assert r["hits"][0]["class"] == "code" and r["hits"][0]["master_may_resolve"] is True
    assert r["cursor"] == "3" and watch.read_cursor(cursor_file) is None          # watch never advances the cursor
    r2 = run_watch(reader, cursor_file)
    assert [h["reason"] for h in r2["hits"]] == ["ruling", "submitted"]          # replayed until acked
    watch.ack(cursor_file, r2["cursor"])
    r3 = run_watch(reader, cursor_file)
    assert r3["reason"] == "timeout" and r3["hits"] == []
    text = watch.format_hits(r2)
    assert text.count("\n") == 1 and '"cursor": "3"' in text


def test_watch_marks_science_and_owner_rulings_as_owner_only(tmp_path):
    reader = FakeReader()
    reader.push("RECEIPT_APPLIED", "T-1", to="WAITING", waiting_on="ruling:T-1-science-train-lam0-abcd1234")
    reader.push("RECEIPT_APPLIED", "T-1", to="WAITING", waiting_on="ruling:owner-afqmc-release")
    r = run_watch(reader, tmp_path / "c")
    assert all(h["master_may_resolve"] is False for h in r["hits"])


def test_watch_daemon_down_stalled_and_until(tmp_path):
    reader = FakeReader({"pid_alive": False})
    assert run_watch(reader, tmp_path / "c")["reason"] == "daemon-down"
    reader = FakeReader({"pid_alive": True, "interval": 30, "last_tick": {"epoch": 800.0}})
    assert run_watch(reader, tmp_path / "c")["reason"] == "daemon-stalled"
    reader = FakeReader({"pid_alive": True, "interval": 30, "last_tick": {"epoch": 1000.0},
                         "observed_jobs": [{"job_id": "77", "terminal": True, "state": "FAILED"}]})
    r = run_watch(reader, tmp_path / "c", until="job:77")
    assert r["reason"] == "job" and r["hits"][0]["state"] == "FAILED"
    (tmp_path / "flag").write_text("x")
    assert run_watch(FakeReader(), tmp_path / "c", until=f"file:{tmp_path / 'flag'}")["reason"] == "file"
    assert run_watch(FakeReader(), tmp_path / "c", until="file:/nonexistent/x")["reason"] == "timeout"


def test_other_events_are_not_hits_but_restart_limit_and_adopted_are():
    reader = FakeReader()
    reader.push("WORKER_LAUNCHED", "T-1", worker_id="W-1")
    reader.push("RESUMED", "T-1", worker_id="W-1")
    assert run_watch(reader, "/dev/null")["reason"] == "timeout"
    reader.push("RESTART_LIMIT_REACHED", "T-1", limit=3)
    reader.push("WORKER_ADOPTED", "T-1", worker_id="W-2", dead_worker_id="W-1")
    r = run_watch(reader, "/dev/null")
    assert [h["reason"] for h in r["hits"]] == ["restart-limit", "adopted"]


def test_cli_runtime_reader_reports_missing_commands():
    def run(argv, **kw):
        return subprocess.CompletedProcess(argv, 2, stdout="", stderr="farm: error: argument command: invalid choice: 'events'")
    reader = CliRuntimeReader("/path/farm", "/farm", run=run)
    with pytest.raises(RuntimeReaderError, match="does not provide"):
        reader.events_after(None)

    def ok(argv, **kw):
        if "status" in argv:
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps({"pid_alive": True}), stderr="")
        return subprocess.CompletedProcess(argv, 0, stdout=json.dumps({"events": [{"type": "X"}], "cursor": "5"}), stderr="")
    reader = CliRuntimeReader("/path/farm", "/farm", run=ok)
    assert reader.status() == {"pid_alive": True} and reader.events_after("4") == ([{"type": "X"}], "5")


def test_health_findings_cover_process_loop_source_and_markers():
    now = 2000.0
    f = health.findings({"pid_alive": True, "pid": 7, "interval": 30, "last_tick": {"epoch": 1990.0},
                         "source_matches": True, "task_counts": {"WAITING": 2}}, now=now)
    assert health.worst(f) == "ok"
    f = health.findings({"pid_alive": False, "interval": 30, "last_tick": {"epoch": 100.0}, "source_matches": False,
                         "handoff": {"phase": "draining"}, "pending_recovery": True}, now=now)
    levels = {x["check"]: x["level"] for x in f}
    assert levels["daemon process"] == "fail" and levels["loop advancing"] == "fail"
    assert levels["code matches manifest"] == "fail" and levels["handoff"] == "warn" and levels["pending_recovery"] == "fail"
    assert health.worst(f) == "fail"
    assert health.findings({}, now=now)[1]["level"] == "unknown"


def test_adopt_creates_a_ledger_record_that_counts_as_in_flight(tmp_path):
    led = Ledger(tmp_path / "attempts")
    rec = adopt.adopt(led, step="afqmc:lam0#1", job_id="47260293", inputs=["trial.h5"], script_version="conductor.py 2026-09-18",
                      budget_used=1, task_id="T-DEMO")
    assert rec["submit"]["status"] == "adopted" and rec["wait"] == "job:47260293" and rec["budget"]["used"] == 1
    assert [r["step"] for r in led.in_flight()] == ["afqmc:lam0#1"]
    with pytest.raises(ValueError):
        adopt.adopt(led, step="", job_id="1", inputs=[], script_version="x")


def test_brief_lint(tmp_path):
    good = tmp_path / "BRIEF.md"
    (tmp_path / "steps.toml").write_text("")
    good.write_text("# T-1\n目标: teach\n边界: only here\n先读: steps.toml\n做法: run `farmkit tick` each wake\n")
    assert brief.lint(good) == []
    bad = tmp_path / "BAD.md"
    bad.write_text("Goal only. see missing_dir/file.md " + "a" * 64 + "\n" + "x" * 2100)
    problems = brief.lint(bad)
    assert any("bytes" in p for p in problems) and any("missing section: boundaries" in p for p in problems)
    assert any("64-hex" in p for p in problems) and any("does not exist: missing_dir/file.md" in p for p in problems)
    assert any("farmkit tick" in p for p in problems)


def test_cli_steps_check_and_brief_lint_exit_codes(tmp_path, capsys):
    ws = tmp_path
    (ws / "steps.toml").write_text('[step."a"]\nrun = "a.sbatch"\noutputs = ["a.json"]\n')
    assert cli.main(["steps", "check", "--workspace", str(ws), "--allow-no-site"]) == 1      # a.sbatch missing
    (ws / "a.sbatch").write_text("#!/bin/bash\n")
    assert cli.main(["steps", "check", "--workspace", str(ws), "--allow-no-site"]) == 0
    (ws / "BRIEF.md").write_text("目标 边界 先读 做法 farmkit tick\n")
    assert cli.main(["brief", "lint", str(ws / "BRIEF.md")]) == 0
    out = capsys.readouterr().out
    assert "steps.toml: ok" in out and "brief: ok" in out


def test_cli_tick_prints_receipt_command_without_running_it(tmp_path, capsys, monkeypatch):
    ws = tmp_path
    (ws / "steps.toml").write_text(f'[step."local"]\nrun = "{sys.executable} -c \'print(1)\'"\noutputs = ["out.json"]\nfinite = []\n')
    monkeypatch.setenv("FARM_TASK_ID", "T-cli")
    monkeypatch.setattr(cli, "_site", lambda args: __import__("farmkit.site", fromlist=["Site"]).Site.minimal(accounting_stores_comment=False))
    rc = cli.main(["tick", "--workspace", str(ws), "--allow-no-site"])
    out = capsys.readouterr().out
    assert rc == 0 and "python .farm_receipt.py AWAITING --waiting-on ruling:T-cli-code-local-" in out
    assert "farmkit never runs it" in out
