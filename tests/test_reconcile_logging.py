"""Daemon logs retain diagnostics without replacing terminal output or task evidence."""
import json
import sys
from datetime import datetime, timedelta, timezone

import pytest

from agent_farm_runtime.reconcile_logging import reconcile_output


def test_log_mirrors_stdout_stderr_and_restores_streams(tmp_path, capsys):
    stdout, stderr = sys.stdout, sys.stderr
    path = tmp_path / "logs" / "reconciler.log"
    with reconcile_output(path) as configured:
        assert configured == path
        print('{"ts":"2026-01-01T00:00:00+00:00","launched":[]}')
        print("adapter diagnostic", file=sys.stderr)
        print("partial final line", end="")
    assert sys.stdout is stdout and sys.stderr is stderr
    lines = path.read_text().splitlines()
    assert json.loads(lines[0])["launched"] == []
    assert lines[1:] == ["adapter diagnostic", "partial final line"]
    captured = capsys.readouterr()
    assert "partial final line" in captured.out and "adapter diagnostic" in captured.err


def test_log_records_exception_and_does_not_suppress_it(tmp_path):
    path = tmp_path / "reconciler.log"
    with pytest.raises(ValueError, match="bad observation"):
        with reconcile_output(path):
            raise ValueError("bad observation")
    assert "ValueError: bad observation" in path.read_text()


def test_daily_rotation_retains_fourteen_backups(tmp_path):
    path = tmp_path / "reconciler.log"
    now = datetime.now(timezone.utc)
    # Existing daily logs predate this process; prune by their standard suffixes.
    for days in range(2, 19):
        (tmp_path / (path.name + "." + (now - timedelta(days=days)).strftime("%Y-%m-%d"))).write_text("old\n")
    path.write_text("previous daemon\n")
    with reconcile_output(path):
        handler = sys.stdout.handler
        assert handler.utc and handler.backupCount == 14
        handler.rolloverAt = int(now.timestamp()) - 1
        print("new daemon tick")
    assert path.read_text() == "new daemon tick\n"
    assert len(list(tmp_path.glob("reconciler.log.*"))) == 14


def test_log_disk_error_propagates_without_recursive_stderr(tmp_path, monkeypatch):
    with pytest.raises(OSError, match="disk full"):
        with reconcile_output(tmp_path / "reconciler.log"):
            def fail(*args):
                raise OSError("disk full")
            monkeypatch.setattr(sys.stdout.handler, "shouldRollover", fail)
            print("a tick")


def test_reconciler_log_timestamp_and_discovery_through_status(tmp_path, capsys):
    from agent_farm_runtime.cli import build_parser
    from agent_farm_runtime.status import farm_status
    from agent_farm_runtime.store import FarmPaths

    path = tmp_path / "logs" / "reconciler.log"
    args = build_parser().parse_args(["--project", str(tmp_path), "reconcile",
                                      "--executor", "local-process", "--log", str(path)])
    assert args.func(args) == 0
    output = capsys.readouterr().out
    assert path.read_text() == output
    tick = json.loads(output)
    assert datetime.fromisoformat(tick["ts"]).tzinfo is not None
    assert tick["launched"] == []
    status = farm_status(FarmPaths(tmp_path / ".farm"))
    assert status["deployment"]["reconcile_log"] == str(path)
