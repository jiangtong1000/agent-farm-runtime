import json
import os

import pytest

from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.storage_probe import probe_storage


pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX filesystem probe")


def test_storage_probe_checks_independent_lock_and_cleans_up(tmp_path):
    existing = tmp_path / "existing"
    existing.write_text("untouched")
    mode = tmp_path.stat().st_mode
    result = probe_storage(tmp_path)
    assert result["ok"] is True
    assert {check["name"] for check in result["checks"]} == {
        "flock_exclusion", "file_fsync", "atomic_replace", "directory_fsync", "temporary_cleanup"}
    assert list(tmp_path.iterdir()) == [existing]
    assert existing.read_text() == "untouched" and tmp_path.stat().st_mode == mode
    assert "cross-node" in result["scope"]


def test_probe_failure_is_reported_and_cleans_up(tmp_path, monkeypatch):
    def fail(_):
        raise OSError("directory sync unsupported")
    monkeypatch.setattr("agent_farm_runtime.storage_probe.sync_directory", fail)
    args = build_parser().parse_args(["storage-probe", str(tmp_path), "--json"])
    assert args.func(args) == 1
    assert not list(tmp_path.iterdir())


def test_probe_refuses_broken_lock_exclusion(tmp_path, monkeypatch):
    monkeypatch.setattr("agent_farm_runtime.storage_probe._lock_observation", lambda _: "ACQUIRED")
    result = probe_storage(tmp_path)
    assert not result["ok"]
    assert result["checks"][-1]["name"] == "flock_exclusion"
    assert not list(tmp_path.iterdir())


def test_probe_cli_json_and_missing_directory(tmp_path, capsys):
    args = build_parser().parse_args(["storage-probe", str(tmp_path), "--json"])
    assert args.func(args) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    with pytest.raises(FileNotFoundError):
        probe_storage(tmp_path / "missing")
    assert not list(tmp_path.iterdir())
