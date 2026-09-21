"""Opt-in tmux canaries confined to a disposable socket; never use the default server."""
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time

import pytest

from agent_farm_runtime.access import Access
from agent_farm_runtime.access.contract import AccessError, farm_id, markers
from agent_farm_runtime.access.observations import Observations


pytestmark = pytest.mark.skipif(
    os.environ.get("FARM_RUN_ACCESS_TMUX_CANARY") != "1" or shutil.which("tmux") is None,
    reason="requires opt-in disposable access tmux canary",
)


@pytest.fixture
def private_tmux():
    # Keep the AF_UNIX pathname short, regardless of pytest's shared-storage root.
    with tempfile.TemporaryDirectory(prefix="farm-access-", dir="/tmp") as directory:
        path = str(Path(directory) / "control.sock")
        env = {**os.environ, "LC_ALL": "C"}
        env.pop("TMUX", None)
        env.pop("BASH_ENV", None)
        def run_argv(argv, **kwargs):
            return subprocess.run(argv, capture_output=True, text=True, timeout=10, env=env, **kwargs)
        def run(*args):
            return run_argv(["tmux", "-f", "/dev/null", "-S", path, *args])
        try:
            out = run("new-session", "-d", "-s", "master", "-n", "main", "exec sleep 120")
            assert out.returncode == 0, out.stderr
            out = run("new-session", "-d", "-s", "master-other", "-n", "main", "exec sleep 120")
            assert out.returncode == 0, out.stderr
            out = run("new-window", "-t", "=master:", "-n", "logs", "exec sleep 120")
            assert out.returncode == 0, out.stderr
            yield path, run_argv, run
        finally:
            run("kill-server")  # This exact private socket only.


def record_for(path):
    root = "/shared/farms/canary/.farm"
    return {"schema_version": 1, "target": "canary/research", "farm_id": farm_id(root),
            "farm_root": root, "execution_epoch": "initial", "runtime_host": "node.example",
            "source_sha256": "a" * 64, "protocol_version": 4, "owner_uid": os.getuid(),
            "scheduler": {"kind": "slurm", "job_id": "123", "scheduler_node": "node",
                          "allocation_started_at": "2026-01-01T00:00:00"},
            "control": Observations().control(path, "master", "main"),
            "published_at": datetime.now(timezone.utc).isoformat()}


def test_exact_session_window_markers_and_returned_attachment(private_tmux):
    path, run_argv, run = private_tmux
    record = record_for(path)
    obs = Observations()
    obs.bind(record)
    assert obs.environment(record) == markers(record)
    argv = Access.verified(record)["attachment"]["argv"]
    assert "-N" not in argv
    # Control mode supplies a disposable noninteractive client to the actual
    # returned argv. The displayed IDs come from its attached session/window.
    out = run_argv([argv[0], "-C", *argv[1:]],
                   input="display-message -p '#{session_id} #{window_id}'\ndetach-client\n")
    assert out.returncode == 0, (out.stdout, out.stderr)
    control = record["control"]
    assert control["session_id"] + " " + control["window_id"] in out.stdout
    assert run("display-message", "-p", "-t", "=master-other:", "#{session_name}").stdout.strip() == "master-other"


def test_similar_sessions_are_never_selected(private_tmux):
    path, _, run = private_tmux
    obs = Observations()
    for session in ("mas", "master-othe"):
        with pytest.raises(AccessError) as caught:
            obs.control(path, session, None)
        assert caught.value.state == "SESSION_MISSING"
    assert run("kill-session", "-t", "=master").returncode == 0
    with pytest.raises(AccessError) as caught:
        obs.control(path, "master", None)
    assert caught.value.state == "SESSION_MISSING"
    assert obs.control(path, "master-other", "main")["session"] == "master-other"
    with pytest.raises(AccessError) as caught:
        obs.control(path, "master-other", "mai")
    assert caught.value.state == "SESSION_MISSING"


def test_observations_and_attach_do_not_start_a_missing_server(private_tmux):
    path, run_argv, run = private_tmux
    record = record_for(path)
    assert run("kill-server").returncode == 0
    deadline = time.monotonic() + 5
    while run("list-sessions").returncode == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert run("list-sessions").returncode != 0
    # tmux 2.7 may leave its socket pathname after exit. Remove only this
    # fixture's socket to exercise the missing and abandoned cases separately.
    Path(path).unlink(missing_ok=True)
    prefix = ["tmux", "-S", path]
    commands = [
        [*prefix, "display-message", "-p", "-t", "=master:", "#{session_name}"],
        [*prefix, "list-windows", "-t", "$0"],
        [*prefix, "show-environment", "-t", "$0"],
        [*prefix, "set-environment", "-t", "$0", "FARM_ID", "canary"],
        Access.verified(record)["attachment"]["argv"],
    ]
    # Both a disappeared socket and an abandoned socket must remain untouched.
    for stale in (False, True):
        if stale:
            with socket.socket(socket.AF_UNIX) as sock:
                sock.bind(path)
        before = {p.name: p.stat().st_ino for p in Path(path).parent.iterdir()}
        for argv in commands:
            out = run_argv(argv)
            assert out.returncode != 0, (argv, out.stdout)
            assert {p.name: p.stat().st_ino for p in Path(path).parent.iterdir()} == before
