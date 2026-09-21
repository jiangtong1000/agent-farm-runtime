"""Exact command/permission contracts; scheduler and tmux subprocesses are stubbed."""
from __future__ import annotations

import os
from pathlib import Path
import socket
import subprocess

import pytest

from agent_farm_runtime.access.contract import AccessError
from agent_farm_runtime.access.observations import Observations
from agent_farm_runtime.access.registry import Registry, persistent_path


def fails(state, call, reason=None):
    with pytest.raises(AccessError) as caught:
        call()
    assert caught.value.state == state
    if reason:
        assert caught.value.reason == reason


def test_slurm_requests_only_the_exact_allocation_and_expands_its_nodes(monkeypatch):
    monkeypatch.setenv("SLURM_CLUSTERS", "another-cluster")
    monkeypatch.setenv("SLURM_TIME_FORMAT", "relative")
    monkeypatch.setenv("TZ", "Etc/GMT+5")
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        assert kwargs["timeout"] == 10 and kwargs["env"]["LC_ALL"] == "C"
        assert kwargs["env"]["SLURM_TIME_FORMAT"] == "standard" and kwargs["env"]["TZ"] == "UTC"
        assert "SLURM_CLUSTERS" not in kwargs["env"]
        if "job" in argv:
            out = "JobId=123 JobName=irrelevant JobState=RUNNING UserId=example(1001) NodeList=node-[1-2] StartTime=2026-01-01T00:00:00\n"
        else:
            out = "node-1\nnode-2\n"
        return subprocess.CompletedProcess(argv, 0, out, "")
    observed = Observations(run=run).scheduler("123")
    assert observed == {"job_id": "123", "state": "RUNNING", "nodes": ["node-1", "node-2"],
                        "owner_uid": 1001, "allocation_started_at": "2026-01-01T00:00:00"}
    assert calls == [["scontrol", "--local", "--oneliner", "show", "job", "123"],
                     ["scontrol", "show", "hostnames", "node-[1-2]"]]


@pytest.mark.parametrize("kind,state", [("timeout", "UNREACHABLE"), ("missing", "UNREACHABLE"),
    ("permission", "AUTH_REQUIRED"), ("exception_permission", "AUTH_REQUIRED")])
def test_failed_scheduler_observation_does_not_query_other_sources(kind, state):
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        if kind == "timeout":
            raise subprocess.TimeoutExpired(argv, 10)
        if kind == "exception_permission":
            raise PermissionError("denied")
        return subprocess.CompletedProcess(argv, 1, "", "permission denied" if kind == "permission" else "Invalid job id specified")
    fails(state, lambda: Observations(run=run).scheduler("123"))
    assert len(calls) == 1


@pytest.mark.parametrize("output", ["", "JobId=123\nJobId=124\n",
    "JobId=123 JobId=124 JobState=RUNNING UserId=x(1) NodeList=n1 StartTime=now",
    "JobId=123 JobState=RUNNING UserId=x(1) NodeList=(null) StartTime=Unknown"])
def test_incomplete_or_ambiguous_slurm_output_is_unknown(output):
    obs = Observations(run=lambda argv, **kw: subprocess.CompletedProcess(argv, 0, output, ""))
    fails("UNREACHABLE", lambda: obs.scheduler("123"))


@pytest.mark.parametrize("value", ["123;id", "123_4", "-1", "0", "123,124"])
def test_job_identifier_is_an_exact_numeric_allocation(value):
    obs = Observations(run=lambda *a, **k: pytest.fail("invalid job must not be queried"))
    fails("CONFLICT", lambda: obs.scheduler(value))


@pytest.fixture
def control_socket(tmp_path):
    path = tmp_path / "control.sock"
    with socket.socket(socket.AF_UNIX) as sock:
        sock.bind(str(path))
        yield path


def test_missing_socket_and_non_socket_never_call_tmux(tmp_path):
    obs = Observations(run=lambda *a, **k: pytest.fail("must check socket first"))
    path = tmp_path / "missing"
    fails("SESSION_MISSING", lambda: obs.control(str(path), "master", None), "socket_missing")
    path.touch()
    fails("CONFLICT", lambda: obs.control(str(path), "master", None), "not_socket")


def test_wrong_socket_owner_fails_before_any_tmux_call(control_socket, monkeypatch):
    uid = os.getuid()
    monkeypatch.setattr(os, "getuid", lambda: uid + 1)
    obs = Observations(run=lambda *a, **k: pytest.fail("foreign socket must not be contacted"))
    fails("AUTH_REQUIRED", lambda: obs.control(str(control_socket), "master", None), "socket_owner")


def tmux_stub(monkeypatch, *, session="master", windows="@1|0|main\n@2|1|logs\n"):
    calls = []
    monkeypatch.setattr("agent_farm_runtime.access.observations.proc_starttime", lambda _: 42)
    monkeypatch.setattr("agent_farm_runtime.access.observations.host_identity", lambda: {"boot_id": "boot-a"})
    def run(argv, **kwargs):
        calls.append(argv)
        assert argv[:2] == ["tmux", "-S"] and "TMUX" not in kwargs["env"]
        assert "-N" not in argv
        assert kwargs["timeout"] == 10
        if argv[3] == "display-message":
            out = f"$7|{session}|321\n"
        else:
            assert argv[3] == "list-windows"
            out = windows
        return subprocess.CompletedProcess(argv, 0, out, "")
    return Observations(run=run), calls


def test_tmux_uses_explicit_socket_exact_name_and_window_id(control_socket, monkeypatch):
    monkeypatch.setenv("TMUX", "/wrong/worker.sock,1,2")
    obs, calls = tmux_stub(monkeypatch)
    result = obs.control(str(control_socket), "master", "main")
    assert result["session_id"] == "$7" and result["window_id"] == "@1"
    assert result["socket_inode"] == control_socket.stat().st_ino
    assert calls[0][3:8] == ["display-message", "-p", "-t", "=master:", "#{session_id}|#{session_name}|#{pid}"]
    assert calls[1][3:6] == ["list-windows", "-t", "$7"]


def test_default_socket_ignores_ambient_worker_server(monkeypatch):
    monkeypatch.setenv("TMUX", "/wrong/worker.sock,1,2")
    monkeypatch.setenv("TMUX_TMPDIR", "/tmp/example-tmux")
    assert Observations.default_socket() == f"/tmp/example-tmux/tmux-{os.getuid()}/default"


@pytest.mark.parametrize("window,state", [("ma", "SESSION_MISSING"), ("missing", "SESSION_MISSING"), ("main", "CONFLICT")])
def test_window_names_are_exact_and_duplicates_fail(control_socket, monkeypatch, window, state):
    obs, _ = tmux_stub(monkeypatch, windows="@1|0|main\n@2|1|main\n")
    fails(state, lambda: obs.control(str(control_socket), "master", window))


@pytest.mark.parametrize("session", ["master-other", "masters", "mas"])
def test_prefix_session_result_is_rejected(control_socket, monkeypatch, session):
    obs, calls = tmux_stub(monkeypatch, session=session)
    fails("CONFLICT", lambda: obs.control(str(control_socket), "master", None))
    assert len(calls) == 1 and "=master:" in calls[0]


@pytest.mark.parametrize("stderr,state", [("can't find session: master", "SESSION_MISSING"),
    ("can't find session master", "SESSION_MISSING"),
    ("no such session: $7", "SESSION_MISSING"),
    ("can't find window: main", "SESSION_MISSING"), ("can't find window main", "SESSION_MISSING"),
    ("permission denied", "AUTH_REQUIRED"), ("access denied", "AUTH_REQUIRED"),
    ("operation not permitted", "AUTH_REQUIRED"),
    ("can't find session master\npermission denied", "AUTH_REQUIRED"),
    ("can't find session master\nserver exited unexpectedly", "UNREACHABLE"),
    ("server exited unexpectedly", "UNREACHABLE"), ("no server running on /private/socket", "UNREACHABLE"),
    ("error connecting to /private/socket (Connection refused)", "UNREACHABLE"), ("", "UNREACHABLE")])
def test_failed_tmux_observation_never_becomes_a_create_request(control_socket, stderr, state):
    calls = []
    def run(argv, **kw):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 1, "", stderr)
    fails(state, lambda: Observations(run=run).control(str(control_socket), "master", None))
    assert len(calls) == 1 and calls[0][3] == "display-message"


@pytest.mark.parametrize("root", ["/tmp/farm-access", "/dev/shm/farm-access", "/run/farm-access", "/var/tmp/farm-access", "relative"])
def test_registry_rejects_volatile_or_relative_storage(root):
    fails("CONFLICT", lambda: Registry(Path(root)))


def test_symlink_into_volatile_storage_is_not_an_escape(tmp_path):
    link = tmp_path / "shared-link"
    link.symlink_to("/tmp", target_is_directory=True)
    fails("CONFLICT", lambda: persistent_path(link / "access"), "volatile_storage")


def test_registry_requires_operator_storage_attestation(tmp_path, monkeypatch):
    monkeypatch.setattr("agent_farm_runtime.access.registry.persistent_path", lambda p: p.resolve())
    registry = Registry(tmp_path / "registry")
    fails("CONFLICT", lambda: registry.initialize(attest_shared_storage=False), "storage_attestation")
    assert not registry.root.exists()
