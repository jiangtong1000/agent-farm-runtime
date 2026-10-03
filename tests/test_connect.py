"""Transport simulation plus real access verification; no SSH or live farm actions."""
from argparse import Namespace
from copy import deepcopy
import json
from pathlib import Path
import shlex
import shutil
import subprocess

import pytest

from agent_farm_runtime import connect as client
from agent_farm_runtime.access.contract import AccessError
from test_access import farm  # noqa: F401 - shared isolated access fixture
from test_access_roles import adopted  # noqa: F401


@pytest.fixture
def endpoint(adopted):
    published = adopted.publish(generation="master-1", expected_current="none", role="interactive", role_pid=999)
    profile = {"login_host": "user@login.example", "registry": str(adopted.registry.root),
               "farm_root": published["record"]["farm_root"], "farm_id": published["record"]["farm_id"],
               "farm_wrapper": "/shared/release with spaces/farm", "jump_hosts": ["jump.example"],
               "control_path": str(adopted.registry.root / "control-%C"), "control_persist": "10m"}
    return adopted, profile, published


def _config(tmp_path, profile):
    config = tmp_path / "connect.toml"
    config.write_text('version = 1\n[targets."cluster/study-a"]\n' +
                       "\n".join(f"{key} = {json.dumps(value)}" for key, value in profile.items()) + "\n")
    return config


def test_configuration_pins_one_farm_and_accepts_paths_with_spaces(endpoint, tmp_path):
    _, profile, _ = endpoint
    config = _config(tmp_path, profile)
    assert client.load_profile(config, "cluster/study-a") == profile
    with pytest.raises(AccessError, match="not configured"):
        client.load_profile(config, "cluster/other")


@pytest.mark.parametrize("field,value", [("login_host", "-oProxyCommand=evil"), ("login_host", "user@host;evil"),
                                        ("jump_hosts", ["host$(evil)"]), ("farm_id", "other-farm"),
                                        ("farm_wrapper", "relative/farm"), ("control_persist", "yes\nProxyCommand evil"),
                                        ("ssh_config", '/absolute/evil"\nHost *')])
def test_config_rejects_injection_and_identity_mismatch(endpoint, tmp_path, field, value):
    _, profile, _ = endpoint
    with pytest.raises(AccessError):
        client.load_profile(_config(tmp_path, {**profile, field: value}), "cluster/study-a")


def test_native_ssh_resolves_verifies_then_reverifies_before_attach(endpoint):
    access_farm, profile, published = endpoint
    calls, configurations = [], []
    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        config = Path(argv[argv.index("-F") + 1])
        configurations.append(config)
        content = config.read_text()
        assert "StrictHostKeyChecking yes" in content and "BatchMode no" in content
        assert "ControlMaster auto" in content and "ControlPersist 10m" in content
        assert "stdin" not in kwargs and "stderr" not in kwargs  # native interactive authentication
        command = shlex.split(argv[-1])
        assert command[0] == profile["farm_wrapper"]
        if "resolve" in command:
            value = access_farm.access.resolve("cluster/study-a", role="interactive")
        elif "verify" in command:
            assert command[command.index("--expected-record") + 1] == published["record_sha256"]
            value = access_farm.access.verify(access_farm.paths, "cluster/study-a",
                                              expected_record=published["record_sha256"], role="interactive")
        else:
            assert command[1] == "connect-attach" and "-t" in argv
            options = dict(zip(command[2::2], command[3::2]))
            assert options == {"--registry": profile["registry"], "--target": "cluster/study-a", "--role": "interactive",
                               "--expected-record": published["record_sha256"], "--farm-root": profile["farm_root"],
                               "--farm-id": profile["farm_id"], "--expected-host": "node-a.example"}
            return subprocess.CompletedProcess(argv, 0)
        return subprocess.CompletedProcess(argv, 0, json.dumps(value))
    assert client.connect(profile, "cluster/study-a", run=run) == 0
    assert len(calls) == 3
    assert calls[0][0][-2] == "user@login.example"
    assert calls[0][0][calls[0][0].index("-J") + 1] == "jump.example"
    assert all(call[0][-2] == "node-a.example" for call in calls[1:])
    assert calls[1][0][calls[1][0].index("-J") + 1] == "jump.example,user@login.example"
    assert all(not config.exists() for config in configurations)


def test_ssh_include_path_is_quoted_and_strict_settings_precede_it(endpoint, tmp_path):
    _, profile, _ = endpoint
    include = tmp_path / "ssh settings" / "config"
    profile = {**profile, "ssh_config": str(include)}
    with client.ssh_configuration(profile) as config:
        content = Path(config).read_text()
        assert f'Include "{include}"' in content
        assert content.index("StrictHostKeyChecking yes") < content.index("Include")


@pytest.mark.skipif(shutil.which("ssh") is None, reason="native SSH is not installed")
def test_native_ssh_parses_strict_jump_and_control_configuration_without_connecting(endpoint, tmp_path):
    _, profile, _ = endpoint
    included = tmp_path / "ssh settings"
    included.write_text("Host *\n  StrictHostKeyChecking no\n  BatchMode yes\n  User compute-user\n")
    profile = {**profile, "ssh_config": str(included)}
    with client.ssh_configuration(profile) as config:
        argv = client.ssh_argv(profile, config, "node-a.example", ["/shared/farm", "access", "verify"], compute=True)
        parsed = subprocess.run([argv[0], "-G", *argv[1:]], capture_output=True, text=True, timeout=10)
    assert parsed.returncode == 0, parsed.stderr
    settings = dict(line.split(" ", 1) for line in parsed.stdout.splitlines() if " " in line)
    assert settings["stricthostkeychecking"] in {"yes", "true"}
    assert settings["batchmode"] in {"no", "false"}
    assert settings["user"] == "compute-user"
    assert settings["proxyjump"] == "jump.example,user@login.example"
    assert settings["controlmaster"] == "auto"
    assert settings["controlpersist"] == "600"


@pytest.mark.parametrize("mutation", ["digest", "farm", "role", "host", "argv", "state", "duplicate"])
def test_untrusted_verification_never_reaches_attach(endpoint, mutation):
    access_farm, profile, published = endpoint
    verified = deepcopy(published)
    if mutation == "digest":
        verified["record_sha256"] = "a" * 64
    elif mutation == "farm":
        verified["record"]["farm_id"] = "other"
    elif mutation == "role":
        verified["record"]["endpoint_role"]["kind"] = "daemon"
    elif mutation == "host":
        verified["attachment"]["host"] = "other.example"
    elif mutation == "argv":
        verified["attachment"]["argv"] = ["tmux", "attach-session"]
    elif mutation == "state":
        verified["state"] = "RESOLVED"
    responses = [json.dumps(access_farm.access.resolve("cluster/study-a", role="interactive")), json.dumps(verified)]
    if mutation == "duplicate":
        responses[1] = '{"state":"VERIFIED", "state":"RESOLVED"}'
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        assert len(calls) <= 2, "invalid verification must not attach"
        return subprocess.CompletedProcess(argv, 0, responses[len(calls) - 1])
    with pytest.raises(AccessError):
        client.connect(profile, "cluster/study-a", run=run)
    assert len(calls) == 2


def _remote_args(profile, published):
    return Namespace(registry=profile["registry"], target="cluster/study-a", role="interactive",
                     farm_root=profile["farm_root"], farm_id=profile["farm_id"],
                     expected_record=published["record_sha256"], expected_host="node-a.example")


def test_remote_attach_reuses_real_access_verification_and_exact_native_argv(endpoint):
    access_farm, profile, published = endpoint
    executed = []
    assert client.remote_attach(_remote_args(profile, published), access=access_farm.access,
                                 execute=lambda executable, argv: executed.append((executable, argv))) == 0
    assert executed == [("tmux", published["attachment"]["argv"])]


def test_record_rotated_after_client_verification_is_not_attached(endpoint):
    access_farm, profile, published = endpoint
    access_farm.publish(generation="master-2", expected_current=published["record_sha256"], role="interactive", role_pid=998)
    with pytest.raises(AccessError, match="no longer current"):
        client.remote_attach(_remote_args(profile, published), access=access_farm.access,
                             execute=lambda *_: pytest.fail("stale record attached"))


@pytest.mark.parametrize("field,value", [("farm_id", "other"), ("expected_host", "node-b.example"), ("role", "daemon")])
def test_remote_attach_rechecks_explicit_identity_and_role(endpoint, field, value):
    access_farm, profile, published = endpoint
    args = _remote_args(profile, published)
    setattr(args, field, value)
    with pytest.raises(AccessError):
        client.remote_attach(args, access=access_farm.access, execute=lambda *_: pytest.fail("mismatched endpoint attached"))


def test_ssh_failure_does_not_retry_an_older_candidate_or_echo_output(endpoint):
    _, profile, _ = endpoint
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 255, "secret-output")
    with pytest.raises(AccessError) as exc:
        client.connect(profile, "cluster/study-a", run=run)
    assert len(calls) == 1 and "secret-output" not in str(exc.value)


def test_remote_refusal_explains_state_without_echoing_remote_details():
    refusal = subprocess.CompletedProcess([], 1, json.dumps({"state": "STALE_REGISTRY", "detail": "secret-output"}))
    with pytest.raises(AccessError, match="Endpoint changed") as exc:
        client._read_response(refusal, "VERIFIED")
    assert "secret-output" not in str(exc.value)
