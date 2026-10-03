"""Remote wire identities must validate independently of the client's OS."""
import json
from pathlib import Path

from agent_farm_runtime.access import Access
from agent_farm_runtime.access.contract import digest, farm_id
from agent_farm_runtime.connect import load_profile, ssh_configuration, validate_response


def record():
    root = "/shared/researcher/study/.farm"
    return {"schema_version": 2, "target": "site/study", "farm_id": farm_id(root), "farm_root": root,
            "execution_epoch": "initial", "runtime_host": "compute.example", "protocol_version": 4,
            "source_sha256": "a" * 64, "owner_uid": 1000, "published_at": "2026-01-01T00:00:00+00:00",
            "generation": "master-1", "previous_record_sha256": None,
            "scheduler": {"kind": "slurm", "job_id": "123", "scheduler_node": "compute",
                          "allocation_started_at": "2026-01-01T00:00:00"},
            "endpoint_role": {"kind": "interactive", "pid": 123, "pid_starttime": 456,
                              "pid_namespace": "pid:[1]", "pane_id": "%1"},
            "control": {"socket": "/run/user/1000/farm.sock", "socket_device": 1, "socket_inode": 2,
                        "session": "master", "session_id": "$1", "default_window": "main", "window_id": "@1",
                        "server_pid": 321, "server_starttime": 42, "boot_id": "boot-a"}}


def test_posix_remote_identity_validates_on_every_client_platform(tmp_path):
    candidate = record()
    config = tmp_path / "connect.toml"
    values = {"login_host": "login.example", "registry": "/shared/researcher/access",
              "farm_root": candidate["farm_root"], "farm_id": candidate["farm_id"],
              "farm_wrapper": "/shared/release/farm"}
    config.write_text('version = 1\n[targets."site/study"]\n' +
                      "\n".join(f"{key} = {json.dumps(value)}" for key, value in values.items()))
    profile = load_profile(config, "site/study")
    verified = Access.verified(candidate)
    assert validate_response(verified, profile, "site/study", "interactive",
                             expected_record=digest(candidate), verified=True) == candidate


def test_native_local_paths_are_rendered_as_ssh_configuration_paths(tmp_path):
    local_config = tmp_path / "settings with spaces" / "config"
    control = tmp_path / "sockets" / "%C"
    with ssh_configuration({"ssh_config": str(local_config), "control_path": str(control)}) as filename:
        content = Path(filename).read_text()
        assert f'Include "{local_config.as_posix()}"' in content
        assert f'ControlPath "{control.as_posix()}"' in content
