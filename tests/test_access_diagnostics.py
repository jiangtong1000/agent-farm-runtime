"""Diagnosis inventories only supplied endpoints and has no mutation operations."""
import json

import pytest

from test_access import farm, contents
from agent_farm_runtime.access.diagnostics import diagnose
from agent_farm_runtime.access.contract import farm_id
from agent_farm_runtime.doctor import run_doctor


class Inventory:
    def __init__(self, root, *, role="interactive", command="node"):
        self.root, self.role, self.current_command = root, role, command
        self.calls = []

    def socket_identity(self, socket):
        self.calls.append(("socket", socket))
        return (1, 2)

    def environment(self, record):
        return {"FARM_ID": farm_id(str(self.root.resolve())), "FARM_ENDPOINT_ROLE": self.role}

    def command(self, argv, **kwargs):
        self.calls.append(tuple(argv))
        if argv[3] == "list-sessions":
            return "$1|master\n"
        if argv[3] == "list-panes":
            return f"0|{self.current_command}\n"
        pytest.fail(f"unexpected command {argv}")


def test_doctor_reports_explicit_split_candidates_and_never_mutates(farm):
    probe = Inventory(farm.paths.root)
    before = contents(farm.paths.root)
    values = diagnose(farm.paths, sockets=("/private/a", "/private/b"), observations=probe)
    assert any(level == "FAIL" and "Multiple Master candidates" in message for level, message in values)
    assert sum("role=interactive" in message for _, message in values) == 2
    assert contents(farm.paths.root) == before
    assert all(call[0] == "socket" or call[3] in {"list-sessions", "list-panes"} for call in probe.calls)


def test_doctor_reports_idle_shell_and_explicit_repair(farm):
    probe = Inventory(farm.paths.root, command="bash")
    values = diagnose(farm.paths, sockets=("/private/a",), observations=probe)
    assert any(level == "FAIL" and "idle shells" in message and "access adopt" in message for level, message in values)


def test_doctor_daemon_only_and_invalid_role_values_are_safe(farm, monkeypatch):
    probe = Inventory(farm.paths.root, role="untyped", command="python")
    monkeypatch.setattr("agent_farm_runtime.access.diagnostics.observe_role", lambda *args: {})
    values = diagnose(farm.paths, sockets=("/private/a",), observations=probe)
    assert any(level == "FAIL" and "Daemon-only endpoint" in message for level, message in values)
    probe.role = "secret-role-value\nBAD"
    values = diagnose(farm.paths, sockets=("/private/a",), observations=probe)
    assert all("secret-role-value" not in message for _, message in values)
    assert any("invalid role marker" in message for _, message in values)


def test_doctor_does_not_choose_uninspected_servers(farm):
    probe = Inventory(farm.paths.root)
    values = diagnose(farm.paths, observations=probe)
    assert not probe.calls
    assert "not inspected" in values[0][1]


def test_doctor_reports_stale_exact_window_without_republishing(farm):
    farm.publish()
    before = contents(farm.registry.root), contents(farm.paths.root)
    del farm.obs.windows["main"]
    # Inventory unavailable is independent of the positive published-window failure.
    values = diagnose(farm.paths, registry_path=farm.registry.root, target="cluster/study-a", observations=farm.obs, access=farm.access)
    assert any(level == "FAIL" and "window_missing" in message for level, message in values)
    assert (contents(farm.registry.root), contents(farm.paths.root)) == before


@pytest.mark.parametrize("value", [None, [], {"handoff": []}])
def test_doctor_invalid_deployment_object_reports_failure(farm, value):
    (farm.paths.runtime / "deployment.json").write_text(json.dumps(value))
    assert any(check.level == "FAIL" and "deployment manifest" in check.message for check in run_doctor(farm.paths))
