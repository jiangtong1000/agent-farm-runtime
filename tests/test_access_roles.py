"""Adoption is explicit, role-preserving, and never creates a process/session."""
from copy import deepcopy

import pytest

from test_access import farm, fails, contents
from agent_farm_runtime.access.contract import markers
from agent_farm_runtime.access import roles


@pytest.fixture
def adopted(farm, monkeypatch):
    def observe(obs, control, kind, pid):
        return {"kind": kind, "pid": pid, "pid_starttime": 99,
                "pid_namespace": "pid:[1]", "pane_id": "%1"}
    monkeypatch.setattr("agent_farm_runtime.access.observe_role", observe)
    return farm


def test_adoption_keeps_original_session_and_selects_interactive_by_default(adopted):
    farm = adopted
    before = deepcopy(farm.obs.sessions), contents(farm.paths.root)
    interactive = farm.publish(default_window=None, generation="master-1", expected_current="none", role="interactive", role_pid=999)
    daemon = farm.publish(default_window=None, control_session="workers", generation="daemon-1", expected_current="none", role="daemon", role_pid=222)
    assert (deepcopy(farm.obs.sessions), contents(farm.paths.root)) == before
    assert farm.access.resolve("cluster/study-a")["record_sha256"] == interactive["record_sha256"]
    assert farm.access.resolve("cluster/study-a", role="daemon")["record_sha256"] == daemon["record_sha256"]
    resolved = farm.access.resolve("cluster/study-a", role="interactive")
    assert resolved["verification"]["role"] == "interactive"
    assert farm.access.verify(farm.paths, "cluster/study-a", role="interactive")["record"] == interactive["record"]
    assert farm.obs.env["$1"] == markers(interactive["record"])
    assert farm.obs.env["$3"] == markers(daemon["record"])
    assert all("new-session" not in call and "new-window" not in call for call in farm.obs.calls)


def test_daemon_only_target_is_never_default(adopted):
    adopted.publish(default_window=None, generation="daemon", expected_current="none", role="daemon", role_pid=222)
    fails("UNPUBLISHED", lambda: adopted.access.resolve("cluster/study-a"))
    fails("UNPUBLISHED", lambda: adopted.access.resolve("cluster/study-a", role="interactive"))
    assert adopted.access.resolve("cluster/study-a", role="daemon")["state"] == "RESOLVED"


def test_untyped_endpoint_cannot_satisfy_role_or_downgrade_adoption(adopted):
    old = adopted.publish(default_window=None)
    fails("CONFLICT", lambda: adopted.access.resolve("cluster/study-a", role="interactive"), "required_role")
    new = adopted.publish(default_window=None, generation="adopt", expected_current=old["record_sha256"], role="interactive", role_pid=999)
    fails("CONFLICT", lambda: adopted.publish(default_window=None, generation="downgrade", expected_current=new["record_sha256"]), "role_required")


def test_reconciler_cannot_be_adopted_as_master(adopted):
    fails("CONFLICT", lambda: adopted.publish(generation="bad", expected_current="none", role="interactive", role_pid=222), "daemon_only")
    assert not adopted.obs.env


def test_role_process_replacement_prevents_attachment(adopted, monkeypatch):
    adopted.publish(default_window=None, generation="master", expected_current="none", role="interactive", role_pid=999)
    def replaced(*args):
        return {"kind": "interactive", "pid": 999, "pid_starttime": 100,
                "pid_namespace": "pid:[1]", "pane_id": "%1"}
    monkeypatch.setattr("agent_farm_runtime.access.observe_role", replaced)
    fails("STALE_REGISTRY", lambda: adopted.access.verify(adopted.paths, "cluster/study-a"), "role_replaced")


def test_role_probe_uses_process_ancestry_and_accepts_node_not_names(monkeypatch):
    processes = {99: {"parent": 50, "starttime": 7, "shell": False},
                 50: {"parent": 1, "starttime": 3, "shell": True}}
    monkeypatch.setattr(roles, "process", lambda pid: processes[pid])
    monkeypatch.setattr(roles, "host_identity", lambda: {"pid_namespace": "pid:[1]"})
    class Probe:
        def command(self, argv, **kwargs):
            assert argv[3] == "list-panes"
            return "%1|50|0\n"
    role = roles.observe_role(Probe(), {"socket": "/private/socket", "session_id": "$1"}, "interactive", 99)
    assert role["pane_id"] == "%1" and role["pid_starttime"] == 7
    processes[99]["shell"] = True
    fails("CONFLICT", lambda: roles.observe_role(Probe(), {"socket": "/private/socket", "session_id": "$1"}, "interactive", 99), "idle_shell")


def test_role_probe_refuses_process_outside_selected_session(monkeypatch):
    monkeypatch.setattr(roles, "process", lambda pid: {"parent": 1, "starttime": 7, "shell": False})
    class Probe:
        def command(self, *args, **kwargs):
            return "%1|50|0\n"
    fails("CONFLICT", lambda: roles.observe_role(Probe(), {"socket": "/private/socket", "session_id": "$1"}, "interactive", 99), "role_membership")


@pytest.mark.parametrize("field,value", [("kind", []), ("kind", {}), ("pid", True),
                                       ("pid_starttime", -1), ("pid_namespace", []), ("pane_id", [])])
def test_malformed_role_fields_fail_closed(field, value):
    role = {"kind": "interactive", "pid": 99, "pid_starttime": 1, "pid_namespace": "pid:[1]", "pane_id": "%1"}
    role[field] = value
    fails("CONFLICT", lambda: roles.validate_role(role), "role_schema")
