"""Access publication faults use synthetic allocations/control sessions, never a live farm."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from threading import Event

import pytest

import agent_farm_runtime.access as access_module
from agent_farm_runtime.access import Access
from agent_farm_runtime.access.contract import AccessError, digest, markers, require
from agent_farm_runtime.access.observations import Observations
from agent_farm_runtime.access.registry import Registry
from agent_farm_runtime import provenance
from agent_farm_runtime.models import Task
from agent_farm_runtime.store import FarmPaths, TaskStore


def write(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


class FakeObservations(Observations):
    def __init__(self, socket):
        self.socket = str(socket)
        self.jobs = {"123": {"job_id": "123", "state": "RUNNING", "nodes": ["node-a"],
                             "owner_uid": os.getuid(), "allocation_started_at": "2026-01-01T00:00:00"}}
        self.sessions = {"master": "$1", "master-other": "$2", "workers": "$3"}
        self.windows = {"main": "@1", "logs": "@2", "0": "@1"}
        self.env = {}
        self.calls = []

    def scheduler(self, job_id):
        self.calls.append(("scheduler", job_id))
        job = self.jobs[job_id]
        if isinstance(job, Exception):
            raise job
        return deepcopy(job)

    def control(self, socket, session, window):
        self.calls.append(("control", socket, session, window))
        require(socket == self.socket, "SESSION_MISSING", "socket_missing", "missing socket")
        require(session in self.sessions, "SESSION_MISSING", "session_missing", "missing session")
        require(window is None or window in self.windows, "SESSION_MISSING", "window_missing", "missing window")
        return {"socket": socket, "socket_device": 1, "socket_inode": 2,
                "session": session, "session_id": self.sessions[session], "default_window": window,
                "window_id": self.windows.get(window), "server_pid": 321, "server_starttime": 42,
                "boot_id": "boot-a"}

    def environment(self, record):
        return self.env.get(record["control"]["session_id"], {}).copy()

    def command(self, argv, **kwargs):
        assert argv[:3] == ["tmux", "-S", self.socket]
        assert argv[3:5] == ["set-environment", "-t"]
        self.calls.append(tuple(argv))
        self.env.setdefault(argv[5], {})[argv[6]] = argv[7]
        return ""

    def default_socket(self):
        return self.socket


class Farm:
    def __init__(self, tmp_path):
        self.paths = FarmPaths(tmp_path / "project" / ".farm")
        self.paths.ensure()
        TaskStore(self.paths).create(Task("T1", "a synthetic task", "result", "review"))
        self.now = datetime.now(timezone.utc)
        self.identity = {**provenance.runtime_identity(), "host": "node-a.example", "boot_id": "boot-a", "pid_namespace": "pid:[1]"}
        self.manifest = {**self.identity, **provenance.farm_identity(str(self.paths.root.resolve())),
                         "execution_epoch": "initial", "pid": 222, "pid_starttime": 33,
                         "started_at": (self.now - timedelta(seconds=2)).isoformat(), "loop": True, "interval": 30,
                         "executor": "codex-tmux", "session": "workers", "tmux_socket": "worker-socket"}
        self.manifest["scheduler_attestation"] = {
            "kind": "slurm", "job_id": "123", "scheduler_node": "node-a",
            "allocation_started_at": "2026-01-01T00:00:00", "owner_uid": os.getuid(),
            "runtime_host": self.identity["host"], "execution_epoch": "initial"}
        self.save()
        self.tick()
        self.registry = Registry(tmp_path / "access")
        self.registry.initialize(attest_shared_storage=True)
        self.obs = FakeObservations(tmp_path / "control.sock")
        self.access = Access(self.registry, observations=self.obs, identity=lambda: self.identity,
                             alive=lambda _: True, clock=lambda: self.now)

    def save(self):
        write(self.paths.runtime / "deployment.json", self.manifest)

    def tick(self):
        write(self.paths.runtime / "last_tick.json", {"ts": self.now.isoformat(), "counts": {},
              "deployment": provenance.deployment_stamp(self.manifest)})

    def publish(self, **kwargs):
        options = {"job_id": "123", "control_session": "master", "default_window": "main", **kwargs}
        return self.access.publish(self.paths, "cluster/study-a", **options)

    @property
    def directory(self):
        return self.registry.target_dir("cluster/study-a")

    def claim(self):
        self.manifest.update(execution_epoch="move-2", host="node-b.example", pid=223,
                             handoff={"phase": "claimed", "id": "move-2", "target_host": "node-b.example"})
        self.identity["host"] = "node-b.example"
        self.obs.jobs["124"] = {**self.obs.jobs["123"], "job_id": "124", "nodes": ["node-b"]}
        self.manifest["scheduler_attestation"] = {
            **self.manifest["scheduler_attestation"], "job_id": "124", "scheduler_node": "node-b",
            "execution_epoch": "move-2", "runtime_host": "node-b.example"}
        self.save()


@pytest.fixture
def farm(tmp_path, monkeypatch):
    # Model a provisioned shared volume even when pytest's own fixture root is /tmp.
    # Production volatile-path rejection has separate tests using the real check.
    monkeypatch.setattr("agent_farm_runtime.access.registry.persistent_path", lambda p: p.resolve())
    monkeypatch.setattr(access_module, "persistent_path", lambda p: p.resolve())
    return Farm(tmp_path)


def fails(state, call, reason=None):
    with pytest.raises(AccessError) as caught:
        call()
    assert caught.value.state == state
    if reason:
        assert caught.value.reason == reason


def contents(root):
    return {str(p.relative_to(root)): (p.read_bytes(), p.stat().st_mtime_ns)
            for p in root.rglob("*") if p.is_file()}


def test_publish_resolve_verify_and_reads_have_no_mutations(farm):
    before = contents(farm.paths.root)
    published = farm.publish()
    assert published["state"] == "VERIFIED" and published["verified"] is True
    assert published["record"]["control"]["session"] == "master"
    assert published["record"]["control"]["socket"] != farm.manifest["tmux_socket"]
    assert farm.obs.env["$1"] == markers(published["record"])
    assert "$2" not in farm.obs.env and "$3" not in farm.obs.env
    assert contents(farm.paths.root) == before  # even task/manifest/tick files are untouched
    before = contents(farm.registry.root)
    calls = len(farm.obs.calls)
    resolved = farm.access.resolve("cluster/study-a")
    assert resolved["state"] == "RESOLVED" and not resolved["verified"] and "attachment" not in resolved
    assert len(farm.obs.calls) == calls  # resolution uses only shared records
    verified = farm.access.verify(farm.paths, "cluster/study-a", expected_record=resolved["record_sha256"])
    assert verified["attachment"]["argv"] == ["tmux", "-S", farm.obs.socket, "if-shell", "-F", "-t",
                                            "$1:@1", "1", "attach-session -t '$1:@1'"]
    assert contents(farm.registry.root) == before


def test_readers_never_create_a_missing_target(farm):
    before = contents(farm.registry.root)
    fails("UNPUBLISHED", lambda: farm.access.resolve("cluster/study-b"))
    fails("UNPUBLISHED", lambda: farm.access.verify(farm.paths, "cluster/study-b"))
    assert contents(farm.registry.root) == before
    assert not (farm.registry.root / "targets").exists()


@pytest.mark.parametrize("which", ["socket", "session", "window"])
def test_missing_control_endpoint_cannot_publish(farm, which):
    opts = {"socket": {"control_socket": "/missing/socket"},
            "session": {"control_session": "missing"}, "window": {"default_window": "missing"}}[which]
    fails("SESSION_MISSING", lambda: farm.publish(**opts), which + "_missing")
    assert not (farm.directory / "current.json").exists() and not farm.obs.env


@pytest.mark.parametrize("phase", ["draining", "released"])
def test_handoff_withholds_publication_and_resolution(farm, phase):
    farm.publish()
    before = contents(farm.registry.root)
    farm.manifest["handoff"] = {"id": "move-2", "phase": phase, "target_host": "node-b.example"}
    farm.save()
    for action in (farm.publish, lambda: farm.access.resolve("cluster/study-a"),
                   lambda: farm.access.verify(farm.paths, "cluster/study-a")):
        fails("PENDING", action, "handoff_" + phase)
    assert contents(farm.registry.root) == before


@pytest.mark.parametrize("kind", ["audit", "recovery", "transaction"])
def test_pending_audit_and_transactions_fail_closed(farm, kind):
    if kind == "audit":
        farm.manifest["pending_event"] = {"id": "incomplete"}
        farm.save()
    else:
        name = "pending-recovery.json" if kind == "recovery" else "pending-task-commit.json"
        write(farm.paths.runtime / name, {})
    fails("PENDING", farm.publish)
    assert not (farm.directory / "current.json").exists()


@pytest.mark.parametrize("field,new,state", [
    ("host", "node-b.example", "HOST_MISMATCH"),
    ("source_sha256", "b" * 64, "STALE_REGISTRY"),
    ("protocol_version", 999, "STALE_REGISTRY"),
])
def test_publication_requires_exact_deployment_host_source_protocol(farm, field, new, state):
    farm.manifest[field] = new
    farm.save()
    fails(state, farm.publish)
    assert not farm.obs.calls


def test_publication_expected_epoch_is_a_precondition(farm):
    fails("EPOCH_MISMATCH", lambda: farm.publish(expected_epoch="obsolete"))
    assert not farm.obs.calls


@pytest.mark.parametrize("state,expected", [("PENDING", "PENDING"), ("CONFIGURING", "PENDING"),
    ("COMPLETED", "JOB_EXPIRED"), ("CANCELLED", "JOB_EXPIRED"), ("TIMEOUT", "JOB_EXPIRED"),
    (None, "UNREACHABLE"), ("UNKNOWN", "UNREACHABLE")])
def test_allocation_must_be_positively_running(farm, state, expected):
    farm.obs.jobs["123"]["state"] = state
    fails(expected, farm.publish)
    assert not farm.obs.env and not (farm.directory / "current.json").exists()


def test_running_job_on_other_node_or_owned_by_other_user_is_rejected(farm):
    farm.obs.jobs["123"]["nodes"] = ["node-z.example"]
    fails("HOST_MISMATCH", farm.publish)
    farm.obs.jobs["123"]["nodes"] = ["node-a"]
    farm.obs.jobs["123"]["owner_uid"] += 1
    fails("AUTH_REQUIRED", farm.publish)


def test_claim_needs_new_tick_running_job_and_new_control_session(farm):
    original = farm.publish()["record"]
    old_pointer = (farm.directory / "current.json").read_bytes()
    farm.claim()
    # Merely claiming, with a fresh-looking old tick, is not reconciler readiness.
    fails("PENDING", lambda: farm.publish(job_id="124", control_session="master-other"), "readiness_generation")
    farm.tick()
    farm.obs.jobs["124"]["state"] = "PENDING"
    fails("PENDING", lambda: farm.publish(job_id="124", control_session="master-other"))
    assert (farm.directory / "current.json").read_bytes() == old_pointer
    fails("EPOCH_MISMATCH", lambda: farm.access.resolve("cluster/study-a"))
    farm.obs.jobs["124"]["state"] = "RUNNING"
    fails("CONFLICT", lambda: farm.publish(job_id="124"), "marker_conflict")
    new = farm.publish(job_id="124", control_session="master-other")
    assert new["record"]["execution_epoch"] == "move-2"
    assert json.loads((farm.directory / "epochs/initial.json").read_text()) == original
    assert farm.access.resolve("cluster/study-a")["record"] == new["record"]


def test_queued_or_running_target_before_claim_cannot_publish(farm):
    farm.publish()
    old = (farm.directory / "current.json").read_bytes()
    farm.identity["host"] = "node-b.example"
    farm.obs.jobs["124"] = {**farm.obs.jobs["123"], "job_id": "124", "nodes": ["node-b.example"]}
    for state in ("PENDING", "RUNNING"):
        farm.obs.jobs["124"]["state"] = state
        fails("HOST_MISMATCH", lambda: farm.publish(job_id="124", control_session="master-other"))
    assert (farm.directory / "current.json").read_bytes() == old


def test_unreachable_current_never_tries_old_generation(farm):
    farm.publish()
    farm.claim()
    farm.tick()
    farm.publish(job_id="124", control_session="master-other")
    farm.obs.calls.clear()
    farm.obs.jobs["124"] = AccessError("UNREACHABLE", "scheduler_timeout", "timeout")
    fails("UNREACHABLE", lambda: farm.access.verify(farm.paths, "cluster/study-a"))
    assert farm.obs.calls == [("scheduler", "124")]
    assert (farm.directory / "epochs/initial.json").exists()


@pytest.mark.parametrize("failure", ["before_pointer", "after_pointer", "after_instance"])
def test_crash_retry_is_idempotent_and_never_selects_an_orphan(farm, monkeypatch, failure):
    original_commit = farm.registry.commit
    original_prepare = farm.registry.prepare
    def crash_commit(directory, record):
        if failure == "after_pointer":
            original_commit(directory, record)
        raise OSError("injected crash")
    def crash_prepare(directory, record):
        original_prepare(directory, record)
        raise OSError("injected crash")
    with monkeypatch.context() as fault:
        fault.setattr(farm.registry, "prepare" if failure == "after_instance" else "commit",
                      crash_prepare if failure == "after_instance" else crash_commit)
        with pytest.raises(OSError, match="injected"):
            farm.publish()
    instance = farm.directory / "epochs/initial.json"
    before = instance.read_bytes(), instance.stat().st_mtime_ns
    if failure != "after_pointer":
        fails("UNPUBLISHED", lambda: farm.access.resolve("cluster/study-a"))
    farm.now += timedelta(seconds=1)
    first = farm.publish()
    second = farm.publish()
    assert first["record"] == second["record"]
    assert (instance.read_bytes(), instance.stat().st_mtime_ns) == before


def test_conflicting_same_epoch_fails_before_binding_another_session(farm):
    farm.publish()
    before = contents(farm.registry.root)
    fails("CONFLICT", lambda: farm.publish(control_session="master-other"), "epoch_publication")
    assert "$2" not in farm.obs.env
    assert contents(farm.registry.root) == before


def test_concurrent_publishers_have_one_immutable_result(farm):
    with ThreadPoolExecutor(max_workers=2) as pool:
        a, b = list(pool.map(lambda _: farm.publish(), range(2)))
    assert a["record"] == b["record"]
    assert len(list((farm.directory / "epochs").glob("*.json"))) == 1


def test_concurrent_conflicting_publishers_do_not_replace_winner(farm):
    def publish(session):
        try:
            return farm.publish(control_session=session)
        except AccessError as exc:
            return {"state": exc.state}
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(publish, ["master", "master-other"]))
    assert sorted(r["state"] for r in results) == ["CONFLICT", "VERIFIED"]
    winner = next(r for r in results if r["state"] == "VERIFIED")
    assert farm.access.resolve("cluster/study-a")["record"] == winner["record"]


def test_marker_mismatch_is_never_repaired_by_verify_or_publish(farm):
    farm.publish()
    farm.obs.env["$1"]["FARM_EXECUTION_EPOCH"] = "old"
    for action in (farm.publish, lambda: farm.access.verify(farm.paths, "cluster/study-a")):
        fails("CONFLICT", action)
        assert farm.obs.env["$1"]["FARM_EXECUTION_EPOCH"] == "old"


def test_partial_marker_binding_retries_without_current(farm, monkeypatch):
    original = farm.obs.command
    def crash(argv, **kw):
        original(argv, **kw)
        raise AccessError("UNREACHABLE", "command_timeout", "set outcome uncertain")
    with monkeypatch.context() as fault:
        fault.setattr(farm.obs, "command", crash)
        fails("UNREACHABLE", farm.publish)
    assert not (farm.directory / "current.json").exists()
    assert len(farm.obs.env["$1"]) == 1
    assert farm.publish()["state"] == "VERIFIED"


@pytest.mark.parametrize("fault", ["loop", "pid", "tick_identity", "tick_missing", "tick_old", "unknown", "dead"])
def test_daemon_readiness_is_an_exact_live_generation(farm, fault):
    if fault in {"loop", "pid"}:
        farm.manifest[fault] = None
        farm.save()
    elif fault == "tick_identity":
        farm.manifest["pid_starttime"] += 1
        farm.save()
    elif fault == "tick_missing":
        (farm.paths.runtime / "last_tick.json").unlink()
    elif fault == "tick_old":
        farm.now += timedelta(seconds=91)
    else:
        farm.access.alive = lambda _: None if fault == "unknown" else False
    fails("UNREACHABLE" if fault == "unknown" else "PENDING", farm.publish)
    assert not (farm.directory / "current.json").exists()


def test_same_name_recreated_session_or_reused_job_is_stale(farm):
    farm.publish()
    farm.obs.sessions["master"] = "$99"
    fails("STALE_REGISTRY", lambda: farm.access.verify(farm.paths, "cluster/study-a"), "control_replaced")
    farm.obs.sessions["master"] = "$1"
    farm.obs.jobs["123"]["allocation_started_at"] = "2026-02-02T00:00:00"
    fails("STALE_REGISTRY", lambda: farm.access.verify(farm.paths, "cluster/study-a"), "allocation_reused")


def test_expected_record_and_project_are_checked_before_live_probes(farm):
    farm.publish()
    farm.obs.calls.clear()
    fails("STALE_REGISTRY", lambda: farm.access.verify(farm.paths, "cluster/study-a", expected_record="a" * 64))
    fails("CONFLICT", lambda: farm.access.verify(FarmPaths(Path("/wrong/.farm")), "cluster/study-a"))
    assert not farm.obs.calls


@pytest.mark.parametrize("fault", ["digest", "epoch", "schema", "truncated", "duplicate_keys"])
def test_corrupt_pointer_never_scans_for_an_instance(farm, fault):
    farm.publish()
    pointer = farm.directory / "current.json"
    data = json.loads(pointer.read_text())
    if fault == "digest":
        data["record_sha256"] = "a" * 64
    elif fault == "epoch":
        data["execution_epoch"] = "absent"
    elif fault == "schema":
        data["schema_version"] = 999
    write(pointer, data)
    if fault == "truncated":
        pointer.write_text('{"schema_version":')
    if fault == "duplicate_keys":
        pointer.write_text('{"schema_version":1,"schema_version":1}')
    fails("STALE_REGISTRY" if fault in {"digest", "epoch"} else "CONFLICT",
          lambda: farm.access.resolve("cluster/study-a"))


def test_deployment_change_during_verification_is_rejected(farm, monkeypatch):
    farm.publish()
    original = farm.obs.environment
    def changed(record):
        farm.manifest["execution_epoch"] = "move-2"
        farm.save()
        return original(record)
    monkeypatch.setattr(farm.obs, "environment", changed)
    fails("PENDING", lambda: farm.access.verify(farm.paths, "cluster/study-a"), "deployment_changed")


def test_deleted_current_is_unpublished_even_if_old_generation_is_valid(farm):
    farm.publish()
    (farm.directory / "current.json").unlink()
    fails("UNPUBLISHED", lambda: farm.access.resolve("cluster/study-a"))
    assert (farm.directory / "epochs/initial.json").exists()


def test_publication_serializes_with_real_drain(farm, monkeypatch):
    from agent_farm_runtime.turnover import drain_farm
    monkeypatch.setattr("agent_farm_runtime.turnover.runtime_identity", lambda: farm.identity)
    entered, release, drain_entered = Event(), Event(), Event()
    original = farm.registry.commit

    def commit(directory, record):
        entered.set()
        assert release.wait(5)
        # The pending drain cannot have changed the manifest while publish holds its lock.
        assert "handoff" not in json.loads((farm.paths.runtime / "deployment.json").read_text())
        return original(directory, record)

    def drain():
        drain_entered.set()
        return drain_farm(farm.paths, request_id="move-2", target_host="node-b.example", actor="master")

    monkeypatch.setattr(farm.registry, "commit", commit)
    with ThreadPoolExecutor(max_workers=2) as pool:
        publishing = pool.submit(farm.publish)
        try:
            assert entered.wait(5)
            draining = pool.submit(drain)
            assert drain_entered.wait(5)
            assert not draining.done()
        finally:
            release.set()
        assert publishing.result()["state"] == "VERIFIED"
        assert draining.result()["phase"] == "draining"
    fails("PENDING", lambda: farm.access.resolve("cluster/study-a"), "handoff_draining")


def test_pointer_changed_during_verify_is_rejected(farm, monkeypatch):
    farm.publish()
    original = farm.obs.environment
    def changed(record):
        pointer = farm.directory / "current.json"
        value = json.loads(pointer.read_text())
        value["record_sha256"] = "b" * 64
        write(pointer, value)
        return original(record)
    monkeypatch.setattr(farm.obs, "environment", changed)
    fails("STALE_REGISTRY", lambda: farm.access.verify(farm.paths, "cluster/study-a"))


@pytest.mark.parametrize("field,new,state", [
    ("execution_epoch", "move-2", "EPOCH_MISMATCH"),
    ("host", "node-b.example", "HOST_MISMATCH"),
    ("source_sha256", "b" * 64, "STALE_REGISTRY"),
    ("protocol_version", 999, "STALE_REGISTRY"),
])
def test_existing_record_must_match_deployment_before_any_probe(farm, field, new, state):
    farm.publish()
    farm.manifest[field] = new
    farm.save()
    farm.obs.calls.clear()
    for action in (lambda: farm.access.resolve("cluster/study-a"),
                   lambda: farm.access.verify(farm.paths, "cluster/study-a")):
        fails(state, action)
    assert not farm.obs.calls


@pytest.mark.parametrize("fault", ["empty_audit", "malformed_handoff", "wrong_claim", "nonfinite_json"])
def test_malformed_deployment_cannot_publish(farm, fault):
    if fault == "empty_audit":
        farm.manifest["pending_event"] = {}
    elif fault == "malformed_handoff":
        farm.manifest["handoff"] = []
    elif fault == "wrong_claim":
        farm.manifest["handoff"] = {"phase": "claimed", "id": "other", "target_host": farm.identity["host"]}
    else:
        farm.manifest["interval"] = float("nan")
    farm.save()
    fails("PENDING" if fault == "empty_audit" else "CONFLICT", farm.publish)
    assert not farm.obs.calls


def test_stale_daemon_cannot_certify_the_new_manifest(farm):
    from agent_farm_runtime.status import write_last_tick
    old_daemon = deepcopy(farm.manifest)
    farm.claim()
    write_last_tick(farm.paths, {}, deployment=old_daemon)
    fails("PENDING", lambda: farm.publish(job_id="124", control_session="master-other"), "readiness_generation")


def test_read_only_cli_does_not_load_mutation_backend(farm, monkeypatch, capsys):
    from agent_farm_runtime.cli import build_parser
    farm.publish()
    before = contents(farm.paths.root), contents(farm.registry.root)
    monkeypatch.setattr("agent_farm_runtime.access.cli.Access", lambda _: farm.access)
    def forbidden():
        pytest.fail("read-only access must not request the write backend")
    monkeypatch.setattr("agent_farm_runtime.adapters.filesystem._implementation", forbidden)
    for action, state in (("resolve", "RESOLVED"), ("verify", "VERIFIED")):
        args = build_parser().parse_args(["--project", str(farm.paths.root.parent), "access", action,
            "--registry", str(farm.registry.root), "--target", "cluster/study-a", "--json"])
        assert args.func(args) == 0
        assert json.loads(capsys.readouterr().out)["state"] == state
    farm.obs.jobs["123"] = AccessError("UNREACHABLE", "timeout", "timeout")
    assert args.func(args) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["state"] == "UNREACHABLE" and not result["verified"] and "attachment" not in result
    assert (contents(farm.paths.root), contents(farm.registry.root)) == before


def test_fqdn_runtime_and_exact_short_scheduler_node_are_separate(farm):
    farm.obs.jobs["123"]["nodes"] = ["node-z", "node-a", "node-b"]
    record = farm.publish()["record"]
    assert record["runtime_host"] == "node-a.example"
    assert record["scheduler"]["scheduler_node"] == "node-a"
    assert farm.obs.env["$1"]["FARM_RUNTIME_HOST"] == "node-a.example"
    assert farm.obs.env["$1"]["FARM_SLURM_NODE"] == "node-a"
    assert farm.obs.env["$1"]["FARM_SLURM_START_TIME"] == record["scheduler"]["allocation_started_at"]
    assert farm.access.verify(farm.paths, "cluster/study-a")["state"] == "VERIFIED"
    farm.obs.jobs["123"]["nodes"] = ["node-a.example", "node-b"]
    fails("HOST_MISMATCH", lambda: farm.access.verify(farm.paths, "cluster/study-a"))


@pytest.mark.parametrize("attestation,state", [(None, "PENDING"), ({}, "CONFLICT"), ([], "CONFLICT")])
def test_publish_requires_scheduler_attestation(farm, attestation, state):
    farm.manifest["scheduler_attestation"] = attestation
    farm.save()
    fails(state, farm.publish)
    assert not farm.obs.calls and not farm.obs.env
    assert not (farm.directory / "current.json").exists()


@pytest.mark.parametrize("field,value,state", [
    ("execution_epoch", "old", "EPOCH_MISMATCH"),
    ("runtime_host", "node-other.example", "HOST_MISMATCH"),
    ("scheduler_node", ["node-a", "node-b"], "CONFLICT"),
    ("scheduler_node", "node-[a-b]", "CONFLICT"),
    ("scheduler_node", "node-a,node-b", "CONFLICT"),
    ("allocation_started_at", "Unknown", "CONFLICT"),
    ("owner_uid", True, "CONFLICT"),
])
def test_invalid_or_old_attestation_fails_before_live_queries(farm, field, value, state):
    farm.manifest["scheduler_attestation"][field] = value
    farm.save()
    fails(state, farm.publish)
    assert not farm.obs.calls


def test_publish_job_id_must_match_startup_even_if_other_job_is_running(farm):
    farm.obs.jobs["124"] = {**farm.obs.jobs["123"], "job_id": "124"}
    fails("CONFLICT", lambda: farm.publish(job_id="124"), "attested_job")
    assert not farm.obs.calls


def test_requeued_job_cannot_publish_against_old_startup(farm):
    farm.obs.jobs["123"]["allocation_started_at"] = "2026-02-01T00:00:00"
    fails("STALE_REGISTRY", farm.publish, "allocation_reused")
    assert not farm.obs.env


def test_attestation_is_part_of_daemon_readiness(farm):
    farm.manifest["scheduler_attestation"]["job_id"] = "124"
    farm.save()
    fails("PENDING", lambda: farm.publish(job_id="124"), "readiness_generation")
    assert not farm.obs.calls


@pytest.mark.parametrize("marker", ["FARM_RUNTIME_HOST", "FARM_SLURM_NODE", "FARM_SLURM_START_TIME"])
def test_scheduler_and_host_markers_are_required_and_never_repaired(farm, marker):
    farm.publish()
    farm.obs.env["$1"][marker] = "wrong"
    fails("CONFLICT", lambda: farm.access.verify(farm.paths, "cluster/study-a"), "marker_mismatch")
    fails("CONFLICT", farm.publish, "marker_conflict")
    assert farm.obs.env["$1"][marker] == "wrong"


def test_schema_one_requires_exact_scheduler_node(farm):
    from agent_farm_runtime.access.contract import validate_record
    record = farm.publish()["record"]
    del record["scheduler"]["scheduler_node"]
    fails("CONFLICT", lambda: validate_record(record, record["target"], record["execution_epoch"]), "scheduler")


def test_target_cannot_move_to_another_farm_but_distinct_targets_can_coexist(farm, tmp_path):
    first = farm.publish()["record"]
    other = Farm(tmp_path / "another-farm")
    other.access.registry = farm.registry
    before = contents(farm.registry.root)
    fails("CONFLICT", other.publish, "target_binding")
    assert contents(farm.registry.root) == before and not other.obs.env
    second = other.access.publish(other.paths, "cluster/another-study", job_id="123", control_session="master")
    assert second["record"]["farm_id"] != first["farm_id"]
    assert farm.access.resolve("cluster/study-a")["record"] == first
