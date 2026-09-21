"""Launcher capture and epoch binding; every scheduler observation is synthetic."""
from copy import deepcopy
import json
import os

import pytest

from agent_farm_runtime.access.allocation import capture_allocation
from agent_farm_runtime.access.contract import AccessError
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.provenance import runtime_identity
from agent_farm_runtime.store import StoreError


ENV = {"SLURM_JOB_ID": "123", "SLURMD_NODENAME": "node-a"}
HOST = {"host": "node-a.example"}


class Slurm:
    def __init__(self):
        self.calls = []
        self.job = {"job_id": "123", "state": "RUNNING", "nodes": ["node-b", "node-a"],
                    "owner_uid": os.getuid(), "allocation_started_at": "2026-01-01T00:00:00"}

    def scheduler(self, job_id):
        self.calls.append(job_id)
        return deepcopy(self.job)


def capture(obs, env=ENV, previous=None, **kwargs):
    return capture_allocation(previous or {}, HOST, "initial", environ=env, observations=obs, **kwargs)


@pytest.mark.parametrize("env,explicit", [
    (ENV, {}),
    ({"SLURM_JOBID": "123", "SLURMD_NODENAME": "node-a"}, {}),
    ({**ENV, "SLURM_JOBID": "123"}, {}),
    ({}, {"job_id": "123", "node": "node-a"}),
    (ENV, {"job_id": "123", "node": "node-a"}),
])
def test_captures_launcher_identity_and_validates_exact_allocation(env, explicit):
    obs = Slurm()
    attestation = capture(obs, env, **explicit)
    assert attestation == {"kind": "slurm", "job_id": "123", "scheduler_node": "node-a",
                           "allocation_started_at": "2026-01-01T00:00:00", "owner_uid": os.getuid(),
                           "runtime_host": "node-a.example", "execution_epoch": "initial"}
    assert obs.calls == ["123"]
    assert capture(obs, env, previous={"scheduler_attestation": attestation}, **explicit) == attestation


@pytest.mark.parametrize("env,explicit", [
    ({"SLURM_JOB_ID": "123"}, {}),
    ({"SLURMD_NODENAME": "node-a"}, {}),
    ({"SLURM_JOB_ID": "123", "SLURM_JOB_NODELIST": "node-[a-b]"}, {}),
    ({**ENV, "SLURM_JOBID": "124"}, {}),
    ({**ENV, "SLURMD_NODENAME": "node-a,node-b"}, {}),
    ({**ENV, "SLURMD_NODENAME": ""}, {}),
    ({**ENV, "SLURM_JOB_ID": "123_4"}, {}),
    (ENV, {"job_id": "124", "node": "node-a"}),
    (ENV, {"job_id": "123", "node": "node-b"}),
    ({}, {"job_id": "123"}),
    ({}, {"node": "node-a"}),
])
def test_ambiguous_partial_or_conflicting_launcher_input_fails_without_query(env, explicit):
    obs = Slurm()
    with pytest.raises(AccessError) as caught:
        capture(obs, env, **explicit)
    assert caught.value.state == "CONFLICT"
    assert not obs.calls


@pytest.mark.parametrize("field,value,state", [
    ("job_id", "124", "CONFLICT"), ("nodes", ["node-b"], "HOST_MISMATCH"),
    ("nodes", ["node-a.example"], "HOST_MISMATCH"),
    ("state", "PENDING", "PENDING"), ("state", "COMPLETED", "JOB_EXPIRED"),
    ("state", "UNKNOWN", "UNREACHABLE"), ("owner_uid", -1, "AUTH_REQUIRED"),
    ("allocation_started_at", "Unknown", "UNREACHABLE"),
])
def test_capture_rejects_scheduler_contradictions(field, value, state):
    obs = Slurm()
    obs.job[field] = value
    with pytest.raises(AccessError) as caught:
        capture(obs)
    assert caught.value.state == state


def test_same_epoch_cannot_rebind_a_requeued_or_different_allocation():
    obs = Slurm()
    previous = {"scheduler_attestation": capture(obs)}
    obs.job["allocation_started_at"] = "2026-02-01T00:00:00"
    with pytest.raises(AccessError) as caught:
        capture(obs, previous=previous)
    assert caught.value.reason == "scheduler_epoch_binding"
    obs.job["job_id"] = "124"
    with pytest.raises(AccessError) as caught:
        capture(obs, {**ENV, "SLURM_JOB_ID": "124"}, previous=previous)
    assert caught.value.reason == "scheduler_epoch_binding"


def test_non_slurm_startup_cannot_reuse_a_prior_attestation():
    obs = Slurm()
    assert capture(obs, {}) is None and not obs.calls
    prior = capture(obs)
    with pytest.raises(AccessError) as caught:
        capture(obs, {}, previous={"scheduler_attestation": prior})
    assert caught.value.state == "PENDING"


@pytest.fixture
def startup(tmp_path, monkeypatch):
    for key in ("SLURM_JOB_ID", "SLURM_JOBID", "SLURMD_NODENAME"):
        monkeypatch.delenv(key, raising=False)
    identity = {**runtime_identity(), **HOST}
    monkeypatch.setattr("agent_farm_runtime.provenance.runtime_identity", lambda: identity)
    obs = Slurm()
    monkeypatch.setattr("agent_farm_runtime.access.allocation.Observations", lambda: obs)
    def run(*args):
        parsed = build_parser().parse_args(["--project", str(tmp_path), "reconcile", *args])
        return parsed.func(parsed)
    return run, obs, tmp_path / ".farm" / "runtime"


def test_cli_startup_persists_attestation_and_stamps_the_completed_tick(startup):
    run, obs, runtime = startup
    assert run("--slurm-job-id", "123", "--slurm-node", "node-a") == 0
    manifest = json.loads((runtime / "deployment.json").read_text())
    attestation = manifest["scheduler_attestation"]
    assert attestation == capture(Slurm())
    tick = json.loads((runtime / "last_tick.json").read_text())
    assert tick["deployment"]["scheduler_attestation"] == attestation
    before = (runtime / "deployment.json").read_bytes()
    obs.job["job_id"] = "124"
    with pytest.raises(StoreError, match="Allocation identity changed"):
        run("--slurm-job-id", "124", "--slurm-node", "node-a")
    assert (runtime / "deployment.json").read_bytes() == before


def test_cli_captures_standard_environment_without_extra_flags(startup, monkeypatch):
    run, _, runtime = startup
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    assert run() == 0
    assert json.loads((runtime / "deployment.json").read_text())["scheduler_attestation"] == capture(Slurm())


def test_startup_refuses_unreachable_scheduler_before_executor_creation(startup, monkeypatch):
    run, obs, runtime = startup
    def unavailable(_):
        raise AccessError("UNREACHABLE", "timeout", "Scheduler unavailable")
    monkeypatch.setattr(obs, "scheduler", unavailable)
    monkeypatch.setattr("agent_farm_runtime.cli._executor", lambda *args: pytest.fail("must not create executor"))
    with pytest.raises(StoreError, match="UNREACHABLE"):
        run("--slurm-job-id", "123", "--slurm-node", "node-a")
    assert not (runtime / "deployment.json").exists()


def test_claim_clears_old_allocation_and_startup_captures_new_one(tmp_path, monkeypatch):
    from test_turnover import make_farm, drain
    from agent_farm_runtime import turnover
    from agent_farm_runtime.store import atomic_write_json

    for key in ("SLURM_JOB_ID", "SLURM_JOBID", "SLURMD_NODENAME"):
        monkeypatch.delenv(key, raising=False)
    paths, _, ex, _, _, identity = make_farm(tmp_path)
    before = {**capture(Slurm()), "runtime_host": identity["host"]}
    manifest = {**turnover.deployment(paths), "execution_epoch": "initial", "scheduler_attestation": before}
    atomic_write_json(paths.runtime / "deployment.json", manifest)
    drain(paths)
    turnover.release_farm(paths, ex, request_id="node-1", actor="master")
    target = {**identity, "host": "target-host"}
    monkeypatch.setattr(turnover, "runtime_identity", lambda: target)
    monkeypatch.setattr("agent_farm_runtime.provenance.runtime_identity", lambda: target)
    turnover.claim_farm(paths, request_id="node-1", actor="master")
    assert turnover.deployment(paths)["scheduler_attestation"] is None
    assert turnover.deployment(paths)["execution_epoch"] == "node-1"
    obs = Slurm()
    obs.job.update(job_id="124", nodes=["node-new"], allocation_started_at="2026-02-01T00:00:00")
    monkeypatch.setattr("agent_farm_runtime.access.allocation.Observations", lambda: obs)
    args = build_parser().parse_args(["--project", str(tmp_path), "reconcile",
                                     "--slurm-job-id", "124", "--slurm-node", "node-new"])
    assert args.func(args) == 0
    after = turnover.deployment(paths)["scheduler_attestation"]
    assert after == {"kind": "slurm", "job_id": "124", "scheduler_node": "node-new",
                     "allocation_started_at": "2026-02-01T00:00:00", "owner_uid": os.getuid(),
                     "execution_epoch": "node-1", "runtime_host": "target-host"}
    archive = json.loads((paths.runtime / "handoffs" / "node-1.json").read_text())
    assert archive["deployment"]["scheduler_attestation"] == before


def test_offline_recovery_also_clears_old_allocation(tmp_path):
    from test_recovery import fixture, apply
    from agent_farm_runtime.store import atomic_write_json

    paths, _, note = fixture(tmp_path)
    path = paths.runtime / "deployment.json"
    before = json.loads(path.read_text())
    atomic_write_json(path, {**before, "scheduler_attestation": capture(Slurm())})
    result = apply(paths, note)
    after = json.loads(path.read_text())
    assert after["scheduler_attestation"] is None
    assert after["execution_epoch"] == result["recovery_id"]
