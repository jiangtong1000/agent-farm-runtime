from __future__ import annotations

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier

import pytest

from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.events import EventLog
from agent_farm_runtime.master import record_decision
from agent_farm_runtime.adapters.prompts import worker_contract
from agent_farm_runtime.models import Lease, Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.observers import evaluate_waiting_on, make_unblock, waiting_on_error
from agent_farm_runtime.provenance import require_compatible_writer, runtime_identity
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.shadow import slurm_state
from agent_farm_runtime.store import FarmPaths, StoreConflict, StoreError, TaskStore, atomic_write_json


def setup_store(tmp_path, state=TaskState.SUBMITTED):
    store = TaskStore(FarmPaths(tmp_path / ".farm"))
    store.create(Task("T1", "objective", "deliverable", "original gate", state=state))
    return store


def evidence(tmp_path, name="decision.md", text="Independent check: inputs and output hashes; gates pass."):
    path = tmp_path / name
    path.write_text(text)
    return str(path)


def decide(store, tmp_path, action, **kwargs):
    return record_decision(store, "T1", expected_revision=store.get("T1").metadata["revision"],
                           action=action, actor="master-test", evidence_file=evidence(tmp_path), **kwargs)


def events(store):
    return [json.loads(line) for line in (store.paths.events / "log.ndjson").read_text().splitlines()]


def test_accept_requires_evidence_state_and_revision(tmp_path):
    store = setup_store(tmp_path)
    with pytest.raises(StoreError):
        decide(store, tmp_path, "accept", outcome="")
    accepted = decide(store, tmp_path, "accept", outcome="REPORT_ONLY")
    assert accepted.state is TaskState.DONE
    record = accepted.metadata["acceptance_receipt"]
    assert record["outcome"] == "REPORT_ONLY"
    assert record["effective_acceptance"] == "original gate"
    assert len(record["sha256"]) == 64
    assert record["actor"] == "master-test"
    assert [event["type"] for event in events(store)] == ["TASK_CREATED", "ACCEPTED"]
    with pytest.raises(StoreConflict):
        record_decision(store, "T1", expected_revision=1, action="accept", actor="master-test",
                        evidence_file=evidence(tmp_path), outcome="PASS")
    with pytest.raises(StoreError):
        decide(store, tmp_path, "accept", outcome="PASS")
    assert len(events(store)) == 2


def test_amend_preserves_previous_contract_and_accepts_effective_revision(tmp_path):
    store = setup_store(tmp_path)
    replacement = evidence(tmp_path, "acceptance.md", "Owner-approved report-only scope; numerical gates unchanged.")
    amended = decide(store, tmp_path, "amend", acceptance_file=replacement)
    assert amended.metadata["contract_revisions"][0]["previous_acceptance"] == "original gate"
    accepted = decide(store, tmp_path, "accept", outcome="PARTIAL")
    assert accepted.metadata["acceptance_receipt"]["contract_revision"] == 1
    assert accepted.metadata["acceptance_receipt"]["effective_acceptance"] == amended.acceptance
    assert "Owner-approved report-only scope" in worker_contract(accepted)


def test_running_contract_cannot_be_changed(tmp_path):
    store = setup_store(tmp_path, TaskState.RUNNING)
    with pytest.raises(StoreError, match="safe boundary"):
        decide(store, tmp_path, "amend", acceptance_file=evidence(tmp_path, "new.md"))


def test_ruling_requests_resume_without_manually_flipping_state(tmp_path):
    store = setup_store(tmp_path, TaskState.READY)
    executor = FakeExecutor()
    rec = Reconciler(store.paths, executor, unblock=make_unblock(store.paths))
    rec.reconcile_once()
    lease = store.get("T1").lease
    executor.set_receipt(Receipt(lease.worker_id, "T1", lease.lease_id,
                                 ReceiptStatus.AWAITING, ts="t", waiting_on="ruling:owner"))
    rec.reconcile_once()
    executor.clear_receipt(lease.worker_id)
    executor.kill(lease.worker_id)  # AWAITING is followed by process exit
    requested = decide(store, tmp_path, "ruling")
    assert requested.state is TaskState.WAITING
    assert requested.lease == lease
    assert rec.reconcile_once().resumed == ["T1"]
    resumed = store.get("T1")
    assert resumed.state is TaskState.RUNNING
    assert resumed.metadata["resume_requested"] is None
    assert "not evidence of success or acceptance" in worker_contract(resumed)
    assert "Independent check" in worker_contract(resumed)


def test_rework_leaves_ownership_to_reconciler(tmp_path):
    store = setup_store(tmp_path)
    requested = decide(store, tmp_path, "rework")
    assert requested.state is TaskState.SUBMITTED and requested.lease is None
    with pytest.raises(StoreError):
        decide(store, tmp_path, "accept", outcome="PASS")
    rec = Reconciler(store.paths, FakeExecutor())
    assert rec.reconcile_once().launched == ["T1"]
    running = store.get("T1")
    assert running.state is TaskState.RUNNING and running.lease is not None
    assert running.metadata["rework_requested"] is None
    assert "REWORK_STARTED" in [event["type"] for event in events(store)]


def test_two_writers_cannot_overwrite_same_snapshot(tmp_path):
    store = setup_store(tmp_path)
    original = store.get("T1")
    barrier = Barrier(2)

    def writer(label):
        barrier.wait(timeout=5)
        try:
            store.commit(replace(original, metadata={**original.metadata, "winner": label}),
                         expected=original, actor=label, event_type="TEST")
            return "ok"
        except StoreConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(writer, ["a", "b"])) == ["conflict", "ok"]
    assert store.get("T1").metadata["revision"] == 2
    assert len(events(store)) == 2


@pytest.mark.parametrize("failure", ["before_state", "before_event", "after_event"])
def test_interrupted_commit_recovers_exactly_once(tmp_path, monkeypatch, failure):
    import agent_farm_runtime.store as module
    store = setup_store(tmp_path)
    before = store.get("T1")
    after = replace(before, metadata={**before.metadata, "answer": 42})
    with monkeypatch.context() as patch:
        if failure == "before_state":
            original = module.atomic_write_json

            def broken(path, data):
                if path == store.path_for("T1"):
                    raise OSError("injected crash before task save")
                return original(path, data)

            patch.setattr(module, "atomic_write_json", broken)
        elif failure == "before_event":
            def broken(self, event):
                raise OSError("injected crash after task save")
            patch.setattr(EventLog, "append", broken)
        else:
            original = Path.unlink

            def broken(path, *args, **kwargs):
                if path.name == "pending-task-commit.json":
                    raise OSError("injected crash after audit append")
                return original(path, *args, **kwargs)

            patch.setattr(Path, "unlink", broken)
        with pytest.raises(OSError):
            store.commit(after, expected=before, actor="test", event_type="TEST")
    assert (store.paths.runtime / "pending-task-commit.json").exists()
    store.recover()
    store.recover()
    assert store.get("T1").metadata["answer"] == 42
    assert store.get("T1").metadata["revision"] == 2
    assert [event["type"] for event in events(store)] == ["TASK_CREATED", "TEST"]


def test_workspace_alias_is_reserved_through_submission(tmp_path):
    workspace = tmp_path / "ws"
    workspace.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(workspace, target_is_directory=True)
    store = TaskStore(FarmPaths(tmp_path / ".farm"))
    store.create(Task("A", "o", "d", "a", state=TaskState.SUBMITTED, metadata={"workspace": str(workspace)}))
    with pytest.raises(StoreConflict, match="reserved by A"):
        store.create(Task("B", "o", "d", "a", metadata={"workspace": str(alias)}))


def test_legacy_duplicate_ready_tasks_never_launch(tmp_path):
    store = TaskStore(FarmPaths(tmp_path / ".farm"))
    for name in ("A", "B"):
        task = Task(name, "o", "d", "a", metadata={"workspace": str(tmp_path / "ws")})
        atomic_write_json(store.path_for(name), task.to_dict())
    report = Reconciler(store.paths, FakeExecutor()).reconcile_once()
    assert report.launched == []
    assert len(report.conflicts) == 2
    assert any(c.level == "FAIL" and "shared" in c.message for c in run_doctor(store.paths))


@pytest.mark.parametrize("wait", ["job:123:strat", "job:123:done", "job:", "job:--help",
                                  "job:12,13", "artifact:relative", "task:../escape", "ruling:"])
def test_invalid_wait_does_not_poll_or_wake(tmp_path, wait):
    def unexpected(_):
        pytest.fail("invalid job syntax must not query scheduler")
    assert waiting_on_error(wait)
    assert not evaluate_waiting_on(wait, store=TaskStore(FarmPaths(tmp_path / ".farm")), slurm=unexpected)


@pytest.mark.parametrize("state", [None, "", "UNKNOWN", "REQUEUED", "SPECIAL_EXIT", "PREEMPTED", "REVOKED"])
def test_unknown_scheduler_states_do_not_mean_ended(tmp_path, state):
    assert not evaluate_waiting_on("job:123:end", store=TaskStore(FarmPaths(tmp_path)), slurm=lambda _: state)


@pytest.mark.parametrize("response", ["error", "timeout", "multi"])
def test_slurm_failure_is_unknown(monkeypatch, response):
    monkeypatch.setattr("agent_farm_runtime.shadow.shutil.which", lambda _: "/bin/test")
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        assert kwargs["timeout"] == 10
        if args[0] == "sacct":  # accounting is healthy in every variant
            return subprocess.CompletedProcess(args, 0, "COMPLETED\n", "")
        if response == "timeout":
            raise subprocess.TimeoutExpired(args, 10)
        return subprocess.CompletedProcess(args, 1 if response == "error" else 0,
                                           "COMPLETED\n" if response == "error" else "COMPLETED\nRUNNING\n", "")

    monkeypatch.setattr("agent_farm_runtime.shadow.subprocess.run", run)
    if response == "multi":
        # several queue rows is a real observation: no single fact, sacct not consulted
        assert slurm_state("123") is None
        assert len(calls) == 1
    else:
        # squeue error/timeout is not an observation: sacct is consulted (D6) and answers
        assert slurm_state("123") == "COMPLETED"
        assert [c[0] for c in calls] == ["squeue", "sacct"]


def test_context_is_snapshotted_and_read_commands_are_read_only(tmp_path, capsys):
    parser = build_parser()
    missing = tmp_path / "missing-project"
    for command in ("status", "task-list", "doctor", "version"):
        args = parser.parse_args(["--project", str(missing), command])
        assert args.func(args) == 0
    assert not missing.exists()
    brief = evidence(tmp_path, "brief.md", "Execute the selected method.")
    method = evidence(tmp_path, "method.md", "Method version 1: planted sign control.")
    args = parser.parse_args(["--project", str(tmp_path), "task-create", "--id", "T1",
                              "--objective", "o", "--deliverable", "d", "--acceptance", "a",
                              "--workspace", str(tmp_path), "--brief-file", brief, "--context-file", method])
    assert args.func(args) == 0
    Path(method).write_text("Method version 2: changed after dispatch.")
    task = TaskStore(FarmPaths(tmp_path / ".farm")).get("T1")
    assert "Method version 1" in task.metadata["brief"]
    assert "Method version 2" not in task.metadata["brief"]
    assert task.metadata["context_manifest"][0]["path"] == method
    assert len(task.metadata["context_manifest"][0]["sha256"]) == 64


def test_provenance_drift_is_visible_without_mutation(tmp_path):
    store = setup_store(tmp_path)
    path = store.paths.runtime / "deployment.json"
    atomic_write_json(path, runtime_identity())
    assert any("source matches" in c.message for c in run_doctor(store.paths))
    data = runtime_identity()
    data["source_sha256"] = "old-source"
    atomic_write_json(path, data)
    assert any("restart required" in c.message for c in run_doctor(store.paths))


def test_legacy_or_mismatched_daemon_blocks_master_writes(tmp_path):
    store = setup_store(tmp_path)
    with pytest.raises(StoreError, match="unverified"):
        require_compatible_writer(store.paths, policy="pinned-host")
    manifest = store.paths.runtime / "deployment.json"
    data = runtime_identity()
    atomic_write_json(manifest, data)
    require_compatible_writer(store.paths)
    for key in ("source_sha256", "host", "protocol_version"):
        atomic_write_json(manifest, {**data, key: "wrong"})
        with pytest.raises(StoreError, match="mismatch"):
            require_compatible_writer(store.paths, policy="pinned-host")
    assert store.get("T1").metadata["revision"] == 1


def test_reconciler_cas_conflict_cannot_actuate(tmp_path):
    store = setup_store(tmp_path, TaskState.READY)
    stale = store.get("T1")
    newer = replace(stale, metadata={**stale.metadata, "owner_hold": "inspect"})
    store.commit(newer, expected=stale, actor="test", event_type="TEST")
    rec = Reconciler(store.paths, FakeExecutor())
    from agent_farm_runtime.reconciler import ReconcileReport
    report = ReconcileReport()
    with pytest.raises(StoreConflict):
        rec._start(stale, report)
    assert report.launched == []
    assert store.get("T1").state is TaskState.READY


def test_misaddressed_receipt_is_ignored_even_with_matching_lease(tmp_path):
    store = setup_store(tmp_path, TaskState.READY)
    executor = FakeExecutor()
    rec = Reconciler(store.paths, executor)
    rec.reconcile_once()
    lease = store.get("T1").lease
    executor.set_receipt(Receipt(lease.worker_id, "wrong-task", lease.lease_id,
                                 ReceiptStatus.SUBMITTED, ts="t"))
    assert rec.reconcile_once().ignored_stale == [lease.worker_id]
    assert store.get("T1").state is TaskState.RUNNING


@pytest.mark.parametrize("tail", ['{"id": "torn"}', '{"id":'])
def test_damaged_audit_fails_closed_before_state_change(tmp_path, tail):
    store = setup_store(tmp_path)
    log = store.paths.events / "log.ndjson"
    with log.open("a") as handle:
        handle.write(tail)
    before = store.get("T1")
    after = replace(before, metadata={**before.metadata, "answer": 42})
    with pytest.raises(ValueError):
        store.commit(after, expected=before, actor="test", event_type="TEST")
    assert store.get("T1").to_dict() == before.to_dict()
    assert (store.paths.runtime / "pending-task-commit.json").exists()


@pytest.mark.parametrize("problem", ["missing_workspace", "missing_brief", "relative_workspace"])
def test_codex_preflight_never_leases_or_actuates_bad_task(tmp_path, problem):
    from agent_farm_runtime.adapters.codex import CodexTmuxExecutor
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    metadata = {"workspace": str(workspace), "brief": "test"}
    if problem == "missing_workspace":
        metadata["workspace"] = str(tmp_path / "missing")
    elif problem == "missing_brief":
        metadata.pop("brief")
    else:
        metadata["workspace"] = "relative"
    store = TaskStore(FarmPaths(tmp_path / ".farm"))
    store.create(Task("T1", "o", "d", "a", metadata=metadata))

    def no_actuation(*args):
        pytest.fail("preflight failure must never call tmux")

    executor = CodexTmuxExecutor(store.paths.runtime, run=no_actuation)
    report = Reconciler(store.paths, executor).reconcile_once()
    assert report.launched == [] and len(report.preflight_failures) == 1
    task = store.get("T1")
    assert task.state is TaskState.READY and task.lease is None
    assert task.metadata["revision"] == 1


def test_bad_task_preflight_does_not_block_other_tasks(tmp_path):
    store = setup_store(tmp_path, TaskState.READY)
    store.create(Task("T2", "o", "d", "a"))

    class CheckingExecutor(FakeExecutor):
        def validate_task(self, task):
            if task.id == "T1":
                raise ValueError("missing input")

    report = Reconciler(store.paths, CheckingExecutor()).reconcile_once()
    assert report.launched == ["T2"]
    assert report.preflight_failures == ["T1: missing input"]


def test_prompt_limit_is_executor_policy_not_a_core_decision_constraint(tmp_path):
    store = TaskStore(FarmPaths(tmp_path / ".farm"))
    store.create(Task("T1", "o", "d", "a", state=TaskState.WAITING, lease=Lease("W1", "L1"),
                      metadata={"workspace": str(tmp_path), "brief": "b" * 60000,
                                "waiting_on": "ruling:hold"}))
    large = evidence(tmp_path, "large.md", "x" * 60000)
    updated = record_decision(store, "T1", expected_revision=1, action="ruling", actor="master-test",
                              evidence_file=large)
    assert updated.metadata["resume_requested"]
    from agent_farm_runtime.adapters.codex import CodexClusterConfig, CodexTmuxExecutor
    executor = CodexTmuxExecutor(store.paths.runtime)
    with pytest.raises(ValueError, match="backend limit"):
        executor.validate_task(updated)
    # A deployment with a different transport limit does not change core policy.
    executor.config = CodexClusterConfig(max_prompt_bytes=125000)
    executor.validate_task(updated)
