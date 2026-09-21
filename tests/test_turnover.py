"""Planned turnover canaries. No real farm, model, scheduler, or remote node."""
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import shlex
from threading import Event as Signal
from concurrent.futures import ThreadPoolExecutor

import pytest

from agent_farm_runtime import turnover
from agent_farm_runtime.adapters.codex import RECEIPT_HELPER
from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.adapters.prompts import worker_contract
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.events import EventLog
from agent_farm_runtime.master import record_decision
from agent_farm_runtime.models import Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.observers import make_unblock
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.recovery import recover_farm
from agent_farm_runtime.store import FarmPaths, StoreConflict, StoreError, TaskStore, atomic_write_json


def make_farm(tmp_path):
    paths = FarmPaths(tmp_path / ".farm")
    paths.ensure()
    identity = turnover.runtime_identity()
    atomic_write_json(paths.runtime / "deployment.json", {
        **identity, "executor": "local-process", "session": "farm2", "tmux_socket": None,
    })
    store = TaskStore(paths)
    store.create(Task("T1", "objective", "output", "check", metadata={"automatic_restarts": 2}))
    ex = FakeExecutor()
    rec = Reconciler(paths, ex, unblock=make_unblock(paths), grace_seconds=0)
    checkpoint = tmp_path / "checkpoint.md"
    checkpoint.write_text("Artifact hash abc; external job 123 untouched. Owner holds remain. Next: inspect output.")
    return paths, store, ex, rec, checkpoint, identity


def request(store, checkpoint=None, request_id="rotate-1"):
    return turnover.request_rotation(store, "T1", expected_revision=store.get("T1").metadata["revision"],
                                     request_id=request_id, actor="master", checkpoint_file=checkpoint)


def yield_worker(store, ex, checkpoint, *, rotation_id=None, waiting_on=None):
    lease = store.get("T1").lease
    ex.set_receipt(Receipt(lease.worker_id, "T1", lease.lease_id, ReceiptStatus.AWAITING, ts="t",
                           waiting_on=waiting_on, rotation_id=rotation_id, checkpoint=str(checkpoint)))
    return lease


def drain(paths):
    return turnover.drain_farm(paths, request_id="node-1", target_host="target-host", actor="master")


@pytest.mark.parametrize("alive", [True, None, False])
def test_rotation_needs_checkpoint_and_positive_exit(tmp_path, alive):
    paths, store, ex, rec, checkpoint, _ = make_farm(tmp_path)
    rec.reconcile_once()
    before = request(store)
    assert before.metadata.get("resume_requested") is None
    old = yield_worker(store, ex, checkpoint, rotation_id="rotate-1")
    rec.reconcile_once()  # receipt -> WAITING, still the old lease
    ex._alive[old.worker_id] = alive
    rec.reconcile_once()
    after = store.get("T1")
    if alive is not False:
        assert after.lease == old and len(ex.launched) == 1 and not ex.stopped
        return
    assert turnover.clean_surrender(after)
    assert after.metadata["automatic_restarts"] == 2
    checkpoint.write_text("later untrusted edit")
    rec.reconcile_once()
    fresh = store.get("T1")
    assert fresh.state is TaskState.RUNNING and fresh.lease != old
    assert fresh.metadata["automatic_restarts"] == 2
    assert len(ex.launched) == 2 and ex.resumed == [] and not ex.stopped
    assert "Artifact hash abc" in worker_contract(fresh)
    assert "later untrusted edit" not in worker_contract(fresh)
    assert request(store).to_dict() == fresh.to_dict()  # master retry after completed rotation
    assert not any(c.level == "FAIL" for c in run_doctor(paths))


@pytest.mark.parametrize("wait", ["ruling:owner-hold", "artifact:/does-not-exist/never", "job:123"])
def test_parked_rotation_preserves_exact_wait_without_waking(tmp_path, wait, monkeypatch):
    paths, store, ex, rec, checkpoint, _ = make_farm(tmp_path)
    rec.reconcile_once()
    lease = yield_worker(store, ex, checkpoint, waiting_on=wait)
    rec.reconcile_once()
    ex.kill(lease.worker_id)
    requested = request(store)  # reuse the previously snapshotted checkpoint
    rec.reconcile_once()
    after = store.get("T1")
    assert turnover.clean_surrender(after) and after.metadata["waiting_on"] == wait
    assert after.metadata.get("resume_requested") is None
    # Inject observer below its default callable to avoid any real scheduler query.
    monkeypatch.setattr("agent_farm_runtime.observers.evaluate_waiting_on", lambda *a, **k: False)
    rec.reconcile_once()
    assert len(ex.launched) == 1 and ex.resumed == []
    assert requested.metadata["rotation_request"]["requested_state"] == "WAITING"
    record_decision(store, "T1", expected_revision=after.metadata["revision"], action="ruling",
                    actor="master", evidence_file=str(checkpoint))
    rec.reconcile_once()
    assert store.get("T1").lease != lease and len(ex.launched) == 2


def test_missing_checkpoint_is_not_fabricated_or_woken(tmp_path):
    paths, store, ex, rec, checkpoint, _ = make_farm(tmp_path)
    rec.reconcile_once()
    lease = store.get("T1").lease
    ex.set_receipt(Receipt(lease.worker_id, "T1", lease.lease_id, ReceiptStatus.AWAITING,
                           ts="t", waiting_on="ruling:owner"))
    rec.reconcile_once()
    ex.kill(lease.worker_id)
    request(store)
    assert rec.reconcile_once().observation_errors
    assert store.get("T1").lease == lease
    request(store, str(checkpoint))  # explicit master checkpoint, CAS-bound, no ruling
    rec.reconcile_once()
    assert turnover.clean_surrender(store.get("T1")) and ex.resumed == []


def test_stale_request_receipt_and_cas_are_fenced(tmp_path):
    _, store, ex, rec, checkpoint, _ = make_farm(tmp_path)
    rec.reconcile_once()
    before = request(store)
    with pytest.raises(StoreConflict):
        turnover.request_rotation(store, "T1", expected_revision=0, request_id="rotate-1", actor="master")
    with pytest.raises(StoreConflict):
        request(store, request_id="different")
    yield_worker(store, ex, checkpoint, rotation_id="wrong-request")
    assert rec.reconcile_once().ignored_stale
    assert store.get("T1").to_dict() == before.to_dict()


def test_full_handoff_preserves_holds_and_new_host_uses_fresh_worker(tmp_path, monkeypatch):
    paths, store, ex, rec, checkpoint, identity = make_farm(tmp_path)
    rec.reconcile_once()
    old = yield_worker(store, ex, checkpoint, waiting_on="ruling:owner-hold")
    rec.reconcile_once()
    ex.kill(old.worker_id)
    drain(paths)
    rec.reconcile_once()  # automatic request + clean surrender of already WAITING worker
    sealed_task = store.get("T1").to_dict()
    sealed = turnover.release_farm(paths, ex, request_id="node-1", actor="master")
    assert sealed["phase"] == "released"
    with pytest.raises(StoreError):
        store.create(Task("T2", "o", "d", "a"))
    with pytest.raises(StoreError):
        rec.reconcile_once()
    with pytest.raises(StoreError):
        turnover.claim_farm(paths, request_id="node-1", actor="master")
    with monkeypatch.context() as target:
        target.setattr(turnover, "runtime_identity", lambda: {**identity, "host": "target-host"})
        claimed = turnover.claim_farm(paths, request_id="node-1", actor="next-master")
        assert claimed["phase"] == "claimed"
        assert store.get("T1").to_dict() == sealed_task
        assert turnover.claim_farm(paths, request_id="node-1", actor="next-master") == claimed
        new_ex = FakeExecutor()
        new_rec = Reconciler(paths, new_ex, unblock=make_unblock(paths))
        new_rec.reconcile_once()
        assert not new_ex.launched
        task = store.get("T1")
        record_decision(store, "T1", expected_revision=task.metadata["revision"], action="ruling",
                        actor="next-master", evidence_file=str(checkpoint))
        new_rec.reconcile_once()
        assert store.get("T1").lease != old and len(new_ex.launched) == 1
        assert store.get("T1").metadata["dispatches"]["epoch"] == "node-1"
    # The old plane remains fenced even with a fresh TaskStore / compatible policy.
    with pytest.raises(StoreConflict):
        TaskStore(paths).create(Task("T2", "o", "d", "a"))
    assert ex.resumed == []


@pytest.mark.parametrize("alive", [True, None, False])
def test_release_checks_unleased_issued_invocations(tmp_path, alive):
    paths, store, ex, rec, _, _ = make_farm(tmp_path)
    rec.reconcile_once()
    lease = store.get("T1").lease
    ex.set_receipt(Receipt(lease.worker_id, "T1", lease.lease_id, ReceiptStatus.SUBMITTED, ts="t"))
    rec.reconcile_once()
    ex._alive[lease.worker_id] = alive  # a submit receipt need not mean exit
    drain(paths)
    if alive is False:
        assert turnover.release_farm(paths, ex, request_id="node-1", actor="master")["phase"] == "released"
    else:
        with pytest.raises(StoreError, match="issued workers"):
            turnover.release_farm(paths, ex, request_id="node-1", actor="master")


def test_drain_race_prevents_persist_and_dispatch(tmp_path, monkeypatch):
    paths, store, ex, rec, _, _ = make_farm(tmp_path)
    monkeypatch.setattr(rec, "_preflight", lambda *args: bool(drain(paths)))
    report = rec.reconcile_once()
    assert report.conflicts and not ex.launched
    assert store.get("T1").state is TaskState.READY and store.get("T1").lease is None


def test_drain_waits_for_already_admitted_dispatch(tmp_path):
    paths, store, ex, rec, _, _ = make_farm(tmp_path)
    entered, finish = Signal(), Signal()
    original = ex.launch

    def launch(task, lease):
        entered.set()
        assert finish.wait(5)
        return original(task, lease)

    ex.launch = launch
    with ThreadPoolExecutor(max_workers=2) as pool:
        running = pool.submit(rec.reconcile_once)
        assert entered.wait(5)
        draining = pool.submit(drain, paths)
        assert not draining.done()
        finish.set()
        running.result(timeout=5)
        draining.result(timeout=5)
    assert len(ex.launched) == 1
    assert store.get("T1").metadata["dispatches"]["workers"] == [ex.launched[0][0]]


@pytest.mark.parametrize("phase", ["FARM_DRAINED", "FARM_RELEASED", "FARM_CLAIMED"])
def test_deployment_audit_failure_blocks_writers_and_retries_once(tmp_path, monkeypatch, phase):
    paths, store, ex, _, _, identity = make_farm(tmp_path)
    if phase != "FARM_DRAINED":
        drain(paths)
    if phase == "FARM_CLAIMED":
        turnover.release_farm(paths, ex, request_id="node-1", actor="master")
        monkeypatch.setattr(turnover, "runtime_identity", lambda: {**identity, "host": "target-host"})
    action = {"FARM_DRAINED": lambda: drain(paths),
              "FARM_RELEASED": lambda: turnover.release_farm(paths, ex, request_id="node-1", actor="master"),
              "FARM_CLAIMED": lambda: turnover.claim_farm(paths, request_id="node-1", actor="master")}[phase]
    original = EventLog.append

    def fail_after_append(log, event):
        original(log, event)
        if event.type == phase:
            raise OSError("injected audit sync error")

    with monkeypatch.context() as fault:
        fault.setattr(EventLog, "append", fail_after_append)
        with pytest.raises(OSError, match="audit sync"):
            action()
    with pytest.raises(StoreError, match="audit pending"):
        store.create(Task("T2", "o", "d", "a"))
    action()
    action()
    assert not turnover.deployment(paths).get("pending_event")
    records = [json.loads(line) for line in (paths.events / "log.ndjson").read_text().splitlines()]
    assert len([e for e in records if e["type"] == phase]) == 1


def test_unsealed_loss_requires_recover_and_keeps_cleanly_parked_task(tmp_path):
    paths, store, ex, rec, checkpoint, _ = make_farm(tmp_path)
    rec.reconcile_once()
    old = yield_worker(store, ex, checkpoint, waiting_on="ruling:owner")
    rec.reconcile_once()
    ex.kill(old.worker_id)
    drain(paths)
    with pytest.raises(StoreError):
        turnover.claim_farm(paths, request_id="node-1", actor="master")
    rec.reconcile_once()
    before = store.get("T1").to_dict()
    plan = recover_farm(paths)
    assert plan["hold_tasks"] == []
    recover_farm(paths, apply=True, expected_plan=plan["plan_sha256"], actor="operator",
                 evidence_file=str(checkpoint), attest_stopped=True)
    assert store.get("T1").to_dict() == before
    assert turnover.deployment(paths).get("handoff") is None


def test_control_helper_is_bounded_read_only_and_lease_fenced(tmp_path):
    paths, store, _, rec, _, _ = make_farm(tmp_path)
    rec.reconcile_once()
    request(store)
    task = store.get("T1")
    helper = tmp_path / ".farm_receipt.py"
    helper.write_text(RECEIPT_HELPER)
    env = {**os.environ, "FARM_TASK_PATH": str(store.path_for("T1")), "FARM_TASK_ID": "T1",
           "FARM_WORKER_ID": task.lease.worker_id, "FARM_LEASE_ID": task.lease.lease_id}
    before = store.path_for("T1").read_bytes()
    result = subprocess.run([sys.executable, str(helper), "control"], env=env, text=True, capture_output=True)
    assert result.returncode == 0 and len(result.stdout) < 128
    assert json.loads(result.stdout) == {"rotation_id": "rotate-1", "checkpoint_required": True}
    assert store.path_for("T1").read_bytes() == before
    assert not list((paths.runtime / "receipts").glob("*"))
    env["FARM_LEASE_ID"] = "old"
    result = subprocess.run([sys.executable, str(helper), "control"], env=env, text=True, capture_output=True)
    assert result.returncode != 0 and "stale worker lease" in result.stderr


def test_restart_cannot_erase_drain_or_release(tmp_path, capsys):
    paths, store, ex, _, _, _ = make_farm(tmp_path)
    drain(paths)
    parser = build_parser()
    args = parser.parse_args(["--project", str(tmp_path), "reconcile"])
    args.func(args)
    assert turnover.deployment(paths)["handoff"]["phase"] == "draining"
    assert store.get("T1").state is TaskState.READY
    turnover.release_farm(paths, ex, request_id="node-1", actor="master")
    with pytest.raises(StoreError):
        args.func(args)
    assert turnover.deployment(paths)["handoff"]["phase"] == "released"


def test_same_lease_resume_cannot_reuse_previous_invocation_checkpoint(tmp_path):
    _, store, ex, rec, checkpoint, _ = make_farm(tmp_path)
    rec.reconcile_once()
    old = yield_worker(store, ex, checkpoint, waiting_on="ruling:first")
    rec.reconcile_once()
    ex.kill(old.worker_id)
    task = store.get("T1")
    record_decision(store, "T1", expected_revision=task.metadata["revision"], action="ruling",
                    actor="master", evidence_file=str(checkpoint))
    rec.reconcile_once()
    assert store.get("T1").lease == old
    ex.set_receipt(Receipt(old.worker_id, "T1", old.lease_id, ReceiptStatus.AWAITING,
                           ts="new", waiting_on="ruling:second"))
    rec.reconcile_once()
    ex.kill(old.worker_id)
    request(store)
    assert rec.reconcile_once().observation_errors
    assert store.get("T1").lease == old


@pytest.mark.parametrize("fault", ["task", "archive", "build", "id"])
def test_claim_rejects_changed_seal(tmp_path, monkeypatch, fault):
    paths, store, ex, _, _, identity = make_farm(tmp_path)
    drain(paths)
    control = turnover.release_farm(paths, ex, request_id="node-1", actor="master")
    if fault == "task":
        task = store.get("T1")
        atomic_write_json(store.path_for("T1"), replace(task, objective="unauthorized edit").to_dict())
    elif fault == "archive":
        Path(control["archive"]).write_text("{}")
    target = {**identity, "host": "target-host"}
    if fault == "build":
        target["source_sha256"] = "bad"
    monkeypatch.setattr(turnover, "runtime_identity", lambda: target)
    with pytest.raises(StoreConflict):
        turnover.claim_farm(paths, request_id="wrong" if fault == "id" else "node-1", actor="master")
    assert turnover.deployment(paths)["handoff"]["phase"] == "released"


def test_real_local_worker_control_yield_and_clean_release(tmp_path):
    from agent_farm_runtime.adapters.local_process import LocalProcessExecutor
    paths, store, _, _, checkpoint, _ = make_farm(tmp_path)
    helper = tmp_path / ".farm_receipt.py"
    helper.write_text(RECEIPT_HELPER)
    worker = tmp_path / "worker.py"
    worker.write_text('''import json, subprocess, sys, time
for _ in range(100):
    control = json.loads(subprocess.check_output([sys.executable, sys.argv[1], "control"], text=True))
    if control["rotation_id"]:
        subprocess.check_call([sys.executable, sys.argv[1], "AWAITING", "--rotation-id",
                               control["rotation_id"], "--checkpoint", sys.argv[2],
                               "--waiting-on", "ruling:owner"])
        break
    time.sleep(0.02)
''')
    task = store.get("T1")
    command = "exec " + shlex.join([sys.executable, str(worker), str(helper), str(checkpoint)])
    store.commit(replace(task, metadata={**task.metadata, "command": command}),
                 expected=task, actor="test", event_type="TEST_COMMAND")
    ex = LocalProcessExecutor(paths.runtime)
    rec = Reconciler(paths, ex, unblock=make_unblock(paths))
    rec.reconcile_once()
    old = store.get("T1").lease
    try:
        drain(paths)
        end = time.monotonic() + 8
        while time.monotonic() < end and not turnover.clean_surrender(store.get("T1")):
            rec.reconcile_once()
            time.sleep(0.02)
        assert turnover.clean_surrender(store.get("T1"))
        assert ex.poll(old.worker_id).alive is False
        assert turnover.release_farm(paths, ex, request_id="node-1", actor="master")["phase"] == "released"
    finally:
        if ex.poll(old.worker_id).alive is True:
            ex.stop(old.worker_id)


@pytest.mark.parametrize("phase", ["draining", "released", "claimed"])
def test_source_upgrade_never_bypasses_pending_turnover(tmp_path, monkeypatch, phase):
    paths, _, ex, _, _, _ = make_farm(tmp_path)
    drain(paths)
    if phase != "draining":
        turnover.release_farm(paths, ex, request_id="node-1", actor="master")
    manifest = turnover.deployment(paths)
    # Model a completed prior handoff on this host; the existing source-upgrade
    # API should remain usable separately, but not while a handoff is pending.
    manifest["handoff"]["phase"] = phase
    manifest["source_sha256"] = "a" * 64
    atomic_write_json(paths.runtime / "deployment.json", manifest)
    monkeypatch.setattr("agent_farm_runtime.adapters.local_process.LocalProcessExecutor", lambda _: ex)
    args = build_parser().parse_args(["--project", str(tmp_path), "--writer-policy", "pinned-host",
                                     "reconcile", "--upgrade-from-source", "a" * 64])
    if phase == "claimed":
        assert args.func(args) == 0
        assert turnover.deployment(paths)["handoff"]["phase"] == phase
    else:
        with pytest.raises(StoreError, match="finish the handoff"):
            args.func(args)
        assert not ex.launched


def test_unavailable_plane_with_pending_control_audit_can_be_recovered(tmp_path, monkeypatch):
    paths, store, _, _, checkpoint, _ = make_farm(tmp_path)
    with monkeypatch.context() as fault:
        fault.setattr(EventLog, "append", lambda *a: (_ for _ in ()).throw(OSError("audit unavailable")))
        with pytest.raises(OSError):
            drain(paths)
    assert turnover.deployment(paths).get("pending_event")
    plan = recover_farm(paths)
    recover_farm(paths, apply=True, expected_plan=plan["plan_sha256"], actor="operator",
                 evidence_file=str(checkpoint), attest_stopped=True)
    assert turnover.deployment(paths).get("pending_event") is None
    assert turnover.deployment(paths).get("handoff") is None
    assert store.get("T1").state is TaskState.READY
    assert "node-1-FARM_DRAINED" in EventLog(paths.events / "log.ndjson").ids()


@pytest.mark.parametrize("invalid", ["empty", "relative", "missing"])
def test_bad_checkpoint_does_not_release_or_terminate_other_work(tmp_path, invalid):
    _, store, ex, rec, checkpoint, _ = make_farm(tmp_path)
    rec.reconcile_once()
    request(store)
    if invalid == "empty":
        checkpoint.write_text("")
    elif invalid == "relative":
        checkpoint = Path("relative.md")
    else:
        checkpoint = checkpoint.with_name("missing.md")
    old = yield_worker(store, ex, checkpoint, rotation_id="rotate-1")
    ex.kill(old.worker_id)
    assert rec.reconcile_once().preflight_failures
    assert store.get("T1").lease == old and len(ex.launched) == 1


def test_clean_continuation_reuses_injected_observer_and_ignores_legacy_notes(tmp_path):
    paths, store, ex, rec, checkpoint, _ = make_farm(tmp_path)
    task = store.get("T1")
    store.commit(replace(task, metadata={**task.metadata, "workspace": str(tmp_path)}),
                 expected=task, actor="test", event_type="TEST_WORKSPACE")
    rec.reconcile_once()
    old = yield_worker(store, ex, checkpoint, waiting_on="job:123")
    rec.reconcile_once()
    ex.kill(old.worker_id)
    request(store)
    rec.reconcile_once()
    (tmp_path / "MASTER_NOTE_001.md").write_text("Legacy notification, not job completion.")
    state = {"job": "RUNNING"}
    rec.unblock = make_unblock(paths, slurm=lambda _: state["job"])
    rec.reconcile_once()
    assert len(ex.launched) == 1
    state["job"] = "COMPLETED"
    rec.reconcile_once()
    assert len(ex.launched) == 2 and ex.resumed == []


@pytest.mark.parametrize("control", [{}, {"id": "x", "phase": "typo"}, "corrupt"])
def test_invalid_handoff_never_defaults_to_dispatch_allowed(tmp_path, control):
    paths, _, ex, rec, _, _ = make_farm(tmp_path)
    atomic_write_json(paths.runtime / "deployment.json", {**turnover.deployment(paths), "handoff": control})
    with pytest.raises(StoreError, match="invalid handoff"):
        rec.reconcile_once()
    assert not ex.launched
