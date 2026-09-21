"""Offline recovery regressions. No old-host probes, signals, model calls or jobs."""
from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path

import pytest

from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.adapters.prompts import worker_contract
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.events import EventLog
from agent_farm_runtime.locking import ReconcilerBusy, single_reconciler
from agent_farm_runtime.master import record_decision
from agent_farm_runtime.models import Lease, Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.provenance import require_compatible_writer, runtime_identity
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.recovery import recover_farm
from agent_farm_runtime.store import FarmPaths, StoreConflict, StoreError, TaskStore, atomic_write_json


def fixture(tmp_path):
    paths = FarmPaths(tmp_path / ".farm")
    paths.ensure()
    store = TaskStore(paths)
    for name, state in [("wait", TaskState.WAITING), ("run", TaskState.RUNNING),
                        ("held", TaskState.BLOCKED), ("done", TaskState.DONE)]:
        # Legacy records: no revision or executor identity. Never used to prove death.
        task = Task(name, "objective", "result", "unchanged criteria", state=state,
                    lease=Lease("W-" + name, "L-" + name) if name in {"wait", "run"} else None,
                    metadata={"brief": "old instructions", "waiting_on": "artifact:/not-ready",
                              "acceptance_receipt": "old evidence"} if name == "done" else
                             {"brief": "old instructions", "waiting_on": "artifact:/not-ready"})
        atomic_write_json(store.path_for(name), task.to_dict())
    atomic_write_json(paths.runtime / "deployment.json", {
        **runtime_identity(), "protocol_version": 2, "host": "old-host",
        "source_sha256": "a" * 64, "started_at": "old", "pid": 42, "loop": True,
    })
    note = tmp_path / "shutdown.md"
    note.write_text("ISOLATED TEST: all old writers, workers and queued dispatches stopped/fenced; no external compute affected.")
    return paths, store, note


def apply(paths, note, plan=None):
    return recover_farm(paths, apply=True, expected_plan=plan or recover_farm(paths)["plan_sha256"],
                        actor="test-operator", evidence_file=str(note), attest_stopped=True)


def events(paths):
    return [json.loads(line) for line in (paths.events / "log.ndjson").read_text().splitlines()]


def files(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_preview_is_read_only_and_pid_independent(tmp_path, monkeypatch):
    paths, _, _ = fixture(tmp_path)
    before = files(paths.root)
    first = recover_farm(paths)
    original = runtime_identity()
    monkeypatch.setattr("agent_farm_runtime.recovery.runtime_identity",
                        lambda: {**original, "pid": 999999, "pid_namespace": "another-cli-sandbox"})
    assert recover_farm(paths) == first
    assert files(paths.root) == before
    assert not (paths.runtime / "reconcile.lock").exists()
    assert [t["id"] for t in first["hold_tasks"]] == ["run", "wait"]


def test_recovery_holds_old_ownership_and_requires_new_ruling(tmp_path):
    paths, store, note = fixture(tmp_path)
    untouched = {key: store.path_for(key).read_bytes() for key in ("held", "done")}
    old_lease = store.get("wait").lease
    result = apply(paths, note)
    archive = json.loads((paths.runtime / "recoveries" / (result["recovery_id"] + ".json")).read_text())
    assert archive["evidence"]["text"] == note.read_text()
    assert archive["plan"]["before"]["protocol_version"] == 2
    for key in ("run", "wait"):
        task = store.get(key)
        assert task.state is TaskState.BLOCKED and task.lease is None
        assert task.metadata["revision"] == 1 and task.metadata["waiting_on"] == "artifact:/not-ready"
    for key, before in untouched.items():
        assert store.path_for(key).read_bytes() == before
    require_compatible_writer(paths, policy="pinned-host")
    manifest = json.loads((paths.runtime / "deployment.json").read_text())
    assert manifest["pid"] is None and manifest["pid_namespace"] is None
    assert manifest["started_at"] is None and not manifest["loop"]
    assert [e["type"] for e in events(paths)] == ["RECOVERY_HELD", "RECOVERY_HELD", "FARM_RECOVERED"]
    assert any("recovery hold" in c.message for c in run_doctor(paths))
    executor = FakeExecutor()
    rec = Reconciler(paths, executor, unblock=lambda _: True)
    assert not rec.reconcile_once().launched and not executor.resumed
    note.write_text("Before continuing, inspect receipts/artifacts/jobs. Do not duplicate compute; await again if prerequisites are absent.")
    record_decision(store, "wait", expected_revision=1, action="ruling", actor="master", evidence_file=str(note))
    assert store.get("wait").state is TaskState.READY
    assert note.read_text() in worker_contract(store.get("wait"))
    assert rec.reconcile_once().launched == ["wait"]
    assert store.get("wait").lease != old_lease
    assert store.get("held").state is TaskState.BLOCKED and store.get("run").state is TaskState.BLOCKED
    current = store.get("wait").lease
    executor.set_receipt(Receipt(old_lease.worker_id, "wait", old_lease.lease_id, ReceiptStatus.SUBMITTED, ts="old"))
    # Deliver stale old ownership as if observed while polling the new holder.
    executor._receipt[current.worker_id] = executor._receipt[old_lease.worker_id]
    assert rec.reconcile_once().ignored_stale == [old_lease.worker_id]
    assert store.get("wait").state is TaskState.RUNNING
    with pytest.raises(StoreError):
        record_decision(store, "held", expected_revision=0, action="ruling", actor="master", evidence_file=str(note))


@pytest.mark.parametrize("change", ["task", "manifest", "source", "root", "new-task"])
def test_stale_plan_rejected_before_authoritative_writes(tmp_path, monkeypatch, change):
    paths, store, note = fixture(tmp_path)
    plan = recover_farm(paths)["plan_sha256"]
    if change == "task":
        task = store.get("wait")
        store.commit(replace(task, objective="changed"), expected=task, actor="test", event_type="EDIT")
    elif change == "manifest":
        manifest = paths.runtime / "deployment.json"
        data = json.loads(manifest.read_text())
        atomic_write_json(manifest, {**data, "source_sha256": "b" * 64})
    elif change == "source":
        original = runtime_identity()
        monkeypatch.setattr("agent_farm_runtime.recovery.runtime_identity", lambda: {**original, "source_sha256": "b" * 64})
    elif change == "root":
        paths, store, note = fixture(tmp_path / "another")
    else:
        store.create(Task("new", "o", "d", "a"))
    before = {p: data for p, data in files(paths.root).items() if not p.endswith(".lock")}
    with pytest.raises(StoreConflict, match="plan changed"):
        apply(paths, note, plan)
    assert {p: data for p, data in files(paths.root).items() if not p.endswith(".lock")} == before


@pytest.mark.parametrize("field", ["attest_stopped", "actor", "evidence_file", "expected_plan"])
def test_apply_requires_explicit_attestation_and_evidence(tmp_path, field):
    paths, _, note = fixture(tmp_path)
    args = dict(apply=True, attest_stopped=True, actor="operator", evidence_file=str(note),
                expected_plan=recover_farm(paths)["plan_sha256"])
    args[field] = None
    before = files(paths.root)
    with pytest.raises(StoreError):
        recover_farm(paths, **args)
    assert files(paths.root) == before


@pytest.mark.parametrize("bad", ["protocol", "pending-task", "lease", "held-lock", "empty-evidence"])
def test_recovery_rejects_unsafe_or_unsupported_inputs(tmp_path, bad):
    paths, store, note = fixture(tmp_path)
    if bad == "protocol":
        manifest = paths.runtime / "deployment.json"
        atomic_write_json(manifest, {**json.loads(manifest.read_text()), "protocol_version": 999})
    elif bad == "pending-task":
        (paths.runtime / "pending-task-commit.json").write_text("preserve me")
    elif bad == "lease":
        atomic_write_json(store.path_for("held"), replace(store.get("held"), lease=Lease("W", "L")).to_dict())
    elif bad == "empty-evidence":
        note.write_text(" ")
    if bad == "held-lock":
        with single_reconciler(paths.runtime), pytest.raises(ReconcilerBusy):
            apply(paths, note)
    else:
        with pytest.raises(StoreError):
            apply(paths, note)
    assert not (paths.runtime / "pending-recovery.json").exists()
    assert store.get("wait").state is TaskState.WAITING


@pytest.mark.parametrize("boundary", ["marker", "archive", "task", "task-event", "farm-event", "manifest", "unlink"])
def test_interrupted_recovery_is_guarded_and_retries_exactly_once(tmp_path, monkeypatch, boundary):
    paths, store, note = fixture(tmp_path)
    plan = recover_farm(paths)["plan_sha256"]
    import agent_farm_runtime.adapters.posix_filesystem as fs
    original_write, original_append, original_unlink = fs.atomic_write_json, EventLog.append, Path.unlink
    original_sync = EventLog.sync

    def write(path, data):
        original_write(path, data)
        if ((boundary == "marker" and path.name == "pending-recovery.json")
                or (boundary == "archive" and path.parent.name == "recoveries")
                or (boundary == "task" and path == store.path_for("run"))
                or (boundary == "manifest" and path.name == "deployment.json")):
            raise OSError("injected persistence failure after visible write")

    def append(log, event):
        original_append(log, event)
        if ((boundary == "task-event" and event.type == "RECOVERY_HELD")
                or (boundary == "farm-event" and event.type == "FARM_RECOVERED")):
            raise OSError("injected persistence failure after visible event")

    def unlink(path, *args, **kwargs):
        if boundary == "unlink" and path.name == "pending-recovery.json":
            raise OSError("injected failure before journal removal")
        return original_unlink(path, *args, **kwargs)

    def sync(log):
        original_sync(log)
        if boundary in {"task-event", "farm-event"}:
            raise OSError("injected repeated failure while re-syncing visible event")

    with monkeypatch.context() as patch:
        patch.setattr(fs, "atomic_write_json", write)
        patch.setattr(EventLog, "append", append)
        patch.setattr(EventLog, "sync", sync)
        patch.setattr(Path, "unlink", unlink)
        for _ in range(2):
            with pytest.raises(OSError):
                apply(paths, note, plan)
            assert recover_farm(paths)["pending"]
            task = store.get("wait")
            with pytest.raises(StoreError, match="recovery pending"):
                store.commit(task, expected=task, event_type="UNSAFE", actor="test")
            with pytest.raises(StoreError, match="recovery pending"):
                Reconciler(paths, FakeExecutor()).reconcile_once()
            with pytest.raises(StoreError, match="recovery pending"):
                require_compatible_writer(paths)
    apply(paths, note, plan)
    assert not recover_farm(paths)["pending"]
    assert [e["type"] for e in events(paths)].count("RECOVERY_HELD") == 2
    assert [e["type"] for e in events(paths)].count("FARM_RECOVERED") == 1
    assert store.get("wait").metadata["revision"] == store.get("run").metadata["revision"] == 1
    with pytest.raises(StoreConflict):
        apply(paths, note, plan)  # Completed old plans cannot be applied to a new snapshot.


def test_pending_recovery_rejects_changed_evidence_and_outside_writes(tmp_path, monkeypatch):
    paths, store, note = fixture(tmp_path)
    plan = recover_farm(paths)["plan_sha256"]
    with monkeypatch.context() as patch:
        patch.setattr("agent_farm_runtime.recovery._finish", lambda *_: (_ for _ in ()).throw(OSError("crash")))
        with pytest.raises(OSError):
            apply(paths, note, plan)
    content = note.read_text()
    note.write_text("different evidence")
    with pytest.raises(StoreConflict, match="original actor and evidence"):
        apply(paths, note, plan)
    note.write_text(content)
    atomic_write_json(store.path_for("wait"), replace(store.get("wait"), objective="outside edit").to_dict())
    with pytest.raises(StoreConflict, match="outside recovery"):
        apply(paths, note, plan)
    assert (paths.runtime / "pending-recovery.json").exists()


def test_cli_preview_and_reconcile_output_flush(tmp_path, monkeypatch):
    paths, _, _ = fixture(tmp_path)
    args = build_parser().parse_args(["--project", str(tmp_path), "recover"])
    assert args.func(args) == 0 and not (paths.runtime / "pending-recovery.json").exists()
    # Avoid any executor construction: observe the flush at the loop output boundary.
    from test_review_regressions import startup_fixture, startup
    _, _, executor = startup_fixture(tmp_path / "other", monkeypatch, runtime_identity())
    calls = []
    monkeypatch.setattr("builtins.print", lambda *a, **kw: calls.append(kw))
    startup(tmp_path / "other", "pinned-host")
    assert executor.launched and any(c.get("flush") is True for c in calls)


@pytest.mark.parametrize("change", ["source", "host", "manifest", "pending-task", "audit"])
def test_pending_recovery_does_not_guess_around_conflicts(tmp_path, monkeypatch, change):
    paths, store, note = fixture(tmp_path)
    plan = recover_farm(paths)["plan_sha256"]
    with monkeypatch.context() as patch:
        patch.setattr("agent_farm_runtime.recovery._finish", lambda *_: (_ for _ in ()).throw(OSError("crash")))
        with pytest.raises(OSError):
            apply(paths, note, plan)
    if change in {"source", "host"}:
        identity = runtime_identity()
        identity["host" if change == "host" else "source_sha256"] = "different"
        monkeypatch.setattr("agent_farm_runtime.recovery.runtime_identity", lambda: identity)
    elif change == "manifest":
        atomic_write_json(paths.runtime / "deployment.json", {"unexpected": "writer"})
    elif change == "pending-task":
        atomic_write_json(paths.runtime / "pending-task-commit.json", {
            "before": None, "after": store.get("wait").to_dict(),
            "event": {"type": "UNRELATED", "payload": {}},
        })
    else:
        (paths.events / "log.ndjson").write_text('{"id": "torn"}')
    before = {t.id: t.to_dict() for t in store.list()}
    with pytest.raises((StoreConflict, ValueError)):
        apply(paths, note, plan)
    assert {t.id: t.to_dict() for t in store.list()} == before
    assert (paths.runtime / "pending-recovery.json").exists()


def test_pending_recovery_blocks_cli_before_executor_or_manifest_write(tmp_path, monkeypatch):
    paths, _, _ = fixture(tmp_path)
    (paths.runtime / "pending-recovery.json").write_text("preserve journal")
    before = (paths.runtime / "deployment.json").read_bytes()
    monkeypatch.setattr("agent_farm_runtime.adapters.local_process.LocalProcessExecutor",
                        lambda *_: pytest.fail("must not initialize an executor"))
    args = build_parser().parse_args(["--project", str(tmp_path), "reconcile"])
    with pytest.raises(StoreError, match="recovery pending"):
        args.func(args)
    assert (paths.runtime / "deployment.json").read_bytes() == before


def test_recovered_codex_task_launches_fresh_without_editing_old_identity(tmp_path):
    from agent_farm_runtime.adapters.codex import CodexTmuxExecutor
    from test_codex_tmux import TmuxRecorder
    paths, store, note = fixture(tmp_path)
    task = store.get("wait")
    old = task.lease
    ws = tmp_path / "workspace"
    ws.mkdir()
    atomic_write_json(store.path_for("wait"), replace(task, metadata={**task.metadata, "workspace": str(ws)}).to_dict())
    old_state = paths.runtime / "codex_workers" / (old.worker_id + ".json")
    atomic_write_json(old_state, {"legacy": True, "host": "old-host", "lease_id": old.lease_id})
    before = old_state.read_bytes()
    apply(paths, note)
    note.write_text("Reuse existing checkpoints; do not resubmit existing compute. Recheck the original wait condition.")
    record_decision(store, "wait", expected_revision=1, action="ruling", actor="master", evidence_file=str(note))
    ex = CodexTmuxExecutor(paths.runtime, run=TmuxRecorder())
    assert Reconciler(paths, ex).reconcile_once().launched == ["wait"]
    fresh = store.get("wait").lease
    assert fresh.worker_id != old.worker_id and fresh.lease_id != old.lease_id
    assert ex._load_state(fresh.worker_id)["attempt_id"]
    assert old_state.read_bytes() == before
    assert "Reuse existing checkpoints" in (ws / f".farm_launch_{fresh.worker_id}.sh").read_text()


@pytest.mark.skipif(not os.environ.get("FARM_RECOVERY_FIXTURE"), reason="opt-in read-only source snapshot rehearsal")
def test_recovery_on_isolated_copy_of_existing_store(tmp_path):
    source = Path(os.environ["FARM_RECOVERY_FIXTURE"]).resolve(strict=True)
    paths = FarmPaths(tmp_path / ".farm")
    paths.ensure()
    originals = {p: p.read_bytes() for p in [source / "runtime" / "deployment.json", *sorted((source / "tasks").glob("*.json"))]}
    for path, data in originals.items():
        destination = paths.root / path.relative_to(source)
        destination.write_bytes(data)
    store = TaskStore(paths)
    before = {t.id: t.to_dict() for t in store.list()}
    note = tmp_path / "copy-only-evidence.md"
    note.write_text("TEST COPY ONLY: no executors exist for this isolated store; no authorization for the source farm.")
    apply(paths, note)
    for task in store.list():
        if before[task.id]["lease"]:
            assert task.state is TaskState.BLOCKED and task.lease is None
        else:
            assert task.to_dict() == before[task.id]
    assert all(p.read_bytes() == data for p, data in originals.items())
    assert not (paths.runtime / "pending-recovery.json").exists()
