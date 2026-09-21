"""Explicit stopped-farm recovery, not a liveness oracle or automatic takeover.

The operator supplies site-specific shutdown/fencing evidence. We preserve it,
bind it to a reviewed snapshot, and retire ownership through TaskStore. The small
pending record only guards this offline procedure; it never actuates or schedules.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone

from .adapters.filesystem import atomic_write_json, sync_directory
from .events import EventLog
from .locking import single_reconciler, task_mutation_lock
from .master import snapshot_file
from .models import Event, Task, TaskState
from .provenance import runtime_identity
from .store import FarmPaths, StoreConflict, StoreError, TaskStore
from .transitions import rotate_lease, transition_task


def _digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _target() -> dict:
    # Recovery does not execute workers. Separate CLI sandboxes may have different
    # PID namespaces; only subsequent daemon/worker startup owns process identity.
    return {k: v for k, v in runtime_identity().items() if k not in {"pid", "pid_namespace"}}


def _plan(paths: FarmPaths) -> dict:
    if (paths.runtime / "pending-task-commit.json").exists():
        raise StoreError("pending task transaction: resolve with its original writer before recovery")
    before = json.loads((paths.runtime / "deployment.json").read_text(encoding="utf-8"))
    if not isinstance(before, dict) or before.get("protocol_version") not in (2, 3, 4):
        raise StoreError("offline recovery supports only known protocol 2/3/4 manifests")
    if not before.get("host") or not re.fullmatch(r"[0-9a-f]{64}", str(before.get("source_sha256", ""))):
        raise StoreError("recovery requires the original host and exact source SHA-256")
    store = TaskStore(paths)
    tasks = store.list()
    if len({t.id for t in tasks}) != len(tasks):
        raise StoreError("duplicate task identities; inspect before recovery")
    leases = [t.lease for t in tasks if t.lease]
    if len({l.lease_id for l in leases}) != len(leases) or len({l.worker_id for l in leases}) != len(leases):
        raise StoreError("duplicate lease/worker ownership; inspect before recovery")
    for task in tasks:
        from .turnover import clean_surrender
        # Do not guess how to repair arbitrary corrupt/unsupported ownership.
        if not clean_surrender(task) and bool(task.lease) != (task.state in {TaskState.RUNNING, TaskState.WAITING}):
            raise StoreError(f"{task.id}: unsupported state/lease combination; inspect before recovery")
        if store.get(task.id).to_dict() != task.to_dict():
            raise StoreConflict("task filenames/identities changed during preview")
    return {"root": str(paths.root.resolve()), "before": before,
            "target": _target(), "tasks": [t.to_dict() for t in tasks]}


def _summary(plan: dict, *, pending: bool = False, recovery_id: str | None = None) -> dict:
    return {"plan_sha256": _digest(plan), "pending": pending, "recovery_id": recovery_id,
            "from": {k: plan["before"].get(k) for k in ("host", "protocol_version", "source_sha256")},
            "to": plan["target"],
            "hold_tasks": [{"id": t["id"], "state": t["state"], "lease": t["lease"]}
                           for t in plan["tasks"] if t["lease"]],
            "unchanged_tasks": [{"id": t["id"], "state": t["state"]}
                                for t in plan["tasks"] if not t["lease"]],
            "warning": "No launch/kill. All old writers, workers and queued dispatches must be stopped/fenced. "
                       "Retired tasks become BLOCKED until a fresh master ruling. Existing READY tasks remain READY."}


def _prepare(plan: dict, actor: str, evidence: dict) -> dict:
    recovery_id = uuid.uuid4().hex
    ts = datetime.now(timezone.utc).isoformat()
    changes = {}
    for data in plan["tasks"]:
        task = Task.from_dict(data)
        if task.lease:
            retired = rotate_lease(task, dead_worker_id=task.lease.worker_id,
                                   reason=f"operator-attested stopped/fenced; recovery {recovery_id}")
            held = transition_task(task, TaskState.BLOCKED, lease_id=task.lease.lease_id,
                                   new_lease=retired.lease, metadata_patch={
                "recovery_hold": {"id": recovery_id, "previous_state": task.state.value},
                "runtime_error": None, "resume_requested": None,
                "rotation_request": None,
            })
            held.metadata["revision"] = int(task.metadata.get("revision", 0)) + 1
            changes[task.id] = held.to_dict()
    after = {**plan["before"], **plan["target"], "pid": None, "pid_namespace": None, "started_at": None,
             "loop": False, "writer_policy": "pinned-host", "recovery_id": recovery_id, "recovered_at": ts}
    after.pop("upgraded_from_source", None)
    after.pop("handoff", None)
    after.pop("pending_event", None)
    after["execution_epoch"] = recovery_id
    return {"id": recovery_id, "plan": plan, "changes": changes, "after": after,
            "actor": actor, "evidence": evidence, "ts": ts}


def _finish(paths: FarmPaths, txn: dict) -> None:
    """Both locks held; partial task commits reuse the existing CAS/audit journal."""
    plan, recovery_id = txn["plan"], txn["id"]
    if not re.fullmatch(r"[0-9a-f]{32}", recovery_id):
        raise StoreError("invalid recovery identity; preserve journal and inspect")
    store = TaskStore(paths)
    expected = {t["id"]: t for t in plan["tasks"]}
    current = {t.id: t.to_dict() for t in store.list()}
    if current.keys() != expected.keys() or any(
        current[key] not in (before, txn["changes"].get(key, before)) for key, before in expected.items()
    ):
        raise StoreConflict("tasks changed outside recovery; preserve journal and inspect")
    manifest = paths.runtime / "deployment.json"
    if json.loads(manifest.read_text()) not in (plan["before"], txn["after"]):
        raise StoreConflict("deployment changed outside recovery; preserve journal and inspect")
    pending_task = paths.runtime / "pending-task-commit.json"
    if pending_task.exists():
        commit = json.loads(pending_task.read_text())
        task_id = commit["after"]["id"]
        if (task_id not in txn["changes"] or commit["before"] != expected[task_id]
                or commit["after"] != txn["changes"][task_id]
                or commit["event"]["type"] != "RECOVERY_HELD"
                or commit["event"]["payload"].get("recovery_id") != recovery_id):
            raise StoreConflict("unrelated task transaction during recovery; inspect before proceeding")
    log = EventLog(paths.events / "log.ndjson")
    log.ids()  # Fail closed on damaged audit before writing anything further.
    archive = paths.runtime / "recoveries" / f"{recovery_id}.json"
    if archive.exists() and json.loads(archive.read_text()) != txn:
        raise StoreConflict("recovery archive changed; preserve and inspect")
    # Retry even if visible: the prior directory fsync may have failed. Persist
    # the archive directory's own entry before any authoritative task change.
    atomic_write_json(archive, txn)
    sync_directory(paths.runtime)
    if plan["before"].get("pending_event"):
        old_event = Event(**plan["before"]["pending_event"])
        if old_event.id not in log.ids():
            log.append(old_event)
        else:
            log.sync()
    store._recover_locked()
    for task_id, after in txn["changes"].items():
        task = store.get(task_id)
        if task.to_dict() != after:
            store._commit_locked(Task.from_dict(after), expected=Task.from_dict(expected[task_id]),
                                 event_type="RECOVERY_HELD", actor=txn["actor"], ts=txn["ts"],
                                 payload={"recovery_id": recovery_id, "evidence_sha256": txn["evidence"]["sha256"]})
    atomic_write_json(manifest, txn["after"])
    event = Event(id=recovery_id, task_id="*", type="FARM_RECOVERED", actor=txn["actor"], ts=txn["ts"],
                  payload={"archive": str(archive), "plan_sha256": _digest(plan),
                           "evidence_sha256": txn["evidence"]["sha256"]})
    if event.id not in log.ids():
        log.append(event)
    else:
        log.sync()
    # Startup remains blocked by the marker until tasks, audit and manifest are
    # all durable. A retry re-syncs visible files, never rewinds completed tasks.
    (paths.runtime / "pending-recovery.json").unlink()
    sync_directory(paths.runtime)


def recover_farm(paths: FarmPaths, *, apply: bool = False, expected_plan: str | None = None,
                 actor: str | None = None, evidence_file: str | None = None,
                 attest_stopped: bool = False) -> dict:
    pending = paths.runtime / "pending-recovery.json"
    if not apply:
        if pending.exists():
            txn = json.loads(pending.read_text())
            return _summary(txn["plan"], pending=True, recovery_id=txn["id"])
        return _summary(_plan(paths))  # No lock creation, recovery, or filesystem writes.
    if not attest_stopped or not actor or not actor.strip() or not evidence_file or not expected_plan:
        raise StoreError("apply requires --attest-stopped, --actor, --evidence-file and --expected-plan")
    evidence = snapshot_file(evidence_file)
    with single_reconciler(paths.runtime), task_mutation_lock(paths.runtime):
        if pending.exists():
            txn = json.loads(pending.read_text())
            if txn["actor"] != actor or txn["evidence"]["sha256"] != evidence["sha256"]:
                raise StoreConflict("pending recovery requires its original actor and evidence contents")
        else:
            plan = _plan(paths)
            txn = _prepare(plan, actor, evidence)
        if _digest(txn["plan"]) != expected_plan:
            raise StoreConflict("recovery plan changed; preview and review again")
        if txn["plan"]["target"] != _target() or txn["plan"]["root"] != str(paths.root.resolve()):
            raise StoreConflict("recovery source/host/boot/root changed; retain original recovery environment")
        # The marker fences cooperating writers even if this process crashes.
        atomic_write_json(pending, txn)
        _finish(paths, txn)
        return _summary(txn["plan"], recovery_id=txn["id"])
