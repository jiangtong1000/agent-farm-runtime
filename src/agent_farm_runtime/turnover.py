"""Planned turnover: task metadata, one deployment record, existing locks/audit.

No scheduler or scientific policy lives here. All callers cooperate with these
locks; a missing process identity is never a clean release certificate.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import replace
from datetime import datetime, timezone

from .adapters.filesystem import atomic_write_json, sync_directory
from .events import EventLog
from .locking import single_reconciler, task_mutation_lock
from .master import snapshot_file
from .models import Event, Task, TaskState
from .provenance import runtime_identity
from .store import FarmPaths, StoreConflict, StoreError, TaskStore, require_no_pending_recovery


def deployment(paths: FarmPaths) -> dict:
    path = paths.runtime / "deployment.json"
    value = json.loads(path.read_text()) if path.exists() else {}
    if not isinstance(value, dict):
        raise StoreError("invalid deployment manifest")
    if "handoff" in value:
        control = value["handoff"]
        if (not isinstance(control, dict) or control.get("phase") not in {"draining", "released", "claimed"}
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", str(control.get("id", "")))):
            raise StoreError("invalid handoff control; preserve deployment and inspect")
    return value


def execution_epoch(manifest: dict) -> str:
    return manifest.get("execution_epoch") or manifest.get("recovery_id") or "initial"


def require_task_writer(paths: FarmPaths, *, dispatch: bool = False, epoch: str | None = None,
                        upgrade_from_source: str | None = None) -> dict:
    """Called INSIDE the mutation lock, not just at CLI argument validation."""
    require_no_pending_recovery(paths)
    manifest = deployment(paths)
    if manifest.get("pending_event"):
        raise StoreError("deployment audit pending; retry handoff or reconciler startup")
    if manifest.get("handoff"):
        current = runtime_identity()
        if manifest.get("protocol_version") != current["protocol_version"]:
            raise StoreError("writer/deployment protocol mismatch")
        # Planned turnover cannot be bypassed by choosing the compatible policy.
        source = upgrade_from_source or current["source_sha256"]
        if upgrade_from_source and manifest["handoff"].get("phase") != "claimed":
            raise StoreError("finish the handoff before a separate source upgrade")
        if manifest.get("host") != current["host"] or manifest.get("source_sha256") != source:
            raise StoreConflict("handoff writer host/source mismatch")
    if epoch is not None and execution_epoch(manifest) != epoch:
        raise StoreConflict("execution epoch changed; retire the old reconciler")
    phase = manifest.get("handoff", {}).get("phase")
    if phase == "released" or (dispatch and phase == "draining"):
        raise StoreConflict("farm handoff withholds writes/dispatch; use the recorded handoff")
    return manifest


def clean_surrender(task: Task) -> bool:
    record = task.metadata.get("clean_surrender")
    if task.state is not TaskState.WAITING or task.lease is not None or not isinstance(record, dict):
        return False
    checkpoint = record.get("checkpoint", {})
    if not isinstance(checkpoint, dict):
        return False
    text = checkpoint.get("text")
    return (all(record.get(k) for k in ("id", "worker_id", "lease_id", "ts"))
            and isinstance(text, str) and bool(text.strip())
            and record.get("waiting_on") == task.metadata.get("waiting_on"))


def request_rotation(store: TaskStore, task_id: str, *, expected_revision: int,
                     request_id: str, actor: str, checkpoint_file: str | None = None) -> Task:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", request_id) or not actor.strip():
        raise StoreError("rotation requires a short stable request ID and nonempty actor")
    task = store.get(task_id)
    if task.metadata.get("revision", 0) != expected_revision:
        raise StoreConflict("rotation revision changed; reread task-show")
    previous = task.metadata.get("rotation_request")
    if task.metadata.get("clean_surrender", {}).get("id") == request_id:
        return task
    if task.state not in {TaskState.RUNNING, TaskState.WAITING} or task.lease is None:
        raise StoreError("rotation requires a RUNNING/WAITING lease")
    if previous and previous["id"] != request_id:
        raise StoreConflict("rotation already requested; reuse the recorded ID")
    if checkpoint_file and task.state is not TaskState.WAITING:
        raise StoreError("master checkpoint attachment requires WAITING; a running worker must yield itself")
    checkpoint = snapshot_file(checkpoint_file) if checkpoint_file else None
    if previous and (checkpoint is None or previous.get("checkpoint") == checkpoint):
        return task
    record = previous or {"id": request_id, "actor": actor,
                          "worker_id": task.lease.worker_id, "lease_id": task.lease.lease_id,
                          "requested_state": task.state.value, "based_on_revision": expected_revision,
                          "ts": datetime.now(timezone.utc).isoformat()}
    record = {**record, **({"checkpoint": checkpoint} if checkpoint else {})}
    return store.commit(replace(task, metadata={**task.metadata, "rotation_request": record}),
                        expected=task, actor=actor, event_type="ROTATION_REQUESTED", payload=record)


def finish_deployment_event(paths: FarmPaths, *, claiming: bool = False) -> dict:
    """Mutation lock held. Embedded redo record blocks ordinary writers on error."""
    manifest = deployment(paths)
    if manifest.get("pending_event"):
        current = runtime_identity()
        host = manifest.get("handoff", {}).get("target_host") if claiming else manifest.get("host")
        if host != current["host"] or any(manifest.get(k) != current[k]
                                          for k in ("source_sha256", "protocol_version")):
            raise StoreError("pending deployment audit requires its recorded host/build (or exact claimant)")
        event = Event(**manifest["pending_event"])
        log = EventLog(paths.events / "log.ndjson")
        if event.id not in log.ids():
            log.append(event)
        else:
            log.sync()
        manifest.pop("pending_event")
        atomic_write_json(paths.runtime / "deployment.json", manifest)
    return manifest


def _publish(paths: FarmPaths, manifest: dict, kind: str, actor: str) -> dict:
    control = manifest["handoff"]
    event = Event(id=f"{control['id']}-{kind}", task_id="*", type=kind, actor=actor,
                  ts=control["ts"], payload={"handoff": control})
    EventLog(paths.events / "log.ndjson").ids()  # Fail closed before changing control.
    atomic_write_json(paths.runtime / "deployment.json", {**manifest, "pending_event": event.to_dict()})
    return finish_deployment_event(paths)


def handoff_status(paths: FarmPaths) -> dict:
    manifest = deployment(paths)
    return {"handoff": manifest.get("handoff"), "audit_pending": bool(manifest.get("pending_event")),
            "leased_tasks": [t.id for t in TaskStore(paths).list() if t.lease]}


def drain_farm(paths: FarmPaths, *, request_id: str, target_host: str, actor: str) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", request_id) or not actor.strip():
        raise StoreError("handoff requires a short stable ID and nonempty actor")
    with task_mutation_lock(paths.runtime):
        require_no_pending_recovery(paths)
        manifest = finish_deployment_event(paths)
        current = runtime_identity()
        if any(manifest.get(k) != current[k] for k in ("host", "source_sha256", "protocol_version")):
            raise StoreError("drain requires the current deployment host and exact build")
        if not target_host.strip() or target_host == current["host"]:
            raise StoreError("provide the exact, different destination hostname; master-only turnover needs no drain")
        prior = manifest.get("handoff")
        if prior and prior["phase"] != "claimed":
            if prior["id"] != request_id or prior["target_host"] != target_host:
                raise StoreConflict("handoff already pending; reuse its ID and target")
            return prior
        # IDs remain single-use even after later generations have taken over.
        log = EventLog(paths.events / "log.ndjson")
        if f"{request_id}-FARM_DRAINED" in log.ids():
            raise StoreConflict("handoff ID already used")
        TaskStore(paths)._recover_locked()
        control = {"id": request_id, "phase": "draining", "target_host": target_host,
                   "from_host": current["host"], "actor": actor,
                   "ts": datetime.now(timezone.utc).isoformat()}
        _publish(paths, {**manifest, "handoff": control}, "FARM_DRAINED", actor)
        return control


def release_farm(paths: FarmPaths, executor, *, request_id: str, actor: str) -> dict:
    """Daemon must have exited its loop; observe ALL dispatched workers, not registry."""
    if not actor.strip():
        raise StoreError("release requires a nonempty actor")
    with single_reconciler(paths.runtime), task_mutation_lock(paths.runtime):
        require_no_pending_recovery(paths)
        manifest = finish_deployment_event(paths)
        current = runtime_identity()
        if any(manifest.get(k) != current[k] for k in ("host", "source_sha256", "protocol_version")):
            raise StoreError("release must run on the draining deployment host/build")
        control = manifest.get("handoff", {})
        if control.get("kind") == "stop":
            raise StoreError("this farm is being stopped (farm stop --drain), not handed off; nothing to release")
        if control.get("id") != request_id or control.get("phase") not in {"draining", "released"}:
            raise StoreConflict("no matching drain to release")
        if control["phase"] == "released":
            return control
        store = TaskStore(paths)
        store._recover_locked()
        tasks = store.list()
        if any(t.lease or t.state is TaskState.RUNNING or
               (t.state is TaskState.WAITING and not clean_surrender(t)) for t in tasks):
            raise StoreError("release requires all leases cleanly surrendered and no unidentified WAITING task")
        workers = sorted({wid for t in tasks
                          if t.metadata.get("dispatches", {}).get("epoch") == execution_epoch(manifest)
                          for wid in t.metadata["dispatches"]["workers"]})
        uncertain = [wid for wid in workers if executor.poll(wid).alive is not False]
        if uncertain:
            raise StoreError(f"release withheld: live/UNKNOWN issued workers or queued dispatches: {uncertain}")
        archive = paths.runtime / "handoffs" / f"{request_id}.json"
        proof = {"root": str(paths.root.resolve()), "deployment": manifest,
                 "tasks": [t.to_dict() for t in tasks], "quiesced_workers": workers}
        atomic_write_json(archive, proof)
        sync_directory(paths.runtime)
        sealed = {**control, "phase": "released", "archive": str(archive.resolve()),
                  "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
        _publish(paths, {**manifest, "handoff": sealed}, "FARM_RELEASED", actor)
        return sealed


def claim_farm(paths: FarmPaths, *, request_id: str, actor: str) -> dict:
    """No old-host probe: only a durable clean release authorizes this path."""
    if not actor.strip():
        raise StoreError("claim requires a nonempty actor")
    with single_reconciler(paths.runtime), task_mutation_lock(paths.runtime):
        require_no_pending_recovery(paths)
        manifest = deployment(paths)
        control = manifest.get("handoff", {})
        if control.get("kind") == "stop":
            raise StoreError("a stopped farm is not claimable; start a new farm (D5) or restart the same release")
        current = runtime_identity()
        if (control.get("id") != request_id or control.get("target_host") != current["host"]
                or any(manifest.get(k) != current[k] for k in ("source_sha256", "protocol_version"))):
            raise StoreConflict("claim requires the exact handoff, target host and unchanged build/protocol")
        if control.get("phase") not in {"released", "claimed"}:
            raise StoreError("old plane has not sealed a clean release; unavailable is not dead")
        manifest = finish_deployment_event(paths, claiming=True)
        if control["phase"] == "claimed":
            return control
        archive = paths.runtime / "handoffs" / f"{request_id}.json"
        raw = archive.read_bytes()
        proof = json.loads(raw)
        if (hashlib.sha256(raw).hexdigest() != control.get("archive_sha256")
                or proof["root"] != str(paths.root.resolve())
                or proof["tasks"] != [t.to_dict() for t in TaskStore(paths).list()]
                or (paths.runtime / "pending-task-commit.json").exists()):
            raise StoreConflict("sealed tasks/root/checkpoints changed; preserve handoff and inspect")
        claimed = {**control, "phase": "claimed", "claimed_by": actor}
        _publish(paths, {**manifest, **current, "pid": None, "pid_namespace": None,
                         "started_at": None, "loop": False, "execution_epoch": request_id,
                         "writer_policy": "pinned-host", "handoff": claimed}, "FARM_CLAIMED", actor)
        return claimed
