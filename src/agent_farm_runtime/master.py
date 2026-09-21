"""Master decisions: evidence is recorded here, scientific judgment stays outside."""
from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from .models import Task, TaskState
from .store import StoreConflict, StoreError, TaskStore
from .transitions import transition_task


def snapshot_file(path: str) -> dict:
    source = Path(path).resolve(strict=True)
    raw = source.read_bytes()
    if not raw.strip() or len(raw) > 65536:
        raise StoreError(f"{source}: expected nonempty UTF-8 evidence/context of at most 64 KiB")
    return {"path": str(source), "sha256": hashlib.sha256(raw).hexdigest(),
            "text": raw.decode("utf-8")}


def record_decision(store: TaskStore, task_id: str, *, expected_revision: int,
                    action: str, evidence_file: str, actor: str,
                    outcome: str | None = None, acceptance_file: str | None = None) -> Task:
    task = store.get(task_id)
    if task.metadata.get("revision", 0) != expected_revision:
        raise StoreConflict(f"{task_id}: revision changed; reread task-show before deciding")
    if not actor.strip():
        raise StoreError("master actor must be nonempty")
    record = {**snapshot_file(evidence_file), "actor": actor,
              "ts": datetime.now(timezone.utc).isoformat(), "action": action,
              "based_on_revision": expected_revision}
    patch = {"master_decisions": [*task.metadata.get("master_decisions", []), record]}
    if action == "accept":
        if task.state is not TaskState.SUBMITTED or task.metadata.get("rework_requested"):
            raise StoreError("accept requires SUBMITTED with no pending rework request")
        outcome = "accepted" if outcome is None else outcome
        if not isinstance(outcome, str) or not outcome.strip():
            raise StoreError("outcome must be a nonempty project-defined label")
        record.update(outcome=outcome, effective_acceptance=task.acceptance,
                      contract_revision=len(task.metadata.get("contract_revisions", [])),
                      contract_sha256=hashlib.sha256(task.acceptance.encode()).hexdigest())
        patch.update(acceptance_receipt=record, outcome=outcome)
        updated = transition_task(task, TaskState.DONE, acceptance_recorded=True,
                                  metadata_patch=patch, new_lease=None)
        event = "ACCEPTED"
    elif action == "amend":
        if task.state not in {TaskState.READY, TaskState.WAITING, TaskState.SUBMITTED, TaskState.BLOCKED}:
            raise StoreError("amend requires a non-running, non-terminal task; wait for a safe boundary")
        if not acceptance_file:
            raise StoreError("amend requires --acceptance-file")
        record.update(previous_acceptance=task.acceptance,
                      replacement=snapshot_file(acceptance_file))
        patch["contract_revisions"] = [*task.metadata.get("contract_revisions", []), record]
        updated = replace(task, acceptance=record["replacement"]["text"],
                          metadata={**task.metadata, **patch})
        event = "CONTRACT_AMENDED"
    elif action == "ruling" and task.state is TaskState.BLOCKED and task.metadata.get("recovery_hold"):
        if task.lease is not None:
            raise StoreError("recovery ruling requires a released lease")
        # Recovery never launches. Only a fresh master decision re-enables this
        # task, through the existing BLOCKED -> READY transition and executor.
        patch.update(recovery_hold=None, latest_master_instruction=record,
                     resume_requested=None, automatic_restarts=0, restart_limit_reached=False)
        updated = transition_task(task, TaskState.READY, metadata_patch=patch)
        event = "RECOVERY_RELEASED"
    elif action in {"ruling", "rework"}:
        from .turnover import clean_surrender
        required = TaskState.WAITING if action == "ruling" else TaskState.SUBMITTED
        if task.state is not required or (action == "ruling" and task.lease is None and not clean_surrender(task)):
            raise StoreError(f"{action} requires {required}" + (" with a lease" if action == "ruling" else ""))
        key = "resume_requested" if action == "ruling" else "rework_requested"
        if task.metadata.get(key):
            raise StoreError(f"{action} request already pending")
        patch.update({key: record["sha256"], "latest_master_instruction": record})
        updated = replace(task, metadata={**task.metadata, **patch})
        event = "RULING_RECORDED" if action == "ruling" else "REWORK_REQUESTED"
    else:
        raise StoreError(f"unknown decision: {action}")
    return store.commit(updated, expected=task, actor=actor, event_type=event,
                        payload={"decision": record})
