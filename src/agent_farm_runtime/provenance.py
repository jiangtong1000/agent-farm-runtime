"""Identify the code actually imported at daemon startup, not just a git label."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from pathlib import Path
from .procutil import host_identity


from .protocol import PROTOCOL_VERSION  # noqa: E402  (protocol/ is the single source)


def farm_identity(root: str) -> dict:
    """Deployment identity at the canonical .farm path (shared across turnover)."""
    return {"farm_root": root, "farm_id": "farm-" + hashlib.sha256(root.encode()).hexdigest()}


def deployment_stamp(manifest: dict) -> dict:
    """Bind completed heartbeats to the exact daemon, build and execution epoch."""
    keys = ("farm_id", "farm_root", "execution_epoch", "host", "boot_id", "pid_namespace",
            "pid", "pid_starttime", "started_at", "source_sha256", "protocol_version", "scheduler_attestation")
    return {key: manifest.get(key) for key in keys}


def runtime_identity() -> dict:
    root = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(path.read_bytes() + b"\0")
    return {"protocol_version": PROTOCOL_VERSION, "source_root": str(root),
            "source_sha256": digest.hexdigest(), "python": sys.executable,
            "pid": os.getpid(), **host_identity(),
            "capabilities": ["cas-decisions", "recoverable-audit", "workspace-reservation",
                             "positive-terminal-wait", "pinned-context", "tri-state-liveness",
                             "bounded-dispatch", "restart-budget", "task-summary", "planned-turnover",
                             "control-access-v1"]}


def require_compatible_writer(paths, *, policy: str = "compatible",
                              upgrade_from_source: str | None = None) -> None:
    """Check protocol compatibility; strict deployment pinning is opt-in.

    Installation path, interpreter and user are provenance, not protocol identity.
    pinned-host is a conservative operator policy, not a distributed lock.
    Only reconciler startup exposes the explicit source-upgrade option; it
    checks the old digest and never relaxes protocol or host matching.
    """
    from .models import TaskState
    from .store import StoreError, TaskStore, require_no_pending_recovery
    require_no_pending_recovery(paths)
    if policy not in {"compatible", "pinned-host"}:
        raise StoreError(f"unknown writer policy: {policy}")
    if upgrade_from_source is not None:
        if policy != "pinned-host" or not re.fullmatch(r"[0-9a-f]{64}", upgrade_from_source):
            raise StoreError("source upgrade requires pinned-host and the exact previous SHA-256")
    manifest = paths.runtime / "deployment.json"
    if not manifest.exists():
        if upgrade_from_source is not None:
            raise StoreError("source upgrade requires an existing deployment manifest")
        if policy == "compatible":
            return  # offline operation; no claim about an unobserved daemon
        tasks = TaskStore(paths).list()
        if not tasks or all(t.state is TaskState.READY and t.metadata.get("revision", 0) >= 1 for t in tasks):
            return  # new, not-yet-launched farm bootstrap
        raise StoreError("daemon version unverified: controlled upgrade/restart required before master writes")
    recorded = json.loads(manifest.read_text())
    if not isinstance(recorded, dict):
        raise StoreError("invalid deployment manifest; preserve it and inspect before writing")
    current = runtime_identity()
    keys = ("protocol_version",)
    if policy == "pinned-host":
        keys += ("host",)
        if upgrade_from_source is None:
            keys += ("source_sha256",)
    if any(recorded.get(key) != current[key] for key in keys):
        raise StoreError(f"writer/daemon mismatch under {policy} policy; inspect protocol/deployment before writing")
    if upgrade_from_source is not None:
        if recorded.get("source_sha256") != upgrade_from_source:
            raise StoreError("source upgrade mismatch: deployment changed or previous SHA-256 is wrong")
        if (paths.runtime / "pending-task-commit.json").exists():
            raise StoreError("finish/inspect the pending task transaction with its original writer before upgrading")


def require_local_executor_host(paths) -> None:
    """A startup manifest must not silently claim another host's active leases."""
    from .store import StoreError, TaskStore
    if not any(task.lease for task in TaskStore(paths).list()):
        return
    manifest = paths.runtime / "deployment.json"
    try:
        previous = json.loads(manifest.read_text())
    except (ValueError, OSError) as exc:
        raise StoreError("active leases without verified prior deployment; inspect old writers before restart") from exc
    if previous.get("host") != host_identity()["host"]:
        raise StoreError("active leases belong to another deployment host; no automatic cross-host takeover")
