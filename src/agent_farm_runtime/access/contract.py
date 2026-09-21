"""Versioned access data, independent of the task/receipt protocol."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from ..provenance import farm_identity

SCHEMA_VERSION = 1
NAME = r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}"
EPOCH = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}"
SHA256 = r"[0-9a-f]{64}"
MARKERS = ("FARM_ID", "FARM_EXECUTION_EPOCH", "FARM_SLURM_JOB_ID", "FARM_ROOT",
           "FARM_ACCESS_TARGET", "FARM_SOURCE_SHA256", "FARM_PROTOCOL_VERSION")


class AccessError(RuntimeError):
    def __init__(self, state: str, reason: str, detail: str):
        super().__init__(detail)
        self.state, self.reason = state, reason


def require(condition, state: str, reason: str, detail: str) -> None:
    if not condition:
        raise AccessError(state, reason, detail)


def identifier(value: str, pattern: str = NAME) -> str:
    require(isinstance(value, str) and re.fullmatch(pattern, value) is not None,
            "CONFLICT", "invalid_identifier", "Invalid access identifier")
    return value


def target_name(value: str) -> str:
    require(isinstance(value, str) and len(value) <= 129, "CONFLICT", "invalid_target", "Invalid target")
    parts = value.split("/")
    require(1 <= len(parts) <= 2, "CONFLICT", "invalid_target", "Use NAME or SITE/NAME")
    for part in parts:
        identifier(part)
    return value


def digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def farm_id(root: str) -> str:
    """Stable across hosts at the same canonical .farm path, as turnover requires."""
    return farm_identity(root)["farm_id"]


def timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            raise ValueError("timezone missing")
        return parsed
    except (TypeError, ValueError) as exc:
        raise AccessError("CONFLICT", "invalid_timestamp", "Expected an ISO timestamp with timezone") from exc


def result(state: str, target: str | None, reason: str, detail: str, **extra) -> dict:
    return {"schema_version": SCHEMA_VERSION, "state": state, "target": target,
            "verified": state == "VERIFIED", "reason": reason, "detail": detail,
            "observed_at": datetime.now(timezone.utc).isoformat(), **extra}


def markers(record: dict) -> dict[str, str]:
    return dict(zip(MARKERS, (record["farm_id"], record["execution_epoch"], record["scheduler"]["job_id"],
                             record["farm_root"], record["target"], record["source_sha256"],
                             str(record["protocol_version"]))))


def validate_record(record: dict, target: str, epoch: str) -> dict:
    fields = {"schema_version", "target", "farm_id", "farm_root", "execution_epoch", "runtime_host",
              "protocol_version", "source_sha256", "scheduler", "control", "owner_uid", "published_at"}
    require(set(record) == fields and type(record.get("schema_version")) is int
            and record["schema_version"] == SCHEMA_VERSION,
            "CONFLICT", "record_schema", "Unsupported or invalid access record")
    require(record["target"] == target and record["execution_epoch"] == epoch,
            "CONFLICT", "record_identity", "Record does not match its target/epoch")
    identifier(epoch, EPOCH)
    root = record["farm_root"]
    require(isinstance(root, str) and Path(root).is_absolute() and Path(root).name == ".farm",
            "CONFLICT", "farm_root", "Record must name an absolute .farm root")
    require(record["farm_id"] == farm_id(root), "CONFLICT", "farm_id", "Farm ID does not match root")
    require(isinstance(record["runtime_host"], str) and bool(record["runtime_host"]),
            "CONFLICT", "host", "Missing runtime host")
    identifier(record["source_sha256"], SHA256)
    require(type(record["protocol_version"]) is int and record["protocol_version"] > 0
            and type(record["owner_uid"]) is int and record["owner_uid"] >= 0,
            "CONFLICT", "record_types", "Invalid protocol or owner")
    scheduler, control = record["scheduler"], record["control"]
    require(isinstance(scheduler, dict) and set(scheduler) == {"kind", "job_id", "allocation_started_at"}
            and scheduler["kind"] == "slurm", "CONFLICT", "scheduler", "Unsupported scheduler record")
    identifier(scheduler["job_id"], r"[1-9][0-9]*")
    require(isinstance(scheduler["allocation_started_at"], str) and bool(scheduler["allocation_started_at"]),
            "CONFLICT", "allocation_identity", "Missing allocation start identity")
    require(isinstance(control, dict) and set(control) == {
        "socket", "socket_device", "socket_inode", "session", "session_id", "default_window", "window_id",
        "server_pid", "server_starttime", "boot_id"}, "CONFLICT", "control", "Invalid control endpoint")
    require(isinstance(control["socket"], str) and Path(control["socket"]).is_absolute(),
            "CONFLICT", "socket_path", "Control socket must be an absolute path")
    identifier(control["session"])
    identifier(control["session_id"], r"\$[0-9]+")
    if control["default_window"] is not None:
        identifier(control["default_window"])
        identifier(control["window_id"], r"@[0-9]+")
    else:
        require(control["window_id"] is None, "CONFLICT", "window", "Unexpected window identity")
    require(all(type(control[k]) is int and control[k] >= 0
                for k in ("socket_device", "socket_inode", "server_pid", "server_starttime"))
            and control["server_pid"] > 0 and isinstance(control["boot_id"], str) and bool(control["boot_id"]),
            "CONFLICT", "server_identity", "Missing socket/server identity")
    timestamp(record["published_at"])
    return record
