"""Persistent target bindings and immutable generations; readers never recover them."""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path

from ..adapters.filesystem import atomic_write_json, exclusive_lock, sync_directory
from .contract import (AccessError, EPOCH, SCHEMA_VERSION, GENERATION_SCHEMA_VERSION, digest,
                       identifier, require, target_name, validate_record)


def persistent_path(path: Path) -> Path:
    require(path.is_absolute(), "CONFLICT", "relative_storage", "Access storage must use an absolute path")
    path = path.resolve()
    # Durability/shared access require operator attestation and a site canary as well.
    # Reject known volatile locations, including symlinks into them, unconditionally.
    volatile = ("/tmp", "/var/tmp", "/dev/shm", "/run", "/private/tmp", "/private/var/tmp")
    require(not any(path.is_relative_to(p) for p in volatile), "CONFLICT", "volatile_storage",
            "Registry and farm root must be on persistent shared storage, not temporary storage")
    return path


def _unique_object(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError("duplicate JSON key")
        out[key] = value
    return out


def _invalid_constant(value):
    raise ValueError("non-finite JSON number")


def read_json(path: Path, *, missing: str = "UNPUBLISHED") -> dict:
    try:
        require(not path.is_symlink(), "CONFLICT", "symlink_record", "Access records must not be symlinks")
        with path.open("rb") as handle:
            raw = handle.read(65537)
        require(len(raw) <= 65536, "CONFLICT", "oversize_record", "Access record exceeds 64 KiB")
        value = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        require(isinstance(value, dict), "CONFLICT", "invalid_record", "Expected a JSON object")
        return value
    except FileNotFoundError as exc:
        raise AccessError(missing, "missing_record", f"Missing {path.name}") from exc
    except PermissionError as exc:
        raise AccessError("AUTH_REQUIRED", "storage_permission", f"Cannot read {path.name}") from exc
    except (ValueError, UnicodeError) as exc:
        raise AccessError("CONFLICT", "invalid_json", f"Invalid JSON in {path.name}") from exc
    except OSError as exc:
        raise AccessError("UNREACHABLE", "storage_unreachable", f"Cannot read {path.name}") from exc


def _directory(path: Path, *, create: bool = False) -> None:
    require(not path.is_symlink(), "CONFLICT", "symlink_directory", "Managed access directories cannot be symlinks")
    if create and not path.exists():
        path.mkdir(mode=0o700, exist_ok=True)
        require(not path.is_symlink(), "CONFLICT", "symlink_directory", "Access directory changed during creation")
    if path.exists():
        require(path.is_dir(), "CONFLICT", "not_directory", "Invalid access directory")
        if create:
            info = path.stat()
            require(info.st_uid == os.getuid() and not info.st_mode & (stat.S_IWGRP | stat.S_IWOTH),
                    "AUTH_REQUIRED", "directory_permissions", permission_detail(path, info, directory=True))
            # A prior attempt may have died between mkdir and parent fsync.
            sync_directory(path.parent)


def permission_detail(path: Path, info, *, directory: bool) -> str:
    mode = stat.S_IMODE(info.st_mode)
    preferred = "0700" if directory else "0600"
    return (f"{path.name}: observed mode {mode:04o}, owner UID {info.st_uid}; required owner UID "
            f"{os.getuid()} and no group/other write bits (recommended mode {preferred}). "
            "On ACL-enabled project storage inspect getfacl; provision a private registry directory "
            "and remove inherited access/default ACLs there if needed. Do not chmod the shared project root.")


def immutable(path: Path, payload: dict) -> None:
    """Caller holds the relevant lock; an existing value is only re-synchronized."""
    if path.exists() or path.is_symlink():
        require(read_json(path) == payload, "CONFLICT", "immutable_record", f"Conflicting {path.name}")
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
        sync_directory(path.parent)
    else:
        atomic_write_json(path, payload)


class Registry:
    def __init__(self, root: Path):
        self.root = persistent_path(root)

    def initialize(self, *, attest_shared_storage: bool) -> dict:
        require(attest_shared_storage, "CONFLICT", "storage_attestation",
                "Initialize only after provisioning and checking persistent shared storage")
        require(hasattr(os, "getuid"), "UNREACHABLE", "unsupported_platform", "Publication requires POSIX")
        # The parent is deliberately provisioned outside this command.
        _directory(self.root, create=True)
        with exclusive_lock(self.root / "registry.lock", blocking=True):
            value = {"schema_version": SCHEMA_VERSION, "storage": "persistent-shared", "owner_uid": os.getuid()}
            immutable(self.root / "registry.json", value)
        return value

    def configuration(self, *, writing: bool = False) -> dict:
        value = read_json(self.root / "registry.json")
        require(set(value) == {"schema_version", "storage", "owner_uid"}
                and type(value["schema_version"]) is int and value["schema_version"] == SCHEMA_VERSION
                and value["storage"] == "persistent-shared"
                and type(value["owner_uid"]) is int and value["owner_uid"] >= 0,
                "CONFLICT", "registry_schema", "Invalid or unsupported access registry")
        if writing:
            require(hasattr(os, "getuid") and value["owner_uid"] == os.getuid(),
                    "AUTH_REQUIRED", "registry_owner", "Publish as the registry owner")
            for path in (self.root, self.root / "registry.json"):
                info = path.stat()
                require(info.st_uid == os.getuid() and not info.st_mode & (stat.S_IWGRP | stat.S_IWOTH),
                        "AUTH_REQUIRED", "registry_permissions", permission_detail(path, info, directory=path.is_dir()))
        return value

    def target_dir(self, target: str, *, create: bool = False) -> Path:
        target_name(target)
        directory = self.root
        for part in ("targets", *target.split("/")):
            directory /= part
            _directory(directory, create=create)
        _directory(directory / "epochs")
        _directory(directory / "generations")
        return directory

    @staticmethod
    def pointer_path(directory: Path, role: str | None = None) -> Path:
        require(role in {None, "interactive", "daemon"}, "CONFLICT", "role", "Unknown endpoint role")
        return directory / ("current.daemon.json" if role == "daemon" else "current.json")

    def current(self, target: str, *, role: str | None = None) -> tuple[dict, dict]:
        self.configuration()
        directory = self.target_dir(target)
        pointer = read_json(self.pointer_path(directory, role))
        version = pointer.get("schema_version")
        fields = {"schema_version", "target", "execution_epoch", "record_sha256"}
        if version == GENERATION_SCHEMA_VERSION:
            fields.add("generation")
        require(set(pointer) == fields
                and type(version) is int and version in {SCHEMA_VERSION, GENERATION_SCHEMA_VERSION}
                and pointer["target"] == target, "CONFLICT", "pointer_schema", "Invalid current pointer")
        epoch = identifier(pointer["execution_epoch"], EPOCH)
        if version == GENERATION_SCHEMA_VERSION:
            generation = identifier(pointer["generation"], EPOCH)
            path = directory / "generations" / f"{generation}.json"
        else:
            path = directory / "epochs" / f"{epoch}.json"
        record = validate_record(read_json(path, missing="STALE_REGISTRY"), target, epoch)
        require(record["schema_version"] == version and
                (version == SCHEMA_VERSION or record["generation"] == pointer["generation"]),
                "CONFLICT", "generation_identity", "Current pointer and immutable generation differ")
        require(digest(record) == pointer["record_sha256"], "STALE_REGISTRY", "record_digest", "Current record digest mismatch")
        binding = read_json(directory / "binding.json", missing="STALE_REGISTRY")
        require(binding == self.binding(record), "CONFLICT", "target_binding", "Target belongs to a different farm")
        require(record["owner_uid"] == self.configuration()["owner_uid"],
                "CONFLICT", "owner_binding", "Record owner differs from registry")
        actual_role = (record.get("endpoint_role") or {}).get("kind")
        require(actual_role != "daemon" or role == "daemon", "CONFLICT", "required_role",
                "Daemon endpoints require an explicit daemon role; never use one for interactive attachment")
        if role is not None:
            require(actual_role == role, "CONFLICT", "required_role", "Endpoint lacks the explicitly requested process role; adopt it first")
        return pointer, record

    @staticmethod
    def binding(record: dict) -> dict:
        # The permanent farm binding predates endpoint generations and never changes.
        return {"schema_version": SCHEMA_VERSION,
                **{k: record[k] for k in ("target", "farm_id", "farm_root")}}

    @staticmethod
    def record_path(directory: Path, record: dict) -> Path:
        if record["schema_version"] == GENERATION_SCHEMA_VERSION:
            return directory / "generations" / f"{record['generation']}.json"
        return directory / "epochs" / f"{record['execution_epoch']}.json"

    def prepare(self, directory: Path, record: dict) -> dict:
        """Write the immutable generation before current. Publication holds both locks."""
        immutable(directory / "binding.json", self.binding(record))
        path = self.record_path(directory, record)
        _directory(path.parent, create=True)
        try:
            prior = validate_record(read_json(path), record["target"], record["execution_epoch"])
        except AccessError as exc:
            if exc.state != "UNPUBLISHED":
                raise
        else:
            # Publication time is an audit field, never a generation-ordering signal.
            record = {**record, "published_at": prior["published_at"]}
        immutable(path, record)
        return record

    def commit(self, directory: Path, record: dict) -> dict:
        pointer = {"schema_version": record["schema_version"], "target": record["target"],
                   "execution_epoch": record["execution_epoch"], "record_sha256": digest(record)}
        if record["schema_version"] == GENERATION_SCHEMA_VERSION:
            pointer["generation"] = record["generation"]
            role = (record.get("endpoint_role") or {}).get("kind")
            current = self.pointer_path(directory, role)
            observed = self.current(record["target"], role="daemon" if role == "daemon" else None)[0]["record_sha256"] if current.exists() or current.is_symlink() else None
            require(observed == record["previous_record_sha256"], "STALE_REGISTRY", "expected_current",
                    "Current endpoint changed; resolve the target and explicitly review a new rotation")
        atomic_write_json(self.pointer_path(directory, (record.get("endpoint_role") or {}).get("kind")), pointer)
        return pointer
