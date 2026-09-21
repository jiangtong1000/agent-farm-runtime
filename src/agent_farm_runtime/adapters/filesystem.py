"""Platform boundary for the durable JSON backend.

Read-only models/store/CLI imports do not require POSIX. An unsupported write
backend fails before opening a lock or changing files, never silently drops
locking or durability. Additional OS backends must implement this same surface.
"""
from __future__ import annotations

import os
from pathlib import Path


class FilesystemCapabilityError(RuntimeError):
    pass


class FileLockBusy(RuntimeError):
    pass


def _implementation():
    if os.name != "posix":
        raise FilesystemCapabilityError(
            "durable JSON writes currently require the POSIX filesystem backend; "
            "read-only inspection is available, but this platform has no supported write backend"
        )
    try:
        from . import posix_filesystem
    except ImportError as exc:
        raise FilesystemCapabilityError("POSIX filesystem backend requires fcntl locking") from exc
    return posix_filesystem


def require_writable_filesystem() -> None:
    _implementation()


def atomic_write_json(path: Path, payload: dict) -> None:
    _implementation().atomic_write_json(path, payload)


def sync_directory(path: Path) -> None:
    _implementation().sync_directory(path)


def exclusive_lock(path: Path, *, blocking: bool, record_pid: bool = False,
                   timeout_seconds: float = 10.0):
    return _implementation().exclusive_lock(path, blocking=blocking, record_pid=record_pid,
                                            timeout_seconds=timeout_seconds)
