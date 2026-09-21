"""POSIX implementation: cooperative flock and fsync+replace persistence.

Filesystem locking/durability guarantees are deployment prerequisites. This is
not a distributed fencing service or a user authentication mechanism.
"""
from __future__ import annotations

import contextlib
import errno
import fcntl
import json
import os
import tempfile
import time
import math
from pathlib import Path

from .filesystem import FileLockBusy


def sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
        sync_directory(path.parent)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


@contextlib.contextmanager
def exclusive_lock(path: Path, *, blocking: bool, record_pid: bool = False,
                   timeout_seconds: float = 10.0):
    if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
        raise ValueError("lock timeout must be finite and nonnegative")
    path.parent.mkdir(parents=True, exist_ok=True)
    # Respect the caller's umask/ACL. We do not chmod another user's farm or
    # create globally shared state; each project's paths are supplied explicitly.
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o666)
    try:
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in {errno.EAGAIN, errno.EACCES}:
                    raise
                if not blocking or time.monotonic() >= deadline:
                    raise FileLockBusy(f"another writer holds {path}; lock not removed or replaced") from exc
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        try:
            if record_pid:
                os.ftruncate(fd, 0)
                os.write(fd, f"{os.getpid()}\n".encode())
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
