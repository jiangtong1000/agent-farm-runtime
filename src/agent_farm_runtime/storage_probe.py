"""Bounded, disposable evidence of the storage primitives this backend uses."""
from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile

from .adapters.filesystem import exclusive_lock, sync_directory


_LOCK_CHILD = """
import errno, fcntl, sys
with open(sys.argv[1], 'r+') as handle:
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno not in (errno.EACCES, errno.EAGAIN):
            raise
        print('BUSY')
    else:
        print('ACQUIRED')
"""


def _lock_observation(path: Path) -> str:
    proc = subprocess.run([sys.executable, "-I", "-c", _LOCK_CHILD, str(path)],
                          capture_output=True, text=True, timeout=10, check=True)
    return proc.stdout.strip()


def probe_storage(directory: Path) -> dict:
    """Never chmod or edit existing files. Passing proves local operations only."""
    path = directory.expanduser().resolve(strict=True)
    if not path.is_dir():
        raise ValueError("storage-probe requires an existing directory")
    info = path.stat()
    result = {
        "schema_version": 1, "path": str(path), "host": socket.gethostname(),
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "uid": os.getuid() if hasattr(os, "getuid") else None,
        "directory_uid": info.st_uid, "directory_mode": f"{stat.S_IMODE(info.st_mode):04o}",
        "device": info.st_dev, "filesystem_type": None, "ok": False, "checks": [],
        "scope": "Local process exclusion and filesystem operations only; shared visibility, cross-node exclusion and persistence across host failure require separate site evidence.",
    }
    if sys.platform.startswith("linux"):
        try:
            proc = subprocess.run(["stat", "-f", "-c", "%T", "--", str(path)],
                                  capture_output=True, text=True, timeout=5, check=True)
            result["filesystem_type"] = proc.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            pass  # Informational; operation results below determine success.
    check = "temporary_directory"
    try:
        with tempfile.TemporaryDirectory(prefix=".farm-storage-probe-", dir=path) as scratch:
            root = Path(scratch)
            check = "flock_exclusion"
            lock = root / "lock"
            with exclusive_lock(lock, blocking=False):
                if _lock_observation(lock) != "BUSY":
                    raise OSError("a separate process acquired the held exclusive lock")
            if _lock_observation(lock) != "ACQUIRED":
                raise OSError("a separate process could not acquire the released lock")
            result["checks"].append({"name": check, "status": "pass",
                                     "detail": "independent process excluded while held and admitted after release"})
            check = "file_fsync"
            source, target = root / "source", root / "target"
            target.write_bytes(b"previous")
            with source.open("wb") as handle:
                handle.write(b"farm-storage-probe\n")
                handle.flush()
                os.fsync(handle.fileno())
            result["checks"].append({"name": check, "status": "pass", "detail": "file fsync completed"})
            check = "atomic_replace"
            os.replace(source, target)
            if source.exists() or target.read_bytes() != b"farm-storage-probe\n":
                raise OSError("replacement contents did not match")
            result["checks"].append({"name": check, "status": "pass", "detail": "same-directory replace completed and contents matched"})
            check = "directory_fsync"
            sync_directory(root)
            result["checks"].append({"name": check, "status": "pass", "detail": "directory fsync completed"})
            check = "temporary_cleanup"
        result["checks"].append({"name": check, "status": "pass", "detail": "probe directory removed"})
        result["ok"] = True
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        result["checks"].append({"name": check, "status": "fail", "detail": str(exc)})
    return result
