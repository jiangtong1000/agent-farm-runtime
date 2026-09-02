from __future__ import annotations

import os


def proc_starttime(pid: int) -> int | None:
    """Linux process start-time (jiffies since boot; /proc/<pid>/stat field 22).

    (pid, starttime) is a stable, unique process identity for the life of a boot:
    it distinguishes a live process from a DIFFERENT process that later reused the
    same pid. `comm` may contain spaces/parens, so parse after the final ')'.
    """
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
            data = fh.read()
    except (FileNotFoundError, ProcessLookupError, OSError):
        return None
    try:
        after = data[data.rindex(")") + 2:].split()
        return int(after[19])  # field 22 == starttime; after ')' starts at field 3
    except (ValueError, IndexError):
        return None


def pid_identity_alive(pid: int | None, starttime: int | None) -> bool:
    """True iff `pid` is alive AND (when known) is the SAME process we launched.

    A recorded starttime that no longer matches means the pid was recycled to a
    different process -> treat as dead. Without a recorded starttime, fall back to
    plain liveness.
    """
    if pid is None:
        return False
    current = proc_starttime(pid)
    if current is None:
        return False
    if starttime is None:
        return True
    return current == starttime


def read_pidfile(path: str) -> tuple[int | None, int | None]:
    """Parse a '<pid> <starttime>' identity file. Missing/garbled -> (None, None)."""
    try:
        with open(path, encoding="utf-8") as fh:
            parts = fh.read().split()
    except (FileNotFoundError, OSError):
        return None, None
    try:
        pid = int(parts[0])
    except (IndexError, ValueError):
        return None, None
    start = None
    if len(parts) > 1:
        try:
            start = int(parts[1])
        except ValueError:
            start = None
    return pid, start


def reap_children() -> None:
    """Best-effort reap of any exited children, so a long-lived reconciler --loop
    does not accumulate zombies for workers it launched and no longer tracks."""
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        except OSError:
            return
        if pid == 0:
            return
