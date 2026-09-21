from __future__ import annotations

import os
from pathlib import Path
import socket


def host_identity() -> dict[str, str | None]:
    """Host and Linux boot identity, not a cross-host liveness oracle."""
    try:
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip() or None
    except OSError:
        boot = None
    try:
        namespace = os.readlink("/proc/self/ns/pid")
    except OSError:
        namespace = None
    return {"host": socket.gethostname(), "boot_id": boot, "pid_namespace": namespace}


def observe_pidfile(path: str, identity: dict, attempt: str | None) -> bool | None:
    """Positive local-process evidence. Missing/legacy identity is UNKNOWN.

    The third pidfile field binds the PID to this launch/resume attempt. A delayed
    send or stale pidfile cannot establish that the current invocation has ended.
    """
    current = host_identity()
    if not identity.get("host") or identity.get("host") != current["host"]:
        return None
    if not identity.get("boot_id") or not current["boot_id"]:
        return None
    if identity["boot_id"] != current["boot_id"]:
        return False  # same host, a different boot: the old process cannot survive
    if not identity.get("pid_namespace") or identity["pid_namespace"] != current["pid_namespace"]:
        return None
    if not attempt:
        return None
    try:
        parts = Path(path).read_text().split()
        # Older generated scripts left the start-time field empty if a child
        # exited before capture. Accept only an exact current-attempt match;
        # a legacy two-field PID/start-time file is still unverified.
        if len(parts) == 2 and parts[1] == attempt:
            parts.insert(1, "unknown")
        if len(parts) != 3 or parts[2] != attempt:
            return None
        pid = int(parts[0])
        if pid <= 0:
            return None
    except (OSError, ValueError):
        return None
    try:
        data = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return False
    except OSError:
        return None
    try:
        fields = data[data.rindex(")") + 2:].split()
        start = int(parts[1])
        observed_start = int(fields[19])
        return observed_start == start and fields[0] not in {"Z", "X"}
    except (ValueError, IndexError):
        return None


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
