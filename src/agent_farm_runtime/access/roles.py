"""Explicit process-role adoption; no command-name or session-name inference.

The operator attests which process is the Master/daemon. We certify that exact
live process belongs to a unique pane of the explicitly selected session.
"""
from __future__ import annotations

import os
from pathlib import Path
import re

from ..procutil import host_identity
from .contract import AccessError, require

ROLES = {"interactive", "daemon"}
SHELLS = {"sh", "bash", "dash", "zsh", "fish", "ksh", "tcsh", "csh"}


def process(pid: int) -> dict:
    try:
        directory = Path(f"/proc/{pid}")
        owner = directory.stat().st_uid
        data = (directory / "stat").read_text()
        fields = data[data.rindex(")") + 2:].split()
        executable = Path(os.readlink(directory / "exe")).name.removesuffix(" (deleted)")
        value = {"parent": int(fields[1]), "starttime": int(fields[19]),
                 "shell": executable in SHELLS}
        require(fields[0] not in {"Z", "X"}, "SESSION_MISSING", "role_missing", "The adopted role process has exited")
        require(owner == os.getuid(), "AUTH_REQUIRED", "role_owner", "Role process belongs to another owner")
        return value
    except (FileNotFoundError, ProcessLookupError) as exc:
        raise AccessError("SESSION_MISSING", "role_missing", "The adopted role process has exited; explicitly adopt a replacement generation") from exc
    except PermissionError as exc:
        raise AccessError("AUTH_REQUIRED", "role_permission", "Cannot inspect the adopted role process") from exc
    except (OSError, ValueError, IndexError) as exc:
        raise AccessError("UNREACHABLE", "role_observation", "Cannot establish the adopted process identity") from exc


def observe_role(observations, control: dict, kind: str, pid: int) -> dict:
    require(isinstance(kind, str) and kind in ROLES and type(pid) is int and pid > 0,
            "CONFLICT", "role_identity", "Select an explicit role and positive process PID")
    observed = process(pid)
    require(not observed["shell"], "CONFLICT", "idle_shell",
            "The selected role PID is a shell. Select the running Master/daemon process; an idle shell cannot attest a role")
    raw = observations.command(["tmux", "-S", control["socket"], "list-panes", "-s", "-t", control["session_id"],
                                "-F", "#{pane_id}|#{pane_pid}|#{pane_dead}"], missing="session_missing")
    panes = []
    for line in raw.splitlines():
        parts = line.split("|")
        require(len(parts) == 3 and re.fullmatch(r"%[0-9]+", parts[0]) is not None
                and parts[1].isdigit() and parts[2] in {"0", "1"},
                "UNREACHABLE", "pane_observation", "Invalid tmux pane observation")
        if parts[2] == "0":
            panes.append((parts[0], int(parts[1])))
    ancestors, cursor = {pid}, observed["parent"]
    pane_pids = {pane_pid for _, pane_pid in panes}
    for _ in range(256):
        if ancestors & pane_pids:
            break
        if cursor <= 1 or cursor in ancestors:
            break
        ancestors.add(cursor)
        cursor = process(cursor)["parent"]
    matches = [pane for pane, pane_pid in panes if pane_pid in ancestors]
    require(len(matches) == 1, "CONFLICT", "role_membership",
            "Selected process must belong to exactly one live pane of the explicit session; no other session was selected")
    require(process(pid) == observed, "STALE_REGISTRY", "role_replaced", "Role process changed during adoption")
    namespace = host_identity()["pid_namespace"]
    require(bool(namespace), "UNREACHABLE", "role_namespace", "Cannot establish process namespace")
    return {"kind": kind, "pid": pid, "pid_starttime": observed["starttime"],
            "pid_namespace": namespace, "pane_id": matches[0]}


def validate_role(value: dict | None) -> None:
    if value is None:
        return
    require(isinstance(value, dict) and set(value) == {"kind", "pid", "pid_starttime", "pid_namespace", "pane_id"}
            and isinstance(value["kind"], str) and value["kind"] in ROLES and type(value["pid"]) is int and value["pid"] > 0
            and type(value["pid_starttime"]) is int and value["pid_starttime"] >= 0
            and isinstance(value["pid_namespace"], str) and bool(value["pid_namespace"])
            and isinstance(value["pane_id"], str) and re.fullmatch(r"%[0-9]+", value["pane_id"]) is not None,
            "CONFLICT", "role_schema", "Invalid adopted endpoint role")
