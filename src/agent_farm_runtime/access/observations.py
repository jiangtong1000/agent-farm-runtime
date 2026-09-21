"""Bounded, exact Slurm and tmux probes. No job/session creation or discovery."""
from __future__ import annotations

import os
import re
import stat
import subprocess
from pathlib import Path

from ..procutil import host_identity, proc_starttime
from .contract import AccessError, MARKERS, identifier, markers, require


class Observations:
    def __init__(self, *, run=None):
        self.run = run or subprocess.run

    def command(self, argv: list[str], *, missing: str | None = None) -> str:
        env = {**os.environ, "LC_ALL": "C"}
        env.pop("TMUX", None)  # Never inherit the worker executor's selected server.
        try:
            out = self.run(argv, capture_output=True, text=True, timeout=10, env=env)
        except PermissionError as exc:
            raise AccessError("AUTH_REQUIRED", "command_permission", "Observation permission denied") from exc
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise AccessError("UNREACHABLE", "observation_unreachable", "Observation command unavailable or timed out") from exc
        if out.returncode:
            error = (out.stderr or "").lower()
            if "permission denied" in error or "access denied" in error:
                raise AccessError("AUTH_REQUIRED", "command_permission", "Observation permission denied")
            if missing and ("can't find session:" in error or "can't find window:" in error):
                raise AccessError("SESSION_MISSING", missing, "The exact control endpoint is missing")
            raise AccessError("UNREACHABLE", "observation_failed", "Observation command failed; no fallback attempted")
        require(isinstance(out.stdout, str) and len(out.stdout) <= 1024 * 1024,
                "UNREACHABLE", "observation_size", "Invalid or oversized observation")
        return out.stdout

    def scheduler(self, job_id: str) -> dict:
        identifier(job_id, r"[1-9][0-9]*")
        raw = self.command(["scontrol", "--local", "--oneliner", "show", "job", job_id])
        require(len(raw.strip().splitlines()) == 1, "UNREACHABLE", "scheduler_rows", "Expected exactly one allocation")
        fields = {}
        for key in ("JobId", "JobState", "UserId", "NodeList", "StartTime"):
            values = re.findall(r"(?:^|\s)" + key + r"=(\S+)", raw)
            require(len(values) == 1, "UNREACHABLE", "scheduler_fields", "Missing or ambiguous allocation fields")
            fields[key] = values[0]
        require(fields["JobId"] == job_id, "CONFLICT", "scheduler_job_id", "Scheduler returned a different job")
        uid = re.fullmatch(r"[^()]+\(([0-9]+)\)", fields["UserId"])
        require(uid is not None, "UNREACHABLE", "scheduler_uid", "Unrecognized scheduler owner")
        nodes = []
        if fields["JobState"] == "RUNNING":
            require(fields["NodeList"] not in {"(null)", "None", "N/A"}
                    and fields["StartTime"] not in {"Unknown", "N/A", "None"},
                    "UNREACHABLE", "allocation_identity", "Running allocation lacks node/start identity")
            nodes = self.command(["scontrol", "show", "hostnames", fields["NodeList"]]).splitlines()
            require(nodes and len(nodes) == len(set(nodes)) and all(re.fullmatch(r"[A-Za-z0-9_.-]+", n) for n in nodes),
                    "UNREACHABLE", "allocation_nodes", "Invalid allocation node list")
        return {"job_id": job_id, "state": fields["JobState"], "nodes": nodes,
                "owner_uid": int(uid.group(1)), "allocation_started_at": fields["StartTime"]}

    @staticmethod
    def default_socket() -> str:
        require(hasattr(os, "getuid"), "UNREACHABLE", "unsupported_platform", "tmux verification requires POSIX")
        return str(Path(os.environ.get("TMUX_TMPDIR", "/tmp")) / f"tmux-{os.getuid()}" / "default")

    @staticmethod
    def socket_identity(socket: str) -> tuple[int, int]:
        path = Path(socket)
        require(path.is_absolute(), "CONFLICT", "socket_path", "Use an absolute control socket path")
        try:
            info = path.lstat()
        except FileNotFoundError as exc:
            raise AccessError("SESSION_MISSING", "socket_missing", "The exact control socket is missing") from exc
        except PermissionError as exc:
            raise AccessError("AUTH_REQUIRED", "socket_permission", "Control socket permission denied") from exc
        except OSError as exc:
            raise AccessError("UNREACHABLE", "socket_unreachable", "Control socket cannot be observed") from exc
        require(stat.S_ISSOCK(info.st_mode), "CONFLICT", "not_socket", "Control endpoint is not a Unix socket")
        require(info.st_uid == os.getuid(), "AUTH_REQUIRED", "socket_owner", "Control socket belongs to another user")
        return info.st_dev, info.st_ino

    def control(self, socket: str, session: str, window: str | None) -> dict:
        identifier(session)
        if window is not None:
            identifier(window)
        device, inode = self.socket_identity(socket)
        prefix = ["tmux", "-N", "-S", socket]
        # '=' disables tmux's usual prefix/glob fallback. Bind immutable IDs too.
        raw = self.command([*prefix, "display-message", "-p", "-t", f"={session}:",
                            "#{session_id}\t#{session_name}\t#{pid}"], missing="session_missing")
        parts = raw.strip().split("\t")
        require(len(parts) == 3 and parts[1] == session and re.fullmatch(r"\$[0-9]+", parts[0])
                and parts[2].isdigit(), "CONFLICT", "session_identity", "Unexpected tmux session identity")
        session_id, _, pid = parts
        window_id = None
        if window is not None:
            raw = self.command([*prefix, "list-windows", "-t", session_id,
                                "-F", "#{window_id}\t#{window_index}\t#{window_name}"], missing="session_missing")
            rows = [line.split("\t") for line in raw.splitlines()]
            require(all(len(row) == 3 and re.fullmatch(r"@[0-9]+", row[0]) and row[1].isdigit() for row in rows),
                    "UNREACHABLE", "window_observation", "Invalid window observation")
            matches = [row for row in rows if row[1 if window.isdecimal() else 2] == window]
            require(matches, "SESSION_MISSING", "window_missing", "The exact default window is missing")
            require(len(matches) == 1, "CONFLICT", "ambiguous_window", "Default window name is not unique")
            window_id = matches[0][0]
        boot = host_identity()["boot_id"]
        start = proc_starttime(int(pid))
        require(start is not None and bool(boot), "UNREACHABLE", "server_process", "Cannot bind the local tmux server process")
        require((device, inode) == self.socket_identity(socket), "CONFLICT", "socket_changed", "Socket changed during observation")
        return {"socket": socket, "socket_device": device, "socket_inode": inode,
                "session": session, "session_id": session_id, "default_window": window, "window_id": window_id,
                "server_pid": int(pid), "server_starttime": start, "boot_id": boot}

    def environment(self, record: dict) -> dict[str, str]:
        control = record["control"]
        raw = self.command(["tmux", "-N", "-S", control["socket"], "show-environment", "-t", control["session_id"]],
                           missing="session_missing")
        # Do not retain or expose unrelated (possibly secret) session environment.
        return {key: value for line in raw.splitlines() if "=" in line
                for key, value in [line.split("=", 1)] if key in MARKERS}

    def bind(self, record: dict) -> None:
        expected, observed = markers(record), self.environment(record)
        require(all(value == expected[key] for key, value in observed.items()),
                "CONFLICT", "marker_conflict", "Control session already has a different farm/epoch binding")
        control = record["control"]
        for key, value in expected.items():
            if key not in observed:
                self.command(["tmux", "-N", "-S", control["socket"], "set-environment",
                              "-t", control["session_id"], key, value], missing="session_missing")
        require(self.environment(record) == expected, "CONFLICT", "marker_mismatch", "Session marker binding did not verify")
