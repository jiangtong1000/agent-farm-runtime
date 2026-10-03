"""Read-only access diagnosis over explicitly supplied sockets and current records."""
from __future__ import annotations

from pathlib import Path
import re

from . import Access
from .contract import AccessError, farm_id
from .observations import Observations
from .registry import Registry, read_json
from .roles import SHELLS, observe_role

REPAIR = ("Inspect the listed endpoints, explicitly choose the intended existing Master, and use access adopt "
          "with a new generation and the current digest. Preserve old sessions and records until reviewed.")


def diagnose(paths, *, registry_path=None, target=None, sockets=(), observations=None, access=None) -> list[tuple[str, str]]:
    observations = observations or Observations()
    checks, candidates = [], []
    socket_paths = list(dict.fromkeys(str(Path(path)) for path in sockets))
    if registry_path is not None or target is not None:
        if registry_path is None or target is None:
            checks.append(("WARN", "Access diagnosis requires both --access-registry and --access-target"))
        else:
            try:
                registry = Registry(Path(registry_path))
                access = access or Access(registry, observations=observations)
                _, record = registry.current(target)
                if record["farm_root"] != str(paths.root.resolve()):
                    raise AccessError("CONFLICT", "project_mismatch", "Published target belongs to a different farm")
                control = record["control"]
                if control["socket"] not in socket_paths:
                    socket_paths.append(control["socket"])
                # Probe roles even when daemon readiness later withholds access.
                observed = observations.control(control["socket"], control["session"], control["default_window"])
                if observed != control:
                    raise AccessError("STALE_REGISTRY", "control_replaced", "Published socket/session/window identity changed")
                role = record.get("endpoint_role")
                if role is None:
                    checks.append(("WARN", "Published endpoint has no adopted interactive process role; explicitly adopt the existing Master"))
                elif observe_role(observations, control, role["kind"], role["pid"]) != role:
                    raise AccessError("STALE_REGISTRY", "role_replaced", "Published role process identity changed")
                access.verify(paths, target)
                checks.append(("PASS", f"Published access target {target} is verified"))
            except AccessError as exc:
                checks.append(("FAIL", f"Access {exc.state}/{exc.reason}: {exc}. {REPAIR}"))
            except OSError:
                checks.append(("WARN", "Access storage cannot be inspected; retry read-only diagnosis"))
    for socket in socket_paths:
        try:
            observations.socket_identity(socket)
            raw = observations.command(["tmux", "-S", socket, "list-sessions", "-F", "#{session_id}|#{session_name}"])
            for line in raw.splitlines():
                fields = line.split("|")
                if len(fields) != 2 or re.fullmatch(r"\$[0-9]+", fields[0]) is None:
                    raise AccessError("UNREACHABLE", "session_observation", "Malformed session inventory")
                session_id, session_name = fields
                control = {"socket": socket, "session_id": session_id}
                environment = observations.environment({"control": control})
                if environment.get("FARM_ID") != farm_id(str(paths.root.resolve())):
                    continue
                role = environment.get("FARM_ENDPOINT_ROLE", "untyped")
                if role not in {"interactive", "daemon", "untyped"}:
                    checks.append(("WARN", "An inspected farm session has an invalid role marker; value withheld"))
                    role = "invalid"
                # Escape terminal control characters in operator-selected metadata.
                label = f"socket={socket!r} session={session_name!r} ({session_id}) role={role}"
                checks.append(("INFO", label))
                if role in {"interactive", "untyped"}:
                    candidates.append(label)
                    panes = observations.command(["tmux", "-S", socket, "list-panes", "-s", "-t", session_id,
                                                  "-F", "#{pane_dead}|#{pane_current_command}"])
                    rows = [row.split("|", 1) for row in panes.splitlines()]
                    if rows and all(len(row) == 2 and row[0] == "0" and row[1] in SHELLS for row in rows):
                        checks.append(("FAIL", f"Master candidate exposes only idle shells: {label}. {REPAIR}"))
                    deployment = read_json(paths.runtime / "deployment.json", missing="PENDING")
                    pid = deployment.get("pid")
                    if type(pid) is int:
                        try:
                            observe_role(observations, control, "daemon", pid)
                        except AccessError:
                            pass
                        else:
                            if role == "untyped":
                                if len(rows) == 1:
                                    checks.append(("FAIL", f"Daemon-only endpoint: the sole pane contains the reconciler and no interactive role is adopted: {label}. {REPAIR}"))
                                else:
                                    checks.append(("WARN", f"Untyped endpoint contains the reconciler; interactive Master role is unverified: {label}"))
        except AccessError as exc:
            checks.append(("WARN", f"Cannot inspect socket={socket}: {exc.state}/{exc.reason}; no alternate socket selected"))
    if len(candidates) > 1:
        checks.append(("FAIL", f"Multiple Master candidates ({len(candidates)}) claim this farm across the inspected sessions. {REPAIR}"))
    if not socket_paths and registry_path is None:
        checks.append(("INFO", "Access endpoints were not inspected; supply explicit --access-socket paths or registry/target"))
    return checks
