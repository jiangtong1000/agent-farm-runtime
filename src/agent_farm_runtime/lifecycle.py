"""Stopping and restarting a farm's daemon as commands, not ceremonies (D36, D39, D5).

`stop --drain`   withholds new dispatch, asks every lease holder to check point and
                 exit, and lets `reconcile --loop` end by itself when no lease is left.
                 It reuses the drain control the reconciler already honours, marked
                 kind="stop" so release/claim refuse to treat it as a node handoff.
`stop --now`     terminates the daemon between two commits: the kill happens while
                 holding task-mutation.lock, which dispatch also holds, so a launch or
                 resume in progress is never cut in half.
`restart --to`   checks a candidate release, refuses it when protocol/ differs (that
                 case is "retire and start a new farm", D5), stops the daemon and
                 prints the exact command for the new release's wrapper.
"""
from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .adapters.filesystem import exclusive_lock
from .adapters.filesystem import atomic_write_json
from .events import EventLog
from .models import Event
from .procutil import host_identity, pid_identity_alive, proc_starttime
from .provenance import runtime_identity
from .store import FarmPaths, StoreConflict, StoreError, TaskStore, require_no_pending_recovery
from .turnover import _publish, deployment, finish_deployment_event


def _current_manifest(paths: FarmPaths) -> dict:
    manifest = finish_deployment_event(paths)
    if not manifest:
        raise StoreError("no deployment manifest: nothing to stop")
    current = runtime_identity()
    if manifest.get("host") != current["host"]:
        raise StoreError("the daemon belongs to another host; stop it there")
    if manifest.get("protocol_version") != current["protocol_version"]:
        raise StoreError("writer/deployment protocol mismatch; use the deployment's own release")
    return manifest


def stop_drain(paths: FarmPaths, *, actor: str) -> dict:
    """Persist a stop-drain control. Idempotent; refuses to override a real handoff."""
    if not actor.strip():
        raise StoreError("stop requires a nonempty actor")
    with exclusive_lock(paths.runtime / "task-mutation.lock", blocking=True):
        require_no_pending_recovery(paths)
        manifest = _current_manifest(paths)
        prior = manifest.get("handoff")
        if prior and prior.get("phase") != "claimed":
            if prior.get("kind") == "stop":
                return prior
            raise StoreConflict("a node handoff is pending; finish or recover it before stopping")
        control = {"id": f"stop-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
                   "kind": "stop", "phase": "draining", "target_host": None,
                   "from_host": manifest.get("host"), "actor": actor,
                   "ts": datetime.now(timezone.utc).isoformat()}
        if f"{control['id']}-FARM_STOP_DRAIN" in EventLog(paths.events / "log.ndjson").ids():
            raise StoreConflict("stop id already used; retry in a second")
        TaskStore(paths)._recover_locked()
        _publish(paths, {**manifest, "handoff": control}, "FARM_STOP_DRAIN", actor)
        return control


def clear_stop_control(paths: FarmPaths, *, actor: str) -> dict | None:
    """Drop a finished stop control at daemon startup (D36).

    Called with the mutation lock held, inside the single-reconciler lock, so the
    stopping daemon is gone by construction. Starting the daemon again through the
    pinned wrapper is the owner's explicit act, as the stop was. Returns the cleared
    control, or None when there was none. Idempotent across a crash between the
    event append and the manifest write.
    """
    manifest = deployment(paths)                 # {} on a farm that never ran: nothing to clear
    control = manifest.get("handoff")
    if not control or control.get("kind") != "stop":
        return None
    log = EventLog(paths.events / "log.ndjson")
    event = Event(id=f"{control['id']}-FARM_STOP_CLEARED", task_id="*", type="FARM_STOP_CLEARED", actor=actor,
                  ts=datetime.now(timezone.utc).isoformat(), payload={"handoff": control})
    if event.id not in log.ids():
        log.append(event)
    atomic_write_json(paths.runtime / "deployment.json", {k: v for k, v in manifest.items() if k != "handoff"})
    return control


def _daemon_pid(manifest: dict) -> int | None:
    pid = manifest.get("pid")
    if pid is None or manifest.get("started_at") is None:
        return None
    try:
        return int(pid)
    except (TypeError, ValueError):
        return None


def _process_alive(pid: int) -> bool:
    """Alive means present and not a zombie/dead entry (a reaped-later child counts as exited)."""
    try:
        data = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    try:
        return data[data.rindex(")") + 2:].split()[0] not in {"Z", "X"}
    except (ValueError, IndexError):
        return False


def daemon_alive(manifest: dict | None) -> bool | None:
    """Is the daemon the manifest describes alive on THIS host?

    True/False only when the manifest was written on this host and boot (and pid
    namespace, when recorded); None when it cannot be judged from here. A recorded
    pid_starttime must match, so a recycled pid never counts as the daemon (R07).
    """
    if not manifest or manifest.get("pid") is None or manifest.get("started_at") is None:
        return None
    current = host_identity()
    if manifest.get("host") != current["host"]:
        return None
    if manifest.get("boot_id") and current.get("boot_id") and manifest["boot_id"] != current["boot_id"]:
        return False                     # written before a reboot: that process cannot exist any more
    if manifest.get("pid_namespace") and current.get("pid_namespace") and manifest["pid_namespace"] != current["pid_namespace"]:
        return None                      # a pid from another namespace is not addressable here
    pid = _daemon_pid(manifest)
    if pid is None:
        return None
    start = manifest.get("pid_starttime")
    if start is None:
        return None                      # legacy manifest: a bare pid is not an identity (F01); restart the daemon to record one
    # identity (same pid AND same start time) and still runnable (not a zombie, F06)
    return pid_identity_alive(pid, int(start)) and _process_alive(pid)


def stop_now(paths: FarmPaths, *, actor: str, wait_seconds: float = 30.0,
             lock_timeout: float = 120.0, kill=os.kill) -> dict:
    """SIGTERM the recorded daemon pid while holding the mutation lock (D31, D39)."""
    if not actor.strip():
        raise StoreError("stop requires a nonempty actor")
    with exclusive_lock(paths.runtime / "task-mutation.lock", blocking=True, timeout_seconds=lock_timeout):
        manifest = _current_manifest(paths)
        pid = _daemon_pid(manifest)
        if pid is None:
            return {"stopped": False, "reason": "manifest records no running daemon (pid/started_at absent)"}
        if pid in (os.getpid(), os.getppid()):
            raise StoreError(f"manifest pid {pid} is this CLI or its parent, not a daemon; refusing to signal it")
        alive = daemon_alive(manifest)
        if alive is None:
            reason = ("manifest records no verifiable daemon identity (pid_starttime missing: written by an older release); "
                      "refusing to signal a bare pid; stop that daemon by hand" if manifest.get("pid_starttime") is None
                      else "manifest was written on another host or pid namespace; stop it there")
            return {"stopped": False, "pid": pid, "reason": reason}
        if not alive:
            return {"stopped": False, "pid": pid, "reason": "recorded daemon is not running on this host (exited, or pid from an earlier boot)"}
        try:
            kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return {"stopped": False, "pid": pid, "reason": "process exited before the signal"}
        except PermissionError as exc:
            raise StoreError(f"cannot signal pid {pid}: {exc}") from exc
        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline and _process_alive(pid):
            time.sleep(0.1)
        exited = not _process_alive(pid)
    return {"stopped": exited, "pid": pid, "actor": actor,
            "reason": None if exited else f"still running {wait_seconds}s after SIGTERM; inspect before repeating"}


# --- restart ------------------------------------------------------------------------

def protocol_digest(source_root: Path) -> str:
    """Content digest of <source_root>/agent_farm_runtime/protocol/ (files that define the on-disk contract)."""
    root = Path(source_root) / "agent_farm_runtime" / "protocol"
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")) if root.exists() else []:
        digest.update(path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def identity_of(source_root: Path, python: str | None = None) -> dict:
    """Identity of another checkout, computed by that checkout's own code in a clean interpreter."""
    code = ("import json, sys; sys.path.insert(0, sys.argv[1]); "
            "from agent_farm_runtime.provenance import runtime_identity; print(json.dumps(runtime_identity()))")
    proc = subprocess.run([python or sys.executable, "-I", "-S", "-c", code, str(source_root)],
                          text=True, capture_output=True, timeout=60)
    if proc.returncode != 0:
        raise StoreError(f"cannot import candidate release at {source_root}: {proc.stderr.strip()[:400]}")
    return json.loads(proc.stdout)


def restart_plan(paths: FarmPaths, *, target_src: Path, actor: str, python: str | None = None) -> dict:
    """Validate a same-protocol upgrade and produce the exact next command (D5, D39).

    Does not start anything: the new daemon must be started through the new
    release's pinned wrapper by the owner. Refuses when protocol/ differs.
    """
    if not actor.strip():
        raise StoreError("restart requires a nonempty actor")
    target_src = Path(target_src).resolve()
    manifest = _current_manifest(paths)
    if manifest.get("source_root") != runtime_identity()["source_root"]:
        raise StoreError("run restart from the release the daemon is currently using")
    candidate = identity_of(target_src, python)
    old_root = Path(manifest["source_root"]).parent
    verdict = {
        "current_source": manifest.get("source_sha256"),
        "candidate_source": candidate["source_sha256"],
        "same_protocol_version": candidate["protocol_version"] == manifest.get("protocol_version"),
        "protocol_dir_unchanged": protocol_digest(old_root) == protocol_digest(target_src),
    }
    if candidate["source_sha256"] == manifest.get("source_sha256"):
        raise StoreError("candidate is the running release; nothing to restart into")
    if not verdict["same_protocol_version"] or not verdict["protocol_dir_unchanged"]:
        raise StoreError("protocol/ changed between releases: retire this farm and start a new one "
                         "with the new release (D5); a restart cannot carry this .farm across")
    options = []
    for key, flag in (("executor", "--executor"), ("session", "--session"), ("tmux_socket", "--tmux-socket"),
                      ("interval", "--interval"), ("grace_seconds", "--grace-seconds"),
                      ("max_auto_restarts", "--max-auto-restarts")):
        if manifest.get(key) is not None:
            options += [flag, str(manifest[key])]
    if manifest.get("auto_unblock") is False:
        options.append("--no-auto-unblock")
    # No --writer-policy here: the pinned wrapper injects `--writer-policy pinned-host` itself
    # and a second copy after the subcommand is an argparse error (canary 2026-09-20).
    command = ["<new-release-farm-wrapper>", "--project", str(paths.root.parent), "reconcile",
               "--upgrade-from-source", str(manifest.get("source_sha256")), *options, "--loop"]
    return {**verdict, "next_command": command,
            "note": "start it with FARM_ACTUATION_ALLOWED=1 through the NEW release's wrapper on this host; "
                    "then rotate live workers so their receipt helper is refreshed"}
