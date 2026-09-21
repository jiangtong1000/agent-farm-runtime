"""Read-only structured status for boards, watchers and health checks (D33, D38).

Nothing here writes farm state. `write_last_tick` is called by the daemon after
each reconcile pass; everything else only reads.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .adapters.filesystem import atomic_write_json
from .models import TaskState
from .procutil import host_identity
from .provenance import deployment_stamp, runtime_identity
from .store import FarmPaths, TaskStore


def write_last_tick(paths: FarmPaths, report: dict, observed_jobs: dict[str, str | None] | None = None,
                    *, deployment: dict | None = None) -> None:
    """Heartbeat of the control loop, independent of log verbosity (D33). `observed_jobs`
    are the scheduler states the daemon itself saw this pass: status, watch and the board
    read them instead of each querying Slurm again (D28/D29). The daemon supplies
    its startup identity; reading a newer manifest cannot certify another daemon."""
    counts = {key: len(value) for key, value in report.items() if isinstance(value, list)}
    atomic_write_json(paths.runtime / "last_tick.json",
                      {"ts": datetime.now(timezone.utc).isoformat(), "counts": counts,
                       **({"deployment": deployment_stamp(deployment)} if deployment is not None else {}),
                       **({"observed_jobs": observed_jobs} if observed_jobs is not None else {})})


def events_tail(paths: FarmPaths, n: int) -> dict:
    """The last n complete events, read from the end of the log without scanning it all."""
    path = paths.events / "log.ndjson"
    try:
        size = path.stat().st_size
    except OSError:
        return {"events": [], "cursor": 0, "truncated": False}
    chunk, data = 65536, b""
    with path.open("rb") as fh:
        pos = size
        while pos > 0 and data.count(b"\n") <= n:
            step = min(chunk, pos)
            pos -= step
            fh.seek(pos)
            data = fh.read(step) + data
    end = data.rfind(b"\n")
    complete = data[:end + 1] if end >= 0 else b""
    lines = complete.split(b"\n")[:-1][-n:]
    events = []
    for line in lines:
        try:
            events.append(json.loads(line))
        except ValueError:
            events.append({"type": "MALFORMED_EVENT", "raw": line[:200].decode("utf-8", "replace")})
    return {"events": events, "cursor": size - (len(data) - end - 1) if end >= 0 else 0, "truncated": False}


def _read_json(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _pid_alive(manifest: dict | None) -> bool | None:
    """Same judgement `farm stop --now` uses: host, boot, namespace and pid start time."""
    from .lifecycle import daemon_alive
    return daemon_alive(manifest)


def farm_status(paths: FarmPaths, *, now: datetime | None = None, slurm=None) -> dict:
    """Structured read-only status (D38). Flat keys are the contract farmkit reads.

    slurm: optional callable job_id -> state used for `observed_jobs`; defaults to the
    runtime's own Slurm observer (read-only squeue/sacct).
    """
    from .adapters.slurm import slurm_job_terminal, slurm_state
    now = now or datetime.now(timezone.utc)
    observe = slurm or slurm_state
    manifest = _read_json(paths.runtime / "deployment.json")
    last_tick = _read_json(paths.runtime / "last_tick.json")
    tick_age = None
    if last_tick and last_tick.get("ts"):
        try:
            when = datetime.fromisoformat(last_tick["ts"])
            tick_age = (now - when).total_seconds()
            last_tick = {**last_tick, "epoch": when.timestamp()}
        except ValueError:
            tick_age = None
    tasks = TaskStore(paths).list() if paths.tasks.exists() else []
    counts = {state.value: 0 for state in TaskState}
    for task in tasks:
        counts[task.state.value] += 1
    current = runtime_identity()
    deployment = None
    if manifest:
        deployment = {
            "farm_id": manifest.get("farm_id"), "farm_root": manifest.get("farm_root"),
            "execution_epoch": manifest.get("execution_epoch"),
            "scheduler_attestation": manifest.get("scheduler_attestation"),
            "host": manifest.get("host"), "pid": manifest.get("pid"),
            "started_at": manifest.get("started_at"), "protocol_version": manifest.get("protocol_version"),
            "source_root": manifest.get("source_root"), "source_sha256": manifest.get("source_sha256"),
            "executor": manifest.get("executor"), "session": manifest.get("session"),
            "interval": manifest.get("interval"), "loop": manifest.get("loop"),
        }
    source_matches = bool(manifest) and all(manifest.get(k) == current[k] for k in ("protocol_version", "source_sha256"))
    handoff = (manifest or {}).get("handoff") or None
    # Prefer what the daemon saw on its last pass (one observer, D28/D29); query the
    # scheduler only for jobs it has not reported, or when the daemon is stale.
    interval = float((manifest or {}).get("interval") or 30.0)
    fresh = tick_age is not None and tick_age <= 2 * interval
    seen = (last_tick or {}).get("observed_jobs") if fresh and last_tick else None
    observed_jobs = []
    for task in tasks:
        wait = task.metadata.get("waiting_on") or ""
        if task.state is TaskState.WAITING and wait.startswith("job:"):
            job_id = wait.split(":")[1]
            if seen is not None and job_id in seen:
                state, source = seen[job_id], "daemon"
            else:
                state, source = observe(job_id), "query"
            observed_jobs.append({"job_id": job_id, "task": task.id, "state": state,
                                  "terminal": slurm_job_terminal(state), "source": source})
    return {
        "farm_root": str(paths.root),
        "observed_at": now.isoformat(),
        # daemon
        "deployment": deployment,
        "pid": (manifest or {}).get("pid"),
        "pid_alive": _pid_alive(manifest),
        "interval": (manifest or {}).get("interval"),
        "last_tick": last_tick,
        "last_tick_age_s": tick_age,
        "source_matches": source_matches,
        "handoff": {"phase": handoff.get("phase"), "id": handoff.get("id"), "kind": handoff.get("kind")} if handoff else None,
        # pending markers
        "pending_task_commit": (paths.runtime / "pending-task-commit.json").exists(),
        "pending_recovery": (paths.runtime / "pending-recovery.json").exists(),
        "pending_deployment_event": bool((manifest or {}).get("pending_event")),
        # tasks
        "task_counts": counts,
        "tasks": [{"id": t.id, "state": t.state.value, "waiting_on": t.metadata.get("waiting_on"),
                   "revision": t.metadata.get("revision", 0),
                   "lease": {"worker_id": t.lease.worker_id, "lease_id": t.lease.lease_id} if t.lease else None}
                  for t in tasks],
        "observed_jobs": observed_jobs,
        "cli": {"protocol_version": current["protocol_version"], "source_sha256": current["source_sha256"]},
    }


def events_after(paths: FarmPaths, cursor: int, *, limit: int = 1000) -> dict:
    """Return complete event records after byte offset `cursor` and the new cursor.

    A trailing partial line (an append in progress) is not returned and the cursor
    stops before it, so a caller that persists the cursor never skips a record.
    Malformed complete lines are reported, not skipped silently.
    """
    path = paths.events / "log.ndjson"
    if cursor < 0:
        raise ValueError("cursor must be nonnegative")
    if not path.exists():
        return {"events": [], "cursor": 0, "truncated": False}
    with path.open("rb") as handle:
        handle.seek(0, 2)
        size = handle.tell()
        if cursor > size:
            raise ValueError("cursor is beyond the end of the event log; the log may have been replaced")
        handle.seek(cursor)
        data = handle.read()
    # Only complete lines are records; an append in progress stays for the next call.
    end = data.rfind(b"\n")
    complete = data[: end + 1] if end >= 0 else b""
    events: list[dict] = []
    position = cursor
    truncated = False
    for raw in complete.split(b"\n")[:-1]:
        if len(events) >= limit:
            truncated = True
            break
        if raw.strip():
            try:
                events.append(json.loads(raw.decode("utf-8")))
            except ValueError as exc:
                events.append({"malformed": raw.decode("utf-8", "replace"), "error": str(exc), "offset": position})
        position += len(raw) + 1
    return {"events": events, "cursor": position, "truncated": truncated}
