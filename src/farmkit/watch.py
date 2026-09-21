"""`farmkit watch`: the master's wake-up call (D10, D25, D32, D37).

Blocks until something a master should look at happens, prints one JSON line per hit
and exits, so a Claude Code background task notifies the master. The persisted
cursor is advanced only by `ack` (after the master handled the events or explicitly
left them to the Owner); until then the same events are replayed on the next watch.
The default timeout makes watch return with "nothing happened" so the master takes a
periodic look at the board even when no event fires (a stuck worker produces none).
"""
from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path

from ._fs import atomic_write_text
from .classify import parse_ruling
from .runtime_reader import RuntimeReader

DEFAULT_TIMEOUT_S = 6 * 3600


def split_cursor(token: str | None) -> tuple[str | None, set[str]]:
    """A cursor token is '<events cursor>' or '<events cursor>+<state hit id>[+...]'.
    The events part is the audit-log position; state ids name status-derived hits
    (daemon-down:<pid>, daemon-stalled:<tick>) the master already acknowledged."""
    if not token:
        return None, set()
    parts = str(token).split("+")
    return (parts[0] or None), {p for p in parts[1:] if p}


def join_cursor(events_cursor: str | None, state_ids: set[str]) -> str:
    return "+".join([events_cursor or "0", *sorted(state_ids)])


def read_cursor(cursor_file: Path) -> str | None:
    try:
        return Path(cursor_file).read_text().strip() or None
    except FileNotFoundError:
        return None


def ack(cursor_file: Path, cursor: str) -> None:
    """Advance the persisted cursor: 'these events have been handled or left to the Owner'."""
    atomic_write_text(Path(cursor_file), str(cursor) + "\n")


def classify_event(event: dict) -> dict | None:
    """Return a hit dict for events the master should see, else None."""
    etype = event.get("type")
    payload = event.get("payload") or {}
    task = event.get("task_id")
    if etype == "RECEIPT_APPLIED":
        waiting_on = payload.get("waiting_on") or ""
        if payload.get("to") == "SUBMITTED":
            return {"reason": "submitted", "task": task, "suggest": "review then task-accept or task-rework"}
        parsed = parse_ruling(waiting_on, task)
        if parsed:
            owner_only = parsed["owner_only"]
            return {"reason": "ruling", "task": task, "waiting_on": waiting_on, "class": parsed["class"],
                    "master_may_resolve": not owner_only, "suggest": "task-ruling" if not owner_only else "leave to Owner"}
        return None
    if etype == "RESTART_LIMIT_REACHED":
        return {"reason": "restart-limit", "task": task, "suggest": "inspect worker logs, then task-ruling"}
    if etype == "WORKER_ADOPTED":
        return {"reason": "adopted", "task": task, "worker": payload.get("worker_id"),
                "dead_worker": payload.get("dead_worker_id"), "suggest": "no action unless it repeats"}
    return None


def daemon_down(status: dict, *, now: float) -> dict | None:
    """Status-derived hit. Carries a stable `state_id` so an acknowledged failure is not
    re-reported until something changes (a new pid, a later tick that then stalls)."""
    if status.get("pid_alive") is False:
        return {"reason": "daemon-down", "detail": "reconciler process not running", "suggest": "Master inspects and restarts within Owner authorization",
                "state_id": f"daemon-down:{status.get('pid')}"}
    last = status.get("last_tick") or {}
    interval = float(status.get("interval") or 30.0)
    ts = last.get("epoch")
    if ts is not None and now - float(ts) > 3 * interval:
        return {"reason": "daemon-stalled", "detail": f"last tick {int(now - float(ts))} s ago",
                "suggest": "Master inspects reconciler.log and restarts if authorized and needed", "state_id": f"daemon-stalled:{ts}"}
    return None


def until_hit(until: str | None, status: dict, events: list[dict]) -> dict | None:
    if not until:
        return None
    kind, _, rest = until.partition(":")
    if kind == "job":
        for j in status.get("observed_jobs", []):
            if j.get("job_id") == rest and j.get("terminal"):
                return {"reason": "job", "job": rest, "state": j.get("state")}
    if kind == "file" and Path(rest).exists():
        return {"reason": "file", "path": rest}
    if kind == "task":
        for t in status.get("tasks", []):
            if t.get("id") == rest and t.get("state") in {"SUBMITTED", "DONE"}:
                return {"reason": "task", "task": rest, "state": t["state"]}
    return None


def watch(reader: RuntimeReader, cursor_file: Path, *, until: str | None = None,
          timeout_s: float = DEFAULT_TIMEOUT_S, poll_s: float = 15.0,
          clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep,
          start_cursor: str | None = None) -> dict:
    """Block until a hit; return {"hits": [...], "cursor": <new cursor>, "reason": ...}."""
    token = start_cursor if start_cursor is not None else read_cursor(cursor_file)
    acked_cursor, acked_states = split_cursor(token)
    scan = acked_cursor                     # scan position advances page by page; ack stays the master's
    deadline = clock() + timeout_s
    while True:
        status = reader.status()
        while True:                         # drain the backlog: a full page of noise must not hide later hits
            events, new_cursor = reader.events_after(scan)
            hits = [h for h in (classify_event(e) for e in events) if h]
            u = until_hit(until, status, events)
            if u:
                hits.append(u)
            if hits:
                return {"reason": hits[0]["reason"], "hits": hits, "cursor": join_cursor(new_cursor, acked_states), "acked": False}
            if not events or new_cursor == scan:
                break
            scan = new_cursor
        down = daemon_down(status, now=clock())
        if down and down["state_id"] not in acked_states:
            return {"reason": down["reason"], "hits": [down], "cursor": join_cursor(scan, acked_states | {down["state_id"]}), "acked": False}
        if clock() >= deadline:
            return {"reason": "timeout", "hits": [], "cursor": join_cursor(scan, acked_states), "acked": False}
        sleep(poll_s)


def format_hits(result: dict) -> str:
    """One JSON line per hit, then a cursor line the master passes to --ack."""
    lines = [json.dumps({**h, "cursor": result["cursor"]}, sort_keys=True) for h in result["hits"]]
    if not lines:
        lines.append(json.dumps({"reason": result["reason"], "cursor": result["cursor"]}, sort_keys=True))
    return "\n".join(lines)
