"""One display model for every farmboard output (TUI, static HTML, plain text) — D28, D29.

Sources, all read-only:
  * runtime state through the pinned `farm` CLI (status --json, events --after,
    task-show --summary) via a farmkit RuntimeReader;
  * each live task's workspace: the farmkit attempts/ ledger, CHECKPOINT.md,
    REVIEW.md, attempts/<id>/FAILURE.md;
  * the agent CLI's own session log (codex rollout) for the last wake's input tokens.
Nothing here writes.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from farmkit.classify import parse_ruling
from farmkit.ledger import Ledger
from farmkit.observe import is_terminal
from farmkit.runtime_reader import tail_events

CLASSES = ("code", "science", "budget", "infra", "unknown", "owner")
RULING_RE = re.compile(r"^ruling:(?P<name>.+)$")


@dataclass
class Attempt:
    step: str
    attempt_id: str
    n: int
    job_id: str | None
    state: str | None
    verdict: str            # ok / fail / pending / -
    failure_class: str | None


@dataclass
class TaskCard:
    id: str
    state: str
    revision: int
    waiting_on: str | None
    worker_id: str | None
    lease_id: str | None
    workspace: str | None
    last_note: str | None
    objective: str | None
    generations: int
    input_tokens_last: int | None
    cached_tokens_last: int | None
    attempts: list[Attempt] = field(default_factory=list)
    in_flight_jobs: list[str] = field(default_factory=list)
    needs_you: str | None = None          # "ruling:<class>" | "submitted" | None
    ruling_class: str | None = None
    evidence: str | None = None           # FAILURE.md path if any
    review: str | None = None             # REVIEW.md path if present
    checkpoint: str | None = None
    actions: list[str] = field(default_factory=list)
    reasons_disabled: dict[str, str] = field(default_factory=dict)


@dataclass
class Board:
    farm: str
    observed_at: str
    status: dict
    needs_you: list[TaskCard]
    live: list[TaskCard]
    history: list[TaskCard]
    events: list[dict]
    token_budget_over: int


# --- rollout token usage -------------------------------------------------------------

def rollout_tokens(workspace: Path | None, worker_id: str | None,
                   sessions_root: Path | None = None) -> tuple[int | None, int | None]:
    """Input and cached tokens of the worker's last model call, from the codex rollout.

    Reads only the tail of the rollout file; None when unavailable (claude workers,
    no session yet, unreadable file).
    """
    if not workspace or not worker_id:
        return None, None
    sid_file = Path(workspace) / f".session_id_{worker_id}"
    try:
        sid = sid_file.read_text().strip()
    except OSError:
        return None, None
    root = sessions_root or Path(os.path.expanduser("~/.codex/sessions"))
    matches = list(root.rglob(f"*{sid}*.jsonl")) if root.exists() else []
    if not matches:
        return None, None
    path = matches[0]
    try:
        with path.open("rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - 400_000))
            tail = fh.read().decode("utf-8", "replace")
    except OSError:
        return None, None
    last = None
    for line in tail.splitlines():
        if '"token_usage_record"' not in line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("type") == "token_usage_record":
            last = rec
    if not last:
        return None, None
    usage = (last.get("payload") or {}).get("usage") or {}
    return usage.get("input_tokens"), usage.get("cached_input_tokens")


# --- classification of what needs the human -----------------------------------------

def ruling_class(waiting_on: str | None, task_id: str | None) -> str | None:
    """Same parser as farmkit watch (one rule for who may rule, D24)."""
    parsed = parse_ruling(waiting_on, task_id)
    return parsed["class"] if parsed else None


def _attempts(ledger: Ledger, observed_jobs: dict[str, str] | None = None) -> tuple[list[Attempt], list[str]]:
    """Ledger attempts, with the runtime's latest scheduler observation filling in the
    state of jobs the worker has not looked at since (status.observed_jobs)."""
    rows, in_flight = [], []
    for rec in ledger.all():
        submit = rec.get("submit") or {}
        observed = rec.get("observed") or {}
        verdict = rec.get("verdict")
        v = "-" if verdict is None else ("ok" if verdict.get("ok") else "fail")
        state = observed.get("state") if observed else None
        if observed_jobs and submit.get("job_id") and not is_terminal(state):
            state = observed_jobs.get(str(submit["job_id"])) or state     # newer than what the worker last saw
        if submit.get("job_id") and not is_terminal(state):     # queued or running: still in flight
            v = "pending" if v == "-" else v
            in_flight.append(str(submit["job_id"]))
        rows.append(Attempt(rec["step"], rec["attempt_id"], rec.get("n", 1), submit.get("job_id"), state, v,
                            (rec.get("failure") or {}).get("class")))
    rows.sort(key=lambda a: (a.step, a.n))
    return rows, in_flight


def _latest_failure_evidence(ledger: Ledger) -> str | None:
    best = None
    for rec in ledger.all():
        if rec.get("failure"):
            path = ledger.attempt_dir(rec["attempt_id"]) / "FAILURE.md"
            if path.exists():
                best = str(path)
    return best


def _actions_for(state: str, has_lease: bool) -> tuple[list[str], dict[str, str]]:
    """Which board actions are offered, and why the others are not (D30)."""
    enabled, disabled = [], {}
    if state == "WAITING" and has_lease:
        enabled += ["ruling", "rotate"]
    else:
        disabled["ruling"] = "requires WAITING with a lease" if state != "WAITING" else "task holds no lease (cleanly surrendered): use amend or wait"
        disabled["rotate"] = "requires a leased RUNNING/WAITING task"
    if state == "RUNNING" and has_lease:
        enabled.append("rotate")
        disabled.pop("rotate", None)
    if state == "SUBMITTED":
        enabled += ["accept", "rework"]
    else:
        disabled["accept"] = disabled["rework"] = "requires SUBMITTED"
    if state in {"READY", "WAITING", "SUBMITTED", "BLOCKED"}:
        enabled.append("amend")
    else:
        disabled["amend"] = "requires a non-running, non-terminal task"
    enabled.append("new-from")
    return enabled, disabled


def build(reader, *, farm: str, sessions_root: Path | None = None, token_budget_over: int = 120_000,
          event_limit: int = 12, now: datetime | None = None) -> Board:
    now = now or datetime.now(timezone.utc)
    status = reader.status()
    observed_jobs = {str(j.get("job_id")): j.get("state") for j in status.get("observed_jobs", []) if j.get("job_id")}
    cards: list[TaskCard] = []
    for t in status.get("tasks", []):
        summary = {}
        try:
            summary = reader.task_summary(t["id"])
        except Exception as exc:  # the board must render even if one task is unreadable
            summary = {"objective": f"(task-show failed: {exc})"}
        lease = t.get("lease") or {}
        workspace = summary.get("workspace")
        ledger = Ledger(Path(workspace) / "attempts") if workspace else None
        attempts, in_flight = _attempts(ledger, observed_jobs) if ledger else ([], [])
        tokens_in, tokens_cached = rollout_tokens(Path(workspace) if workspace else None,
                                                  lease.get("worker_id"), sessions_root)
        cls = ruling_class(t.get("waiting_on"), t["id"])
        needs = None
        if t["state"] == "SUBMITTED":
            needs = "submitted"
        elif cls:
            needs = f"ruling:{cls}"
        enabled, disabled = _actions_for(t["state"], bool(lease))
        ws = Path(workspace) if workspace else None
        card = TaskCard(
            id=t["id"], state=t["state"], revision=t.get("revision", 0), waiting_on=t.get("waiting_on"),
            worker_id=lease.get("worker_id"), lease_id=lease.get("lease_id"), workspace=workspace,
            last_note=summary.get("last_receipt_note"), objective=summary.get("objective"),
            generations=len(((summary.get("dispatches") or {}).get("workers") or [])) or (1 if lease else 0),
            input_tokens_last=tokens_in, cached_tokens_last=tokens_cached,
            attempts=attempts, in_flight_jobs=in_flight, needs_you=needs, ruling_class=cls,
            evidence=_latest_failure_evidence(ledger) if ledger else None,
            review=str(ws / "REVIEW.md") if ws and (ws / "REVIEW.md").exists() else None,
            checkpoint=str(ws / "CHECKPOINT.md") if ws and (ws / "CHECKPOINT.md").exists() else None,
            actions=enabled, reasons_disabled=disabled,
        )
        cards.append(card)
    needs_you = [c for c in cards if c.needs_you]
    history = [c for c in cards if c.state in {"DONE", "FAILED"}]
    live = [c for c in cards if c not in needs_you and c not in history]
    events = tail_events(reader, event_limit)
    return Board(farm=farm, observed_at=now.isoformat(), status=status, needs_you=needs_you, live=live,
                 history=history, events=events, token_budget_over=token_budget_over)
