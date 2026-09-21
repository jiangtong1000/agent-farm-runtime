"""Failure classes and budgets (RFC §3.7, D4 rule 4, D24).

infra   node/boot failure, preemption, transient sbatch trouble -> one automatic retry
code    traceback, non-zero exit, missing or stale output, cancelled chain -> park
science verifier science checks, non-finite metrics -> park, never loosen a threshold
budget  wall-time limit on production work -> park (the cap is part of the contract)
unknown scheduler could not be asked -> keep waiting, escalate after a while
"""
from __future__ import annotations

import re

from .observe import normalized
from .verify import Verdict

INFRA_STATES = frozenset({"NODE_FAIL", "BOOT_FAIL", "PREEMPTED"})
MAX_AUTO = {"infra": 1, "code": 0, "science": 0, "budget": 0, "unknown": 0}
MASTER_MAY_RESOLVE = frozenset({"code"})   # D24: everything else waits for the Owner
# Verifier reasons that mean "the output is not a usable product of this run" (code class),
# as opposed to "the numbers are wrong" (science class).
CODE_MARKERS = ("belongs to a different attempt", "required output missing", "is not readable JSON")


def classify(state: str | None, *, verdict: Verdict | None = None, output_present: bool = True,
             exit_code: int | None = None, stderr_tail: str = "") -> str | None:
    """None means 'not a failure'."""
    norm = normalized(state)
    if not norm:
        return "unknown"
    if norm in INFRA_STATES:
        return "infra"
    if norm == "TIMEOUT":
        return "budget"
    if norm in {"FAILED", "OUT_OF_MEMORY", "CANCELLED", "DEADLINE"}:
        return "code"
    if norm == "COMPLETED":
        if exit_code not in (None, 0):
            return "code"
        if not output_present:
            return "code"
        if verdict is not None and not verdict.ok:
            if any(marker in r for r in verdict.reasons for marker in CODE_MARKERS):
                return "code"
            return "science"
        return None
    return "unknown"


def sanitize(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", text).strip("-") or "x"


CLASSES = ("code", "science", "budget", "infra", "unknown")


def ruling_name(task_id: str | None, cls: str, step: str, attempt_id: str) -> str:
    """Wait reference for a parked failure; matches the runtime's `ruling:` grammar (non-empty)."""
    parts = [sanitize(task_id) if task_id else "task", cls, sanitize(step), attempt_id[:8]]
    return "ruling:" + "-".join(parts)


def parse_ruling(waiting_on: str | None, task_id: str | None) -> dict | None:
    """Inverse of ruling_name, shared by watch and the board (D24).

    Returns {"class", "owner_only"} or None when waiting_on is not a ruling. The task
    prefix is removed exactly (sanitized like ruling_name) before the class is read, so
    a task called "code-study" cannot turn a science park into a master-resolvable one.
    Anything that does not parse is owner-only.
    """
    if not waiting_on or not waiting_on.startswith("ruling:"):
        return None
    name = waiting_on[len("ruling:"):]
    if name.startswith("owner"):
        return {"class": "owner", "owner_only": True}
    prefix = (sanitize(task_id) if task_id else "task") + "-"
    if not name.startswith(prefix):
        return {"class": "unknown", "owner_only": True}
    head = name[len(prefix):].split("-", 1)[0]
    cls = head if head in CLASSES else "unknown"
    return {"class": cls, "owner_only": cls != "code"}


def retry_allowed(cls: str, used: int) -> bool:
    return used < MAX_AUTO.get(cls, 0)
