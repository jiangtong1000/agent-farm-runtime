"""Read-only health findings from `farm status --json` (D33, D38). Pure function; no side effects."""
from __future__ import annotations

import time


def findings(status: dict, *, now: float | None = None) -> list[dict]:
    now = time.time() if now is None else now
    out: list[dict] = []
    level = lambda ok: "ok" if ok else "fail"     # noqa: E731

    alive = status.get("pid_alive")
    out.append({"check": "daemon process", "level": "ok" if alive else ("fail" if alive is False else "unknown"),
                "detail": f"pid {status.get('pid')}" if alive else "no live reconciler process recorded"})

    last = status.get("last_tick") or {}
    interval = float(status.get("interval") or 30.0)
    if last.get("epoch") is None:
        out.append({"check": "loop advancing", "level": "unknown", "detail": "no last_tick record (older release?)"})
    else:
        age = now - float(last["epoch"])
        out.append({"check": "loop advancing", "level": level(age <= 3 * interval),
                    "detail": f"last tick {int(age)} s ago (interval {int(interval)} s)"})

    if "source_matches" in status:
        out.append({"check": "code matches manifest", "level": level(bool(status["source_matches"])),
                    "detail": "CLI source equals the daemon's recorded source" if status["source_matches"]
                    else "CLI and daemon run different code; controlled restart required"})
    phase = (status.get("handoff") or {}).get("phase")
    if phase:
        out.append({"check": "handoff", "level": "warn" if phase in {"draining", "released"} else "ok",
                    "detail": f"phase {phase}"})
    for marker in ("pending_task_commit", "pending_recovery", "pending_deployment_event"):
        if status.get(marker):
            out.append({"check": marker, "level": "fail", "detail": "durable marker present; ordinary writes may be blocked"})
    counts = status.get("task_counts") or {}
    if counts:
        out.append({"check": "tasks", "level": "ok", "detail": ", ".join(f"{k} {v}" for k, v in sorted(counts.items()))})
    return out


def worst(findings_: list[dict]) -> str:
    order = {"fail": 3, "warn": 2, "unknown": 1, "ok": 0}
    return max((f["level"] for f in findings_), key=lambda l: order.get(l, 0), default="ok")
