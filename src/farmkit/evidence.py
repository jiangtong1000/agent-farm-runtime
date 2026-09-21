"""FAILURE.md: what a human needs to rule, in plain names (D8)."""
from __future__ import annotations

from pathlib import Path

from ._fs import atomic_write_text
from .verify import Verdict


def stderr_tail(path: str | Path | None, lines: int = 40) -> str:
    if not path:
        return "(no stderr path recorded)"
    try:
        text = Path(path).read_text(errors="replace").splitlines()
    except OSError as exc:
        return f"(stderr unreadable: {exc})"
    return "\n".join(text[-lines:]) if text else "(stderr empty)"


def write_failure(attempt_dir: Path, attempt: dict, *, cls: str, sacct_row: str | None,
                  stderr_path: str | Path | None, verdict: Verdict | None, ruling: str | None,
                  budget_note: str) -> Path:
    sub = attempt.get("submit") or {}
    obs = attempt.get("observed") or {}
    lines = [
        f"# FAILURE — {attempt['step']} (attempt {attempt['attempt_id'][:8]}, run {attempt.get('n')})",
        "",
        f"Class: **{cls}**. {budget_note}",
        f"Park as: `{ruling}`" if ruling else "Not parked (retry in progress).",
        "",
        "## Job",
        f"- job id: {sub.get('job_id') or 'never confirmed'}",
        f"- scheduler state: {obs.get('state') or 'unknown'}",
        f"- sacct: `{sacct_row}`" if sacct_row else "- sacct: no row available",
        f"- submitted: {sub.get('intent_ts')}",
        f"- script: {attempt.get('code_snapshot') or '(no snapshot)'}",
        "",
        "## Checks",
    ]
    if verdict is None:
        lines.append("- verifier did not run (job did not complete)")
    else:
        lines += [f"- {r}" for r in verdict.reasons] or ["- all checks passed"]
        if verdict.checked:
            lines.append(f"- checked: {', '.join(verdict.checked)}")
    lines += ["", "## stderr (last 40 lines)", "```", stderr_tail(stderr_path), "```", ""]
    path = Path(attempt_dir) / "FAILURE.md"
    atomic_write_text(path, "\n".join(lines))
    return path
