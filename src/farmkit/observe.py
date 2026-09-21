"""Read-only Slurm observation for the worker side.

Semantics are pinned by tests/vectors/slurm_states.jsonl (the post-D6 "expect" column):
squeue is asked first; if it gives one line that is the state; if it errors, is
missing, or the job has left the queue, sacct is consulted; anything ambiguous or
failing is None (UNKNOWN). UNKNOWN never means "ended".
"""
from __future__ import annotations

import re
import shutil
import subprocess
import time
from collections.abc import Callable

JOB_ID = re.compile(r"[0-9]+(?:_[0-9]+)?")

ACTIVE_STATES = frozenset({"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED", "RESIZING"})
TERMINAL_STATES = frozenset({"COMPLETED", "FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL",
                             "CANCELLED", "BOOT_FAIL", "DEADLINE"})

Which = Callable[[str], "str | None"]
Run = Callable[..., subprocess.CompletedProcess]


def normalized(state: str | None) -> str:
    words = (state or "").strip().split()
    return words[0].upper().rstrip("+") if words else ""


def is_terminal(state: str | None) -> bool:
    return normalized(state) in TERMINAL_STATES


def is_active(state: str | None) -> bool:
    return normalized(state) in ACTIVE_STATES


def _run(run: Run, argv: list[str]) -> subprocess.CompletedProcess | None:
    try:
        return run(argv, text=True, capture_output=True, check=False, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return None


def slurm_state(job_id: str, *, which: Which = shutil.which, run: Run = subprocess.run) -> str | None:
    """State of ONE job or array element, or None when it cannot be established."""
    if not JOB_ID.fullmatch(job_id):
        return None
    if which("squeue"):
        proc = _run(run, ["squeue", "-h", "-j", job_id, "-o", "%T"])
        if proc is not None and proc.returncode == 0:
            lines = proc.stdout.strip().splitlines()
            if len(lines) == 1:
                return lines[0]
            if lines:
                return None  # several rows: no single fact
    # D6: sacct is consulted even when squeue is missing or failed.
    if which("sacct"):
        proc = _run(run, ["sacct", "-n", "-X", "-j", job_id, "--format=State%30"])
        if proc is not None and proc.returncode == 0:
            states = [line.strip().split()[0] for line in proc.stdout.splitlines() if line.strip()]
            if len(states) == 1:
                return states[0]
    return None


def sacct_row(job_id: str, *, which: Which = shutil.which, run: Run = subprocess.run) -> str | None:
    """One pipe-separated accounting row for evidence files; None if unavailable."""
    if not JOB_ID.fullmatch(job_id) or not which("sacct"):
        return None
    proc = _run(run, ["sacct", "-n", "-X", "-P", "-j", job_id,
                      "--format=JobID,JobName%40,State,ExitCode,Elapsed,End,Comment%60"])
    if proc is None or proc.returncode != 0:
        return None
    rows = [line for line in proc.stdout.splitlines() if line.strip()]
    return rows[0] if len(rows) == 1 else None


class StateCache:
    """Batch observation with a short TTL so a tick never asks Slurm twice for one job."""

    def __init__(self, *, ttl_s: float = 30.0, which: Which = shutil.which, run: Run = subprocess.run,
                 clock: Callable[[], float] = time.monotonic):
        self.ttl_s, self.which, self.run, self.clock = ttl_s, which, run, clock
        self._cache: dict[str, tuple[float, str | None]] = {}

    def states(self, job_ids: list[str]) -> dict[str, str | None]:
        now = self.clock()
        wanted = [j for j in job_ids if JOB_ID.fullmatch(j)]
        fresh = {j: v for j, (t, v) in self._cache.items() if j in wanted and now - t < self.ttl_s}
        missing = [j for j in wanted if j not in fresh]
        if missing:
            found = self._batch(missing)
            for j in missing:
                self._cache[j] = (now, found.get(j))
                fresh[j] = found.get(j)
        return {j: fresh.get(j) for j in job_ids}

    def _batch(self, job_ids: list[str]) -> dict[str, str | None]:
        out: dict[str, str | None] = {j: None for j in job_ids}
        queued: set[str] = set()
        if self.which("squeue"):
            proc = _run(self.run, ["squeue", "-h", "-j", ",".join(job_ids), "-o", "%i %T"])
            if proc is not None and proc.returncode == 0:
                for line in proc.stdout.splitlines():
                    parts = line.split()
                    if len(parts) == 2 and parts[0] in out:
                        out[parts[0]] = parts[1]
                        queued.add(parts[0])
        rest = [j for j in job_ids if j not in queued]
        if rest and self.which("sacct"):
            proc = _run(self.run, ["sacct", "-n", "-X", "-P", "-j", ",".join(rest), "--format=JobID,State"])
            if proc is not None and proc.returncode == 0:
                seen: dict[str, list[str]] = {}
                for line in proc.stdout.splitlines():
                    parts = line.strip().split("|")
                    if len(parts) >= 2 and parts[0] in out:
                        seen.setdefault(parts[0], []).append(parts[1].split()[0] if parts[1].strip() else "")
                for j, states in seen.items():
                    out[j] = states[0] if len(states) == 1 and states[0] else None
        return out
