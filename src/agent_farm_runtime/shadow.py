from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class ShadowRecord:
    workspace: str
    observed_facts: dict
    v2_would: str
    reason: str

    def to_dict(self) -> dict:
        return asdict(self)


def codex_cwds() -> set[Path]:
    """Read-only Linux observation. Returns empty set when /proc/pgrep is unavailable."""
    if not Path("/proc").exists() or shutil.which("pgrep") is None:
        return set()
    proc = subprocess.run(["pgrep", "-x", "codex"], text=True, capture_output=True, check=False)
    out: set[Path] = set()
    for token in proc.stdout.split():
        try:
            out.add(Path(os.readlink(f"/proc/{int(token)}/cwd")).resolve())
        except (FileNotFoundError, PermissionError, ValueError):
            continue
    return out


def slurm_state(job_id: str) -> str | None:
    """Observe SLURM only; never mutates jobs."""
    if shutil.which("squeue"):
        proc = subprocess.run(["squeue", "-h", "-j", job_id, "-o", "%T"], text=True, capture_output=True, check=False)
        state = proc.stdout.strip().splitlines()
        if state:
            return state[0]
    if shutil.which("sacct"):
        proc = subprocess.run(["sacct", "-n", "-j", job_id, "--format=State", "-X"], text=True, capture_output=True, check=False)
        state = [line.strip().split()[0] for line in proc.stdout.splitlines() if line.strip()]
        if state:
            return state[0]
    return None


def parse_awaiting(text: str) -> tuple[str, str] | None:
    """Best-effort explicit-content parser. Never relies on mtime."""
    match = re.search(r"\b(job|artifact|ruling|task)\s*:\s*([^\s]+)", text, re.IGNORECASE)
    if not match:
        return None
    return match.group(1).lower(), match.group(2)


def inspect_workspace(workspace: Path, live_cwds: Iterable[Path] | None = None) -> ShadowRecord:
    workspace = workspace.resolve()
    live = set(live_cwds if live_cwds is not None else codex_cwds())
    facts: dict = {"live_executor": workspace in live}

    if facts["live_executor"]:
        return ShadowRecord(str(workspace), facts, "WOULD_NOOP", "live executor present")

    awaiting = workspace / ".awaiting"
    if awaiting.exists():
        raw = awaiting.read_text(encoding="utf-8", errors="replace")
        facts["awaiting_present"] = True
        parsed = parse_awaiting(raw)
        if parsed is None:
            facts["awaiting_explicit_ref"] = None
            return ShadowRecord(str(workspace), facts, "UNKNOWN(observability_gap:awaiting_not_structured)", ".awaiting exists but contains no explicit supported reference")
        kind, ref = parsed
        facts["awaiting_explicit_ref"] = f"{kind}:{ref}"

        if kind == "job":
            state = slurm_state(ref)
            facts["slurm_state"] = state
            if state is None:
                return ShadowRecord(str(workspace), facts, "UNKNOWN(observability_gap:job_state_unavailable)", "job state unavailable")
            active = state.upper().split("+")[0] in {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED", "RESIZING"}
            if active:
                return ShadowRecord(str(workspace), facts, f"WOULD_WAIT(job:{ref})", f"SLURM job is {state}")
            return ShadowRecord(str(workspace), facts, "WOULD_WAKE", f"SLURM job is no longer active ({state})")

        if kind == "artifact":
            target = Path(ref)
            if not target.is_absolute():
                target = workspace / target
            exists = target.exists()
            facts["artifact_exists"] = exists
            return ShadowRecord(str(workspace), facts, "WOULD_WAKE" if exists else f"WOULD_WAIT(artifact:{ref})", "artifact condition satisfied" if exists else "artifact condition not yet satisfied")

        return ShadowRecord(str(workspace), facts, f"UNKNOWN(observability_gap:{kind}_consumption_not_explicit)", "explicit dependency exists but consumption/unblock is not observable without new instrumentation")

    return ShadowRecord(str(workspace), facts, "UNKNOWN(observability_gap:no_explicit_wait_or_live_worker)", "no explicit existing fact is sufficient to infer the next action")
