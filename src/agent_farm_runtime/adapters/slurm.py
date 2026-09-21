"""Read-only SLURM observer; scheduler details do not belong to task storage."""
from __future__ import annotations

import re
import shutil
import subprocess


def slurm_state(job_id: str) -> str | None:
    """Observe SLURM only; never mutates jobs.

    squeue answers for jobs still in the queue; sacct answers for jobs that have
    left it. sacct is consulted whenever squeue gives no single answer, including
    when squeue is missing or fails (D6): a host that only exposes sacct, or a
    transient squeue error, must not leave a finished job UNKNOWN forever. A
    squeue answer that names several jobs/array elements is a real observation
    ("no single fact") and is returned as UNKNOWN without asking sacct.
    """
    if not re.fullmatch(r"[0-9]+(?:_[0-9]+)?", job_id):
        return None  # one job/array element, not a flag, list, or aggregate array
    if shutil.which("squeue"):
        try:
            proc = subprocess.run(["squeue", "-h", "-j", job_id, "-o", "%T"], text=True, capture_output=True, check=False, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            proc = None
        if proc is not None and proc.returncode == 0:
            state = proc.stdout.strip().splitlines()
            if len(state) == 1:
                return state[0]
            if state:
                return None  # several jobs/array elements: no single terminal fact
    if shutil.which("sacct"):
        try:
            proc = subprocess.run(["sacct", "-n", "-j", job_id, "--format=State%30", "-X"], text=True, capture_output=True, check=False, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return None
        if proc.returncode != 0:
            return None
        state = [line.strip().split()[0] for line in proc.stdout.splitlines() if line.strip()]
        if len(state) == 1:
            return state[0]
    return None


# States in which a SLURM job is still in the queue. Verbatim from the set that
# was previously inlined in inspect_workspace().
ACTIVE_SLURM_STATES = frozenset(
    {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED", "RESIZING"}
)

TERMINAL_SLURM_STATES = frozenset({
    "COMPLETED", "FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL",
    "CANCELLED", "BOOT_FAIL", "DEADLINE",
})


def normalized_slurm_state(state: str | None) -> str:
    words = (state or "").strip().split()
    return words[0].upper().rstrip("+") if words else ""


def slurm_job_terminal(state: str | None) -> bool:
    """Positive terminal observation only. Unknown is never evidence of ending."""
    return normalized_slurm_state(state) in TERMINAL_SLURM_STATES


def slurm_job_active(state: str | None) -> bool:
    """Known active state; its negation does NOT establish job completion."""
    return normalized_slurm_state(state) in ACTIVE_SLURM_STATES
