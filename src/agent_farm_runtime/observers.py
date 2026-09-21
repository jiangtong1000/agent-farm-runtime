from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from pathlib import Path

from .models import Task, TaskState
from .adapters.slurm import normalized_slurm_state, slurm_job_terminal, slurm_state
from .store import FarmPaths, TaskStore

# The mechanical WAITING->RUNNING unblock predicate: the runtime's replacement
# for the live farm's nudger. It evaluates ONLY explicit, named conditions
# recorded in metadata.waiting_on (INV-4: state, not clock). It makes no
# scientific judgment; `ruling:` conditions are intentionally left to the master.
#
# Supported waiting_on forms:
#   job:<id>[:end]      unblock on a positively observed terminal SLURM state
#   job:<id>:start      unblock when the SLURM job is RUNNING
#   artifact:/abs/path  unblock when the path exists
#   file:/abs/path      alias of artifact:
#   task:<id>[#...]     unblock when task <id> is DONE
#   ruling:<name>       NEVER auto-unblocked here — the master decides (returns False)

SlurmState = Callable[[str], "str | None"]


def waiting_on_error(value: str | None) -> str | None:
    if not isinstance(value, str) or ":" not in value:
        return "expected job:, artifact:, file:, task:, or ruling: reference"
    kind, _, rest = value.partition(":")
    if kind == "job":
        if re.fullmatch(r"[0-9]+(?:_[0-9]+)?(?::(?:start|end))?", rest):
            return None
        return "job requires one numeric job/array-element id and optional :start or :end"
    if kind in {"artifact", "file"}:
        return None if rest and Path(rest).is_absolute() else "artifact/file path must be absolute"
    if kind == "task":
        return None if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*(?:#[^\s]+)?", rest) else "invalid dependency task id"
    if kind == "ruling":
        return None if rest.strip() else "ruling requires a name"
    if kind == "rotation":
        return None if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", rest) else "invalid rotation ID"
    return f"unsupported wait kind: {kind}"


def evaluate_waiting_on(
    waiting_on: str | None,
    *,
    store: TaskStore,
    slurm: SlurmState = slurm_state,
) -> bool:
    if waiting_on_error(waiting_on):
        return False
    kind, _, rest = waiting_on.partition(":")
    if kind == "job":
        job_id, _, mode = rest.partition(":")
        state = slurm(job_id)
        if mode == "start":
            return normalized_slurm_state(state) == "RUNNING"
        return slurm_job_terminal(state)
    if kind in ("artifact", "file"):
        return Path(rest).exists()
    if kind == "task":
        dep = rest.split("#", 1)[0]
        try:
            return store.get(dep).state is TaskState.DONE
        except Exception:
            return False
    # ruling:<name> and anything else -> the master's call, not mechanical
    return False


MASTER_NOTE_GLOBS = ("MASTER_*.md",)


def master_note_digest(workspace: str | None) -> str | None:
    """Content digest of the master's rulings in a workspace.

    Sorted (name, sha256) over MASTER_*.md, hashed. None when there is no
    workspace or no note. Content-addressed rather than mtime-based, so this is
    state and not clock (INV-4).
    """
    if not workspace:
        return None
    root = Path(workspace)
    if not root.is_dir():
        return None
    entries: list[tuple[str, str]] = []
    for pattern in MASTER_NOTE_GLOBS:
        for path in sorted(root.glob(pattern)):
            try:
                entries.append((path.name, hashlib.sha256(path.read_bytes()).hexdigest()))
            except OSError:
                continue
    if not entries:
        return None
    joined = "\n".join(f"{name}:{digest}" for name, digest in sorted(entries))
    return hashlib.sha256(joined.encode()).hexdigest()


def unread_master_note(task: Task) -> bool:
    """True when the workspace's ruling set differs from what this task has seen."""
    current = master_note_digest(task.metadata.get("workspace"))
    if current is None:
        return False
    return current != task.metadata.get("master_notes_digest")


def make_unblock(
    paths: FarmPaths, *, slurm: SlurmState = slurm_state
) -> Callable[[Task], bool]:
    """Build the reconciler's unblock predicate for a farm's Task Store."""
    store = TaskStore(paths)

    def unblock(task: Task) -> bool:
        if task.metadata.get("resume_requested"):
            return True
        # Either the named condition fired, or the master has spoken since this
        # task last ran. The second disjunct restores the retired nudger's
        # "unread MASTER_*.md" trigger (runtime defect 002).
        if evaluate_waiting_on(task.metadata.get("waiting_on"), store=store, slurm=slurm):
            return True
        from .turnover import clean_surrender
        if clean_surrender(task):
            return False  # infrastructure turnover never converts a legacy note into a ruling
        return unread_master_note(task)

    return unblock
