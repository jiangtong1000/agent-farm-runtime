from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from .models import Task, TaskState
from .shadow import slurm_state
from .store import FarmPaths, TaskStore

# The mechanical WAITING->RUNNING unblock predicate: the runtime's replacement
# for the live farm's nudger. It evaluates ONLY explicit, named conditions
# recorded in metadata.waiting_on (INV-4: state, not clock). It makes no
# scientific judgment; `ruling:` conditions are intentionally left to the master.
#
# Supported waiting_on forms:
#   job:<id>            unblock when the SLURM job is no longer in the queue (ended)
#   job:<id>:start      unblock when the SLURM job is RUNNING
#   artifact:/abs/path  unblock when the path exists
#   file:/abs/path      alias of artifact:
#   task:<id>[#...]     unblock when task <id> is DONE
#   ruling:<name>       NEVER auto-unblocked here — the master decides (returns False)

SlurmState = Callable[[str], "str | None"]


def evaluate_waiting_on(
    waiting_on: str | None,
    *,
    store: TaskStore,
    slurm: SlurmState = slurm_state,
) -> bool:
    if not waiting_on:
        return False
    kind, _, rest = waiting_on.partition(":")
    if kind == "job":
        job_id, _, mode = rest.partition(":")
        if not job_id:
            return False
        state = slurm(job_id)
        if mode == "start":
            return state == "RUNNING"
        # default: wait for END — unblock once the job leaves the queue
        return state is None
    if kind in ("artifact", "file"):
        return bool(rest) and Path(rest).exists()
    if kind == "task":
        dep = rest.split("#", 1)[0]
        if not dep:
            return False
        try:
            return store.get(dep).state is TaskState.DONE
        except Exception:
            return False
    # ruling:<name> and anything else -> the master's call, not mechanical
    return False


def make_unblock(
    paths: FarmPaths, *, slurm: SlurmState = slurm_state
) -> Callable[[Task], bool]:
    """Build the reconciler's unblock predicate for a farm's Task Store."""
    store = TaskStore(paths)

    def unblock(task: Task) -> bool:
        return evaluate_waiting_on(task.metadata.get("waiting_on"), store=store, slurm=slurm)

    return unblock
