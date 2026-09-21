"""Which `waiting_on` a worker registers after submitting work (rule 1, D4).

Wait on a scheduler terminal state, never on an output artifact: `job:<id>` fires on
COMPLETED and FAILED alike, so a crash cannot become an endless wait. With several
jobs in flight the default is the one expected to end first (or the lowest id when
nothing is known); a barrier job (`afterany`) is an opt-in per group.
"""
from __future__ import annotations

import re


class ArtifactWaitForbidden(ValueError):
    pass


def forbid_artifact_wait(waiting_on: str) -> None:
    if waiting_on.startswith(("artifact:", "file:")):
        raise ArtifactWaitForbidden("job outputs are verified after the job ends; wait on job:<id>, not on the file")


def _numeric(job_id: str) -> int:
    return int(re.split(r"[^0-9]", job_id, 1)[0] or 0)


def wait_reference(job_ids: list[str], *, expected_end: dict[str, float] | None = None,
                   barrier_job: str | None = None) -> str:
    """`job:<id>` to register. Barrier wins when given; else earliest expected end; else lowest id."""
    if barrier_job:
        return f"job:{barrier_job}"
    if not job_ids:
        raise ValueError("nothing to wait on")
    if expected_end:
        known = [j for j in job_ids if j in expected_end]
        if known:
            return f"job:{min(known, key=lambda j: (expected_end[j], _numeric(j)))}"
    return f"job:{min(job_ids, key=_numeric)}"


def barrier_argv(group: str, job_ids: list[str], *, partition: str | None, time_limit: str = "0:05:00",
                 attempt_id: str) -> list[str]:
    """sbatch argv for a trivial job released when every member has ended, however it ended."""
    argv = ["sbatch", "--parsable", f"--job-name=barrier-{group}-{attempt_id[:8]}",
            f"--comment=attempt:{attempt_id}", f"--dependency=afterany:{':'.join(job_ids)}",
            f"--time={time_limit}", "-c", "1"]
    if partition:
        argv += ["-p", partition]
    return argv + ["--wrap", "exit 0"]
