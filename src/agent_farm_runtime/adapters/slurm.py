from __future__ import annotations

from agent_farm_runtime.shadow import slurm_state


class SlurmObserver:
    """Read-only V0 adapter. Job submission/cancellation is intentionally absent."""

    def observe_job(self, job_id: str) -> str | None:
        return slurm_state(job_id)
