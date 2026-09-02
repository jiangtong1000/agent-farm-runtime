from __future__ import annotations

from typing import Protocol


class WorkerExecutor(Protocol):
    """Future boundary for disposable worker backends."""

    def observe(self) -> dict: ...


class ComputeObserver(Protocol):
    """Future boundary for external compute substrates such as SLURM."""

    def observe_job(self, job_id: str) -> str | None: ...
