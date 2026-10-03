"""Synthetic farms must not inherit the test runner's real allocation identity."""
import pytest


@pytest.fixture(autouse=True)
def isolated_launcher_environment(monkeypatch):
    # Allocation tests explicitly supply their own launcher identity. This also
    # keeps CLI subprocess tests from querying the runner's real Slurm job.
    for name in ("SLURM_JOB_ID", "SLURM_JOBID", "SLURMD_NODENAME"):
        monkeypatch.delenv(name, raising=False)
