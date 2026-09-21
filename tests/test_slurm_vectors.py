"""Runtime side of the shared scheduler vectors (tests/vectors/slurm_states.jsonl)."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from agent_farm_runtime.adapters import slurm

VECTORS = Path(__file__).parent / "vectors" / "slurm_states.jsonl"
CASES = [json.loads(line) for line in VECTORS.read_text().splitlines() if line.strip()]
# D6 landed: the runtime is judged against the "expect" column; "expect_pre_d6" documents
# the behaviour before sacct was consulted on squeue failure.
EXPECT_KEY = "expect"


def _fake_tools(monkeypatch, case):
    def which(name):
        return f"/usr/bin/{name}" if case[name]["available"] else None

    def run(cmd, **kw):
        tool = case[cmd[0]]
        if tool.get("raise") == "timeout":
            raise subprocess.TimeoutExpired(cmd, kw.get("timeout", 10))
        return subprocess.CompletedProcess(cmd, tool["rc"], stdout=tool["stdout"], stderr="")

    monkeypatch.setattr(slurm.shutil, "which", which)
    monkeypatch.setattr(slurm.subprocess, "run", run)


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_slurm_state_matches_shared_vectors(monkeypatch, case):
    _fake_tools(monkeypatch, case)
    expect = case.get(EXPECT_KEY, case["expect"])
    state = slurm.slurm_state("4711")
    assert state == expect["state"]
    assert slurm.slurm_job_terminal(state) is expect["terminal"]
    assert slurm.slurm_job_active(state) is expect["active"]
