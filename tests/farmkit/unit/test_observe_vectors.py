"""farmkit side of the shared scheduler vectors (post-D6 'expect' column)."""
import json
import subprocess
from pathlib import Path

import pytest

from farmkit import observe

VECTORS = Path(__file__).parents[2] / "vectors" / "slurm_states.jsonl"
CASES = [json.loads(line) for line in VECTORS.read_text().splitlines() if line.strip()]


def tools(case):
    def which(name):
        return f"/usr/bin/{name}" if case[name]["available"] else None

    def run(argv, **kw):
        tool = case[argv[0]]
        if tool.get("raise") == "timeout":
            raise subprocess.TimeoutExpired(argv, 10)
        return subprocess.CompletedProcess(argv, tool["rc"], stdout=tool["stdout"], stderr="")
    return which, run


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_slurm_state_matches_vectors(case):
    which, run = tools(case)
    state = observe.slurm_state("4711", which=which, run=run)
    assert state == case["expect"]["state"]
    assert observe.is_terminal(state) is case["expect"]["terminal"]
    assert observe.is_active(state) is case["expect"]["active"]


def test_batch_states_uses_one_call_per_tool_and_caches():
    calls = []

    def which(name):
        return "/usr/bin/" + name

    def run(argv, **kw):
        calls.append(argv[0])
        if argv[0] == "squeue":
            return subprocess.CompletedProcess(argv, 0, stdout="11 RUNNING\n", stderr="")
        return subprocess.CompletedProcess(argv, 0, stdout="12|COMPLETED\n13|FAILED\n", stderr="")
    t = [0.0]
    cache = observe.StateCache(which=which, run=run, clock=lambda: t[0])
    assert cache.states(["11", "12", "13", "99"]) == {"11": "RUNNING", "12": "COMPLETED", "13": "FAILED", "99": None}
    assert calls == ["squeue", "sacct"]
    cache.states(["11", "12"])            # within TTL: no new calls
    assert calls == ["squeue", "sacct"]
    t[0] = 31.0
    cache.states(["11"])                # TTL expired: squeue again; sacct not needed for a queued job
    assert calls == ["squeue", "sacct", "squeue"]


def test_sacct_row_returns_single_row_or_none():
    def run(argv, **kw):
        return subprocess.CompletedProcess(argv, 0, stdout="4711|train-lam0-3f9c1a2b|FAILED|1:0|00:01:08|2026-09-17T22:08:32|attempt:3f9c\n", stderr="")
    row = observe.sacct_row("4711", which=lambda n: "/usr/bin/sacct", run=run)
    assert row.startswith("4711|") and "FAILED" in row
    assert observe.sacct_row("4711", which=lambda n: None, run=run) is None
