"""A fake Slurm and a workspace factory for replaying failure scenarios without a cluster."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from farmkit.ledger import Ledger
from farmkit.observe import StateCache
from farmkit.runner import Runner
from farmkit.site import Site
from farmkit.steps import Steps


class FakeSlurm:
    """Jobs are created by sbatch and driven by the test; sacct/squeue answer from `states`."""

    def __init__(self):
        self.next_id = 1000
        self.states: dict[str, str | None] = {}
        self.submissions: list[list[str]] = []
        self.query_fails = False
        self.sbatch_mode = "ok"          # ok | reject | crash | garbage

    # scheduler side ---------------------------------------------------------
    def sbatch(self, argv):
        self.submissions.append(list(argv))
        if self.sbatch_mode == "crash":
            raise OSError("connection lost after submission")
        if self.sbatch_mode == "reject":
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr="sbatch: error: invalid partition")
        if self.sbatch_mode == "garbage":
            return subprocess.CompletedProcess(argv, 0, stdout="Submitted batch job ???\n", stderr="")
        self.next_id += 1
        jid = str(self.next_id)
        self.states[jid] = "PENDING"
        return subprocess.CompletedProcess(argv, 0, stdout=f"{jid}\n", stderr="")

    def which(self, name):
        return f"/usr/bin/{name}"

    def run(self, argv, **kw):
        if self.query_fails:
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr="slurm_load_jobs error")
        ids = argv[argv.index("-j") + 1].split(",")
        if argv[0] == "squeue":
            lines = [f"{j} {self.states[j]}" for j in ids if self.states.get(j) in {"PENDING", "RUNNING"}]
            return subprocess.CompletedProcess(argv, 0, stdout="\n".join(lines) + ("\n" if lines else ""), stderr="")
        lines = [f"{j}|{self.states[j]}" for j in ids if self.states.get(j) not in {None, "PENDING", "RUNNING"}]
        return subprocess.CompletedProcess(argv, 0, stdout="\n".join(lines) + ("\n" if lines else ""), stderr="")

    def sacct_row(self, job_id):
        return f"{job_id}|fake|{self.states.get(job_id)}|0:0|00:01:00|end|attempt:?"

    def accounting(self, comment):
        if self.query_fails:
            return None
        return [a[a.index([x for x in a if x.startswith("--comment=")][0]) + 0].split("=", 1)[1] and self._job_for(a)
                for a in self.submissions if f"--comment={comment}" in a and self._job_for(a)]

    def _job_for(self, argv):
        # the fake assigns ids in submission order
        idx = self.submissions.index(argv)
        return str(1000 + idx + 1) if str(1000 + idx + 1) in self.states else None

    # test controls ---------------------------------------------------------
    def job_of(self, ledger: Ledger, step: str) -> str:
        return ledger.latest(step)["submit"]["job_id"]

    def finish(self, job_id: str, state: str = "COMPLETED"):
        self.states[job_id] = state

    def cache(self):
        return StateCache(which=self.which, run=self.run, clock=lambda: 0.0, ttl_s=0.0)


STEPS_TOML = '''
[defaults]
retry = { infra = 1 }
snapshot = ["*.py", "*.sbatch"]
finite = ["selected.S_val"]

[step."train:{arm}"]
matrix.arm = ["lam0", "lam1"]
run = "train.sbatch"
env = { ARM = "{arm}" }
outputs = ["run_{arm}/RUN.json"]
group = "round1"

[step."select"]
after = ["group:round1"]
run = "PYTHON -c 'import json,os; open(\\"select.json\\",\\"w\\").write(json.dumps({\\"provenance\\":{\\"attempt_id\\":os.environ[\\"FARMKIT_ATTEMPT_ID\\"]},\\"winner\\":\\"lam0\\"}))'"
outputs = ["select.json"]
finite = []
'''


def make_workspace(tmp_path: Path, toml: str = STEPS_TOML) -> tuple[Path, Steps, Ledger]:
    ws = tmp_path / "ws"
    ws.mkdir(parents=True)
    (ws / "train.sbatch").write_text("#!/bin/bash\necho train\n")
    (ws / "train.py").write_text("print('train')\n")
    (ws / "steps.toml").write_text(toml.replace("PYTHON", sys.executable))
    steps = Steps.load(ws / "steps.toml")
    return ws, steps, Ledger(ws / "attempts")


def make_runner(ws, steps, ledger, slurm: FakeSlurm, **kw) -> Runner:
    params = dict(task_id="T-9", lease_id="L1", sbatch=slurm.sbatch, states=slurm.cache(),
                  sacct_row=slurm.sacct_row, accounting=slurm.accounting)
    params.update(kw)
    return Runner(Site.minimal(barrier_partition="test"), ledger, steps, workspace=ws, **params)


def write_output(ws: Path, ledger: Ledger, step: str, rel: str, payload: dict | None = None, attempt_id: str | None = None):
    aid = attempt_id or ledger.latest(step)["attempt_id"]
    body = {"provenance": {"attempt_id": aid}, "selected": {"S_val": 0.5}}
    if payload:
        body.update(payload)
    path = ws / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body))
    return path
