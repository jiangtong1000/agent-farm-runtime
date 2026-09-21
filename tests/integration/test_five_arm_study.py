"""End to end: REAL runtime + REAL farmkit + fake Slurm, no tmux, no model.

The farm lives in tmp. The Reconciler runs in-process with a LocalProcessExecutor whose
worker is tests/integration/worker.py: it runs `farmkit tick --checkpoint` in the
example workspace, then executes exactly the receipt command tick printed, then exits.
The test plays Slurm (advancing job states, writing job outputs) and the master (one
ruling through the real `farm task-ruling` CLI).
"""
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

import pytest

from agent_farm_runtime.adapters.local_process import LocalProcessExecutor
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.models import Task, TaskState
from agent_farm_runtime.observers import make_unblock
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.status import farm_status, write_last_tick
from agent_farm_runtime.store import FarmPaths, TaskStore

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SRC = ROOT / "src"
EXAMPLE = ROOT / "examples" / "five_arm_study"
FAKE = HERE / "fake_slurm"
sys.path.insert(0, str(FAKE))
from fakeslurm import State, slurm_state_fn  # noqa: E402

TID = "T-5ARM"
PYTHON = sys.executable


def _pythonpath() -> str:
    extra = os.environ.get("PYTHONPATH")
    return str(SRC) + (os.pathsep + extra if extra else "")


class Harness:
    """One farm, one task, one in-process reconciler, one fake scheduler."""

    def __init__(self, tmp_path: Path, monkeypatch):
        self.ws = tmp_path / "ws"
        shutil.copytree(EXAMPLE, self.ws)
        self.project = tmp_path / "farm"
        self.state = tmp_path / "fake_slurm.json"
        self.wrapper = tmp_path / "farm-wrapper"
        self.wrapper.write_text("#!/bin/bash\n"
                                f"export PYTHONPATH={shlex.quote(_pythonpath())}\n"
                                f"exec {shlex.quote(PYTHON)} -m agent_farm_runtime.cli \"$@\"\n")
        self.wrapper.chmod(0o755)
        site = tmp_path / "site.toml"
        site.write_text('[site]\nname = "fake"\nhostname_pattern = ".*"\n'
                        '[scheduler]\nkind = "slurm"\nsqueue = "squeue"\nsacct = "sacct"\nsbatch = "sbatch"\n'
                        'accounting_stores_comment = true\ndependency_kill_invalid = true\n'
                        f'[paths]\nfarm_wrapper = "{self.wrapper}"\n')
        monkeypatch.setenv("PATH", str(FAKE) + os.pathsep + os.environ["PATH"])
        monkeypatch.setenv("FAKE_SLURM_STATE", str(self.state))
        monkeypatch.setenv("FARMKIT_SITE", str(site))
        monkeypatch.setenv("FIVE_ARM_WS", str(self.ws))
        monkeypatch.setenv("PYTHONPATH", _pythonpath())
        # env_digest walks the whole interpreter environment (seconds on a network FS);
        # eight worker generations would spend half the test on it. Pin it here.
        monkeypatch.setenv("FARMKIT_ENV_DIGEST", "pinned-by-integration-test")

        self.paths = FarmPaths(self.project / ".farm")
        self.paths.ensure()
        self.store = TaskStore(self.paths)
        self.store.create(Task(TID, "five-arm study", "runs + selection + afqmc", "all steps verified",
                               metadata={"command": f"{shlex.quote(PYTHON)} {shlex.quote(str(HERE / 'worker.py'))}",
                                         "cwd": str(self.ws), "workspace": str(self.ws)}))
        self.executor = LocalProcessExecutor(self.paths.runtime)
        self.rec = Reconciler(self.paths, self.executor,
                              unblock=make_unblock(self.paths, slurm=slurm_state_fn(self.state)),
                              grace_seconds=0)
        self.wakes = 0

    # -- runtime side -----------------------------------------------------------
    def reconcile(self):
        report = self.rec.reconcile_once()
        write_last_tick(self.paths, asdict(report))
        return report

    def task(self) -> Task:
        return self.store.get(TID)

    def wake(self) -> Task:
        """One worker generation: launch (or resume), let it finish, apply its receipt."""
        self.reconcile()
        task = self.task()
        assert task.state is TaskState.RUNNING and task.lease, f"worker not launched: {task.state} {task.metadata.get('runtime_error')}"
        wid = task.lease.worker_id
        deadline = time.time() + 90
        while self.executor.poll(wid).alive is not False:
            assert time.time() < deadline, "worker did not exit"
            time.sleep(0.2)
        obs = self.executor.poll(wid)
        assert obs.receipt is not None, "worker exited without a receipt:\n" + self.worker_log()
        self.reconcile()
        self.wakes += 1
        task = self.task()
        assert task.state is not TaskState.RUNNING, "receipt was not applied"
        return task

    def worker_log(self) -> str:
        parts = []
        for name in ("worker_last_tick.out", "worker_last_receipt.out", "worker_last_release.out"):
            path = self.ws / name
            if path.exists():
                parts.append(f"--- {name}\n{path.read_text()}")
        return "\n".join(parts)

    def farm(self, *args: str) -> str:
        proc = subprocess.run([str(self.wrapper), "--project", str(self.project), *args],
                              text=True, capture_output=True, timeout=120)
        assert proc.returncode == 0, proc.stderr
        return proc.stdout

    def farmkit(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run([PYTHON, "-m", "farmkit.cli", *args, "--workspace", str(self.ws)],
                              text=True, capture_output=True, timeout=120, cwd=str(self.ws))

    # -- scheduler side ---------------------------------------------------------
    def jobs(self) -> dict:
        return json.loads(self.state.read_text())["jobs"]

    def job_for(self, step: str) -> str:
        """Job id of the latest attempt of a step, from the ledger (no digest in the test)."""
        latest = self.latest(step)
        assert latest and latest.get("submit", {}).get("job_id"), f"{step} has no submitted attempt"
        return latest["submit"]["job_id"]

    def latest(self, step: str) -> dict | None:
        rows = [json.loads(p.read_text()) for p in (self.ws / "attempts").glob("*.json")
                if p.name != "INDEX.json" and not p.name.startswith(".")]
        rows = [r for r in rows if r.get("step") == step and r.get("schema") == "farmkit.attempt.v1"]
        rows.sort(key=lambda r: r["created_ts"])
        return rows[-1] if rows else None

    def finish(self, job_id: str, state: str = "COMPLETED") -> None:
        with State(self.state) as st:
            st.finish(job_id, state)

    def write_train(self, job_id: str, metric: float, *, attempt: str | None = None,
                    stop_reason: str = "plateau") -> Path:
        with State(self.state) as st:
            env, comment_attempt = st.env_of(job_id), st.attempt_of(job_id)
        folder = "runs" if env["SEED"] == "1" else "runs2"
        out = self.ws / folder / env["ARM"] / "RUN.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"provenance": {"attempt_id": attempt or comment_attempt},
                                   "selected": {"metric": metric}, "stop_reason": stop_reason}))
        return out

    def write_segment(self, job_id: str, status: str = "PASS") -> Path:
        with State(self.state) as st:
            env, attempt = st.env_of(job_id), st.attempt_of(job_id)
        folder = "afqmc" if env["ROUND"] == "1" else "afqmc2"
        out = self.ws / folder / env["ARM"] / f"seg{env['SEGMENT']}" / "SEGMENT_END.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"provenance": {"attempt_id": attempt}, "status": status}))
        return out

    def in_flight_jobs(self) -> list[str]:
        return sorted(j for j, rec in self.jobs().items() if rec["state"] in {"PENDING", "RUNNING"})


@pytest.fixture
def h(tmp_path, monkeypatch):
    return Harness(tmp_path, monkeypatch)


def test_example_project_passes_its_own_lints(h):
    assert h.farmkit("steps", "check", "--verifiers", "verifiers").returncode == 0
    lint = subprocess.run([PYTHON, "-m", "farmkit.cli", "brief", "lint", "BRIEF.md"], cwd=str(h.ws),
                          text=True, capture_output=True)
    assert lint.returncode == 0, lint.stdout
    assert (h.ws / "BRIEF.md").stat().st_size <= 2048


def test_fake_slurm_cancels_afterok_dependents(h):
    with State(h.state) as st:
        a = st.submit(["--parsable", "--job-name=a", "a.sbatch"])
        b = st.submit(["--parsable", f"--dependency=afterok:{a}", "b.sbatch"])
        c = st.submit(["--parsable", f"--dependency=afterok:{b}", "c.sbatch"])
        st.finish(a, "NODE_FAIL")
    assert [h.jobs()[j]["state"] for j in (a, b, c)] == ["NODE_FAIL", "CANCELLED", "CANCELLED"]


def test_five_arm_study_end_to_end(h):
    started = time.time()

    # ---- wake 1: three round-1 arms submitted, wait on the lowest job id --------------
    task = h.wake()
    assert task.state is TaskState.WAITING
    round1 = {arm: h.job_for(f"train:{arm}") for arm in ("lam0", "lam0.1", "lam1")}
    assert task.metadata["waiting_on"] == f"job:{min(round1.values(), key=int)}"
    for arm, job in round1.items():                  # intent-before-sbatch left its mark on the job (D2)
        assert h.jobs()[job]["comment"] == f"attempt:{h.latest('train:' + arm)['attempt_id']}"
        assert h.jobs()[job]["name"].startswith("train-")
        assert h.jobs()[job]["dependency"] is None
        assert (h.ws / "attempts" / h.latest("train:" + arm)["attempt_id"] / "code" / "train.sbatch").exists()

    # the runtime sees the waited-on job, both in-process and through the read-only CLI
    status = farm_status(h.paths, slurm=slurm_state_fn(h.state))
    assert status["observed_jobs"] == [{"job_id": task.metadata["waiting_on"][4:], "task": TID,
                                        "state": "PENDING", "terminal": False, "source": "query"}]   # in-process harness: no daemon tick to trust
    cli_status = json.loads(h.farm("status", "--json"))
    assert cli_status["observed_jobs"][0]["job_id"] == task.metadata["waiting_on"][4:]
    assert cli_status["observed_jobs"][0]["state"] == "PENDING"

    # nothing terminal yet: the reconciler must not wake the worker
    h.reconcile()
    assert h.task().state is TaskState.WAITING

    # Slurm: lam0 completes properly, lam0.1 loses its node, lam1 completes but writes nothing
    h.write_train(round1["lam0"], 0.30)
    h.finish(round1["lam0"])
    h.finish(round1["lam0.1"], "NODE_FAIL")
    h.finish(round1["lam1"])

    # ---- wake 2: verify, resubmit the infra failure once, park the code failure --------
    task = h.wake()
    assert task.state is TaskState.WAITING
    lam01_attempts = [r for r in (json.loads(p.read_text()) for p in (h.ws / "attempts").glob("*.json")
                                  if not p.name.startswith(".") and p.name != "INDEX.json")
                      if r.get("step") == "train:lam0.1"]
    assert len(lam01_attempts) == 2, "NODE_FAIL must cause exactly one automatic resubmission"
    first, second = sorted(lam01_attempts, key=lambda r: r["created_ts"])
    assert first["failure"]["class"] == "infra" and first["failure"]["retried"] is True
    assert first["failure"]["parked"] is False
    assert second["submit"]["status"] == "submitted"
    resubmitted = second["submit"]["job_id"]
    assert h.jobs()[resubmitted]["comment"] == f"attempt:{second['attempt_id']}"

    lam1 = h.latest("train:lam1")
    assert lam1["failure"]["class"] == "code" and lam1["failure"]["parked"] is True
    ruling_lam1 = lam1["failure"]["ruling"]
    assert ruling_lam1.startswith(f"ruling:{TID}-code-")
    assert Path(lam1["failure"]["evidence"]).name == "FAILURE.md"
    evidence = Path(lam1["failure"]["evidence"]).read_text()
    assert "code" in evidence and "sacct" in evidence.lower()

    # lam0's AFQMC chain started (two segments, afterok), and "parked last": the task
    # waits on the earliest in-flight job, not on the ruling
    seg1, seg2 = h.job_for("afqmc:lam0#1"), h.job_for("afqmc:lam0#2")
    assert h.jobs()[seg2]["dependency"] == f"afterok:{seg1}"
    assert task.metadata["waiting_on"] == f"job:{min(resubmitted, seg1, seg2, key=int)}"
    assert (h.ws / "CHECKPOINT.md").exists() and ruling_lam1 in (h.ws / "CHECKPOINT.md").read_text()

    # Slurm: the resubmitted lam0.1 completes, but its RUN.json is a stale copy carrying the
    # FIRST attempt's id (someone copied it from an earlier attempt)
    h.write_train(resubmitted, 0.10, attempt=first["attempt_id"])
    h.finish(resubmitted)
    h.write_segment(seg1)
    h.finish(seg1)

    # ---- wake 3: stale output rejected; segment 1 verified; wait on segment 2 -------------
    task = h.wake()
    lam01 = h.latest("train:lam0.1")
    assert lam01["attempt_id"] == second["attempt_id"]
    assert lam01["verdict"]["ok"] is False
    assert any("belongs to a different attempt" in r for r in lam01["verdict"]["reasons"])
    assert lam01["failure"]["class"] == "code" and lam01["failure"]["parked"] is True
    ruling_lam01 = lam01["failure"]["ruling"]
    assert h.latest("afqmc:lam0#1")["verdict"]["ok"] is True
    assert task.metadata["waiting_on"] == f"job:{seg2}"

    h.write_segment(seg2)
    h.finish(seg2)

    # ---- wake 4: nothing in flight -> the oldest ruling becomes the wait (parked last) -----
    task = h.wake()
    assert task.state is TaskState.WAITING
    assert task.metadata["waiting_on"] == ruling_lam1
    assert h.in_flight_jobs() == []
    h.reconcile()                                   # a ruling never unblocks mechanically
    assert h.task().state is TaskState.WAITING

    # ---- master: one ruling (real CLI, evidence file, optimistic locking) -----------------
    note = h.project / "ruling-round1.md"
    note.write_text("Both failures are code class: the writer forgot RUN.json / copied a stale one.\n"
                    "Scripts fixed. Retry each step once.\n"
                    f"farmkit release --step train:lam1 --ruling {shlex.quote('master: writer fixed, retry once')}\n"
                    f"farmkit release --step train:lam0.1 --ruling {shlex.quote('master: stale copy removed, retry once')}\n")
    out = json.loads(h.farm("task-ruling", TID, "--expected-revision", str(task.metadata["revision"]),
                            "--actor", "master-test", "--evidence-file", str(note)))
    assert out["state"] == "WAITING"
    assert h.task().metadata["resume_requested"]

    # ---- wake 5: worker applies the releases, both steps resubmitted --------------------
    task = h.wake()
    assert task.state is TaskState.WAITING
    assert not h.task().metadata.get("resume_requested")
    lam1_retry, lam01_retry = h.job_for("train:lam1"), h.job_for("train:lam0.1")
    assert lam1_retry != round1["lam1"] and lam01_retry not in {round1["lam0.1"], resubmitted}
    assert task.metadata["waiting_on"] == f"job:{min(lam1_retry, lam01_retry, key=int)}"
    assert (h.ws / "worker_last_release.out").exists()

    h.write_train(lam1_retry, 0.90)
    h.finish(lam1_retry)
    h.write_train(lam01_retry, 0.10)
    h.finish(lam01_retry)

    # ---- wake 6: round 1 complete -> local select -> round 2 + remaining AFQMC chains -------
    task = h.wake()
    selection = json.loads((h.ws / "selection.json").read_text())
    assert selection["winner"] == "lam0.1"
    select_rec = h.latest("select")
    assert select_rec["verdict"]["ok"] is True and select_rec["submit"]["status"] == "local"
    assert selection["provenance"]["attempt_id"] == select_rec["attempt_id"]
    train2 = {arm: h.job_for(f"train2:{arm}") for arm in ("lam0", "mixed")}
    chains = {arm: (h.job_for(f"afqmc:{arm}#1"), h.job_for(f"afqmc:{arm}#2")) for arm in ("lam0.1", "lam1")}
    for a, b in chains.values():
        assert h.jobs()[b]["dependency"] == f"afterok:{a}"
    assert task.metadata["waiting_on"] == f"job:{min(h.in_flight_jobs(), key=int)}"

    for job in train2.values():
        h.write_train(job, 0.20)
        h.finish(job)
    for a, b in chains.values():
        h.write_segment(a); h.finish(a)
        h.write_segment(b); h.finish(b)

    # ---- wake 7: round-2 AFQMC chains --------------------------------------------------
    task = h.wake()
    chains2 = {arm: (h.job_for(f"afqmc2:{arm}#1"), h.job_for(f"afqmc2:{arm}#2")) for arm in ("lam0", "mixed")}
    assert task.state is TaskState.WAITING
    for a, b in chains2.values():
        h.write_segment(a); h.finish(a)
        h.write_segment(b); h.finish(b)

    # ---- wake 8: everything verified -> SUBMITTED -----------------------------------------
    task = h.wake()
    assert task.state is TaskState.SUBMITTED, h.worker_log()
    assert "deliverable ready" in task.metadata["last_receipt_note"]
    assert (h.ws / "CHECKPOINT.md").exists()

    # ledger totals: 16 steps ok (3 train, select, 2 train2, 10 segments); 3 recorded failures
    rows = [json.loads(p.read_text()) for p in (h.ws / "attempts").glob("*.json")
            if not p.name.startswith(".") and p.name != "INDEX.json"]
    ok = [r for r in rows if (r.get("verdict") or {}).get("ok")]
    failed = [r for r in rows if r.get("failure")]
    assert len(ok) == 16 and len(failed) == 3 and len(rows) == 19
    assert {r["failure"]["class"] for r in failed} == {"infra", "code"}
    assert all((r["failure"].get("released") or r["failure"].get("retried")) for r in failed)
    assert not any(c.level == "FAIL" for c in run_doctor(h.paths))

    # ---- health through the read-only CLI reader: no daemon process -> says so ---------------
    proc = subprocess.run([PYTHON, "-m", "farmkit.cli", "health", "--project", str(h.project)],
                          text=True, capture_output=True)
    lines = proc.stdout.splitlines()
    daemon = [l for l in lines if "daemon process" in l]
    assert daemon and not daemon[0].startswith("ok"), proc.stdout
    assert any(l.startswith("ok") and "loop advancing" in l for l in lines), proc.stdout
    assert proc.returncode == 1                      # honest: unknown daemon is not "ok"

    from farmkit.runtime_reader import CliRuntimeReader
    reader = CliRuntimeReader(str(h.wrapper), str(h.project))
    events, cursor = reader.events_after(None)
    assert int(cursor) > 0 and any(e.get("type") == "RULING_RECORDED" for e in events)

    assert h.wakes == 8
    assert time.time() - started < 180, "integration test is meant to finish in about a minute"
