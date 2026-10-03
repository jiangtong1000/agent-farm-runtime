"""The shipped two-step task exercises a real worker against the fake scheduler."""
import json
import os
import subprocess
from pathlib import Path

from agent_farm_runtime.models import TaskState
from farmkit.brief import lint
from farmkit.runtime_reader import CliRuntimeReader
from farmkit.watch import watch

from test_five_arm_study import Harness, ROOT, State


def test_shipped_smoke_dispatches_waits_verifies_and_is_accepted(tmp_path, monkeypatch):
    harness = Harness(tmp_path, monkeypatch, example=ROOT / "examples" / "smoke_test",
                      worker=ROOT / "tools" / "local_worker.py", task_id="T-SMOKE")
    assert lint(harness.ws / "BRIEF.md") == []
    checked = harness.farmkit("steps", "check")
    assert checked.returncode == 0, checked.stdout + checked.stderr

    task = harness.wake()
    assert task.state is TaskState.WAITING
    assert harness.latest("local")["verdict"]["ok"] is True
    job = harness.job_for("slurm")
    assert task.metadata["waiting_on"] == f"job:{job}"
    harness.reconcile()
    assert harness.task().state is TaskState.WAITING

    # Execute the actual snapshotted batch script with the exported attempt env;
    # only the scheduler and its state transitions are simulated.
    with State(harness.state) as state:
        exported = state.env_of(job)
        state.start(job)
    run_dir = Path(harness.latest("slurm")["run_dir"])
    result = subprocess.run(["bash", str(run_dir / "smoke.sbatch")], cwd=run_dir,
                            env={**os.environ, **exported}, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    harness.finish(job)

    task = harness.wake()
    assert task.state is TaskState.SUBMITTED
    for step in ("local", "slurm"):
        attempt = harness.latest(step)
        assert attempt["verdict"]["ok"] is True
        output = json.loads((harness.ws / "results" / f"{step}.json").read_text())
        assert output == {"provenance": {"attempt_id": attempt["attempt_id"]}, "metric": 1.0}
    checkpoint = (harness.ws / "CHECKPOINT.md").read_text()
    assert "Synthetic plumbing check" in checkpoint  # judgment survives tick rewrites

    reader = CliRuntimeReader(str(harness.wrapper), str(harness.project))
    hit = watch(reader, tmp_path / "watch.cursor", until="task:T-SMOKE", timeout_s=0)
    assert any(h.get("reason") == "submitted" or h.get("state") == "SUBMITTED" for h in hit["hits"])
    evidence = tmp_path / "acceptance.md"
    evidence.write_text("Both synthetic outputs verified with current attempt provenance and metric 1.0.\n")
    harness.farm("task-accept", "T-SMOKE", "--expected-revision", str(task.metadata["revision"]),
                 "--actor", "smoke-reviewer", "--evidence-file", str(evidence))
    assert harness.task().state is TaskState.DONE
