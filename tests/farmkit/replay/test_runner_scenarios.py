"""Failure-replay scenarios for Runner.tick() against a fake scheduler (D2, RFC §11)."""
import json
import math

from fakes import FakeSlurm, make_runner, make_workspace, write_output


def test_success_path_and_idempotency(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm()
    r = make_runner(ws, steps, led, slurm)
    t1 = r.tick()
    assert sorted(t1.submitted) == ["train:lam0", "train:lam1"]
    assert t1.receipt_status == "AWAITING" and t1.waiting_on == "job:1001"      # lowest id
    assert "--comment=attempt:" in " ".join(slurm.submissions[0])
    assert (ws / "attempts" / led.latest("train:lam0")["attempt_id"] / "code" / "train.sbatch").exists()
    assert t1.receipt_command.startswith("python .farm_receipt.py AWAITING --waiting-on job:1001")
    t1b = r.tick()                                                            # nothing changed: no new submissions
    assert t1b.submitted == [] and len(slurm.submissions) == 2
    (ws / "train.py").write_text("print('edited while queued')\n")            # later edit must not reach the snapshot
    for step in ("train:lam0", "train:lam1"):
        write_output(ws, led, step, f"run_{step.split(':')[1]}/RUN.json")
        slurm.finish(slurm.job_of(led, step))
    t2 = r.tick()
    assert led.ok("train:lam0") and led.ok("train:lam1")
    assert led.ok("select") and t2.receipt_status == "SUBMITTED" and t2.waiting_on is None
    assert json.loads((ws / "select.json").read_text())["winner"] == "lam0"
    snap = (ws / "attempts" / led.latest("train:lam0")["attempt_id"] / "code" / "train.py").read_text()
    assert "edited while queued" not in snap


def test_failed_job_without_output_parks_last(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm()
    r = make_runner(ws, steps, led, slurm)
    r.tick()
    slurm.finish(slurm.job_of(led, "train:lam0"), "FAILED")                    # no RUN.json written
    t = r.tick()
    rec = led.latest("train:lam0")
    assert rec["failure"]["class"] == "code" and rec["failure"]["parked"]
    assert (ws / "attempts" / rec["attempt_id"] / "FAILURE.md").exists()
    assert t.waiting_on == f"job:{slurm.job_of(led, 'train:lam1')}"          # park LAST: other arm still running
    write_output(ws, led, "train:lam1", "run_lam1/RUN.json"); slurm.finish(slurm.job_of(led, "train:lam1"))
    t = r.tick()
    assert t.receipt_status == "AWAITING" and t.waiting_on.startswith("ruling:T-9-code-train-lam0-")
    assert not led.ok("select")                                                # dependency on the group blocks select
    assert len(slurm.submissions) == 2                                         # code class: no automatic resubmission


def test_completed_without_output_is_code_class(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm(); r = make_runner(ws, steps, led, slurm); r.tick()
    slurm.finish(slurm.job_of(led, "train:lam0"), "COMPLETED")
    r.tick()
    f = led.latest("train:lam0")["failure"]
    assert f["class"] == "code" and f["parked"] and "required output missing: run_lam0/RUN.json" in f["reasons"]


def test_scheduler_query_failure_keeps_waiting_without_resubmit(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm(); r = make_runner(ws, steps, led, slurm); r.tick()
    slurm.query_fails = True
    t = r.tick()
    rec = led.latest("train:lam0")
    assert rec["observed"]["state"] is None and rec["failure"] is None
    assert t.waiting_on == "job:1001" and len(slurm.submissions) == 2


def test_sbatch_then_crash_reconciles_by_comment_or_stays_unverified(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm(); slurm.sbatch_mode = "crash"
    r = make_runner(ws, steps, led, slurm)
    t = r.tick()
    assert all((rec["submit"] or {}).get("status") == "intent" for rec in led.all())
    assert t.waiting_on.startswith("ruling:T-9-unknown-submit-train-lam0-")   # nothing in flight: ask a human
    # accounting says nothing yet: stays unverified, no resubmission
    slurm.sbatch_mode = "ok"
    t = r.tick()
    assert all((rec["submit"] or {}).get("status") == "unverified" for rec in led.all())
    assert len(slurm.submissions) == 2
    # a fresh scenario where accounting finds exactly one job: adopted, wait switches to it
    ws2, steps2, led2 = make_workspace(tmp_path / "b")
    s2 = FakeSlurm(); s2.sbatch_mode = "crash"
    r2 = make_runner(ws2, steps2, led2, s2, accounting=lambda comment: ["777"])
    r2.tick()
    s2.states["777"] = "RUNNING"
    t = r2.tick()
    assert all(rec["submit"]["status"] == "submitted" and rec["submit"]["job_id"] == "777" for rec in led2.all())
    assert t.waiting_on == "job:777"


def test_infra_failure_retries_once_then_parks(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm(); r = make_runner(ws, steps, led, slurm); r.tick()
    slurm.finish(slurm.job_of(led, "train:lam0"), "NODE_FAIL")
    t = r.tick()
    assert "train:lam0" in t.submitted and len(led.by_step("train:lam0")) == 2
    first, second = led.by_step("train:lam0")
    assert first["failure"]["class"] == "infra" and first["failure"]["retried"] and not first["failure"]["parked"]
    assert second["submit"]["status"] == "submitted"
    slurm.finish(second["submit"]["job_id"], "NODE_FAIL")
    r.tick()
    second = led.latest("train:lam0")
    assert second["failure"]["parked"] and second["budget"] == {"class": "infra", "max_auto": 1, "used": 1}
    assert len(led.by_step("train:lam0")) == 2                                 # budget exhausted: no third attempt


def test_stale_output_from_previous_attempt_is_rejected(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm(); r = make_runner(ws, steps, led, slurm); r.tick()
    write_output(ws, led, "train:lam0", "run_lam0/RUN.json", attempt_id="deadbeef00")   # left over from an earlier run
    slurm.finish(slurm.job_of(led, "train:lam0"))
    r.tick()
    f = led.latest("train:lam0")["failure"]
    assert f["class"] == "code" and any("different attempt" in x for x in f["reasons"])


def test_nan_metric_is_science_class(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm(); r = make_runner(ws, steps, led, slurm); r.tick()
    write_output(ws, led, "train:lam0", "run_lam0/RUN.json", {"selected": {"S_val": math.nan}})
    slurm.finish(slurm.job_of(led, "train:lam0"))
    r.tick()
    f = led.latest("train:lam0")["failure"]
    assert f["class"] == "science" and f["parked"] and any("S_val" in x for x in f["reasons"])


CHAIN_TOML = '''
[step."gate"]
run = "gate.sbatch"
outputs = ["gate/RUN.json"]

[step."afqmc:{arm}"]
matrix.arm = ["a", "b"]
run = "seg.sbatch"
outputs = ["afqmc_{arm}/seg{seg}/END.json"]
segments = 3
chain = "afterok"
concurrency = 1
gate = "gate"
'''


def test_chain_segments_queue_together_and_cancelled_dependents_fail_the_chain(tmp_path):
    ws, steps, led = make_workspace(tmp_path, CHAIN_TOML)
    (ws / "gate.sbatch").write_text("#!/bin/bash\n"); (ws / "seg.sbatch").write_text("#!/bin/bash\n")
    slurm = FakeSlurm(); r = make_runner(ws, steps, led, slurm)
    t = r.tick()
    assert t.submitted == ["gate", "afqmc:a#1", "afqmc:a#2", "afqmc:a#3"]      # concurrency 1: chain b waits
    dep = [a for a in slurm.submissions[2] if a.startswith("--dependency=")]
    assert dep == [f"--dependency=afterok:{slurm.job_of(led, 'afqmc:a#1')}"]
    write_output(ws, led, "gate", "gate/RUN.json"); slurm.finish(slurm.job_of(led, "gate"))
    slurm.finish(slurm.job_of(led, "afqmc:a#1"), "FAILED")
    slurm.finish(slurm.job_of(led, "afqmc:a#2"), "CANCELLED")                # kill_invalid_depend
    slurm.finish(slurm.job_of(led, "afqmc:a#3"), "CANCELLED")
    t = r.tick()
    assert all(led.latest(f"afqmc:a#{k}")["failure"]["parked"] for k in (1, 2, 3))
    assert "afqmc:b#1" in t.submitted                                          # chain b may start once a is resolved
    assert led.ok("gate")


def test_gate_binding_rejects_code_that_differs_from_the_certified_smoke(tmp_path):
    ws, steps, led = make_workspace(tmp_path, CHAIN_TOML)
    (ws / "gate.sbatch").write_text("#!/bin/bash\n"); (ws / "seg.sbatch").write_text("#!/bin/bash\n")
    slurm = FakeSlurm(); r = make_runner(ws, steps, led, slurm); r.tick()
    write_output(ws, led, "gate", "gate/RUN.json"); slurm.finish(slurm.job_of(led, "gate"))
    r.tick()
    assert led.ok("gate")
    assert "seg.sbatch" not in led.latest("gate")["code"]                       # the gate never ran seg.sbatch
    write_output(ws, led, "afqmc:a#1", "afqmc_a/seg1/END.json"); slurm.finish(slurm.job_of(led, "afqmc:a#1"))
    r.tick()
    v = led.latest("afqmc:a#1")["verdict"]
    # gate certified {gate.sbatch, train.py, train.sbatch}; the segment ran seg.sbatch -> reported by name
    assert not v["ok"] and any(reason.startswith("code differs from the certified gate: ") for reason in v["reasons"])


def test_checkpoint_keeps_the_agents_judgment(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm(); r = make_runner(ws, steps, led, slurm)
    t = r.tick(checkpoint=True)
    path = ws / "CHECKPOINT.md"
    assert t.checkpoint_path == str(path) and "| train:lam0 |" in path.read_text()
    text = path.read_text()
    marker_line = [l for l in text.splitlines() if l.startswith("<!-- judgment")][0]
    path.write_text(text.split(marker_line)[0] + marker_line + "\n\nHypothesis: lam1 will win.\n")
    r.tick(checkpoint=True)
    assert "Hypothesis: lam1 will win." in path.read_text() and "| train:lam0 |" in path.read_text()
    assert "--checkpoint" in t.receipt_command


def test_summary_is_bounded_and_reports_changed_files(tmp_path):
    ws, steps, led = make_workspace(tmp_path)
    slurm = FakeSlurm(); r = make_runner(ws, steps, led, slurm)
    r.tick()
    (ws / "NOTES.md").write_text("new trap")
    t = r.tick()
    assert len(t.summary.splitlines()) <= 30 and "NOTES.md" in t.changed_files and len(t.note) <= 200
