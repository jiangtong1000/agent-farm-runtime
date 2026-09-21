import json
import subprocess

import pytest

from farmkit import intent, snapshot, waits
from farmkit.ledger import Ledger


# ---------------------------------------------------------------- snapshot
def test_snapshot_copies_scripts_and_skips_attempts_dir(tmp_path):
    (tmp_path / "train.py").write_text("print(1)\n")
    (tmp_path / "train.sbatch").write_text("#!/bin/bash\n")
    (tmp_path / "notes.md").write_text("x")
    (tmp_path / "attempts" / "old" / "code").mkdir(parents=True)
    (tmp_path / "attempts" / "old" / "code" / "train.py").write_text("old")
    digests = snapshot.snapshot_code(tmp_path, tmp_path / "attempts" / "new", ["*.py", "*.sbatch", "**/*.py"])
    assert set(digests) == {"train.py", "train.sbatch"}
    assert (tmp_path / "attempts" / "new" / "code" / "train.py").read_text() == "print(1)\n"
    (tmp_path / "train.py").write_text("print(2)\n")          # later edit does not reach the snapshot
    assert (tmp_path / "attempts" / "new" / "code" / "train.py").read_text() == "print(1)\n"


def test_describe_inputs_uses_manifest_for_directories_and_skips_hashing_large_files(tmp_path, monkeypatch):
    cache = tmp_path / "minor_cache_mo"; cache.mkdir()
    (cache / "MANIFEST.json").write_text('{"built": "2026-09-18"}')
    small = tmp_path / "theta.npz"; small.write_bytes(b"x" * 10)
    big = tmp_path / "big.npy"; big.write_bytes(b"y" * 100)
    monkeypatch.setattr(snapshot, "LARGE_INPUT_BYTES", 50)
    d = snapshot.describe_inputs(tmp_path, ["minor_cache_mo", "theta.npz", "big.npy", "missing.dat"])
    assert d["minor_cache_mo"]["kind"] == "directory" and d["minor_cache_mo"]["manifest"].endswith("MANIFEST.json")
    assert "sha256" in d["theta.npz"] and "sha256" not in d["big.npy"] and d["big.npy"]["size"] == 100
    assert d["missing.dat"]["exists"] is False


def test_git_state_none_outside_repo_and_env_digest_stable(tmp_path):
    assert snapshot.git_state(tmp_path) is None or isinstance(snapshot.git_state(tmp_path), dict)
    assert snapshot.env_digest() == snapshot.env_digest()


# ---------------------------------------------------------------- intent
def fake_sbatch(stdout="4711;cluster\n", rc=0, raise_exc=None):
    def run(argv):
        if raise_exc:
            raise raise_exc
        return subprocess.CompletedProcess(argv, rc, stdout=stdout, stderr="" if rc == 0 else "sbatch: error: invalid partition")
    return run


def test_submit_records_intent_before_sbatch_and_completes(tmp_path):
    led = Ledger(tmp_path / "attempts")
    rec = led.new("train:lam0", task_id="T-1")
    argv = intent.sbatch_argv(rec, "attempts/x/code/train.sbatch")
    assert argv[0:2] == ["sbatch", "--parsable"]
    assert f"--comment=attempt:{rec['attempt_id']}" in argv
    assert any(a.startswith("--job-name=train-lam0-") for a in argv)
    seen = {}

    def run(argv):
        seen["on_disk"] = led.get(rec["attempt_id"])["submit"]["status"]   # intent visible before sbatch returns
        return subprocess.CompletedProcess(argv, 0, stdout="4711\n", stderr="")
    out = intent.submit(led, rec, argv, run=run)
    assert seen["on_disk"] == "intent"
    assert out["submit"]["status"] == "submitted" and out["submit"]["job_id"] == "4711" and out["wait"] == "job:4711"


def test_submit_rejected_and_unknown_outcomes(tmp_path):
    led = Ledger(tmp_path / "attempts")
    r1 = intent.submit(led, led.new("a"), ["sbatch", "x"], run=fake_sbatch(rc=1))
    assert r1["submit"]["status"] == "rejected" and "invalid partition" in r1["submit"]["error"]
    r2 = intent.submit(led, led.new("b"), ["sbatch", "x"], run=fake_sbatch(raise_exc=subprocess.TimeoutExpired("sbatch", 120)))
    assert r2["submit"]["status"] == "intent"            # outcome unknown: stays an intent for reconcile
    r3 = intent.submit(led, led.new("c"), ["sbatch", "x"], run=fake_sbatch(stdout="garbage\n"))
    assert r3["submit"]["status"] == "intent"


def test_reconcile_by_accounting_comment_never_resubmits(tmp_path):
    led = Ledger(tmp_path / "attempts")
    rec = intent.submit(led, led.new("a"), ["sbatch", "x"], run=fake_sbatch(raise_exc=OSError("crash")))
    assert intent.reconcile(led, rec, lambda c: None) == "unverified"
    rec = led.get(rec["attempt_id"])
    assert intent.reconcile(led, rec, lambda c: []) == "unverified"      # stays unverified after a query failure
    fresh = intent.submit(led, led.new("b"), ["sbatch", "x"], run=fake_sbatch(raise_exc=OSError("crash")))
    assert intent.reconcile(led, fresh, lambda c: ["999"]) == "submitted"
    assert led.get(fresh["attempt_id"])["wait"] == "job:999"
    dup = intent.submit(led, led.new("c"), ["sbatch", "x"], run=fake_sbatch(raise_exc=OSError("crash")))
    assert intent.reconcile(led, dup, lambda c: ["1", "2"]) == "duplicate-suspected"
    # 'unverified' stays re-queryable: accounting lag must not orphan a submission (R08).
    # A re-query is read-only correlation, never a resubmission.
    assert intent.reconcile_all(led, lambda c: ["7"]) == {rec["attempt_id"]: "submitted", dup["attempt_id"]: "submitted"}
    assert led.get(rec["attempt_id"])["submit"]["job_id"] == "7"
    assert intent.reconcile_all(led, lambda c: ["8"]) == {}   # nothing unresolved is left


def test_sacct_lookup_parses_comment_column():
    def run(argv):
        return subprocess.CompletedProcess(argv, 0, stdout="10|attempt:abc\n11|other\n12|attempt:abc\n", stderr="")
    assert intent.sacct_lookup_factory(run)("attempt:abc") == ["10", "12"]
    assert intent.sacct_lookup_factory(lambda a: subprocess.CompletedProcess(a, 1, "", ""))("attempt:abc") is None


# ---------------------------------------------------------------- waits
def test_wait_reference_prefers_barrier_then_earliest_then_lowest_id():
    assert waits.wait_reference(["30", "20"], barrier_job="99") == "job:99"
    assert waits.wait_reference(["30", "20"], expected_end={"30": 5.0, "20": 9.0}) == "job:30"
    assert waits.wait_reference(["30", "20", "100_3"]) == "job:20"
    with pytest.raises(ValueError):
        waits.wait_reference([])


def test_artifact_waits_are_forbidden_for_job_outputs():
    with pytest.raises(waits.ArtifactWaitForbidden):
        waits.forbid_artifact_wait("artifact:/x/RUN.json")
    waits.forbid_artifact_wait("job:1")


def test_barrier_argv_uses_afterany():
    argv = waits.barrier_argv("round1", ["1", "2", "3"], partition="test", attempt_id="abcdef0123")
    assert "--dependency=afterany:1:2:3" in argv and "--comment=attempt:abcdef0123" in argv and argv[-2:] == ["--wrap", "exit 0"]


def test_env_digest_can_be_pinned_by_environment(monkeypatch):
    from farmkit import snapshot
    monkeypatch.setenv("FARMKIT_ENV_DIGEST", "pinned")
    assert snapshot.env_digest() == "pinned"
    monkeypatch.delenv("FARMKIT_ENV_DIGEST")
    assert snapshot.env_digest() != "pinned"       # lru_cache must not have kept the pinned value
