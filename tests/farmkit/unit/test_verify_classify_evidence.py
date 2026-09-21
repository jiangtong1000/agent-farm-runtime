import json

from farmkit import classify, evidence
from farmkit.verify import Verdict, default_checks, run_verifier


def attempt(aid="A2", code=None):
    return {"attempt_id": aid, "step": "train:lam0", "n": 1, "code": code or {"train.py": "aa", "slt.py": "bb"},
            "submit": {"job_id": "4711", "intent_ts": "t"}, "observed": {"state": "FAILED"}, "code_snapshot": "attempts/A2/code"}


def run_json(tmp_path, payload, name="RUN.json"):
    p = tmp_path / name
    p.write_text(json.dumps(payload))
    return p


GOOD = {"provenance": {"attempt_id": "A2"}, "selected": {"validation": {"S_val": 0.6}, "grad_inf": 1e-3}}
FINITE = ["selected.validation.S_val", "selected.grad_inf"]


def test_default_checks_accept_matching_finite_output(tmp_path):
    v = default_checks(attempt(), {"RUN.json": run_json(tmp_path, GOOD)}, required=["RUN.json"], finite=FINITE,
                       gate={"code": {"train.py": "aa", "slt.py": "bb"}})
    assert v.ok and v.reasons == [] and "gate" in v.checked


def test_default_checks_reject_stale_attempt_nan_missing_and_gate_drift(tmp_path):
    stale = dict(GOOD, provenance={"attempt_id": "A1"})
    assert "different attempt" in default_checks(attempt(), {"RUN.json": run_json(tmp_path, stale, "a.json")}).reasons[0]
    nan = json.loads(json.dumps(GOOD)); nan["selected"]["validation"]["S_val"] = float("nan")
    v = default_checks(attempt(), {"RUN.json": run_json(tmp_path, nan, "b.json")}, finite=FINITE)
    assert any("S_val" in r for r in v.reasons)
    missing = json.loads(json.dumps(GOOD)); del missing["selected"]["grad_inf"]
    v = default_checks(attempt(), {"RUN.json": run_json(tmp_path, missing, "c.json")}, finite=FINITE)
    assert any("grad_inf" in r for r in v.reasons)
    v = default_checks(attempt(code={"train.py": "changed", "slt.py": "bb"}), {"RUN.json": run_json(tmp_path, GOOD, "d.json")},
                       gate={"code": {"train.py": "aa", "slt.py": "bb"}})
    assert v.reasons == ["code differs from the certified gate: train.py"]
    v = default_checks(attempt(), {"RUN.json": tmp_path / "nope.json"}, required=["RUN.json"])
    assert v.reasons == ["required output missing: RUN.json"]
    for r in v.reasons:
        assert len([w for w in r.split() if len(w) == 64]) == 0       # no digests in prose


def test_run_verifier_wraps_exceptions_and_bools():
    def boom(a, arts):
        raise KeyError("selected")
    v = run_verifier(boom, attempt(), {})
    assert not v.ok and "raised KeyError" in v.reasons[0]
    assert run_verifier(lambda a, b: True, attempt(), {}).ok
    assert not run_verifier(lambda a, b: False, attempt(), {}).ok


def test_classify_matrix():
    assert classify.classify(None) == "unknown"
    assert classify.classify("NODE_FAIL") == "infra"
    assert classify.classify("PREEMPTED") == "infra"
    assert classify.classify("TIMEOUT") == "budget"
    assert classify.classify("FAILED") == "code"
    assert classify.classify("OUT_OF_MEMORY") == "code"
    assert classify.classify("CANCELLED by 12345") == "code"
    assert classify.classify("COMPLETED", output_present=False) == "code"
    assert classify.classify("COMPLETED", verdict=Verdict(False, ["RUN.json belongs to a different attempt"])) == "code"
    assert classify.classify("COMPLETED", verdict=Verdict(False, ["selected.grad_inf is missing or not a finite number in RUN.json"])) == "science"
    assert classify.classify("COMPLETED", verdict=Verdict(True)) is None
    assert classify.retry_allowed("infra", 0) and not classify.retry_allowed("infra", 1) and not classify.retry_allowed("code", 0)


def test_ruling_name_is_valid_for_the_runtime_grammar():
    name = classify.ruling_name("T-DEMO-sample-study", "code", "train:lam0.1", "8b21e3f0aa")
    assert name == "ruling:T-DEMO-sample-study-code-train-lam0.1-8b21e3f0"
    assert name.startswith("ruling:") and name.split(":", 1)[1].strip()
    assert classify.ruling_name(None, "infra", "afqmc:lam0#3", "abcdefgh") == "ruling:task-infra-afqmc-lam0-3-abcdefgh"


def test_failure_md_is_plain_and_complete(tmp_path):
    err = tmp_path / "job.err"
    err.write_text("\n".join(f"line {i}" for i in range(60)))
    verdict = Verdict(False, ["required output missing: RUN.json"], ["required:RUN.json"])
    path = evidence.write_failure(tmp_path / "attempts" / "A2", attempt(), cls="code", sacct_row="4711|train-lam0|FAILED|1:0|00:01:08|end|attempt:A2",
                                  stderr_path=err, verdict=verdict, ruling="ruling:T-code-train-lam0-A2",
                                  budget_note="Automatic retries for this class: 0.")
    text = path.read_text()
    assert "Class: **code**" in text and "ruling:T-code-train-lam0-A2" in text and "line 59" in text and "line 19" not in text
    assert "required output missing: RUN.json" in text
    assert evidence.stderr_tail(None).startswith("(no stderr")
