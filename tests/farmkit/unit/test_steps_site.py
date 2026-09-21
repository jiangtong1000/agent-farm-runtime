import pytest

from farmkit.ledger import Ledger
from farmkit.site import Site, SiteError
from farmkit.steps import Steps


def test_matrix_expansion_substitutes_everywhere(tmp_path):
    steps = Steps.from_dict({
        "defaults": {"snapshot": ["*.py"], "finite": ["x"]},
        "step": {
            "train:{arm}": {"matrix": {"arm": ["lam0", "lam1"]}, "run": "train.sbatch",
                            "env": {"ARM": "{arm}"}, "outputs": ["run_{arm}/RUN.json"], "group": "r1"},
            "select": {"after": ["group:r1"], "run": "python select.py", "outputs": ["sel.json"]},
            "afqmc:{arm}": {"matrix": [{"arm": "lam0"}], "run": "seg.sbatch", "outputs": ["o/{seg}.json"],
                            "segments": 2, "after": ["select"], "concurrency": 2},
        }})
    ids = [s.id for s in steps.specs]
    assert ids == ["train:lam0", "train:lam1", "select", "afqmc:lam0#1", "afqmc:lam0#2"]
    t = steps.by_id["train:lam1"]
    assert t.kind == "sbatch" and t.env == {"ARM": "lam1"} and t.outputs == ["run_lam1/RUN.json"] and t.finite == ["x"]
    assert steps.by_id["select"].kind == "local"
    seg2 = steps.by_id["afqmc:lam0#2"]
    assert seg2.after == ["chain:afqmc:lam0#1"] and seg2.outputs == ["o/2.json"] and seg2.env["SEGMENT"] == "2"
    assert steps.groups() == {"r1": ["train:lam0", "train:lam1"]}
    assert steps.check() == []


def test_check_reports_unknown_keys_missing_refs_and_scripts(tmp_path):
    steps = Steps.from_dict({
        "defaults": {"bogus": 1},
        "step": {"a": {"run": "a.sbatch", "outputs": ["x"], "after": ["group:nope", "zzz"], "gate": "gone",
                       "verify": "check_a", "wait": "sometimes", "colour": "red"},
                 "b:{v}": {"matrix": {"v": ["1"]}, "run": "b.sbatch"},
                 "c": {"run": "c.sbatch", "outputs": ["{missing}/x.json"]}}}, verifiers={})
    problems = steps.check(workspace=tmp_path)
    joined = "\n".join(problems)
    for needle in ("defaults.bogus: unknown key", 'step."a".colour: unknown key', "unknown group nope",
                   "unknown step zzz", "unknown step gone", "verifier check_a not found", "wait must be",
                   "run script a.sbatch not found", "b:1: sbatch step declares no outputs", 'step."c": unknown variable {missing}'):
        assert needle in joined, needle


def test_ready_respects_dependencies_unresolved_attempts_and_concurrency(tmp_path):
    steps = Steps.from_dict({"step": {
        "a": {"run": "a.sbatch", "outputs": ["a.json"], "group": "g"},
        "b": {"run": "b.sbatch", "outputs": ["b.json"], "after": ["group:g"]},
        "c:{k}": {"matrix": {"k": ["1", "2"]}, "run": "c.sbatch", "outputs": ["c{k}.json"], "segments": 2, "concurrency": 1},
    }})
    led = Ledger(tmp_path / "attempts")
    assert [s.id for s in steps.ready(led)] == ["a", "c:1#1", "c:2#1"] or [s.id for s in steps.ready(led)][0] == "a"
    ready_ids = [s.id for s in steps.ready(led)]
    assert "b" not in ready_ids and "c:1#2" not in ready_ids
    rec = led.new("a"); rec["submit"] = {"status": "submitted", "job_id": "5"}; led.save(rec)
    assert "a" not in [s.id for s in steps.ready(led)]                        # unresolved attempt: not ready again
    rec["observed"] = {"state": "COMPLETED"}; rec["verdict"] = {"ok": True}; led.save(rec)
    assert "b" in [s.id for s in steps.ready(led)]
    c1 = led.new("c:1#1"); c1["submit"] = {"status": "submitted", "job_id": "7"}; led.save(c1)
    ids = [s.id for s in steps.ready(led)]
    assert "c:1#2" in ids and "c:2#1" not in ids                              # chain dep met; pool concurrency 1
    parked = led.new("b"); parked["failure"] = {"class": "code", "parked": True}; led.save(parked)
    assert "b" not in [s.id for s in steps.ready(led)]


def test_site_detect_by_env_and_hostname(tmp_path):
    (tmp_path / "alpha.toml").write_text('[site]\nname="alpha"\nhostname_pattern="^alpha"\n[scheduler]\nbarrier_partition="test"\n')
    (tmp_path / "beta.toml").write_text('[site]\nname="beta"\nhostname_pattern="beta"\n')
    env = {"XDG_CONFIG_HOME": str(tmp_path / "cfg")}
    (tmp_path / "cfg" / "agent-farm" / "sites").mkdir(parents=True)
    for f in ("alpha.toml", "beta.toml"):
        (tmp_path / "cfg" / "agent-farm" / "sites" / f).write_text((tmp_path / f).read_text())
    assert Site.detect("alpha01.example.org", env).name == "alpha"
    assert Site.detect("beta01.example.org", env).name == "beta"
    assert Site.detect("beta01.example.org", {"FARMKIT_SITE": str(tmp_path / "alpha.toml")}).name == "alpha"
    with pytest.raises(SiteError):
        Site.detect("unknown-host", env)
    s = Site.load(tmp_path / "alpha.toml")
    assert s.scheduler["barrier_partition"] == "test" and s.scheduler["sacct"] == "sacct"
    assert s.worker["rotate_when_input_tokens_over"] == 120000 and s.executor("codex") == {}
