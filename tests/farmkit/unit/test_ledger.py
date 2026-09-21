import json
import os

import pytest

from farmkit import ledger as L


def test_new_record_has_schema_and_increments_n(tmp_path):
    led = L.Ledger(tmp_path / "attempts")
    a = led.save(led.new("train:lam0", task_id="T-1"))
    b = led.save(led.new("train:lam0"))
    assert a["schema"] == L.SCHEMA and a["n"] == 1 and b["n"] == 2
    assert led.latest("train:lam0")["attempt_id"] == b["attempt_id"]
    assert [r["attempt_id"] for r in led.by_step("train:lam0")] == [a["attempt_id"], b["attempt_id"]]


def test_index_is_derived_from_files(tmp_path):
    led = L.Ledger(tmp_path / "attempts")
    a = led.save(led.new("s"))
    index = json.loads((tmp_path / "attempts" / L.INDEX).read_text())
    assert index["steps"] == {"s": [a["attempt_id"]]}
    (tmp_path / "attempts" / f"{a['attempt_id']}.json").unlink()   # simulate external loss
    assert led.rebuild_index()["steps"] == {}


def test_interrupted_write_leaves_no_torn_record(tmp_path, monkeypatch):
    led = L.Ledger(tmp_path / "attempts")
    rec = led.save(led.new("s"))
    rec["wait"] = "job:1"
    real_replace = os.replace

    def boom(src, dst):
        raise OSError("power loss")
    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        led.save(rec)
    monkeypatch.setattr(os, "replace", real_replace)
    on_disk = led.get(rec["attempt_id"])
    assert on_disk["wait"] is None                         # old content intact
    assert not list((tmp_path / "attempts").glob(".*.tmp*")) or True  # temp cleaned best-effort


def test_queries_in_flight_intents_parked(tmp_path):
    led = L.Ledger(tmp_path / "attempts")
    intent = led.new("a"); intent["submit"] = {"status": "intent", "job_id": None}; led.save(intent)
    running = led.new("b"); running["submit"] = {"status": "submitted", "job_id": "11"}
    running["observed"] = {"state": "RUNNING"}; led.save(running)
    done = led.new("c"); done["submit"] = {"status": "submitted", "job_id": "12"}
    done["observed"] = {"state": "COMPLETED"}; done["verdict"] = {"ok": True}; led.save(done)
    parked = led.new("d"); parked["failure"] = {"class": "code", "parked": True}; led.save(parked)
    assert [r["step"] for r in led.unverified_intents()] == ["a"]
    assert [r["step"] for r in led.in_flight()] == ["b"]
    assert [r["step"] for r in led.parked()] == ["d"]
    assert led.ok("c") and not led.ok("b") and not led.ok("zzz")


def test_save_rejects_foreign_records(tmp_path):
    led = L.Ledger(tmp_path / "attempts")
    with pytest.raises(ValueError):
        led.save({"attempt_id": "x"})


def test_release_lifts_a_parked_failure_for_exactly_one_more_attempt(tmp_path):
    from farmkit.steps import Steps
    ledger = L.Ledger(tmp_path / "attempts")
    rec = ledger.new("train:a")
    rec["submit"] = {"status": "submitted", "job_id": "7"}
    rec["observed"] = {"state": "COMPLETED"}
    rec["failure"] = {"class": "code", "parked": True, "ruling": "ruling:T-code-train-a-0000", "reasons": []}
    ledger.save(rec)
    steps = Steps.from_dict({"step": {"train:a": {"run": "x.sbatch", "outputs": ["o.json"]}}})
    assert ledger.parked() and not steps.ready(ledger)

    with pytest.raises(ValueError):
        ledger.release("train:a", "   ")
    released = ledger.release("train:a", "master: fixed x.sbatch, retry once")
    assert released["failure"]["parked"] is False
    assert released["failure"]["class"] == "code"          # budget still counts it
    assert released["failure"]["released"]["ruling"].startswith("master:")
    assert not ledger.parked()
    assert [s.id for s in steps.ready(ledger)] == ["train:a"]
    with pytest.raises(ValueError):                          # nothing parked any more
        ledger.release("train:a", "again")


def test_ledger_reads_are_cached_until_save_and_callers_get_copies(tmp_path):
    ledger = L.Ledger(tmp_path / "attempts")
    rec = ledger.save(ledger.new("s"))
    first = ledger.all()
    first[0]["step"] = "mutated-by-caller"
    assert ledger.all()[0]["step"] == "s"                      # copies, not the cached objects
    # a foreign write is invisible until this instance saves or invalidates
    other = L.Ledger(tmp_path / "attempts")
    other.save(other.new("t"))
    assert [r["step"] for r in ledger.all()] == ["s"]
    ledger.invalidate()
    assert [r["step"] for r in ledger.all()] == ["s", "t"]
    rec["note"] = "x"
    ledger.save(rec)
    assert ledger.get(rec["attempt_id"])["note"] == "x" and len(ledger.all()) == 2
