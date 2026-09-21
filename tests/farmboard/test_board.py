"""farmboard: model, text/HTML renders, and command-generating actions (D19, D28–D30)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from farmboard import actions
from farmboard.model import build, rollout_tokens, ruling_class
from farmboard.render import render_html, render_text
from farmkit.ledger import Ledger


class FakeReader:
    def __init__(self, status, summaries, events):
        self._status, self._summaries, self._events = status, summaries, events

    def status(self):
        return self._status

    def task_summary(self, task_id):
        return self._summaries[task_id]

    def events_after(self, cursor):
        return self._events, "99"


def _workspace(tmp_path, name, *, failure=False, review=False):
    ws = tmp_path / name; ws.mkdir()
    ledger = Ledger(ws / "attempts")
    rec = ledger.new("train:lam0", task_id="T-DEMO")
    rec["submit"] = {"job_id": "4711", "status": "submitted"}
    ledger.save(rec)
    rec0 = ledger.new("train:lam2", task_id="T-DEMO")
    rec0["submit"] = {"job_id": "4710", "status": "submitted"}
    rec0["observed"] = {"state": "RUNNING"}                 # observed but not terminal: still in flight
    ledger.save(rec0)
    rec2 = ledger.new("train:lam1", task_id="T-DEMO")
    rec2["submit"] = {"job_id": "4712", "status": "submitted"}
    rec2["observed"] = {"state": "COMPLETED"}
    rec2["verdict"] = {"ok": not failure, "reasons": [] if not failure else ["required output missing"]}
    if failure:
        rec2["failure"] = {"class": "code"}
        ledger.attempt_dir(rec2["attempt_id"]).mkdir(parents=True)
        (ledger.attempt_dir(rec2["attempt_id"]) / "FAILURE.md").write_text("# failure\n")
    ledger.save(rec2)
    (ws / "CHECKPOINT.md").write_text("next: wait\n")
    if review:
        (ws / "REVIEW.md").write_text("# review\n")
    return ws


def _status(tasks):
    return {"pid": 4242, "pid_alive": True, "interval": 30, "last_tick": {"ts": "2026-09-20T10:00:00+00:00", "epoch": 0},
            "source_matches": True, "handoff": None, "pending_task_commit": False, "pending_recovery": False,
            "pending_deployment_event": False, "task_counts": {"WAITING": 2, "SUBMITTED": 1, "DONE": 1},
            "tasks": tasks, "observed_jobs": []}


@pytest.fixture
def board(tmp_path):
    ws_live = _workspace(tmp_path, "ws_live")
    ws_park = _workspace(tmp_path, "ws_park", failure=True)
    ws_sub = _workspace(tmp_path, "ws_sub", review=True)
    # a fake codex rollout for the live worker
    (ws_live / ".session_id_W-live").write_text("0000aaaa-1111-2222-3333-444444444444\n")
    sessions = tmp_path / "sessions" / "2026" / "09" / "20"; sessions.mkdir(parents=True)
    rollout = sessions / "rollout-2026-09-20T10-00-00-0000aaaa-1111-2222-3333-444444444444.jsonl"
    rollout.write_text("\n".join([
        json.dumps({"type": "session_meta"}),
        json.dumps({"type": "token_usage_record", "payload": {"usage": {"input_tokens": 21000, "cached_input_tokens": 20000}}}),
        json.dumps({"type": "token_usage_record", "payload": {"usage": {"input_tokens": 152000, "cached_input_tokens": 151000}}}),
    ]) + "\n")
    tasks = [
        {"id": "T-DEMO", "state": "WAITING", "waiting_on": "job:4711", "revision": 12, "lease": {"worker_id": "W-live", "lease_id": "L1"}},
        {"id": "T-PARKED", "state": "WAITING", "waiting_on": "ruling:T-PARKED-code-train-lam1-abcd1234", "revision": 3, "lease": {"worker_id": "W-park", "lease_id": "L2"}},
        {"id": "T-SUBMITTED", "state": "SUBMITTED", "waiting_on": None, "revision": 16, "lease": None},
        {"id": "T-DONE", "state": "DONE", "waiting_on": None, "revision": 44, "lease": None},
    ]
    summaries = {
        "T-DEMO": {"workspace": str(ws_live), "last_receipt_note": "round1 submitted", "objective": "teach", "dispatches": {"workers": ["W-old", "W-live"]}},
        "T-PARKED": {"workspace": str(ws_park), "last_receipt_note": "parked", "objective": "x", "dispatches": {"workers": ["W-park"]}},
        "T-SUBMITTED": {"workspace": str(ws_sub), "last_receipt_note": "report ready", "objective": "y", "dispatches": {"workers": []}},
        "T-DONE": {"workspace": None, "last_receipt_note": None, "objective": "z", "dispatches": {"workers": []}},
    }
    events = [{"ts": "2026-09-20T09:59:00+00:00", "type": "RECEIPT_APPLIED", "task_id": "T-DEMO", "payload": {"status": "AWAITING"}}]
    return build(FakeReader(_status(tasks), summaries, events), farm="example-farm", sessions_root=tmp_path / "sessions",
                 token_budget_over=120_000)


def test_model_classifies_needs_you_live_history(board):
    assert [c.id for c in board.needs_you] == ["T-PARKED", "T-SUBMITTED"]
    assert [c.id for c in board.live] == ["T-DEMO"]
    assert [c.id for c in board.history] == ["T-DONE"]
    park = board.needs_you[0]
    assert park.needs_you == "ruling:code" and park.ruling_class == "code" and park.evidence.endswith("FAILURE.md")
    sub = board.needs_you[1]
    assert sub.needs_you == "submitted" and sub.review.endswith("REVIEW.md")
    assert "accept" in sub.actions and "ruling" not in sub.actions
    live = board.live[0]
    assert live.input_tokens_last == 152000 and live.cached_tokens_last == 151000     # last record wins
    assert live.generations == 2 and sorted(live.in_flight_jobs) == ["4710", "4711"]
    assert [a.verdict for a in live.attempts] == ["pending", "ok", "pending"]
    assert "ruling" in live.actions and "rotate" in live.actions and "accept" not in live.actions


def test_text_and_html_renders_contain_the_facts(board):
    text = render_text(board)
    assert "daemon ALIVE pid 4242" in text and "NEEDS YOU" in text and "T-PARKED" in text
    assert "152k ▲" in text                                     # over the rotation threshold
    assert "T-DONE DONE rev 44" in text and "RECEIPT_APPLIED" in text
    page = render_html(board)
    assert "<title>farmboard · example-farm</title>" in page and "FAILURE.md" in page and "REVIEW.md" in page
    assert "sha256" not in page and "sha256" not in text        # D8


def test_ruling_class_parsing():
    assert ruling_class("ruling:T-9-code-train-lam0-abcd1234", "T-9") == "code"
    assert ruling_class("ruling:owner-afqmc-hold", "T-9") == "owner"
    assert ruling_class("ruling:T115-owner-multi-UHF-decision", "T-115") == "unknown"
    assert ruling_class("job:1", "T-9") is None


def test_rollout_tokens_missing_is_none(tmp_path):
    assert rollout_tokens(tmp_path, "W-x", tmp_path) == (None, None)


def test_actions_generate_exact_commands_and_respect_preconditions(board, tmp_path):
    live, park, sub = board.live[0], board.needs_you[0], board.needs_you[1]
    argv = actions.command(park, "ruling", farm_wrapper="/f/farm", project="/p", actor="master", evidence_file="/e/note.md")
    assert argv == ["/f/farm", "--project", "/p", "task-ruling", "T-PARKED", "--expected-revision", "3", "--actor", "master", "--evidence-file", "/e/note.md"]
    argv = actions.command(sub, "accept", farm_wrapper="/f/farm", project="/p", actor="m", evidence_file="/e/a.md")
    assert argv[3:5] == ["task-accept", "T-SUBMITTED"] and "--expected-revision" in argv and "16" in argv
    with pytest.raises(ValueError, match="requires SUBMITTED"):
        actions.command(live, "accept", farm_wrapper="/f/farm", project="/p", actor="m", evidence_file="/e/a.md")
    with pytest.raises(ValueError, match="needs an evidence file"):
        actions.command(live, "ruling", farm_wrapper="/f/farm", project="/p", actor="m")
    rot = actions.command(live, "rotate", farm_wrapper="/f/farm", project="/p", actor="m")
    assert rot[3:] == ["task-rotate", "T-DEMO", "--expected-revision", "12", "--actor", "m", "--request-id", "rotate-012"]
    new = actions.command(sub, "new-from", farm_wrapper="/f/farm", project="/p", actor="m", new_task_id="T-119", brief_file="/b/BRIEF.md",
                          deliverable="report.md", acceptance="reviewer reads report.md")
    assert "--context-file" in new and sub.review in new and "--workspace" in new
    assert actions.describe(rot).startswith("/f/farm --project /p task-rotate")
    # nothing in this module writes farm state: only argv construction until run()
    calls = []
    actions.run(rot, runner=lambda argv, **kw: calls.append(argv) or __import__("subprocess").CompletedProcess(argv, 0, "", ""))
    assert calls == [rot]


def test_farmkit_board_alias_delegates(monkeypatch):
    import farmkit.cli as fk
    import farmboard.cli as fb
    seen = {}
    monkeypatch.setattr(fb, "main", lambda argv: (seen.setdefault("argv", argv), 0)[1])
    assert fk.main(["board", "--project", "/p", "--once"]) == 0
    assert seen["argv"] == ["--project", "/p", "--once"]


def test_new_from_contract_parser_and_command(board):
    sub = board.needs_you[1]
    text = actions.CONTRACT_TEMPLATE.format(new_id="T-SUBMITTED-next", old_id="T-SUBMITTED", read_first="REVIEW.md")
    parsed = actions.parse_contract(text)
    assert parsed["deliverable"] is None and parsed["acceptance"] is None          # placeholders count as missing
    with pytest.raises(ValueError, match="deliverable"):
        actions.command(sub, "new-from", farm_wrapper="/f", project="/p", actor="m", new_task_id="T-SUBMITTED-next", brief_file="/b.md")
    filled = text.replace("<the file(s) or result the worker must produce>", "report.md").replace(
        "<project-defined checks a reviewer applies>", "reviewer reads report.md")
    parsed = actions.parse_contract(filled)
    assert parsed["deliverable"] == "report.md" and "## Goal" in parsed["brief"]
    argv = actions.command(sub, "new-from", farm_wrapper="/f", project="/p", actor="m", new_task_id="T-SUBMITTED-next",
                           brief_file="/b.md", **{k: parsed[k] for k in ("objective", "deliverable", "acceptance")})
    assert "<fill>" not in argv and "report.md" in argv
