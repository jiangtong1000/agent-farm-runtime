"""D8: digests are for machine comparison only; nothing human-facing shows one."""
from __future__ import annotations

import hashlib
import json

from agent_farm_runtime.adapters.codex import _same_as_previous
from agent_farm_runtime.adapters.prompts import worker_contract
from agent_farm_runtime.cli import task_summary
from agent_farm_runtime.models import Task, TaskState
from agent_farm_runtime.turnover import clean_surrender


def _task():
    return Task("T-1", "o", "d", "acceptance text", state=TaskState.WAITING, metadata={
        "latest_master_instruction": {"path": "/x/note.md", "sha256": "a" * 64, "text": "do this", "ts": "t", "actor": "m"},
        "clean_surrender": {"id": "r1", "worker_id": "W", "lease_id": "L", "ts": "t", "waiting_on": "job:1",
                            "checkpoint": {"path": "/x/c.md", "sha256": "b" * 64, "text": "next: wait"}},
        "waiting_on": "job:1",
        "acceptance_receipt": {"path": "/x/acc.md", "sha256": "c" * 64, "actor": "m", "ts": "t", "text": "ok"},
        "context_manifest": [{"path": "/m/skill.md", "sha256": "d" * 64, "text": "skill"}],
    })


def test_worker_prompt_and_summary_carry_no_digests():
    task = _task()
    prompt = worker_contract(task)
    assert "do this" in prompt and "next: wait" in prompt and "/x/note.md" in prompt
    assert "sha256" not in prompt and "a" * 64 not in prompt and "b" * 64 not in prompt
    summary = json.dumps(task_summary(task))
    assert "sha256" not in summary and "c" * 64 not in summary


def test_clean_surrender_judges_content_not_digest():
    task = _task()
    assert clean_surrender(task)
    task.metadata["clean_surrender"]["checkpoint"]["sha256"] = "wrong"
    assert clean_surrender(task)                      # the text is what matters
    task.metadata["clean_surrender"]["checkpoint"]["text"] = "   "
    assert not clean_surrender(task)


def test_receipt_dedup_by_content_with_legacy_digest_fallback():
    raw = b'{"status": "AWAITING"}'
    assert _same_as_previous(raw, {"previous_receipt": raw.decode()})
    assert not _same_as_previous(raw, {"previous_receipt": '{"status": "SUBMITTED"}'})
    assert not _same_as_previous(raw, {})
    legacy = {"previous_receipt_sha256": hashlib.sha256(raw).hexdigest()}
    assert _same_as_previous(raw, legacy)            # state written by an earlier release
    assert not _same_as_previous(b"other", legacy)
