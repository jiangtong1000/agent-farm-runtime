"""Board actions are command generators (D19, D30).

Each action: check the precondition on the current card → open $EDITOR on an evidence
file → build the exact `farm` command with --expected-revision → show it → on
confirmation run it through the pinned wrapper. farmboard never writes .farm/.
"""
from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

from .model import TaskCard

ACTIONS = {
    "ruling": ("task-ruling", "instruction for the parked/ waiting worker"),
    "accept": ("task-accept", "acceptance note: what was reviewed and why it passes"),
    "rework": ("task-rework", "what must change before resubmission"),
    "amend": ("task-amend", "reason for changing the contract (acceptance file separate)"),
    "rotate": ("task-rotate", None),
    "new-from": ("task-create", None),
}


def precondition(card: TaskCard, action: str) -> str | None:
    """None when allowed; otherwise the reason it is disabled."""
    if action not in ACTIONS:
        return f"unknown action {action}"
    if action in card.actions:
        return None
    return card.reasons_disabled.get(action, "not applicable in this state")


CONTRACT_TEMPLATE = """# Contract for {new_id} (continues {old_id})

## Objective
continues {old_id}: <one sentence>

## Deliverable
<the file(s) or result the worker must produce>

## Acceptance
<project-defined checks a reviewer applies>

## Brief
# {new_id}

## Goal

## Boundaries

## Read first
{read_first}

## Method
farmkit tick --checkpoint, then the receipt command it prints.
"""


def parse_contract(text: str) -> dict:
    """Sections of the new-from contract file: objective, deliverable, acceptance, brief.
    A section left at its <placeholder> counts as missing."""
    out, current = {}, None
    for line in text.splitlines():
        if line.startswith("## ") and current != "brief":
            current = line[3:].strip().lower()
            out[current] = []
            continue
        if current:
            out[current].append(line)
    result = {}
    for key in ("objective", "deliverable", "acceptance"):
        body = "\n".join(out.get(key, [])).strip()
        result[key] = None if (not body or body.startswith("<")) else body
    result["brief"] = "\n".join(out.get("brief", [])).strip() or None
    return result


def command(card: TaskCard, action: str, *, farm_wrapper: str, project: str, actor: str,
            evidence_file: str | None = None, acceptance_file: str | None = None,
            request_id: str | None = None, new_task_id: str | None = None, brief_file: str | None = None,
            context_files: list[str] | None = None, objective: str | None = None,
            deliverable: str | None = None, acceptance: str | None = None) -> list[str]:
    reason = precondition(card, action)
    if reason:
        raise ValueError(f"{action} not allowed for {card.id}: {reason}")
    base = [farm_wrapper, "--project", project]
    if action == "new-from":
        if not (new_task_id and brief_file):
            raise ValueError("new-from needs --id and a brief file")
        if not (deliverable and acceptance):
            raise ValueError("new-from needs a deliverable and an acceptance (fill the contract file)")
        argv = base + ["task-create", "--id", new_task_id, "--objective", objective or f"continues {card.id}",
                       "--deliverable", deliverable, "--acceptance", acceptance, "--brief-file", brief_file]
        for ctx in (context_files or ([card.review] if card.review else ([card.checkpoint] if card.checkpoint else []))):
            argv += ["--context-file", ctx]
        if card.workspace:
            argv += ["--workspace", card.workspace]
        return argv
    sub = ACTIONS[action][0]
    argv = base + [sub, card.id, "--expected-revision", str(card.revision), "--actor", actor]
    if action == "rotate":
        argv += ["--request-id", request_id or f"rotate-{card.revision:03d}"]
        return argv
    if not evidence_file:
        raise ValueError(f"{action} needs an evidence file")
    argv += ["--evidence-file", evidence_file]
    if action == "amend":
        if not acceptance_file:
            raise ValueError("amend needs --acceptance-file")
        argv += ["--acceptance-file", acceptance_file]
    return argv


def edit_evidence(path: Path, template: str) -> Path:
    """Open $EDITOR on a prefilled evidence file; returns the path. Non-interactive when $EDITOR is unset."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(template)
    editor = os.environ.get("EDITOR")
    if editor:
        subprocess.run([*shlex.split(editor), str(path)], check=False)
    return path


def run(argv: list[str], *, runner=subprocess.run) -> subprocess.CompletedProcess:
    return runner(argv, text=True, capture_output=True)


def describe(argv: list[str]) -> str:
    return " ".join(shlex.quote(a) for a in argv)
