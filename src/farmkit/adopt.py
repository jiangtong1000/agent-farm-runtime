"""Adopt a pre-existing Slurm job into the ledger (D34).

Only for jobs that have NO ledger record (legacy launches). A carried-over workspace
brings its attempts/ directory along and needs nothing here. Adoption records what a
verifier will later need: the step, the inputs, the script version note and the
retry budget already spent, so budgets do not reset when a farm changes.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .ledger import Ledger


def adopt(ledger: Ledger, *, step: str, job_id: str, inputs: list[str], script_version: str,
          budget_used: int = 0, budget_class: str = "infra", task_id: str | None = None,
          site: str | None = None, note: str = "") -> dict:
    if not job_id.strip() or not step.strip():
        raise ValueError("adopt needs a step and a job id")
    rec = ledger.new(step, task_id=task_id, site=site)
    rec["submit"] = {"intent_ts": datetime.now(timezone.utc).isoformat(), "argv": None, "cwd": None,
                     "job_id": job_id, "status": "adopted", "error": None}
    rec["wait"] = f"job:{job_id}"
    rec["inputs"] = {i: {"path": i, "declared_at_adoption": True} for i in inputs}
    rec["adopted"] = {"script_version": script_version, "note": note}
    rec["budget"] = {"class": budget_class, "max_auto": 1 if budget_class == "infra" else 0,
                     "used": int(budget_used), "prior_used": int(budget_used)}   # prior_used: spent before adoption
    return ledger.save(rec)
