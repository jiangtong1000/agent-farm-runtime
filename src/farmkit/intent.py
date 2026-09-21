"""Submit intent: write it down before sbatch, correlate afterwards (RFC §3.3, D2 rule 2).

The attempt id is carried to Slurm in --comment and in the job name. This site stores
job_comment in accounting, so a submission whose job id was never saved can be found
again. A missing accounting row is NOT proof that sbatch never ran: the record stays
'unverified' and a human decides. Nothing here ever resubmits.
"""
from __future__ import annotations

import re
import subprocess
from collections.abc import Callable
from datetime import datetime, timezone

from .ledger import Ledger

Runner = Callable[[list[str]], subprocess.CompletedProcess]
AccountingLookup = Callable[[str], "list[str] | None"]   # comment -> job ids, None = query failed


def sanitize(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", text).strip("-") or "step"


def job_name(step: str, attempt_id: str) -> str:
    return f"{sanitize(step)}-{attempt_id[:8]}"


def comment(attempt_id: str) -> str:
    return f"attempt:{attempt_id}"


def sbatch_argv(record: dict, script: str, *, extra: list[str] = ()) -> list[str]:
    """Argv for sbatch; the script path should be the attempt's code snapshot copy."""
    return ["sbatch", "--parsable", f"--job-name={job_name(record['step'], record['attempt_id'])}",
            f"--comment={comment(record['attempt_id'])}", *extra, script]


def default_sbatch(argv: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(argv, text=True, capture_output=True, timeout=120)


def submit(ledger: Ledger, record: dict, argv: list[str], *, run: Runner = default_sbatch,
           cwd: str | None = None) -> dict:
    """Record intent, call sbatch once, record the job id. Never retries."""
    record["submit"] = {"intent_ts": datetime.now(timezone.utc).isoformat(), "argv": list(argv),
                        "cwd": cwd, "job_id": None, "status": "intent", "error": None}
    ledger.save(record)
    try:
        proc = run(argv)
    except (OSError, subprocess.TimeoutExpired) as exc:
        record["submit"]["error"] = f"sbatch did not return: {exc}"
        return ledger.save(record)             # stays 'intent': outcome unknown
    if proc.returncode != 0:
        record["submit"]["error"] = (proc.stderr or "").strip()[:500]
        record["submit"]["status"] = "rejected"  # sbatch positively refused: nothing was queued
        return ledger.save(record)
    job_id = proc.stdout.strip().split(";")[0].split()[0] if proc.stdout.strip() else ""
    if not re.fullmatch(r"[0-9]+(?:_[0-9]+)?", job_id):
        record["submit"]["error"] = f"unparseable sbatch output: {proc.stdout.strip()[:200]}"
        return ledger.save(record)             # stays 'intent'
    record["submit"].update(job_id=job_id, status="submitted")
    record["wait"] = f"job:{job_id}"
    return ledger.save(record)


def reconcile(ledger: Ledger, record: dict, lookup: AccountingLookup) -> str:
    """Resolve a record left at 'intent'. Returns submitted | unverified | duplicate-suspected."""
    sub = record.get("submit") or {}
    if sub.get("status") not in {"intent", "unverified"}:
        return sub.get("status") or "none"
    found = lookup(comment(record["attempt_id"]))
    if found is None:
        sub["status"] = "unverified"; sub["error"] = "accounting query failed"
    elif len(found) == 1:
        sub.update(job_id=found[0], status="submitted", error=None)
        record["wait"] = f"job:{found[0]}"
    elif len(found) > 1:
        sub["status"] = "unverified"; sub["error"] = f"several jobs carry this attempt id: {found}"
        ledger.save(record)
        return "duplicate-suspected"
    else:
        sub["status"] = "unverified"; sub["error"] = "no accounting row yet (lag, or sbatch never ran)"
    ledger.save(record)
    return sub["status"]


def reconcile_all(ledger: Ledger, lookup: AccountingLookup) -> dict[str, str]:
    return {r["attempt_id"]: reconcile(ledger, r, lookup) for r in ledger.unverified_intents()}


def sacct_lookup_factory(run: Runner = default_sbatch) -> AccountingLookup:
    """Real accounting lookup by comment; a failed query returns None."""
    def lookup(comment_value: str) -> list[str] | None:
        try:
            proc = run(["sacct", "-n", "-X", "-P", "-S", "now-2days", "--format=JobID,Comment%80"])
        except (OSError, subprocess.TimeoutExpired):
            return None
        if proc.returncode != 0:
            return None
        return [line.split("|")[0] for line in proc.stdout.splitlines()
                if len(line.split("|")) >= 2 and line.split("|")[1].strip() == comment_value]
    return lookup
