"""The attempt ledger: one JSON record per execution of one step, in the workspace.

This is the worker's durable memory (D18) and the carry-over record between farms
(D34). It lives under <workspace>/attempts/ and is never read by the runtime.
Hash-valued fields are for machine comparison only (D8); nothing here renders them
for humans.
"""
from __future__ import annotations

import copy
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import __version__
from ._fs import atomic_write_json, read_json
from .observe import is_terminal

SCHEMA = "farmkit.attempt.v1"
INDEX = "INDEX.json"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_record(step: str, *, task_id: str | None = None, lease_id: str | None = None,
               n: int = 1, site: str | None = None, attempt_id: str | None = None) -> dict:
    return {
        "schema": SCHEMA,
        "attempt_id": attempt_id or uuid.uuid4().hex,
        "task_id": task_id, "lease_id": lease_id,
        "step": step, "n": n,
        "code": {}, "code_snapshot": None, "env_sha256": None, "inputs": {},
        "submit": None, "wait": None, "observed": None, "verdict": None,
        "artifacts": {}, "failure": None,
        "budget": {"class": None, "max_auto": 0, "used": 0},
        "farmkit_version": __version__, "site": site,
        "created_ts": utcnow(),
    }


class Ledger:
    """Attempt records under <workspace>/attempts/<id>.json; INDEX.json is derived.

    Reads are cached per Ledger instance until the next save through this instance.
    A workspace has one writer at a time (the worker holding the lease), and every
    `all()` used to re-read every record from disk, which made a tick O(attempts^2)
    file opens on a network filesystem. Callers still receive fresh copies.
    """

    def __init__(self, root: Path):
        self.root = Path(root)
        self._cache: list[dict] | None = None

    def invalidate(self) -> None:
        self._cache = None

    # -- storage -------------------------------------------------------------
    def path(self, attempt_id: str) -> Path:
        return self.root / f"{attempt_id}.json"

    def attempt_dir(self, attempt_id: str) -> Path:
        return self.root / attempt_id

    def new(self, step: str, **kw) -> dict:
        prior = self.by_step(step)
        kw.setdefault("n", len(prior) + 1)
        return new_record(step, **kw)

    def save(self, record: dict) -> dict:
        if record.get("schema") != SCHEMA:
            raise ValueError("not a farmkit attempt record")
        record["updated_ts"] = utcnow()
        atomic_write_json(self.path(record["attempt_id"]), record)
        self.invalidate()
        self.rebuild_index()
        return record

    def get(self, attempt_id: str) -> dict:
        return read_json(self.path(attempt_id))

    def all(self) -> list[dict]:
        if self._cache is None:
            self._cache = self._read_all()
        return copy.deepcopy(self._cache)

    def _read_all(self) -> list[dict]:
        if not self.root.exists():
            return []
        records = []
        for p in self.root.glob("*.json"):
            if p.name == INDEX or p.name.startswith("."):
                continue
            data = read_json(p)
            if isinstance(data, dict) and data.get("schema") == SCHEMA:
                records.append(data)
        records.sort(key=lambda r: (r.get("created_ts") or "", r["attempt_id"]))
        return records

    def rebuild_index(self) -> dict:
        """INDEX.json is derived from the files, never the other way round."""
        self.invalidate()                      # an explicit rebuild always looks at the files
        steps: dict[str, list[str]] = {}
        for r in self.all():
            steps.setdefault(r["step"], []).append(r["attempt_id"])
        index = {"schema": "farmkit.index.v1", "steps": steps}
        atomic_write_json(self.root / INDEX, index)
        return index

    # -- queries -------------------------------------------------------------
    def by_step(self, step: str) -> list[dict]:
        return [r for r in self.all() if r["step"] == step]

    def latest(self, step: str) -> dict | None:
        rows = self.by_step(step)
        return rows[-1] if rows else None

    def unverified_intents(self) -> list[dict]:
        """Submissions whose job id is still unknown: fresh intents and ones an earlier
        accounting query could not resolve (re-querying is read-only, never a resubmit)."""
        return [r for r in self.all() if (r.get("submit") or {}).get("status") in {"intent", "unverified"}]

    def unfinalized(self) -> list[dict]:
        """Terminal observation persisted, but neither verdict nor failure recorded
        (a worker crash between execution end and verification)."""
        out = []
        for r in self.all():
            obs = r.get("observed") or {}
            if is_terminal(obs.get("state")) and r.get("verdict") is None and not r.get("failure"):
                out.append(r)
        return out

    def in_flight(self) -> list[dict]:
        """Submitted (or adopted) with a job id and no terminal observation yet."""
        out = []
        for r in self.all():
            sub = r.get("submit") or {}
            if sub.get("status") in {"submitted", "adopted"} and sub.get("job_id"):
                obs = r.get("observed") or {}
                if not is_terminal(obs.get("state")):
                    out.append(r)
        return out

    def parked(self) -> list[dict]:
        """Attempts whose failure exhausted its budget and awaits a ruling."""
        return [r for r in self.all() if (r.get("failure") or {}).get("parked")]

    def ok(self, step: str) -> bool:
        latest = self.latest(step)
        return bool(latest and (latest.get("verdict") or {}).get("ok"))

    def release(self, step: str, ruling: str) -> dict:
        """Lift a parked failure after a ruling (D24): the step may run exactly once more.

        The failure record keeps its class, so the budget count still sees it; a repeat
        failure of the same class parks again immediately.
        """
        latest = self.latest(step)
        if latest is None or not (latest.get("failure") or {}).get("parked"):
            raise ValueError(f"{step}: latest attempt is not parked; nothing to release")
        if not ruling.strip():
            raise ValueError("release needs the ruling text (who decided what)")
        latest["failure"]["parked"] = False
        latest["failure"]["released"] = {"ts": utcnow(), "ruling": ruling.strip()}
        self.save(latest)
        return latest
