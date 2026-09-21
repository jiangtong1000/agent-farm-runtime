"""Shared state for the fake Slurm scripts and the test's control handle.

State file: $FAKE_SLURM_STATE (JSON). Jobs: id -> {name, comment, dependency, state,
argv, exit_code}. The test advances states; `finish()` honours kill_invalid_depend:
when a job ends in anything but COMPLETED, its afterok dependents become CANCELLED.
"""
from __future__ import annotations

import fcntl
import json
import os
import re
from pathlib import Path

ACTIVE = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING"}
BAD = {"FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "BOOT_FAIL", "DEADLINE", "PREEMPTED"}


def state_path() -> Path:
    return Path(os.environ["FAKE_SLURM_STATE"])


class State:
    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else state_path()

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = open(str(self.path) + ".lock", "w")
        fcntl.flock(self.lock, fcntl.LOCK_EX)
        self.data = json.loads(self.path.read_text()) if self.path.exists() else {"next_id": 1000, "jobs": {}}
        return self

    def __exit__(self, *exc):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=1))
        os.replace(tmp, self.path)
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()

    # scheduler side -------------------------------------------------------
    def submit(self, argv: list[str]) -> str:
        self.data["next_id"] += 1
        jid = str(self.data["next_id"])
        opts = {}
        for a in argv:
            for key in ("--job-name", "--comment", "--dependency", "--export", "--chdir", "--output", "--error"):
                if a.startswith(key + "="):
                    opts[key[2:]] = a.split("=", 1)[1]
        self.data["jobs"][jid] = {"name": opts.get("job-name", ""), "comment": opts.get("comment", ""),
                                  "dependency": opts.get("dependency"), "export": opts.get("export", ""),
                                  "state": "PENDING", "exit_code": "0:0", "argv": argv}
        return jid

    def job(self, jid: str) -> dict | None:
        return self.data["jobs"].get(jid)

    # test side ------------------------------------------------------------
    def finish(self, jid: str, state: str = "COMPLETED") -> None:
        self.data["jobs"][jid]["state"] = state
        if state != "COMPLETED":
            self.data["jobs"][jid]["exit_code"] = "1:0"
            self._cancel_dependents(jid)

    def start(self, jid: str) -> None:
        self.data["jobs"][jid]["state"] = "RUNNING"

    def _cancel_dependents(self, jid: str) -> None:
        for other, rec in self.data["jobs"].items():
            dep = rec.get("dependency") or ""
            if dep.startswith("afterok:") and jid in dep.split(":")[1:] and rec["state"] in ACTIVE:
                rec["state"] = "CANCELLED"
                rec["exit_code"] = "0:0"
                self._cancel_dependents(other)

    def attempt_of(self, jid: str) -> str:
        m = re.search(r"attempt:([0-9a-f]+)", self.data["jobs"][jid].get("comment", ""))
        return m.group(1) if m else ""

    def env_of(self, jid: str) -> dict:
        exported = self.data["jobs"][jid].get("export", "")
        out = {}
        for item in exported.split(",")[1:] if exported.startswith("ALL,") else exported.split(","):
            if "=" in item:
                k, v = item.split("=", 1)
                out[k] = v
        return out


def read_state() -> dict:
    path = state_path()
    return json.loads(path.read_text()) if path.exists() else {"next_id": 1000, "jobs": {}}


def slurm_state_fn(path: Path):
    """A `job_id -> state` callable for the runtime's make_unblock (reads the state file)."""
    def fn(job_id: str):
        data = json.loads(Path(path).read_text()) if Path(path).exists() else {"jobs": {}}
        job = data["jobs"].get(job_id)
        return job["state"] if job else None
    return fn
