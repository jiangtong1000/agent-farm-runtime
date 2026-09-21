"""Read-only access to a farm's runtime state through the pinned `farm` CLI (D35, D38).

farmkit never opens a farm's .farm/ directory itself. watch, health and board ask the
runtime through two read-only commands that WP1 adds to the runtime:

    farm --project <farm> status --json
    farm --project <farm> events --after <cursor>

A RuntimeReader is any object with `status()` and `events_after(cursor)`; tests use
an in-memory fake.
"""
from __future__ import annotations

import json
import subprocess
from typing import Protocol


class RuntimeReaderError(RuntimeError):
    pass


class RuntimeReader(Protocol):
    def status(self) -> dict: ...
    def events_after(self, cursor: str | None) -> tuple[list[dict], str]: ...
    def task_summary(self, task_id: str) -> dict: ...


def tail_events(reader, n: int, *, max_pages: int = 10_000) -> list[dict]:
    """The last n events. Uses the runtime's `events --last` (reads from the end of the
    log) when the reader offers it, else pages through the log."""
    last = getattr(reader, "events_last", None)
    if last is not None:
        try:
            return last(n)
        except RuntimeReaderError:       # older wrapper without --last: fall back to paging
            pass
    cursor, tail = None, []
    for _ in range(max_pages):
        events, new_cursor = reader.events_after(cursor)
        if not events or new_cursor == cursor:
            break
        tail = (tail + events)[-n:]
        cursor = new_cursor
    return tail


class CliRuntimeReader:
    def __init__(self, farm_wrapper: str, project: str, *, run=subprocess.run, timeout_s: float = 60.0):
        self.farm_wrapper, self.project, self.run, self.timeout_s = farm_wrapper, project, run, timeout_s

    def _call(self, *args: str) -> str:
        argv = [self.farm_wrapper, "--project", self.project, *args]
        try:
            proc = self.run(argv, text=True, capture_output=True, timeout=self.timeout_s)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeReaderError(f"cannot run {argv[0]} {args[0]}: {exc}") from exc
        if proc.returncode != 0:
            err = (proc.stderr or "").strip()
            hint = " (this runtime release does not provide the read-only command; upgrade to a release with status --json / events --after)" \
                if "invalid choice" in err or "unrecognized arguments" in err else ""
            raise RuntimeReaderError(f"{args[0]} failed: {err[:300]}{hint}")
        return proc.stdout

    def status(self) -> dict:
        return json.loads(self._call("status", "--json"))

    def task_summary(self, task_id: str) -> dict:
        return json.loads(self._call("task-show", task_id, "--summary"))

    def events_last(self, n: int) -> list[dict]:
        """The runtime's `events --last N` (tail read); RuntimeReaderError on an older wrapper."""
        return json.loads(self._call("events", "--last", str(n))).get("events", [])

    def events_after(self, cursor: str | None) -> tuple[list[dict], str]:
        args = ["events"] + (["--after", cursor] if cursor else [])
        payload = json.loads(self._call(*args))
        return payload.get("events", []), str(payload.get("cursor", cursor or "0"))
