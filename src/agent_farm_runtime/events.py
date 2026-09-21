from __future__ import annotations

import json
import os
from pathlib import Path

from .adapters.filesystem import sync_directory
from .models import Event


class EventLog:
    """Append-only audit log. Not authoritative and not a signal bus in V2.0."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, event: Event) -> None:
        line = json.dumps(event.to_dict(), sort_keys=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        sync_directory(self.path.parent)

    def sync(self) -> None:
        """Retry persistence after a failed append whose record is already visible."""
        with self.path.open("r+b") as handle:
            os.fsync(handle.fileno())
        sync_directory(self.path.parent)

    def ids(self) -> set[str]:
        if not self.path.exists():
            return set()
        ids: set[str] = set()
        contents = self.path.read_text(encoding="utf-8")
        if contents and not contents.endswith("\n"):
            raise ValueError("audit log has an incomplete final record; inspect before recovery")
        for line in contents.splitlines():
            if not line.strip():
                continue
            ids.add(json.loads(line)["id"])
        return ids
