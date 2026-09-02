from __future__ import annotations

import json
from pathlib import Path

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

    def ids(self) -> set[str]:
        if not self.path.exists():
            return set()
        ids: set[str] = set()
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            ids.add(json.loads(line)["id"])
        return ids
