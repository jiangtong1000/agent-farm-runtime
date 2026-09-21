#!/usr/bin/env python3
"""Local step: pick the round-1 arm with the lowest finite validation metric.

Writes selection.json with a provenance block naming THIS attempt (farmkit exports
FARMKIT_ATTEMPT_ID to every step it runs), so the verifier can tell a fresh selection
from a stale file.
"""
import json
import math
import os
from pathlib import Path

runs = {}
for path in sorted(Path("runs").glob("*/RUN.json")):
    data = json.loads(path.read_text())
    metric = data.get("selected", {}).get("metric")
    if isinstance(metric, (int, float)) and math.isfinite(metric):
        runs[path.parent.name] = metric
if not runs:
    raise SystemExit("no finite round-1 metrics to select from")
winner = min(runs, key=lambda arm: (runs[arm], arm))
Path("selection.json").write_text(json.dumps({
    "provenance": {"attempt_id": os.environ.get("FARMKIT_ATTEMPT_ID")},
    "winner": winner, "metrics": runs, "rule": "lowest finite validation metric; ties by name",
}, indent=1))
print(f"selected {winner}")
