#!/usr/bin/env python3
"""The integration test's worker: what a codex/claude agent does on every wake, minus the model.

1. run `farmkit tick --checkpoint --verifiers verifiers` in the workspace,
2. run EXACTLY the receipt command tick printed (the runtime's own receipt helper),
3. exit.

The runtime's LocalProcessExecutor sets FARM_RECEIPT_PATH / FARM_WORKER_ID / FARM_TASK_ID
/ FARM_LEASE_ID; the receipt helper reads them. The helper text is the runtime's
(adapters/codex.py RECEIPT_HELPER); the codex executor drops it into the workspace,
the local executor does not, so this worker writes it once.
"""
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

from agent_farm_runtime.adapters.codex import RECEIPT_HELPER

ws = Path(os.environ["FIVE_ARM_WS"])
helper = ws / ".farm_receipt.py"
if not helper.exists():
    helper.write_text(RECEIPT_HELPER)

# The model's part, reduced to its mechanical core: if the master's latest ruling names
# `farmkit release` lines, run them once (tracked by the ruling's digest) before ticking.
task_path = os.environ.get("FARM_TASK_PATH")
if task_path and Path(task_path).exists():
    task = json.loads(Path(task_path).read_text())
    ruling = (task.get("metadata") or {}).get("latest_master_instruction") or {}
    applied = ws / ".rulings_applied"
    seen = applied.read_text().split() if applied.exists() else []
    if ruling.get("sha256") and ruling["sha256"] not in seen:
        for line in ruling.get("text", "").splitlines():
            if line.strip().startswith("farmkit release"):
                argv = [sys.executable, "-m", "farmkit.cli", *shlex.split(line.strip())[1:], "--workspace", str(ws)]
                rel = subprocess.run(argv, text=True, capture_output=True, cwd=str(ws))
                (ws / "worker_last_release.out").write_text(rel.stdout + rel.stderr)
                if rel.returncode != 0:
                    print(rel.stderr, file=sys.stderr)
                    sys.exit(rel.returncode)
        applied.write_text("\n".join([*seen, ruling["sha256"]]) + "\n")

tick = subprocess.run([sys.executable, "-m", "farmkit.cli", "tick", "--workspace", str(ws), "--checkpoint",
                       "--verifiers", "verifiers"], text=True, capture_output=True, cwd=str(ws))
(ws / "worker_last_tick.out").write_text(tick.stdout + "\n--- stderr ---\n" + tick.stderr)
if tick.returncode != 0:
    print(tick.stdout); print(tick.stderr, file=sys.stderr)
    sys.exit(tick.returncode)

command = None
lines = tick.stdout.splitlines()
for i, line in enumerate(lines):
    if line.startswith("REPORT WITH EXACTLY THIS COMMAND"):
        command = lines[i + 1].strip()
if not command:
    sys.exit("tick printed no receipt command")
argv = shlex.split(command)
argv[0] = sys.executable                     # "python" -> this interpreter
receipt = subprocess.run(argv, text=True, capture_output=True, cwd=str(ws))
(ws / "worker_last_receipt.out").write_text(receipt.stdout + receipt.stderr)
sys.exit(receipt.returncode)
