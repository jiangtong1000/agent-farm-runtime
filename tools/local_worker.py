#!/usr/bin/env python3
"""One mechanical local-process wake: tick, write its exact receipt, exit.

Use an installed runtime interpreter and --workspace. Optional project verifiers
are passed through. This synthetic worker parks failures for a human to review;
it makes no scientific decisions and does not accept tasks or apply rulings.
"""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

from agent_farm_runtime.adapters.codex import RECEIPT_HELPER
from farmkit._fs import atomic_write_text


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--verifiers", help="optional verifier module in the workspace")
    args = parser.parse_args(argv)
    ws = args.workspace.resolve()
    if not ws.is_dir():
        parser.error(f"workspace does not exist: {ws}")
    # The local executor supplies fenced receipt environment variables but does
    # not install the helper. Reuse the helper belonging to this runtime release.
    atomic_write_text(ws / ".farm_receipt.py", RECEIPT_HELPER)
    command = [sys.executable, "-m", "farmkit.cli", "tick", "--workspace", str(ws), "--checkpoint"]
    if args.verifiers:
        command += ["--verifiers", args.verifiers]
    tick = subprocess.run(command, text=True, capture_output=True, cwd=ws)
    (ws / "worker_last_tick.out").write_text(tick.stdout + "\n--- stderr ---\n" + tick.stderr)
    if tick.returncode:
        print(tick.stdout, end="")
        print(tick.stderr, end="", file=sys.stderr)
        return tick.returncode
    lines = tick.stdout.splitlines()
    receipt_commands = [lines[i + 1].strip() for i, line in enumerate(lines[:-1])
                        if line.startswith("REPORT WITH EXACTLY THIS COMMAND")]
    if len(receipt_commands) != 1:
        print("tick did not print exactly one receipt command", file=sys.stderr)
        return 1
    receipt_argv = shlex.split(receipt_commands[0])
    if not receipt_argv:
        print("tick printed an empty receipt command", file=sys.stderr)
        return 1
    receipt_argv[0] = sys.executable
    receipt = subprocess.run(receipt_argv, text=True, capture_output=True, cwd=ws)
    (ws / "worker_last_receipt.out").write_text(receipt.stdout + receipt.stderr)
    return receipt.returncode


if __name__ == "__main__":
    raise SystemExit(main())
