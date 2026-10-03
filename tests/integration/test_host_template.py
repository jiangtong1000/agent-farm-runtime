"""Exercise allocation startup without Slurm, tmux servers, or live farm actions."""
import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("existing", [False, True])
def test_host_allocation_uses_explicit_socket_and_refuses_existing_session(tmp_path, existing):
    binary = tmp_path / "bin"
    binary.mkdir()
    tmux = binary / "tmux"
    tmux.write_text("#!/usr/bin/env python3\nimport json, os, sys\n"
                    "with open(os.environ['TMUX_CALLS'], 'a') as f: f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                    "if sys.argv[3] == 'has-session': sys.exit(0 if os.environ['EXISTING'] == '1' else 1)\n")
    tmux.chmod(0o755)
    calls = tmp_path / "calls.jsonl"
    shared = tmp_path / "shared"
    env = {**os.environ, "PATH": str(binary) + os.pathsep + os.environ["PATH"],
           "TMUX_CALLS": str(calls), "EXISTING": str(int(existing)), "SLURM_JOB_ID": "1234",
           "FARM_SHARED": str(shared), "FARM_NODE_TMP": str(tmp_path / "node"), "FARM_SESSION": "demo"}
    result = subprocess.run(["bash", str(ROOT / "templates" / "host_job.sbatch")],
                            env=env, capture_output=True, text=True, timeout=10)
    observed = [json.loads(line) for line in calls.read_text().splitlines()]
    assert all(call[:2] == ["-L", "farm-host-demo"] for call in observed)
    assert not any("kill-server" in call or "kill-session" in call for call in observed)
    if existing:
        assert result.returncode != 0
        assert "refusing existing session" in result.stderr
        assert len(observed) == 1
        assert not (shared / "host-1234.txt").exists()
    else:
        assert result.returncode == 0, result.stderr
        assert [call[2] for call in observed] == ["has-session", "new-session", "new-window", "new-window", "has-session"]
        assert observed[1][3:] == ["-d", "-s", "demo", "-n", "master"]
        record = (shared / "host-1234.txt").read_text()
        assert "runtime --tmux-socket=farm-host-demo" in record
        assert "attach on this node: tmux -S" in record
