"""Skip the integration suite cleanly when the fake Slurm scripts cannot execute."""
import os
import shutil
import subprocess
from pathlib import Path

import pytest

FAKE = Path(__file__).parent / "fake_slurm"


def _fake_slurm_works() -> bool:
    if shutil.which("python3") is None:
        return False
    try:
        env = {**os.environ, "FAKE_SLURM_STATE": str(FAKE / ".probe_state.json")}
        proc = subprocess.run([str(FAKE / "squeue"), "-h", "-j", "1", "-o", "%T"], text=True,
                              capture_output=True, timeout=20, env=env)
        return proc.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False
    finally:
        for name in (".probe_state.json", ".probe_state.json.lock"):
            try:
                (FAKE / name).unlink()
            except FileNotFoundError:
                pass


def pytest_collection_modifyitems(config, items):
    if _fake_slurm_works():
        return
    skip = pytest.mark.skip(reason="fake Slurm scripts are not executable here (python3 or exec bit missing)")
    for item in items:
        if str(item.fspath).startswith(str(Path(__file__).parent)):
            item.add_marker(skip)
