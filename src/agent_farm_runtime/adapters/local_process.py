from __future__ import annotations

import json
import os
import signal
import subprocess
from pathlib import Path

from ..models import Lease, Receipt, Task
from .base import LaunchHandle, WorkerObservation


class LocalProcessExecutor:
    """Drives real OS subprocesses as disposable workers.

    This is the first NON-fake executor: it proves the reconciler can launch,
    observe, fence, and adopt an actual process — without depending on codex or
    tmux. It is the honest stepping stone to CodexTmuxExecutor (which specializes
    launch/resume to a tmux window running `codex exec`).

    A worker is a subprocess running `task.metadata["command"]` (a shell string).
    The command is handed, via env, everything it needs to write a fenced
    receipt: FARM_RECEIPT_PATH, FARM_WORKER_ID, FARM_TASK_ID, FARM_LEASE_ID.
    A cooperative worker writes its receipt there; the executor reads it back and
    checks liveness by pid. Receipts live under <runtime>/receipts/<worker>.json.
    """

    def __init__(self, runtime_dir: Path):
        self.receipts_dir = Path(runtime_dir) / "receipts"
        self.receipts_dir.mkdir(parents=True, exist_ok=True)
        self.procs_dir = Path(runtime_dir) / "procs"
        self.procs_dir.mkdir(parents=True, exist_ok=True)

    def _receipt_path(self, worker_id: str) -> Path:
        return self.receipts_dir / f"{worker_id}.json"

    def _pid_path(self, worker_id: str) -> Path:
        return self.procs_dir / f"{worker_id}.pid"

    def _read_pid(self, worker_id: str) -> int | None:
        p = self._pid_path(worker_id)
        if not p.exists():
            return None
        try:
            return int(p.read_text().strip())
        except ValueError:
            return None

    def _pid_alive(self, worker_id: str) -> bool:
        pid = self._read_pid(worker_id)
        if pid is None:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def launch(self, task: Task, lease: Lease) -> LaunchHandle:
        wid = lease.worker_id
        # idempotent for a given lease: if this worker is already running, do not
        # start a second process (crash-consistency defense-in-depth).
        if self._pid_alive(wid):
            return LaunchHandle(worker_id=wid, session_handle=f"pid:{self._read_pid(wid)}")
        receipt_path = self._receipt_path(wid)
        # fresh generation: do not inherit a stale receipt file
        receipt_path.unlink(missing_ok=True)
        env = dict(os.environ)
        env.update(
            FARM_RECEIPT_PATH=str(receipt_path),
            FARM_WORKER_ID=wid,
            FARM_TASK_ID=task.id,
            FARM_LEASE_ID=lease.lease_id,
        )
        command = task.metadata.get("command")
        if not command:
            raise ValueError(f"task {task.id} has no metadata.command for LocalProcessExecutor")
        proc = subprocess.Popen(
            command,
            shell=True,
            env=env,
            cwd=task.metadata.get("cwd") or None,
            start_new_session=True,
        )
        self._pid_path(wid).write_text(str(proc.pid))
        return LaunchHandle(worker_id=wid, session_handle=f"pid:{proc.pid}")

    def resume(self, task: Task, worker_id: str, lease: Lease) -> None:
        # Local processes are not re-woken; a WAITING->RUNNING resume for a
        # crashed local worker is handled by adoption (relaunch) instead.
        return None

    def poll(self, worker_id: str) -> WorkerObservation:
        receipt = None
        rp = self._receipt_path(worker_id)
        if rp.exists():
            try:
                receipt = Receipt.from_dict(json.loads(rp.read_text()))
            except (ValueError, KeyError):
                receipt = None
        pid = self._read_pid(worker_id)
        alive = False
        if pid is not None:
            try:
                os.kill(pid, 0)
                alive = True
            except ProcessLookupError:
                alive = False
            except PermissionError:
                alive = True
        return WorkerObservation(worker_id=worker_id, alive=alive, receipt=receipt)

    def stop(self, worker_id: str) -> None:
        pid = self._read_pid(worker_id)
        if pid is None:
            return
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
