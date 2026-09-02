from __future__ import annotations

import json
import os
import signal
import subprocess
from pathlib import Path

from ..models import Lease, Receipt, Task
from ..procutil import pid_identity_alive, proc_starttime, read_pidfile, reap_children
from .base import LaunchHandle, WorkerObservation


class LocalProcessExecutor:
    """Drives real OS subprocesses as disposable workers.

    This is the first NON-fake executor: it proves the reconciler can launch,
    observe, fence, and adopt an actual process — without depending on codex or
    tmux. It is the honest stepping stone to CodexTmuxExecutor.

    A worker is a subprocess running `task.metadata["command"]` (a shell string),
    handed via env everything it needs to write a fenced receipt: FARM_RECEIPT_PATH,
    FARM_WORKER_ID, FARM_TASK_ID, FARM_LEASE_ID. Liveness uses a (pid, starttime)
    identity file, so a recycled pid is never mistaken for a live worker, and
    exited children are reaped so a `--loop` reconciler does not leak zombies.
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

    def _identity(self, worker_id: str) -> tuple[int | None, int | None]:
        p = self._pid_path(worker_id)
        return read_pidfile(str(p)) if p.exists() else (None, None)

    def _alive(self, worker_id: str) -> bool:
        reap_children()  # clear zombies before judging liveness
        pid, start = self._identity(worker_id)
        return pid_identity_alive(pid, start)

    def launch(self, task: Task, lease: Lease) -> LaunchHandle:
        wid = lease.worker_id
        # idempotent for a given lease: if THIS worker (pid+starttime) is already
        # running, do not start a second process.
        if self._alive(wid):
            pid, _ = self._identity(wid)
            return LaunchHandle(worker_id=wid, session_handle=f"pid:{pid}")
        receipt_path = self._receipt_path(wid)
        receipt_path.unlink(missing_ok=True)  # fresh generation: no stale receipt
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
            command, shell=True, env=env,
            cwd=task.metadata.get("cwd") or None, start_new_session=True,
        )
        # record a stable identity: pid + start-time, so pid reuse cannot alias it
        self._pid_path(wid).write_text(f"{proc.pid} {proc_starttime(proc.pid)}")
        return LaunchHandle(worker_id=wid, session_handle=f"pid:{proc.pid}")

    def resume(self, task: Task, worker_id: str, lease: Lease) -> None:
        # Local processes are not re-woken; a crashed local worker is re-actuated
        # by adoption (relaunch) instead.
        return None

    def poll(self, worker_id: str) -> WorkerObservation:
        receipt = None
        rp = self._receipt_path(worker_id)
        if rp.exists():
            try:
                receipt = Receipt.from_dict(json.loads(rp.read_text()))
            except (ValueError, KeyError):
                receipt = None
        return WorkerObservation(worker_id=worker_id, alive=self._alive(worker_id), receipt=receipt)

    def stop(self, worker_id: str) -> None:
        pid, start = self._identity(worker_id)
        if not pid_identity_alive(pid, start):  # only signal the process we launched
            reap_children()
            return
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
        reap_children()
