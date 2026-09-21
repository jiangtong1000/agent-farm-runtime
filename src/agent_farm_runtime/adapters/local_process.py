from __future__ import annotations

import json
import hashlib
import os
import signal
import subprocess
import uuid
from pathlib import Path

from ..models import Lease, Receipt, Task
from ..procutil import host_identity, observe_pidfile, proc_starttime, read_pidfile, reap_children
from .base import ExecutorUnavailable, LaunchHandle, WorkerObservation
from .filesystem import atomic_write_json


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

    def _state_path(self, worker_id: str) -> Path:
        return self.procs_dir / f"{worker_id}.json"

    def _state(self, worker_id: str) -> dict:
        try:
            value = json.loads(self._state_path(worker_id).read_text())
            if not isinstance(value, dict):
                raise ValueError("not an object")
            return value
        except FileNotFoundError:
            return {}
        except (ValueError, OSError) as exc:
            raise ExecutorUnavailable(f"{worker_id}: unreadable local executor identity") from exc

    def _alive(self, worker_id: str) -> bool | None:
        state = self._state(worker_id)
        alive = observe_pidfile(str(self._pid_path(worker_id)), state, state.get("attempt_id"))
        # Reap after observing: a worker can exit between an earlier reap and
        # the liveness check, leaving a zombie even though we report it dead.
        reap_children()
        return alive

    def validate_task(self, task: Task) -> None:
        command = task.metadata.get("command")
        if not isinstance(command, str) or not command.strip():
            raise ValueError("local-process requires a nonempty command")
        cwd = task.metadata.get("cwd")
        if cwd and (not isinstance(cwd, str) or not Path(cwd).is_dir()):
            raise ValueError("local-process cwd must be an existing directory")

    def launch(self, task: Task, lease: Lease) -> LaunchHandle:
        wid = lease.worker_id
        # idempotent for a given lease: if THIS worker (pid+starttime) is already
        # running, do not start a second process.
        if self._state_path(wid).exists() or self._pid_path(wid).exists():
            alive = self._alive(wid)
            if alive is True:
                pid, _ = self._identity(wid)
                return LaunchHandle(worker_id=wid, session_handle=f"pid:{pid}")
            if alive is None:
                raise ExecutorUnavailable(f"{wid}: prior local dispatch unverified; launch withheld")
        receipt_path = self._receipt_path(wid)
        try:
            prior = receipt_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            prior = None
        attempt = uuid.uuid4().hex
        atomic_write_json(self._state_path(wid), {
            **host_identity(), "attempt_id": attempt, "previous_receipt": prior,
        })
        receipt_path.unlink(missing_ok=True)  # fresh generation: no stale receipt
        env = dict(os.environ)
        env.update(
            FARM_RECEIPT_PATH=str(receipt_path),
            FARM_WORKER_ID=wid,
            FARM_TASK_ID=task.id,
            FARM_LEASE_ID=lease.lease_id,
            FARM_TASK_PATH=str(self.procs_dir.parent.parent / "tasks" / f"{task.id}.json"),
        )
        command = task.metadata.get("command")
        if not command:
            raise ValueError(f"task {task.id} has no metadata.command for LocalProcessExecutor")
        proc = subprocess.Popen(
            command, shell=True, env=env,
            cwd=task.metadata.get("cwd") or None, start_new_session=True,
        )
        # record a stable identity: pid + start-time, so pid reuse cannot alias it
        self._pid_path(wid).write_text(f"{proc.pid} {proc_starttime(proc.pid)} {attempt}")
        return LaunchHandle(worker_id=wid, session_handle=f"pid:{proc.pid}")

    def resume(self, task: Task, worker_id: str, lease: Lease) -> None:
        # A fresh invocation under the retained lease; launch refuses uncertainty.
        self.launch(task, lease)

    def poll(self, worker_id: str) -> WorkerObservation:
        receipt = None
        rp = self._receipt_path(worker_id)
        if rp.exists():
            try:
                raw = rp.read_bytes()
                from .codex import _same_as_previous
                if not _same_as_previous(raw, self._state(worker_id)):
                    receipt = Receipt.from_dict(json.loads(raw))
            except (ValueError, KeyError):
                receipt = None
        alive = self._alive(worker_id)
        return WorkerObservation(worker_id=worker_id, alive=alive, receipt=receipt,
                                 detail="local dispatch or host identity unverified" if alive is None else None)

    def stop(self, worker_id: str) -> None:
        pid, start = self._identity(worker_id)
        alive = self._alive(worker_id)
        if alive is None:
            raise ExecutorUnavailable(f"{worker_id}: cannot verify signal target; stop withheld")
        if alive is False:
            reap_children()
            return
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
        reap_children()
