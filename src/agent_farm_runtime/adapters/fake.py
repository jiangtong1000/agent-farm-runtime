from __future__ import annotations

from ..models import Lease, Receipt, Task
from .base import LaunchHandle, WorkerObservation


class FakeExecutor:
    """In-process, scriptable executor for tests.

    Records launches and returns programmed observations. Tests drive it with
    set_receipt() and kill() to simulate worker behavior and crashes without any
    real process, tmux, or codex.
    """

    def __init__(self) -> None:
        self.launched: list[tuple[str, str]] = []   # (worker_id, task_id)
        self.resumed: list[str] = []
        self.stopped: list[str] = []
        self._alive: dict[str, bool] = {}
        self._receipt: dict[str, Receipt] = {}
        self._task: dict[str, str] = {}

    # WorkerExecutor protocol -------------------------------------------------

    def launch(self, task: Task, lease: Lease) -> LaunchHandle:
        wid = lease.worker_id
        self.launched.append((wid, task.id))
        self._alive[wid] = True
        self._task[wid] = task.id
        return LaunchHandle(worker_id=wid, session_handle=f"fake:{wid}")

    def resume(self, task: Task, worker_id: str, lease: Lease) -> None:
        self.resumed.append(worker_id)
        self._alive[worker_id] = True

    def poll(self, worker_id: str) -> WorkerObservation:
        return WorkerObservation(
            worker_id=worker_id,
            alive=self._alive.get(worker_id, False),
            receipt=self._receipt.get(worker_id),
        )

    def stop(self, worker_id: str) -> None:
        self.stopped.append(worker_id)
        self._alive[worker_id] = False

    # test controls -----------------------------------------------------------

    def set_receipt(self, receipt: Receipt) -> None:
        self._receipt[receipt.worker_id] = receipt

    def clear_receipt(self, worker_id: str) -> None:
        self._receipt.pop(worker_id, None)

    def kill(self, worker_id: str) -> None:
        self._alive[worker_id] = False
