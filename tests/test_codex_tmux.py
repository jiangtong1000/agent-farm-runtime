from __future__ import annotations

import json
import subprocess
from pathlib import Path

from agent_farm_runtime.adapters.codex import CodexTmuxExecutor
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.models import Lease, Receipt, ReceiptStatus, Task, TaskState
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.store import FarmPaths, TaskStore


class TmuxRecorder:
    """Fake `tmux`/subprocess runner that models just enough tmux state."""

    def __init__(self):
        self.calls: list[list[str]] = []
        self._session = False
        self._windows: list[str] = []

    def __call__(self, cmd: list[str]) -> subprocess.CompletedProcess:
        self.calls.append(cmd)
        if cmd[:2] == ["tmux", "has-session"]:
            return subprocess.CompletedProcess(cmd, 0 if self._session else 1, "", "")
        if cmd[:2] == ["tmux", "list-windows"]:
            return subprocess.CompletedProcess(cmd, 0, "\n".join(self._windows), "")
        if cmd[:2] == ["tmux", "new-session"]:
            self._session = True
            self._windows.append(cmd[cmd.index("-n") + 1])
        if cmd[:2] == ["tmux", "new-window"]:
            self._windows.append(cmd[cmd.index("-n") + 1])
        return subprocess.CompletedProcess(cmd, 0, "", "")

    def sent(self) -> list[str]:
        return [c[c.index("-t") + 2] for c in self.calls
                if c[:2] == ["tmux", "send-keys"] and c[-1] == "Enter"]

    def tmux_targets(self) -> list[str]:
        out = []
        for c in self.calls:
            if "-t" in c:
                out.append(c[c.index("-t") + 1])
            if "-s" in c:
                out.append(c[c.index("-s") + 1])
        return out


def _task(ws: Path, tid="T-1", label="A") -> Task:
    return Task(
        id=tid, objective="o", deliverable="d", acceptance="a",
        metadata={"workspace": str(ws), "brief": "You are agent A. Do the thing.",
                  "agent_label": label},
    )


def _paths(tmp: Path) -> FarmPaths:
    p = FarmPaths(tmp / ".farm")
    p.ensure()
    return p


def test_launch_writes_scripts_and_sends_keys(tmp_path):
    ws = tmp_path / "ws"; ws.mkdir()
    rt = tmp_path / ".farm" / "runtime"; rt.mkdir(parents=True)
    rec = TmuxRecorder()
    ex = CodexTmuxExecutor(rt, run=rec)
    lease = Lease("W-1", "LEASE-XYZ")

    handle = ex.launch(_task(ws), lease)

    # helper + launch script land in the workspace
    assert (ws / ".farm_receipt.py").exists()
    launch = next(ws.glob(".farm_launch_*.sh")).read_text()
    assert "cd " in launch and str(ws.resolve()) in launch
    assert "codex exec --skip-git-repo-check --sandbox danger-full-access" in launch
    assert "FARM_LEASE_ID=LEASE-XYZ" in launch          # lease is wired in
    assert "You are agent A. Do the thing." in launch    # brief carried
    assert "RUNTIME RECEIPT PROTOCOL" in launch          # receipt protocol appended
    # tmux: created the window and sent the launch command
    assert any("bash" in s and ".farm_launch_" in s for s in rec.sent())
    assert handle.session_handle == "farm2:A"
    # state persisted so poll/resume can find the worker later
    assert (rt / "codex_workers" / "W-1.json").exists()


def test_defaults_to_a_separate_session_never_touches_live_farm(tmp_path):
    rt = tmp_path / "runtime"; rt.mkdir()
    ws = tmp_path / "ws"; ws.mkdir()
    rec = TmuxRecorder()
    ex = CodexTmuxExecutor(rt, run=rec)          # default session
    ex.launch(_task(ws), Lease("W-1", "L1"))
    # safety invariant: canary must never target the live `farm` session
    assert "farm" not in rec.tmux_targets()
    assert all(t == "farm2" or t.startswith("farm2:") for t in rec.tmux_targets())


def test_poll_reads_fenced_receipt(tmp_path):
    rt = tmp_path / "runtime"; rt.mkdir()
    ws = tmp_path / "ws"; ws.mkdir()
    alive = {"v": True}
    ex = CodexTmuxExecutor(rt, run=TmuxRecorder(), is_alive=lambda w: alive["v"])
    ex.launch(_task(ws), Lease("W-1", "L1"))
    rp = rt / "receipts" / "W-1.json"
    rp.write_text(json.dumps(Receipt("W-1", "T-1", "L1", ReceiptStatus.AWAITING,
                                     ts="t", waiting_on="job:5").to_dict()))
    obs = ex.poll("W-1")
    assert obs.alive is True
    assert obs.receipt is not None and obs.receipt.status is ReceiptStatus.AWAITING
    alive["v"] = False
    assert ex.poll("W-1").alive is False


def test_reconciler_drives_codex_executor_end_to_end(tmp_path):
    paths = _paths(tmp_path)
    ws = tmp_path / "ws"; ws.mkdir()
    TaskStore(paths).create(_task(ws))
    alive = {"v": True}
    ex = CodexTmuxExecutor(paths.runtime, run=TmuxRecorder(), is_alive=lambda w: alive["v"])
    rec = Reconciler(paths, ex)

    rec.reconcile_once()                                  # READY -> RUNNING (launch)
    t = TaskStore(paths).get("T-1")
    assert t.state is TaskState.RUNNING and t.lease is not None

    # simulate the codex agent finishing and writing a fenced SUBMITTED receipt
    rp = paths.runtime / "receipts" / f"{t.lease.worker_id}.json"
    rp.write_text(json.dumps(Receipt(t.lease.worker_id, "T-1", t.lease.lease_id,
                                     ReceiptStatus.SUBMITTED, ts="t").to_dict()))
    alive["v"] = False

    rec.reconcile_once()                                  # apply receipt -> SUBMITTED
    assert TaskStore(paths).get("T-1").state is TaskState.SUBMITTED
    assert not any(c.level == "FAIL" for c in run_doctor(paths))


def test_stale_receipt_from_old_generation_is_fenced(tmp_path):
    paths = _paths(tmp_path)
    ws = tmp_path / "ws"; ws.mkdir()
    TaskStore(paths).create(_task(ws))
    ex = CodexTmuxExecutor(paths.runtime, run=TmuxRecorder(), is_alive=lambda w: True)
    rec = Reconciler(paths, ex)
    rec.reconcile_once()
    t = TaskStore(paths).get("T-1")
    rp = paths.runtime / "receipts" / f"{t.lease.worker_id}.json"
    # a receipt echoing the WRONG lease id must not advance the task
    rp.write_text(json.dumps(Receipt(t.lease.worker_id, "T-1", "STALE",
                                     ReceiptStatus.SUBMITTED, ts="t").to_dict()))
    report = rec.reconcile_once()
    assert report.ignored_stale == [t.lease.worker_id]
    assert TaskStore(paths).get("T-1").state is TaskState.RUNNING


def test_resume_and_stop_never_do_window_surgery(tmp_path):
    rt = tmp_path / "runtime"; rt.mkdir()
    ws = tmp_path / "ws"; ws.mkdir()
    rec = TmuxRecorder()
    ex = CodexTmuxExecutor(rt, run=rec)
    task = _task(ws)
    lease = Lease("W-1", "L1")
    ex.launch(task, lease)
    # write a session id so resume takes the resume path (not fresh launch)
    (ws / ".session_id_W-1").write_text("dead-beef-0000-0000-0000-000000000000")
    ex.resume(task, "W-1", lease)
    resume = next(ws.glob(".farm_resume_*.sh")).read_text()
    assert "codex exec --skip-git-repo-check --sandbox danger-full-access resume" in resume
    ex.stop("W-1")
    # the crash-lesson invariant: the executor never kills or moves windows
    for c in rec.calls:
        assert "kill-window" not in c and "kill-session" not in c and "move-window" not in c
