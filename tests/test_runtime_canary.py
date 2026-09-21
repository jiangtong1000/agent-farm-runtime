"""Executable canaries: no model calls, accounts, production tasks or compute jobs."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time
import uuid

import pytest

from agent_farm_runtime.adapters.codex import CodexClusterConfig, CodexTmuxExecutor
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.master import record_decision
from agent_farm_runtime.models import ReceiptStatus, Task, TaskState
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.store import FarmPaths, TaskStore
from test_codex_tmux import TmuxRecorder


STUB = '''\
import os, pathlib, subprocess, sys
ws = pathlib.Path.cwd()
with (ws / "invocations.ndjson").open("a") as f:
    f.write(str(sys.argv[1:2]) + "\\n")
print("session id: 01a00000-0000-7000-8000-000000000000", flush=True)
if sys.argv[1:2] == ["resume"]:
    (ws / "result.txt").write_text("verified canary output")
    args = ["SUBMITTED", "--note", "result.txt ready"]
else:
    args = ["AWAITING", "--waiting-on", "artifact:" + str(ws / "ready"), "--note", "waiting"]
subprocess.run([sys.executable, ".farm_receipt.py", *args], check=True)
'''


def wait_for(predicate):
    end = time.monotonic() + 10
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.05)
    pytest.fail("canary boundary not reached within 10 seconds")


def exercise(tmp_path, runner, socket=None):
    ws = tmp_path / "workspace"
    ws.mkdir()
    stub = tmp_path / "stub.py"
    stub.write_text(STUB)
    paths = FarmPaths(tmp_path / ".farm")
    store = TaskStore(paths)
    store.create(Task("T1", "exercise lifecycle", "result.txt", "exact canary output", metadata={
        "workspace": str(ws), "brief": "local test worker; no external calls",
    }))
    cfg = CodexClusterConfig(codex_cmd=shlex.join([sys.executable, "-B", str(stub)]),
                             session_id_capture_delay=3, command_timeout_seconds=5)

    def reconciler():
        ex = CodexTmuxExecutor(paths.runtime, session="canary", tmux_socket=socket, config=cfg, run=runner)
        return ex, Reconciler(paths, ex, unblock=lambda _: (ws / "ready").exists())

    ex, rec = reconciler()
    assert rec.reconcile_once().launched == ["T1"]
    first = store.get("T1").lease
    initial_script = ws / f".farm_launch_{first.worker_id}.sh"

    def finish_round(ex, rec):
        lease = store.get("T1").lease
        wait_for(lambda: ex.poll(lease.worker_id).receipt is not None)
        rec.reconcile_once()
        assert store.get("T1").state is TaskState.WAITING
        wait_for(lambda: ex.poll(lease.worker_id).alive is False)
        wait_for(lambda: (ws / f".session_id_{lease.worker_id}").exists())
        (ws / "ready").write_text("dependency checked")
        # Reconstruct the executor and control loop from durable records.
        ex, rec = reconciler()
        assert rec.reconcile_once().resumed == ["T1"]
        wait_for(lambda: (obs := ex.poll(lease.worker_id)).receipt is not None
                 and obs.receipt.status is ReceiptStatus.SUBMITTED)
        wait_for(lambda: ex.poll(lease.worker_id).alive is False)
        rec.reconcile_once()
        assert store.get("T1").state is TaskState.SUBMITTED
        assert (ws / "result.txt").read_text() == "verified canary output"
        return ex, rec

    ex, rec = finish_round(ex, rec)
    note = tmp_path / "review.md"
    note.write_text("Checked exact output; request one bounded rework for canary coverage.")
    record_decision(store, "T1", expected_revision=store.get("T1").metadata["revision"],
                    action="rework", actor="canary-reviewer", evidence_file=str(note))
    assert rec.reconcile_once().launched == ["T1"]
    assert store.get("T1").lease != first
    finish_round(ex, rec)
    note.write_text("Independently checked exact result.txt contents after rework.")
    record_decision(store, "T1", expected_revision=store.get("T1").metadata["revision"],
                    action="accept", actor="canary-reviewer", evidence_file=str(note))
    assert store.get("T1").state is TaskState.DONE
    assert not any(check.level == "FAIL" for check in run_doctor(paths))
    # Replaying a queued command must not run an already-issued invocation again.
    repeated = subprocess.run(["bash", str(initial_script)], capture_output=True, text=True, timeout=5)
    assert repeated.returncode == 4
    assert len((ws / "invocations.ndjson").read_text().splitlines()) == 4
    assert not (paths.runtime / "pending-task-commit.json").exists()


@pytest.mark.skipif(sys.platform != "linux" or not shutil.which("bash"), reason="Linux/bash execution backend")
def test_bash_stub_launch_wait_resume_rework_accept_canary(tmp_path):
    class ExecutingRecorder(TmuxRecorder):
        def __call__(self, cmd):
            if cmd[:2] == ["tmux", "send-keys"] and cmd[-1] == "Enter":
                self.calls.append(cmd)
                return subprocess.run(shlex.split(cmd[-2]), capture_output=True, text=True, timeout=10)
            return super().__call__(cmd)
    exercise(tmp_path, ExecutingRecorder())


@pytest.mark.skipif(os.environ.get("FARM_RUN_TMUX_CANARY") != "1" or not shutil.which("tmux"),
                    reason="opt-in private tmux server, never a live farm")
def test_private_tmux_stub_canary(tmp_path):
    name = "runtime-canary-" + uuid.uuid4().hex
    control = str(tmp_path / "canary.sock")

    def runner(cmd):
        assert cmd[:3] == ["tmux", "-L", name]
        # Only this test's socket; bypass personal tmux config and shell profiles.
        scoped = ["tmux", "-S", control, "-f", "/dev/null", *cmd[3:]]
        if cmd[3] in {"new-session", "new-window"}:
            scoped.append("bash --noprofile --norc")
        return subprocess.run(scoped, capture_output=True, text=True, timeout=5)

    try:
        exercise(tmp_path, runner, name)
    finally:
        # Target is a unique socket inside this test's temporary directory.
        subprocess.run(["tmux", "-S", control, "kill-server"], capture_output=True, timeout=5)
        probe = subprocess.run(["tmux", "-S", control, "has-session", "-t", "canary"],
                               capture_output=True, timeout=5)
        assert probe.returncode != 0, "private canary server was not stopped"
