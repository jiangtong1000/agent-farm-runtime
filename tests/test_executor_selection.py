"""Per-task executor routing, fresh resume mode, and the Claude Code backend (D6, D16, D18)."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from agent_farm_runtime.adapters.claude import ClaudeClusterConfig, ClaudeTmuxExecutor
from agent_farm_runtime.adapters.codex import CodexClusterConfig, CodexTmuxExecutor
from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.adapters.multi import MultiExecutor
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.models import Lease, Task, TaskState
from agent_farm_runtime.reconciler import Reconciler
from agent_farm_runtime.store import FarmPaths, TaskStore


def _farm(tmp_path):
    paths = FarmPaths(tmp_path / ".farm"); paths.ensure()
    return paths, TaskStore(paths)


def test_tasks_route_to_their_named_executor_and_workers_route_back(tmp_path):
    paths, store = _farm(tmp_path)
    a, b = FakeExecutor(), FakeExecutor()
    multi = MultiExecutor("a", {"a": lambda: a, "b": lambda: b}, store)
    rec = Reconciler(paths, multi, grace_seconds=0)
    store.create(Task("T-a", "o", "d", "a", metadata={"command": "true"}))
    store.create(Task("T-b", "o", "d", "a", metadata={"command": "true", "executor": "b"}))
    rec.reconcile_once()
    assert [t for _, t in a.launched] == ["T-a"] and [t for _, t in b.launched] == ["T-b"]
    wb = store.get("T-b").lease.worker_id
    # a fresh MultiExecutor (e.g. after a daemon restart) resolves the worker through the store
    multi2 = MultiExecutor("a", {"a": lambda: a, "b": lambda: b}, store)
    b.kill(wb)
    assert multi2.poll(wb).alive is False and multi2.poll(store.get("T-a").lease.worker_id).alive is True


def test_unknown_executor_name_fails_preflight_without_leasing(tmp_path):
    paths, store = _farm(tmp_path)
    multi = MultiExecutor("a", {"a": FakeExecutor}, store)
    rec = Reconciler(paths, multi, grace_seconds=0)
    store.create(Task("T-x", "o", "d", "a", metadata={"executor": "nope"}))
    rep = rec.reconcile_once()
    assert rep.preflight_failures and store.get("T-x").state is TaskState.READY and store.get("T-x").lease is None


class Recorder:
    def __init__(self):
        self.cmds = []

    def __call__(self, cmd):
        self.cmds.append(cmd)
        if "list-windows" in cmd:
            return subprocess.CompletedProcess(cmd, 0, "", "")
        return subprocess.CompletedProcess(cmd, 0, "", "")


def _agent_task(ws, **meta):
    return Task("T-1", "o", "d", "acc", metadata={"workspace": str(ws), "brief": "Brief text.", **meta})


def test_fresh_resume_mode_starts_a_new_session_under_the_same_lease(tmp_path):
    ws = tmp_path / "ws"; ws.mkdir(); rt = tmp_path / "rt"; rt.mkdir()
    ex = CodexTmuxExecutor(rt, run=Recorder(), is_alive=lambda st: False, config=CodexClusterConfig())
    lease = Lease("W-1", "L-1")
    ex.launch(_agent_task(ws), lease)
    ex.resume(_agent_task(ws, resume_mode="fresh"), "W-1", lease)
    script = (ws / ".farm_resume_W-1.sh").read_text()
    assert 'resume "$SID"' not in script                    # not a resume of the old conversation
    assert "codex exec" in script and "FRESH SESSION" in script and "Brief text." in script
    assert "session id:" in script                          # a new session id will be captured
    assert "FARM_LEASE_ID=L-1" in script and "FARM_WORKER_ID=W-1" in script
    # default mode still resumes the saved session
    ex.resume(_agent_task(ws), "W-1", lease)
    assert 'resume "$SID"' in (ws / ".farm_resume_W-1.sh").read_text()


def test_claude_executor_renders_claude_commands_and_session_capture(tmp_path):
    ws = tmp_path / "ws"; ws.mkdir(); rt = tmp_path / "rt"; rt.mkdir()
    ex = ClaudeTmuxExecutor(rt, run=Recorder(), is_alive=lambda st: False, config=ClaudeClusterConfig())
    lease = Lease("W-c", "L-c")
    ex.launch(_agent_task(ws), lease)
    launch = (ws / ".farm_launch_W-c.sh").read_text()
    assert "claude -p 'Brief text." in launch
    assert "--dangerously-skip-permissions --output-format json" in launch
    assert "session_id" in launch and "session id:" not in launch   # claude's JSON field, not codex's banner
    ex.resume(_agent_task(ws), "W-c", lease)
    resume = (ws / ".farm_resume_W-c.sh").read_text()
    assert 'claude --resume "$SID" -p ' in resume and "--output-format json" in resume
    assert ex.agent_name == "claude"


def test_task_create_records_executor_and_resume_mode(tmp_path, capsys):
    ws = tmp_path / "ws"; ws.mkdir()
    args = build_parser().parse_args(["--project", str(tmp_path), "init"]); args.func(args)
    args = build_parser().parse_args(["--project", str(tmp_path), "task-create", "--id", "T-9",
                                      "--objective", "o", "--deliverable", "d", "--acceptance", "a",
                                      "--workspace", str(ws), "--brief", "b",
                                      "--executor", "claude-tmux", "--resume-mode", "fresh"])
    assert args.func(args) == 0
    task = TaskStore(FarmPaths(tmp_path / ".farm")).get("T-9")
    assert task.metadata["executor"] == "claude-tmux" and task.metadata["resume_mode"] == "fresh"
