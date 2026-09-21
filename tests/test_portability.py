"""Portable read/control contracts; native backend coverage is stated separately."""
from __future__ import annotations

import builtins
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pytest

from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.doctor import run_doctor
from agent_farm_runtime.master import record_decision
from agent_farm_runtime.models import Task, TaskState
from agent_farm_runtime.provenance import require_compatible_writer, runtime_identity
from agent_farm_runtime.store import FarmPaths, StoreError, TaskStore
from agent_farm_runtime.transitions import transition_task


def write_fixture(paths, task):
    paths.tasks.mkdir(parents=True)
    (paths.tasks / f"{task.id}.json").write_text(json.dumps(task.to_dict()), encoding="utf-8")


def write_manifest(paths, data):
    paths.runtime.mkdir(parents=True, exist_ok=True)
    (paths.runtime / "deployment.json").write_text(json.dumps(data), encoding="utf-8")


def test_read_only_commands_and_models_without_posix_lock_import(tmp_path):
    # Fresh interpreter, not a cached import: importing CLI/store must not pull
    # in fcntl. This simulates a missing capability, not a native Windows run.
    script = '''
import builtins, sys
from pathlib import Path
original = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == "fcntl":
        raise ImportError("capability intentionally unavailable")
    return original(name, *args, **kwargs)
builtins.__import__ = guarded
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.models import Task
from agent_farm_runtime.store import FarmPaths, TaskStore
from agent_farm_runtime.adapters.filesystem import FilesystemCapabilityError
project = Path(sys.argv[1])
parser = build_parser()
for command in ("status", "task-list", "doctor", "version"):
    args = parser.parse_args(["--project", str(project), command])
    assert args.func(args) == 0
import os
os.environ.pop("FARM_ACCESS_REGISTRY", None)
for action in ("resolve", "verify"):
    args = parser.parse_args(["--project", str(project), "access", action, "--target", "primary", "--json"])
    assert args.func(args) == 1  # unconfigured, still no write backend or directories
assert "fcntl" not in sys.modules
assert not project.exists()
try:
    TaskStore(FarmPaths(project / ".farm")).create(Task("T1", "o", "d", "a"))
except FilesystemCapabilityError:
    pass
else:
    raise AssertionError("unsupported writer did not fail closed")
assert not project.exists()
'''
    proc = subprocess.run([sys.executable, "-B", "-c", script, str(tmp_path / "absent")],
                          text=True, capture_output=True, timeout=20)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_existing_task_inspection_needs_no_write_backend(tmp_path, monkeypatch, capsys):
    paths = FarmPaths(tmp_path / ".farm")
    write_fixture(paths, Task("T1", "inspect", "artifact", "check", state=TaskState.SUBMITTED))
    def forbidden():
        pytest.fail("observation must not request a write backend")
    monkeypatch.setattr("agent_farm_runtime.adapters.filesystem._implementation", forbidden)
    for command in (["status"], ["task-list"], ["doctor"], ["task-show", "T1"]):
        args = build_parser().parse_args(["--project", str(tmp_path), *command])
        assert args.func(args) == 0
    assert "SUBMITTED" in capsys.readouterr().out
    assert not paths.runtime.exists()


def test_protocol_check_ignores_install_location_and_interpreter(tmp_path):
    paths = FarmPaths(tmp_path / ".farm")
    original = runtime_identity()
    moved = {**original, "source_root": str(tmp_path / "another-user" / "installation"),
             "python": str(tmp_path / "another-environment" / "python")}
    write_manifest(paths, moved)
    require_compatible_writer(paths)
    require_compatible_writer(paths, policy="pinned-host")
    assert any("source matches" in c.message for c in run_doctor(paths))


def test_host_and_exact_source_pin_are_explicit_deployment_policy(tmp_path):
    paths = FarmPaths(tmp_path / ".farm")
    data = runtime_identity()
    write_manifest(paths, {**data, "host": "another-host.example", "source_sha256": "another-build"})
    require_compatible_writer(paths)  # compatibility is a protocol question
    with pytest.raises(StoreError, match="pinned-host"):
        require_compatible_writer(paths, policy="pinned-host")
    write_manifest(paths, {**data, "protocol_version": -1})
    for policy in ("compatible", "pinned-host"):
        with pytest.raises(StoreError, match="mismatch"):
            require_compatible_writer(paths, policy=policy)


def test_offline_project_needs_no_daemon_manifest(tmp_path):
    paths = FarmPaths(tmp_path / ".farm")
    write_fixture(paths, Task("T1", "o", "d", "a", state=TaskState.SUBMITTED))
    require_compatible_writer(paths)
    with pytest.raises(StoreError, match="unverified"):
        require_compatible_writer(paths, policy="pinned-host")


def test_source_digest_is_location_independent(tmp_path, monkeypatch):
    import agent_farm_runtime.provenance as provenance
    digests = []
    for name in ("installation-a", "installation-b"):
        root = tmp_path / name
        root.mkdir()
        (root / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
        monkeypatch.setattr(provenance, "__file__", str(root / "provenance.py"))
        digests.append(provenance.runtime_identity()["source_sha256"])
    assert digests[0] == digests[1]


@pytest.mark.skipif(os.name != "posix", reason="native durable write backend is POSIX")
def test_independent_projects_and_arbitrary_decision_authors(tmp_path):
    # Logical project/actor isolation, not an OS permissions or shared-user test.
    for project, actor, label in (("project-a", "reviewer-a", "approved-for-release"),
                                  ("project-b", "reviewer-b", "ready-for-integration")):
        root = tmp_path / project
        paths = FarmPaths(root / ".farm")
        store = TaskStore(paths)
        store.create(Task("T1", "o", "d", "a", state=TaskState.SUBMITTED))
        note = root / "review.md"
        note.write_text("Acceptance criteria checked for this project.", encoding="utf-8")
        saved = record_decision(store, "T1", expected_revision=1, action="accept", actor=actor,
                                evidence_file=str(note), outcome=label)
        assert saved.metadata["outcome"] == label
        assert saved.metadata["acceptance_receipt"]["actor"] == actor
        assert len(store.list()) == 1
    first = TaskStore(FarmPaths(tmp_path / "project-a" / ".farm")).get("T1")
    assert first.metadata["outcome"] == "approved-for-release"


def test_neutral_cli_and_transition_contract():
    args = build_parser().parse_args(["task-accept", "T1", "--expected-revision", "1",
                                      "--actor", "reviewer", "--evidence-file", "review.md"])
    assert args.outcome == "accepted" and args.writer_policy == "compatible"
    task = Task("T1", "o", "d", "a", state=TaskState.SUBMITTED)
    done = transition_task(task, TaskState.DONE, acceptance_recorded=True,
                           metadata_patch={"acceptance_receipt": "project-defined evidence"})
    assert done.state is TaskState.DONE


def test_entry_docs_are_self_contained_and_no_personal_mount_paths():
    root = Path(__file__).resolve().parents[1]
    docs = [root / "README.md", *sorted((root / "docs").glob("*.md")),
            root / "templates" / "BRIEF.md", root / "templates" / "WORKSPACE_AGENTS.md"]
    for path in docs:
        content = path.read_text(encoding="utf-8")
        # Generic regression against accidentally linking local deployment docs.
        for target in re.findall(r"\]\(([^)]+)\)", content):
            if "://" in target or target.startswith("#"):
                continue
            resolved = (path.parent / target.split("#", 1)[0]).resolve()
            assert resolved.is_relative_to(root), (path, target)
            assert resolved.exists(), (path, target)
        assert not re.search(r"/(?:home\d*|Users)/[^/\s]+/", content), path
