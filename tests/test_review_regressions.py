"""Review regressions: startup policy, early exits, and persistence ordering."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from unittest.mock import patch

import pytest

from agent_farm_runtime.adapters.codex import (
    CodexClusterConfig, RECEIPT_HELPER, render_launch_script,
)
from agent_farm_runtime.adapters.fake import FakeExecutor
from agent_farm_runtime.cli import build_parser
from agent_farm_runtime.events import EventLog
from agent_farm_runtime.locking import ReconcilerBusy, single_reconciler
from agent_farm_runtime.models import Task
from agent_farm_runtime.procutil import host_identity, observe_pidfile
from agent_farm_runtime.provenance import runtime_identity
from agent_farm_runtime.store import FarmPaths, StoreError, TaskStore, atomic_write_json


def startup_fixture(tmp_path, monkeypatch, previous):
    paths = FarmPaths(tmp_path / ".farm")
    store = TaskStore(paths)
    store.create(Task("T1", "test", "output", "check"))
    manifest = paths.runtime / "deployment.json"
    if previous is not None:
        atomic_write_json(manifest, previous)
    executor = FakeExecutor()
    monkeypatch.setattr("agent_farm_runtime.adapters.local_process.LocalProcessExecutor", lambda _: executor)
    return paths, store, executor


def startup(tmp_path, policy="pinned-host", *extra):
    args = build_parser().parse_args([
        "--project", str(tmp_path), "--writer-policy", policy, "reconcile", *extra,
    ])
    return args.func(args)


@pytest.mark.parametrize("policy,key,value", [
    ("pinned-host", "source_sha256", "a" * 64),
    ("pinned-host", "host", "another-host"),
    ("pinned-host", "protocol_version", -1),
    ("compatible", "protocol_version", -1),
])
def test_reconciler_rejects_policy_mismatch_before_manifest_or_dispatch(tmp_path, monkeypatch, policy, key, value):
    previous = {**runtime_identity(), key: value}
    paths, store, executor = startup_fixture(tmp_path, monkeypatch, previous)
    manifest = paths.runtime / "deployment.json"
    before = manifest.read_bytes(), store.get("T1").to_dict()
    with pytest.raises(StoreError, match="mismatch"):
        startup(tmp_path, policy)
    assert (manifest.read_bytes(), store.get("T1").to_dict()) == before
    assert executor.launched == []


@pytest.mark.parametrize("previous", [None, "matching"])
def test_pinned_bootstrap_and_matching_restart_check_policy_under_lock(tmp_path, monkeypatch, previous):
    from agent_farm_runtime import provenance
    paths, _, executor = startup_fixture(tmp_path, monkeypatch, runtime_identity() if previous else None)
    original = provenance.require_compatible_writer
    calls = []

    def checked(checked_paths, **kwargs):
        with pytest.raises(ReconcilerBusy):
            with single_reconciler(paths.runtime):
                pytest.fail("policy must run under the daemon lock")
        calls.append(kwargs["policy"])
        return original(checked_paths, **kwargs)

    monkeypatch.setattr(provenance, "require_compatible_writer", checked)
    assert startup(tmp_path) == 0
    assert calls == ["pinned-host"] and len(executor.launched) == 1


def test_compatible_startup_allows_source_and_host_change_without_active_leases(tmp_path, monkeypatch):
    previous = {**runtime_identity(), "host": "another-host", "source_sha256": "a" * 64}
    _, _, executor = startup_fixture(tmp_path, monkeypatch, previous)
    assert startup(tmp_path, "compatible") == 0
    assert len(executor.launched) == 1


def test_compatible_startup_still_rejects_foreign_active_leases(tmp_path, monkeypatch):
    from agent_farm_runtime.reconciler import Reconciler
    previous = {**runtime_identity(), "host": "another-host", "source_sha256": "a" * 64}
    paths, store, executor = startup_fixture(tmp_path, monkeypatch, previous)
    Reconciler(paths, executor).reconcile_once()  # fixture has one observed active lease
    before = store.get("T1").to_dict(), (paths.runtime / "deployment.json").read_bytes()
    with pytest.raises(StoreError, match="cross-host"):
        startup(tmp_path, "compatible")
    assert (store.get("T1").to_dict(), (paths.runtime / "deployment.json").read_bytes()) == before
    assert len(executor.launched) == 1


def test_explicit_source_upgrade_keeps_protocol_and_host_pins(tmp_path, monkeypatch):
    old_source = "a" * 64
    paths, _, executor = startup_fixture(tmp_path, monkeypatch, {**runtime_identity(), "source_sha256": old_source})
    assert startup(tmp_path, "pinned-host", "--upgrade-from-source", old_source) == 0
    deployed = json.loads((paths.runtime / "deployment.json").read_text())
    assert deployed["source_sha256"] == runtime_identity()["source_sha256"]
    assert deployed["upgraded_from_source"] == old_source
    assert len(executor.launched) == 1


@pytest.mark.parametrize("case", ["wrong-source", "foreign-host", "protocol", "missing", "compatible", "pending", "malformed-sha"])
def test_source_upgrade_is_not_a_general_bypass(tmp_path, monkeypatch, case):
    old_source = "a" * 64
    previous = {**runtime_identity(), "source_sha256": old_source}
    if case == "foreign-host":
        previous["host"] = "another-host"
    if case == "protocol":
        previous["protocol_version"] = -1
    paths, store, executor = startup_fixture(tmp_path, monkeypatch, None if case == "missing" else previous)
    manifest = paths.runtime / "deployment.json"
    before = manifest.read_bytes() if manifest.exists() else None
    task_before = store.get("T1").to_dict()
    if case == "pending":
        (paths.runtime / "pending-task-commit.json").write_text("preserve unfinished transaction")
    expected = "b" * 64 if case == "wrong-source" else "invalid" if case == "malformed-sha" else old_source
    with pytest.raises(StoreError):
        startup(tmp_path, "compatible" if case == "compatible" else "pinned-host", "--upgrade-from-source", expected)
    assert (manifest.read_bytes() if manifest.exists() else None) == before
    assert store.get("T1").to_dict() == task_before and executor.launched == []


@pytest.mark.parametrize("record", ["424242  attempt", "424242 None attempt", "424242 unknown attempt"])
@pytest.mark.parametrize("proc_exists", [False, True])
def test_uncaptured_starttime_checks_death_without_adopting_a_live_pid(record, proc_exists):
    identity = {"host": "host", "boot_id": "boot", "pid_namespace": "namespace"}
    stat = "424242 (worker) S " + "0 " * 18 + "12345"
    with patch("agent_farm_runtime.procutil.host_identity", return_value=identity), \
         patch("agent_farm_runtime.procutil.Path.read_text", side_effect=[record, stat if proc_exists else FileNotFoundError()]):
        assert observe_pidfile("/mock/pid", identity, "attempt") is (None if proc_exists else False)


@pytest.mark.parametrize("record", ["424242", "424242 old-attempt", "424242 None old-attempt", "-1 attempt", "not-a-pid attempt"])
def test_missing_starttime_never_bypasses_pid_or_attempt_validation(record):
    identity = {"host": "host", "boot_id": "boot", "pid_namespace": "namespace"}
    with patch("agent_farm_runtime.procutil.host_identity", return_value=identity), \
         patch("agent_farm_runtime.procutil.Path.read_text", return_value=record) as read:
        assert observe_pidfile("/mock/pid", identity, "attempt") is None
        assert read.call_count == 1


@pytest.mark.skipif(sys.platform != "linux" or not shutil.which("bash"), reason="Linux/bash backend")
def test_generated_codex_script_process_exits_before_starttime_capture(tmp_path):
    # Force the capture command to observe an already-exited child. No Codex/model.
    prelude = '''awk() {
      for ((i=0; i<200; i++)); do
        if [[ ! -e /proc/$_CPID/stat ]]; then return 1; fi
        sleep 0.01
      done
      return 2
    }'''
    pidfile = tmp_path / "worker.pid"
    script = render_launch_script(
        cfg=CodexClusterConfig(codex_cmd="bash -c 'exit 17'", path_prelude=prelude, session_id_capture_delay=0),
        workspace=str(tmp_path), worker_id="W1", task_id="T1", lease_id="L1", attempt_id="attempt",
        receipt_path=str(tmp_path / "receipt.json"), brief="stub only", log_name=str(tmp_path / "worker.log"),
        sid_path=str(tmp_path / "sid"), pid_file=str(pidfile),
    )
    proc = subprocess.run(["bash"], input=script, capture_output=True, text=True, timeout=5)
    assert proc.returncode == 17, proc.stdout + proc.stderr
    assert pidfile.read_text().split()[1:] == ["unknown", "attempt"]
    assert observe_pidfile(str(pidfile), host_identity(), "attempt") is False


@pytest.mark.skipif(sys.platform != "linux", reason="Linux process backend")
def test_local_process_exits_before_starttime_capture(tmp_path, monkeypatch):
    from agent_farm_runtime.adapters.local_process import LocalProcessExecutor
    from agent_farm_runtime.models import Lease

    def capture_after_exit(pid):
        os.waitpid(pid, 0)
        return None

    monkeypatch.setattr("agent_farm_runtime.adapters.local_process.proc_starttime", capture_after_exit)
    executor = LocalProcessExecutor(tmp_path / "runtime")
    executor.launch(Task("T1", "o", "d", "a", metadata={"command": "exit 17"}), Lease("W1", "L1"))
    assert executor.poll("W1").alive is False


def test_resume_refreshes_receipt_helper_only_after_confirmed_exit(tmp_path, monkeypatch):
    from agent_farm_runtime.adapters.base import ExecutorUnavailable
    from agent_farm_runtime.adapters.codex import CodexTmuxExecutor
    from agent_farm_runtime.models import Lease
    from test_codex_tmux import TmuxRecorder
    task = Task("T1", "o", "d", "a", metadata={"workspace": str(tmp_path), "brief": "stub"})
    lease = Lease("W1", "L1")
    executor = CodexTmuxExecutor(tmp_path / "runtime", run=TmuxRecorder())
    executor.launch(task, lease)
    helper = tmp_path / ".farm_receipt.py"
    helper.write_text("old helper")
    monkeypatch.setattr(executor, "_alive", lambda _: True)
    executor.resume(task, "W1", lease)
    assert helper.read_text() == "old helper"
    monkeypatch.setattr(executor, "_alive", lambda _: None)
    with pytest.raises(ExecutorUnavailable):
        executor.resume(task, "W1", lease)
    assert helper.read_text() == "old helper"
    monkeypatch.setattr(executor, "_alive", lambda _: False)
    executor.resume(task, "W1", lease)
    assert helper.read_text() == RECEIPT_HELPER


def test_event_append_and_recovery_sync_file_before_directory(tmp_path, monkeypatch):
    from agent_farm_runtime import events as event_module
    from agent_farm_runtime.models import Event
    log = EventLog(tmp_path / "events" / "log.ndjson")
    calls = []
    real_fsync = os.fsync

    def file_sync(fd):
        calls.append("file")
        real_fsync(fd)

    def directory_sync(path):
        assert path == log.path.parent and len(log.ids()) == 1
        assert calls[-1] == "file"
        calls.append("directory")

    monkeypatch.setattr(os, "fsync", file_sync)
    monkeypatch.setattr(event_module, "sync_directory", directory_sync)
    log.append(Event("E1", "T1", "TEST", "reviewer", {}, "t"))
    log.sync()
    assert calls == ["file", "directory", "file", "directory"]


def test_event_directory_sync_failure_keeps_journal_and_recovery_retries_sync(tmp_path, monkeypatch):
    from agent_farm_runtime import events as event_module
    store = TaskStore(FarmPaths(tmp_path / ".farm"))
    with monkeypatch.context() as scoped:
        scoped.setattr(event_module, "sync_directory", lambda _: (_ for _ in ()).throw(OSError("directory sync failed")))
        with pytest.raises(OSError, match="directory sync"):
            store.create(Task("T1", "o", "d", "a"))
    pending = store.paths.runtime / "pending-task-commit.json"
    assert pending.exists()
    log = EventLog(store.paths.events / "log.ndjson")
    assert len(log.ids()) == 1  # append already happened; recovery must not duplicate it
    with monkeypatch.context() as scoped:
        scoped.setattr(event_module, "sync_directory", lambda _: (_ for _ in ()).throw(OSError("still not synced")))
        with pytest.raises(OSError, match="still not synced"):
            store.recover()
    assert pending.exists()
    store.recover()
    assert len(log.ids()) == 1 and not pending.exists()


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("fail_directory", [False, True])
def test_receipt_helper_syncs_replacement_directory_before_ack(tmp_path, monkeypatch, existing, fail_directory):
    receipt = tmp_path / "receipt.json"
    if existing:
        receipt.write_text("old receipt")
    for key, value in {"FARM_RECEIPT_PATH": str(receipt), "FARM_WORKER_ID": "W1", "FARM_TASK_ID": "T1", "FARM_LEASE_ID": "L1"}.items():
        monkeypatch.setenv(key, value)
    calls = []
    real_fsync = os.fsync

    def sync(fd):
        import stat
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            assert json.loads(receipt.read_text())["status"] == "SUBMITTED"
            calls.append("directory")
            if fail_directory:
                raise OSError("receipt directory sync failed")
        else:
            calls.append("file")
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", sync)
    monkeypatch.setattr(sys, "argv", [".farm_receipt.py", "SUBMITTED"])
    output = io.StringIO()
    with redirect_stdout(output):
        if fail_directory:
            with pytest.raises(OSError, match="receipt directory"):
                exec(RECEIPT_HELPER, {"__name__": "__main__"})
        else:
            exec(RECEIPT_HELPER, {"__name__": "__main__"})
    assert calls == ["file", "directory"]
    assert ("receipt SUBMITTED" in output.getvalue()) is not fail_directory
