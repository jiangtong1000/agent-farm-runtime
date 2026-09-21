"""Regressions from the follow-up review of 605bc3d (2026-09-20), verbatim apart from this
header. Each failed on 605bc3d and passes after the fixes recorded in docs/LESSONS.md.
"""
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from agent_farm_runtime.adapters.claude import ClaudeClusterConfig, ClaudeTmuxExecutor
from agent_farm_runtime.adapters.filesystem import atomic_write_json
from agent_farm_runtime.lifecycle import daemon_alive, stop_now
from agent_farm_runtime.models import Lease, Task
from agent_farm_runtime.procutil import proc_starttime
from agent_farm_runtime.provenance import runtime_identity
from agent_farm_runtime.status import events_after
from agent_farm_runtime.store import FarmPaths
from farmboard.model import _attempts
from farmkit import watch
from farmkit.ledger import Ledger
from farmkit.runner import Runner
from farmkit.site import Site
from farmkit.steps import Steps
from farmkit.verify import default_checks


@pytest.fixture(autouse=True)
def cheap_environment(monkeypatch):
    monkeypatch.setenv('FARMKIT_ENV_DIGEST', 'followup-review-fixture')


def runner_for(ws, *, run='train.py', snapshot=('*.py',), inputs=(), outputs=('RUN.json',)):
    steps = Steps.from_dict({'step': {'train': {
        'run': shlex.quote(sys.executable) + ' ' + run,
        'snapshot': list(snapshot), 'inputs': list(inputs),
        'outputs': list(outputs), 'finite': ['metric'],
    }}})
    ledger = Ledger(ws / 'attempts')
    runner = Runner(Site.minimal(), ledger, steps, workspace=ws,
                    task_id='T-followup', sacct_row=lambda _: None)
    return runner, ledger


ARTIFACT = "{'provenance': {'attempt_id': os.environ['FARMKIT_ATTEMPT_ID']}, 'metric': 1}"


def test_snapshot_preserves_declared_input_beside_copied_code(tmp_path):
    project = tmp_path / 'project'
    project.mkdir()
    (project / 'input.txt').write_text('input value')
    (project / 'train.py').write_text(
        "import json, os\nfrom pathlib import Path\n"
        "assert Path('project/input.txt').read_text() == 'input value'\n"
        f"json.dump({ARTIFACT}, open('RUN.json', 'w'))\n")
    runner, ledger = runner_for(tmp_path, run='project/train.py',
                                snapshot=['project/*.py'], inputs=['project/input.txt'])
    result = runner.tick()
    assert result.receipt_status == 'SUBMITTED', (result.waiting_on, ledger.latest('train'))


def test_failed_attempt_does_not_overwrite_existing_published_top_level_output(tmp_path):
    previous = json.dumps({'provenance': {'attempt_id': 'previous'}, 'metric': 1})
    (tmp_path / 'RUN.json').write_text(previous)
    assert default_checks({'attempt_id': 'previous'}, {'RUN.json': tmp_path / 'RUN.json'},
                          required=['RUN.json'], finite=['metric']).ok
    invalid_artifact = ARTIFACT.replace("'metric': 1", "'metric': float('nan')")
    (tmp_path / 'train.py').write_text(
        'import json, os\n' +
        f"json.dump({invalid_artifact}, open('RUN.json', 'w'))\n")
    runner, ledger = runner_for(tmp_path)
    result = runner.tick()
    assert result.receipt_status == 'AWAITING'
    assert not ledger.ok('train')
    assert (tmp_path / 'RUN.json').read_text() == previous, 'verification failed but the published output was overwritten'


def test_directory_artifact_can_be_published(tmp_path):
    (tmp_path / 'train.py').write_text(
        "import json, os\nfrom pathlib import Path\n"
        "Path('model').mkdir()\nPath('model/weights.dat').write_text('weights')\n"
        f"json.dump({ARTIFACT}, open('RUN.json', 'w'))\n")
    runner, ledger = runner_for(tmp_path, outputs=['RUN.json', 'model'])
    result = runner.tick()
    assert result.receipt_status == 'SUBMITTED'
    assert (tmp_path / 'model/weights.dat').read_text() == 'weights'


def test_claude_sid_capture_waits_for_log_writer(tmp_path):
    session = '11111111-2222-3333-4444-555555555555'
    fake_cli = tmp_path / 'fake_cli.py'
    fake_cli.write_text('import json, time\ntime.sleep(1.5)\n' +
                        f"print(json.dumps({{'session_id': '{session}'}}), flush=True)\n")
    # Reproduce a delayed process-substitution log writer (slow scheduling/filesystem).
    # This is not an agent delay: the CLI finishes before tee has drained the pipe.
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    tee = bindir / 'tee'
    tee.write_text('#!/bin/bash\nsleep 2.5\nexec ' + shlex.quote(shutil.which('tee')) + ' "$@"\n')
    tee.chmod(0o755)
    cfg = ClaudeClusterConfig(
        codex_cmd=f'{shlex.quote(sys.executable)} {shlex.quote(str(fake_cli))} -p --output-format json',
        session_id_capture_delay=1,
        path_prelude='export PATH=' + shlex.quote(str(bindir)) + ':"$PATH"')
    executor = ClaudeTmuxExecutor(tmp_path / 'runtime', config=cfg,
                                 run=lambda argv: subprocess.CompletedProcess(argv, 0, '', ''),
                                 is_alive=lambda _: False)
    executor.launch(Task('T-followup', 'o', 'd', 'a', metadata={'workspace': str(tmp_path), 'brief': 'stub'}),
                    Lease('W-followup', 'L-followup'))
    proc = subprocess.run(['bash', str(tmp_path / '.farm_launch_W-followup.sh')],
                          capture_output=True, text=True, timeout=10)
    assert proc.returncode == 0, proc.stderr
    assert session in proc.stdout, 'the fake CLI did produce a valid session id'
    sid = tmp_path / '.session_id_W-followup'
    assert sid.exists() and sid.read_text().strip() == session, 'CLI wait completed before tee wrote the final JSON'


def test_stop_refuses_legacy_manifest_with_unverifiable_pid_identity(tmp_path):
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
    try:
        paths = FarmPaths(tmp_path / '.farm')
        paths.ensure()
        # Same-boot legacy manifest, as written by 6b38783, without pid_starttime.
        # A reused pid cannot establish that this unrelated child is the daemon.
        atomic_write_json(paths.runtime / 'deployment.json', {
            **runtime_identity(), 'pid': child.pid,
            'started_at': (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()})
        signals = []
        stop_now(paths, actor='review', wait_seconds=0, kill=lambda pid, sig: signals.append((pid, sig)))
        assert signals == [], 'missing identity must not authorize a signal to an unrelated process'
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_daemon_liveness_rejects_zombie_with_matching_starttime():
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(0.2)'])
    try:
        start = proc_starttime(child.pid)
        assert start is not None
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = Path(f'/proc/{child.pid}/stat').read_text().rsplit(')', 1)[1].split()[0]
            if state == 'Z':
                break
            time.sleep(0.01)
        assert state == 'Z'
        manifest = {**runtime_identity(), 'pid': child.pid, 'pid_starttime': start,
                    'started_at': '2026-09-20T00:00:00+00:00'}
        assert daemon_alive(manifest) is False, 'an exited unreaped daemon is reported alive'
    finally:
        child.wait(timeout=5)


def test_event_ack_preserves_previously_acknowledged_daemon_fault(tmp_path):
    paths = FarmPaths(tmp_path / '.farm')
    paths.ensure()
    log = paths.events / 'log.ndjson'
    log.write_text('')
    clock = [1000.0]

    class Reader:
        def status(self):
            return {'pid': 101, 'pid_alive': False, 'last_tick': {'epoch': clock[0]}}

        def events_after(self, cursor):
            page = events_after(paths, int(cursor or 0))
            return page['events'], str(page['cursor'])

    def sleep(seconds):
        clock[0] += seconds

    def run():
        return watch.watch(Reader(), tmp_path / 'cursor', clock=lambda: clock[0],
                           sleep=sleep, timeout_s=2, poll_s=1)

    first = run()
    assert first['reason'] == 'daemon-down'
    watch.ack(tmp_path / 'cursor', first['cursor'])
    log.write_text(json.dumps({'type': 'RECEIPT_APPLIED', 'task_id': 'T-followup',
                               'payload': {'to': 'SUBMITTED'}}) + '\n')
    second = run()
    assert second['reason'] == 'submitted'
    watch.ack(tmp_path / 'cursor', second['cursor'])
    third = run()
    assert third['reason'] == 'timeout', third


def test_board_replaces_stale_nonterminal_ledger_state_with_fresh_observation(tmp_path):
    ledger = Ledger(tmp_path / 'attempts')
    rec = ledger.new('train')
    rec['submit'] = {'status': 'submitted', 'job_id': '1001'}
    rec['observed'] = {'state': 'RUNNING', 'observed_ts': '2026-09-19T00:00:00+00:00'}
    ledger.save(rec)
    attempts, in_flight = _attempts(ledger, {'1001': 'COMPLETED'})
    assert attempts[0].state == 'COMPLETED', attempts
    assert in_flight == []
