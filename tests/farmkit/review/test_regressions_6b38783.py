"""Regressions from the independent review of 6b38783 (2026-09-20), kept verbatim apart from
this header so the repository re-runs them. Each case failed on 6b38783 and passes after the
fixes recorded in docs/LESSONS.md. Only temporary workspaces, a fake scheduler, a delayed CLI
stub and a short-lived local child process are used.
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from agent_farm_runtime.adapters.claude import ClaudeClusterConfig, ClaudeTmuxExecutor
from agent_farm_runtime.adapters.filesystem import atomic_write_json
from agent_farm_runtime.lifecycle import stop_now
from agent_farm_runtime.models import Lease, Task
from agent_farm_runtime.provenance import runtime_identity
from agent_farm_runtime.status import events_after
from agent_farm_runtime.store import FarmPaths
from farmkit import adopt, intent, watch
from farmkit.ledger import Ledger
from farmkit.runner import Runner
from farmkit.site import Site
from farmkit.steps import Steps
from farmkit.verify import default_checks


@pytest.fixture(autouse=True)
def cheap_environment(monkeypatch):
    monkeypatch.setenv('FARMKIT_ENV_DIGEST', 'review-fixture')


class Scheduler:
    def __init__(self):
        self.calls = []
        self.values = {}

    def submit(self, argv):
        self.calls.append(argv)
        job = str(1000 + len(self.calls))
        self.values[job] = 'PENDING'
        return subprocess.CompletedProcess(argv, 0, job + '\n', '')

    def states(self, jobs):
        return {job: self.values.get(job) for job in jobs}


def make_runner(ws, *, local=False, verifier=None):
    ws.mkdir(parents=True, exist_ok=True)
    (ws / 'train.py').write_text(
        "import json, os\n"
        "json.dump({'provenance': {'attempt_id': os.environ['FARMKIT_ATTEMPT_ID']}, "
        "'metric': 1, 'version': 'original'}, open('RUN.json', 'w'))\n"
    )
    (ws / 'train.sbatch').write_text('#!/bin/bash\n' + shlex.quote(sys.executable) + ' train.py\n')
    spec = {'run': shlex.quote(sys.executable) + ' train.py' if local else 'train.sbatch',
            'outputs': ['RUN.json'], 'finite': ['metric'], 'snapshot': ['*.py', '*.sbatch']}
    if verifier:
        spec['verify'] = verifier
    steps = Steps.from_dict({'step': {'train': spec}})
    ledger = Ledger(ws / 'attempts')
    scheduler = Scheduler()
    runner = Runner(Site.minimal(), ledger, steps, workspace=ws, task_id='T-review',
                    sbatch=scheduler.submit, states=scheduler, sacct_row=lambda _: None)
    return runner, ledger, scheduler


@pytest.mark.parametrize('body', ['null', '[]'])
def test_non_object_output_is_rejected_without_crashing(tmp_path, body):
    path = tmp_path / 'RUN.json'
    path.write_text(body)
    verdict = default_checks({'attempt_id': 'current'}, {'RUN.json': path},
                             required=['RUN.json'], finite=['metric'])
    assert not verdict.ok, 'JSON null must not pass attempt and finite checks'


def test_queued_job_executes_the_snapshotted_python(tmp_path):
    runner, ledger, scheduler = make_runner(tmp_path)
    runner.tick()
    rec = ledger.latest('train')
    source = tmp_path / 'train.py'
    source.write_text(source.read_text().replace("'original'", "'edited-after-submit'"))
    argv = scheduler.calls[0]
    cwd = next(a.split('=', 1)[1] for a in argv if a.startswith('--chdir='))
    # Execute exactly the recorded sbatch script after it has waited in a fake queue.
    subprocess.run(['bash', argv[-1]], cwd=cwd, check=True, capture_output=True,
                   env={**os.environ, 'FARMKIT_ATTEMPT_ID': rec['attempt_id']})
    scheduler.values[rec['submit']['job_id']] = 'COMPLETED'
    runner.tick()
    assert ledger.ok('train')
    assert json.loads((tmp_path / 'RUN.json').read_text())['version'] == 'original'


def test_accounting_can_recover_after_an_initial_empty_query(tmp_path):
    ledger = Ledger(tmp_path / 'attempts')
    rec = ledger.new('train')

    def lost_response(argv):
        raise subprocess.TimeoutExpired(argv, 1)

    intent.submit(ledger, rec, ['sbatch', 'train.sbatch'], run=lost_response)
    intent.reconcile_all(ledger, lambda _: [])
    intent.reconcile_all(ledger, lambda _: ['1001'])
    assert ledger.latest('train')['submit']['job_id'] == '1001'


class Reader:
    def __init__(self, paths, clock, alive=True):
        self.paths, self.clock, self.alive = paths, clock, alive

    def status(self):
        return {'pid_alive': self.alive, 'interval': 30, 'last_tick': {'epoch': self.clock[0]}}

    def events_after(self, cursor):
        page = events_after(self.paths, int(cursor or 0))
        return page['events'], str(page['cursor'])


def watch_fixture(tmp_path, events=(), alive=True):
    paths = FarmPaths(tmp_path / '.farm')
    paths.ensure()
    (paths.events / 'log.ndjson').write_text(''.join(json.dumps(e) + '\n' for e in events))
    clock = [1000.0]
    reader = Reader(paths, clock, alive)

    def sleep(seconds):
        clock[0] += seconds

    def run():
        return watch.watch(reader, tmp_path / 'cursor', clock=lambda: clock[0], sleep=sleep,
                           poll_s=1, timeout_s=2)

    return run


def test_watch_reaches_notifications_after_a_full_nonmatching_page(tmp_path):
    events = [{'type': 'RESUMED', 'task_id': 'T-review', 'payload': {}} for _ in range(1000)]
    events.append({'type': 'RECEIPT_APPLIED', 'task_id': 'T-review', 'payload': {'to': 'SUBMITTED'}})
    result = watch_fixture(tmp_path, events)()
    assert result['reason'] == 'submitted', result


def test_acknowledged_daemon_failure_does_not_immediately_notify_again(tmp_path):
    run = watch_fixture(tmp_path, alive=False)
    first = run()
    watch.ack(tmp_path / 'cursor', first['cursor'])
    second = run()
    assert second['reason'] == 'timeout', second


def test_task_name_cannot_turn_science_into_code_permission():
    event = {'type': 'RECEIPT_APPLIED', 'task_id': 'code-study',
             'payload': {'to': 'WAITING', 'waiting_on': 'ruling:code-study-science-train-12345678'}}
    result = watch.classify_event(event)
    assert result['class'] == 'science' and result['master_may_resolve'] is False, result


def test_adopted_retry_budget_is_honoured(tmp_path):
    runner, ledger, scheduler = make_runner(tmp_path)
    adopt.adopt(ledger, step='train', job_id='99', inputs=[], script_version='legacy', budget_used=1)
    scheduler.values['99'] = 'NODE_FAIL'
    runner.tick()
    assert scheduler.calls == [], 'An already exhausted infra budget must not grant another submission'


def test_completed_local_attempt_is_verified_after_worker_crash(tmp_path, monkeypatch):
    runner, ledger, scheduler = make_runner(tmp_path, local=True)

    def crash(*args):
        raise RuntimeError('injected worker crash after local output and COMPLETED were persisted')

    monkeypatch.setattr(runner, '_finalize', crash)
    with pytest.raises(RuntimeError, match='injected'):
        runner.tick()
    resumed = Runner(runner.site, Ledger(ledger.root), runner.steps, workspace=tmp_path,
                     states=scheduler, sbatch=scheduler.submit, sacct_row=lambda _: None)
    result = resumed.tick()
    assert result.receipt_status == 'SUBMITTED', result.waiting_on


def test_retry_decision_survives_crash_before_new_submission(tmp_path, monkeypatch):
    runner, ledger, scheduler = make_runner(tmp_path)
    runner.tick()
    scheduler.values[ledger.latest('train')['submit']['job_id']] = 'NODE_FAIL'

    def crash(*args):
        raise RuntimeError('injected crash after retry decision but before launch')

    monkeypatch.setattr(runner, '_launch', crash)
    with pytest.raises(RuntimeError, match='injected'):
        runner.tick()
    resumed = Runner(runner.site, Ledger(ledger.root), runner.steps, workspace=tmp_path,
                     states=scheduler, sbatch=scheduler.submit, sacct_row=lambda _: None)
    resumed.tick()
    assert len(scheduler.calls) == 2, 'The pending retry must recover once, not park as no-progress'


def test_declared_verifier_cannot_be_silently_omitted(tmp_path):
    try:
        runner, ledger, scheduler = make_runner(tmp_path, verifier='required_project_check')
    except ValueError:
        return  # Rejecting the missing verifier before execution is also correct.
    runner.tick()
    rec = ledger.latest('train')
    (tmp_path / 'RUN.json').write_text(json.dumps({
        'provenance': {'attempt_id': rec['attempt_id']}, 'metric': 1}))
    scheduler.values[rec['submit']['job_id']] = 'COMPLETED'
    result = runner.tick()
    assert result.receipt_status != 'SUBMITTED', 'The configured project check was never loaded or run'


def test_claude_session_id_is_captured_after_a_long_initial_turn(tmp_path):
    fake_cli = tmp_path / 'fake_cli.py'
    session = '11111111-2222-3333-4444-555555555555'
    fake_cli.write_text('import json, time\ntime.sleep(1.5)\n'
                        + f"print(json.dumps({{'session_id': '{session}'}}), flush=True)\n")
    cfg = ClaudeClusterConfig(codex_cmd=f'{shlex.quote(sys.executable)} {shlex.quote(str(fake_cli))} -p --output-format json',
                              session_id_capture_delay=1)
    executor = ClaudeTmuxExecutor(tmp_path / 'runtime', config=cfg,
                                  run=lambda argv: subprocess.CompletedProcess(argv, 0, '', ''),
                                  is_alive=lambda _: False)
    executor.launch(Task('T-review', 'o', 'd', 'a', metadata={'workspace': str(tmp_path), 'brief': 'review stub'}),
                    Lease('W-review', 'L-review'))
    proc = subprocess.run(['bash', str(tmp_path / '.farm_launch_W-review.sh')],
                          capture_output=True, text=True, timeout=10)
    assert proc.returncode == 0, proc.stderr
    sid_file = tmp_path / '.session_id_W-review'
    assert sid_file.exists() and sid_file.read_text().strip() == session


def test_stop_does_not_signal_a_pid_from_a_different_boot(tmp_path):
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
    try:
        paths = FarmPaths(tmp_path / '.farm')
        paths.ensure()
        atomic_write_json(paths.runtime / 'deployment.json', {
            **runtime_identity(), 'pid': child.pid, 'boot_id': 'an-earlier-boot',
            'started_at': '2026-09-19T00:00:00+00:00'})
        signals = []
        stop_now(paths, actor='review', wait_seconds=0, kill=lambda pid, sig: signals.append((pid, sig)))
        assert signals == [], 'A stale daemon PID must not target an unrelated live process'
    finally:
        child.terminate()
        child.wait(timeout=5)
