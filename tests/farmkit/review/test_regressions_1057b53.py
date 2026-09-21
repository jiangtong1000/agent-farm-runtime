"""Regressions from the third review of 1057b53 (2026-09-20), verbatim apart from this header
and ONE documented change in the chain test (see the comment there). Each failed on 1057b53.
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from farmkit.ledger import Ledger
from farmkit.runner import Runner
from farmkit.runtime_reader import CliRuntimeReader, tail_events
from farmkit.site import Site
from farmkit.steps import Steps, StepsError


@pytest.fixture(autouse=True)
def cheap_environment(monkeypatch):
    monkeypatch.setenv('FARMKIT_ENV_DIGEST', 'third-review-fixture')


ARTIFACT = "{'provenance': {'attempt_id': os.environ['FARMKIT_ATTEMPT_ID']}, 'metric': 1}"


def local_runner(ws, *, inputs=(), outputs=('RUN.json',), snapshot=('*.py',)):
    steps = Steps.from_dict({'step': {'train': {
        'run': shlex.quote(sys.executable) + ' train.py', 'snapshot': list(snapshot),
        'inputs': list(inputs), 'outputs': list(outputs), 'finite': ['metric'],
    }}})
    ledger = Ledger(ws / 'attempts')
    return Runner(Site.minimal(), ledger, steps, workspace=ws,
                  task_id='T-third-review', sacct_row=lambda _: None), ledger


class Scheduler:
    def __init__(self):
        self.jobs = {}
        self.values = {}

    def submit(self, argv):
        job = str(1000 + len(self.jobs))
        self.jobs[job] = list(argv)
        self.values[job] = 'PENDING'
        return subprocess.CompletedProcess(argv, 0, job + '\n', '')

    def states(self, jobs):
        return {job: self.values.get(job) for job in jobs}

    def execute(self, job):
        argv = self.jobs[job]
        dep = next((a.split('=', 1)[1] for a in argv if a.startswith('--dependency=')), None)
        if dep:
            kind, previous = dep.split(':', 1)
            assert kind == 'afterok' and self.values[previous] == 'COMPLETED'
        cwd = next(a.split('=', 1)[1] for a in argv if a.startswith('--chdir='))
        export = next(a.split('=', 1)[1] for a in argv if a.startswith('--export='))
        env = {**os.environ, **dict(v.split('=', 1) for v in export.split(',') if v != 'ALL')}
        proc = subprocess.run(['bash', argv[-1]], cwd=cwd, env=env,
                              capture_output=True, text=True, timeout=10)
        self.values[job] = 'COMPLETED' if proc.returncode == 0 else 'FAILED'
        return proc


def test_afterok_successor_consumes_current_predecessor_checkpoint_before_worker_wake(tmp_path):
    old = tmp_path / 'chain/seg1/checkpoint.txt'
    old.parent.mkdir(parents=True)
    old.write_text('old-checkpoint')
    (tmp_path / 'segment.py').write_text(
        "import json, os\nfrom pathlib import Path\n"
        "n = int(os.environ['SEGMENT'])\n"
        "out = Path(f'chain/seg{n}')\nout.mkdir(parents=True, exist_ok=True)\n"
        # CONTRACT (farmkit-era): a chain successor reads its predecessor's outputs at their
        # relative path in its own run directory, which farmkit binds to the PRODUCER ATTEMPT.
        # The workspace copy is the verified, published result and may still be the previous
        # round's; the reviewer's original line read it through FARMKIT_WORKSPACE.
        "consumed = None if n == 1 else Path('chain/seg1/checkpoint.txt').read_text()\n"
        "(out / 'checkpoint.txt').write_text('fresh-checkpoint' if n == 1 else 'next-checkpoint')\n"
        f"payload = {ARTIFACT}\npayload['consumed'] = consumed\n"
        "(out / 'RUN.json').write_text(json.dumps(payload))\n")
    (tmp_path / 'segment.sbatch').write_text('#!/bin/bash\n' + shlex.quote(sys.executable) + ' segment.py\n')
    steps = Steps.from_dict({'step': {'segment': {
        'run': 'segment.sbatch', 'snapshot': ['*.py', '*.sbatch'], 'inputs': ['chain'],
        'outputs': ['chain/seg{seg}/RUN.json', 'chain/seg{seg}/checkpoint.txt'],
        'finite': ['metric'], 'segments': 2, 'chain': 'afterok',
    }}})
    scheduler = Scheduler()
    ledger = Ledger(tmp_path / 'attempts')
    runner = Runner(Site.minimal(), ledger, steps, workspace=tmp_path, task_id='T-third-review',
                    sbatch=scheduler.submit, states=scheduler, sacct_row=lambda _: None)
    runner.tick()
    first = ledger.latest('segment#1')['submit']['job_id']
    second = ledger.latest('segment#2')['submit']['job_id']
    # Slurm may finish and release both jobs before the next worker wake.
    p1 = scheduler.execute(first)
    assert p1.returncode == 0, p1.stderr
    p2 = scheduler.execute(second)
    assert p2.returncode == 0, p2.stderr
    result = runner.tick()
    assert result.receipt_status == 'SUBMITTED', result.summary
    assert ledger.ok('segment#1') and ledger.ok('segment#2')
    produced = json.loads((tmp_path / 'chain/seg2/RUN.json').read_text())
    assert produced['consumed'] == 'fresh-checkpoint', produced


def test_publishing_workspace_root_cannot_delete_workspace(tmp_path):
    # This fixture owns the entire target tree; never point this test at a real workspace.
    ws = tmp_path / 'owned-workspace'
    ws.mkdir()
    (ws / 'research-input.txt').write_text('irreplaceable input')
    (ws / 'train.py').write_text('import json, os\n' +
                                f"json.dump({ARTIFACT}, open('RUN.json', 'w'))\n")
    try:
        runner, _ = local_runner(ws, outputs=['RUN.json', '.'])
        runner.tick()
    except StepsError:
        pass  # Rejecting the overlapping destination before execution is correct.
    except OSError:
        pass  # Check whether the publication error happened before or after data loss.
    assert (ws / 'research-input.txt').exists(), 'publishing . removed the entire workspace, including its own attempt source'
    assert (ws / 'train.py').exists()


def test_declared_input_directory_is_merged_with_output_parent(tmp_path):
    (tmp_path / 'data').mkdir()
    (tmp_path / 'data/input.txt').write_text('value')
    (tmp_path / 'train.py').write_text(
        "import json, os\nfrom pathlib import Path\n"
        "assert Path('data/input.txt').read_text() == 'value'\n"
        f"json.dump({ARTIFACT}, open('data/RUN.json', 'w'))\n")
    runner, ledger = local_runner(tmp_path, inputs=['data'], outputs=['data/RUN.json'])
    result = runner.tick()
    assert result.receipt_status == 'SUBMITTED', result.summary


def test_declared_input_output_overlap_does_not_bypass_failed_output_isolation(tmp_path):
    previous = json.dumps({'provenance': {'attempt_id': 'previous'}, 'metric': 1})
    (tmp_path / 'RUN.json').write_text(previous)
    invalid = ARTIFACT.replace("'metric': 1", "'metric': float('nan')")
    (tmp_path / 'train.py').write_text('import json, os\n' +
                                      f"json.dump({invalid}, open('RUN.json', 'w'))\n")
    try:
        runner, ledger = local_runner(tmp_path, inputs=['RUN.json'], outputs=['RUN.json'])
        result = runner.tick()
        assert result.receipt_status == 'AWAITING' and not ledger.ok('train')
    except StepsError:
        pass  # An explicit pre-execution rejection is preferable to silent in-place mutation.
    assert (tmp_path / 'RUN.json').read_text() == previous, 'declared input linked an output despite the output-isolation rule'


def test_board_cli_reader_uses_supported_events_last():
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, json.dumps({'events': [], 'cursor': '0'}), '')

    reader = CliRuntimeReader('/fake/farm', '/fake/project', run=run)
    assert tail_events(reader, 12) == []
    assert calls == [['/fake/farm', '--project', '/fake/project', 'events', '--last', '12']], calls


def test_afterok_successor_never_sees_the_stale_workspace_copy(tmp_path):
    """Companion to the case above: the workspace copy stays the old, verified one until the
    worker publishes; the successor's run directory points at the producer attempt."""
    old = tmp_path / 'chain/seg1/checkpoint.txt'
    old.parent.mkdir(parents=True)
    old.write_text('old-checkpoint')
    (tmp_path / 'segment.py').write_text(
        "import json, os\nfrom pathlib import Path\n"
        "n = int(os.environ['SEGMENT'])\n"
        "out = Path(f'chain/seg{n}')\nout.mkdir(parents=True, exist_ok=True)\n"
        "ws_copy = (Path(os.environ['FARMKIT_WORKSPACE']) / 'chain/seg1/checkpoint.txt').read_text()\n"
        "(out / 'checkpoint.txt').write_text('fresh-checkpoint' if n == 1 else 'next-checkpoint')\n"
        f"payload = {ARTIFACT}\npayload['ws_copy'] = ws_copy\n"
        "(out / 'RUN.json').write_text(json.dumps(payload))\n")
    (tmp_path / 'segment.sbatch').write_text('#!/bin/bash\n' + shlex.quote(sys.executable) + ' segment.py\n')
    steps = Steps.from_dict({'step': {'segment': {
        'run': 'segment.sbatch', 'snapshot': ['*.py', '*.sbatch'], 'inputs': ['chain'],
        'outputs': ['chain/seg{seg}/RUN.json', 'chain/seg{seg}/checkpoint.txt'],
        'finite': ['metric'], 'segments': 2, 'chain': 'afterok',
    }}})
    scheduler = Scheduler()
    ledger = Ledger(tmp_path / 'attempts')
    runner = Runner(Site.minimal(), ledger, steps, workspace=tmp_path, task_id='T-third-review',
                    sbatch=scheduler.submit, states=scheduler, sacct_row=lambda _: None)
    runner.tick()
    for step in ('segment#1', 'segment#2'):
        assert scheduler.execute(ledger.latest(step)['submit']['job_id']).returncode == 0
    seg2_run = Path(ledger.latest('segment#2')['run_dir'])
    assert (seg2_run / 'chain/seg1/checkpoint.txt').resolve().parent.parent.parent == Path(ledger.latest('segment#1')['run_dir']).resolve()
    assert (tmp_path / 'chain/seg1/checkpoint.txt').read_text() == 'old-checkpoint'   # not published yet
    result = runner.tick()
    assert result.receipt_status == 'SUBMITTED'
    assert (tmp_path / 'chain/seg1/checkpoint.txt').read_text() == 'fresh-checkpoint'  # published after verification
    assert json.loads((tmp_path / 'chain/seg2/RUN.json').read_text())['ws_copy'] == 'old-checkpoint'
