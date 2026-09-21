"""Regressions from the fourth review of 81eb4e3 (2026-09-20), verbatim apart from this header.
Both failed on 81eb4e3.
"""
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from farmkit.ledger import Ledger
from farmkit.runner import Runner
from farmkit.site import Site
from farmkit.steps import Steps


@pytest.fixture(autouse=True)
def cheap_environment(monkeypatch):
    monkeypatch.setenv('FARMKIT_ENV_DIGEST', 'fourth-review-fixture')


ARTIFACT = "{'provenance': {'attempt_id': os.environ['FARMKIT_ATTEMPT_ID']}, 'metric': 1}"


def test_directory_publish_recovers_after_swap_before_backup_cleanup(tmp_path, monkeypatch):
    (tmp_path / 'model').mkdir()
    (tmp_path / 'model/weights.dat').write_text('previous weights')
    (tmp_path / 'train.py').write_text(
        "import json, os\nfrom pathlib import Path\n"
        "Path('model').mkdir()\nPath('model/weights.dat').write_text('new weights')\n"
        f"json.dump({ARTIFACT}, open('RUN.json', 'w'))\n")
    steps = Steps.from_dict({'step': {'train': {
        'run': shlex.quote(sys.executable) + ' train.py', 'snapshot': ['*.py'],
        'outputs': ['RUN.json', 'model'], 'finite': ['metric'],
    }}})
    ledger = Ledger(tmp_path / 'attempts')
    runner = Runner(Site.minimal(), ledger, steps, workspace=tmp_path, sacct_row=lambda _: None)
    actual_rmtree = shutil.rmtree

    def crash_before_cleanup(path, *args, **kwargs):
        if Path(path).name.startswith('.model.replaced-'):
            raise RuntimeError('injected crash after directory swap, before old-directory cleanup')
        return actual_rmtree(path, *args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr(shutil, 'rmtree', crash_before_cleanup)
        with pytest.raises(RuntimeError, match='injected crash'):
            runner.tick()
    assert (tmp_path / 'model/weights.dat').read_text() == 'new weights'
    assert len(list(tmp_path.glob('.model.replaced-*'))) == 1
    assert ledger.latest('train')['verdict'] is None

    def must_not_rerun(*args, **kwargs):
        raise AssertionError('recovery must publish/verify the existing attempt, not rerun its command')

    resumed_ledger = Ledger(ledger.root)
    resumed = Runner(Site.minimal(), resumed_ledger, steps, workspace=tmp_path,
                     local_run=must_not_rerun, sacct_row=lambda _: None)
    result = resumed.tick()
    assert result.receipt_status == 'SUBMITTED', result.summary
    assert resumed_ledger.ok('train')
    assert (tmp_path / 'model/weights.dat').read_text() == 'new weights'


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


def test_chain_input_digest_describes_the_bytes_actually_consumed(tmp_path):
    (tmp_path / 'A.json').write_text(json.dumps({
        'provenance': {'attempt_id': 'previous'}, 'metric': 1, 'version': 'old'}))
    (tmp_path / 'step.py').write_text(
        'import hashlib, json, os\nfrom pathlib import Path\n'
        f'payload = {ARTIFACT}\n'
        "if os.environ['MODE'] == 'produce':\n"
        "    payload['version'] = 'fresh'\n    target = 'A.json'\n"
        'else:\n'
        # Follow the new contract: relative path binds to the producing attempt.
        "    raw = Path('A.json').read_bytes()\n"
        "    payload['consumed_sha256'] = hashlib.sha256(raw).hexdigest()\n"
        "    payload['consumed_version'] = json.loads(raw)['version']\n    target = 'B.json'\n"
        'Path(target).write_text(json.dumps(payload))\n')
    (tmp_path / 'step.sbatch').write_text('#!/bin/bash\n' + shlex.quote(sys.executable) + ' step.py\n')
    steps = Steps.from_dict({
        'defaults': {'snapshot': ['*.py', '*.sbatch'], 'finite': ['metric']},
        'step': {
            'produce': {'run': 'step.sbatch', 'env': {'MODE': 'produce'}, 'outputs': ['A.json']},
            'consume': {'run': 'step.sbatch', 'env': {'MODE': 'consume'}, 'after': ['chain:produce'],
                        'inputs': ['A.json'], 'outputs': ['B.json']},
        },
    })
    scheduler = Scheduler()
    ledger = Ledger(tmp_path / 'attempts')
    runner = Runner(Site.minimal(), ledger, steps, workspace=tmp_path,
                    sbatch=scheduler.submit, states=scheduler, sacct_row=lambda _: None)
    runner.tick()
    for step in ('produce', 'consume'):
        proc = scheduler.execute(ledger.latest(step)['submit']['job_id'])
        assert proc.returncode == 0, proc.stderr
    result = runner.tick()
    assert result.receipt_status == 'SUBMITTED', result.summary
    payload = json.loads((tmp_path / 'B.json').read_text())
    assert payload['consumed_version'] == 'fresh', 'the new relative-path contract must supply the current producer output'
    recorded_input = ledger.latest('consume')['inputs']['A.json']
    assert recorded_input['sha256'] == payload['consumed_sha256'], {
        'recorded_input': recorded_input, 'actual_consumed_sha256': payload['consumed_sha256']}
