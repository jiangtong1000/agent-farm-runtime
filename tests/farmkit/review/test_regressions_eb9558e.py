"""Regressions from the fifth review of eb9558e (2026-09-20), verbatim apart from this header.
Both failed on eb9558e.
"""
from __future__ import annotations

import errno
import hashlib
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
    monkeypatch.setenv('FARMKIT_ENV_DIGEST', 'fifth-review-fixture')


def test_interrupted_publish_retains_backup_when_recovery_copy_fails(tmp_path, monkeypatch):
    # Only this test's temporary trees are used. No research workspace is touched.
    src, dst = tmp_path / 'attempt-output', tmp_path / 'model'
    src.mkdir()
    dst.mkdir()
    (src / 'weights.dat').write_text('new weights')
    (dst / 'weights.dat').write_text('previous verified weights')
    aid = 'abcd1234abcd1234abcd1234abcd1234'
    fresh = tmp_path / '.model.publishing-abcd1234'
    old = tmp_path / '.model.replaced-abcd1234'
    rename = Path.rename

    def interrupt_before_install(path, target):
        if path == fresh:
            raise RuntimeError('injected interruption after old rename and before fresh rename')
        return rename(path, target)

    with monkeypatch.context() as m:
        m.setattr(Path, 'rename', interrupt_before_install)
        with pytest.raises(RuntimeError, match='injected interruption'):
            Runner._replace_dir(src, dst, aid)
    assert not dst.exists()
    assert (old / 'weights.dat').read_text() == 'previous verified weights'
    assert (fresh / 'weights.dat').read_text() == 'new weights'

    def no_space(*args, **kwargs):
        raise OSError(errno.ENOSPC, 'injected full filesystem during recovery copy')

    with monkeypatch.context() as m:
        m.setattr(shutil, 'copytree', no_space)
        try:
            Runner._replace_dir(src, dst, aid)
        except OSError as exc:
            assert exc.errno == errno.ENOSPC
    # Recovery can finish the ready swap without copying; if it elects to copy and
    # cannot, it must preserve the previous verified version for rollback/recovery.
    installed = (dst / 'weights.dat').is_file()
    rollback = (old / 'weights.dat').is_file()
    assert installed or rollback, 'recovery removed both complete staging and the old verified backup before copying'


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
        cwd = next(a.split('=', 1)[1] for a in argv if a.startswith('--chdir='))
        export = next(a.split('=', 1)[1] for a in argv if a.startswith('--export='))
        env = {**os.environ, **dict(v.split('=', 1) for v in export.split(',') if v != 'ALL')}
        proc = subprocess.run(['bash', argv[-1]], cwd=cwd, env=env,
                              capture_output=True, text=True, timeout=10)
        self.values[job] = 'COMPLETED' if proc.returncode == 0 else 'FAILED'
        return proc


def test_completed_job_input_identity_is_not_rewritten_by_later_input_edit(tmp_path):
    theta = tmp_path / 'theta.txt'
    theta.write_text('input consumed by this job')
    expected = hashlib.sha256(theta.read_bytes()).hexdigest()
    (tmp_path / 'train.py').write_text(
        'import hashlib, json, os\nfrom pathlib import Path\n'
        "raw = Path('theta.txt').read_bytes()\n"
        "payload = {'provenance': {'attempt_id': os.environ['FARMKIT_ATTEMPT_ID']}, 'metric': 1,\n"
        "           'consumed_sha256': hashlib.sha256(raw).hexdigest()}\n"
        "Path('RUN.json').write_text(json.dumps(payload))\n")
    (tmp_path / 'train.sbatch').write_text('#!/bin/bash\n' + shlex.quote(sys.executable) + ' train.py\n')
    steps = Steps.from_dict({'step': {'train': {
        'run': 'train.sbatch', 'snapshot': ['*.py', '*.sbatch'], 'inputs': ['theta.txt'],
        'outputs': ['RUN.json'], 'finite': ['metric'],
    }}})
    ledger = Ledger(tmp_path / 'attempts')
    scheduler = Scheduler()
    runner = Runner(Site.minimal(), ledger, steps, workspace=tmp_path,
                    sbatch=scheduler.submit, states=scheduler, sacct_row=lambda _: None)
    runner.tick()
    rec = ledger.latest('train')
    assert rec['inputs']['theta.txt']['sha256'] == expected
    proc = scheduler.execute(rec['submit']['job_id'])
    assert proc.returncode == 0, proc.stderr
    # The command already exited, but the worker has not yet woken up to verify it.
    theta.write_text('later input that this finished job never read')
    result = runner.tick()
    rec = ledger.latest('train')
    if result.receipt_status != 'SUBMITTED':
        assert rec.get('failure'), 'explicitly rejecting a changed input is also acceptable'
        return
    payload = json.loads((tmp_path / 'RUN.json').read_text())
    assert payload['consumed_sha256'] == expected
    assert rec['inputs']['theta.txt']['sha256'] == expected, 'verification replaced correct input provenance with bytes written after job completion'
