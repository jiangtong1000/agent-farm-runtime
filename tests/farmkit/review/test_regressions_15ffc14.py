"""Regressions from the sixth review of 15ffc14 (2026-09-20), verbatim apart from this header.
The failed-cleanup case failed on 15ffc14; the crash matrix passed and is kept as coverage.
"""
from __future__ import annotations

import json
import shlex
import shutil
import sys
from pathlib import Path

import pytest

from farmkit.ledger import Ledger
from farmkit.runner import Runner
from farmkit.site import Site
from farmkit.steps import Steps, StepsError
from farmkit.verify import Verdict


@pytest.fixture(autouse=True)
def cheap_environment(monkeypatch):
    monkeypatch.setenv('FARMKIT_ENV_DIGEST', 'sixth-review-fixture')


class SimulatedCrash(RuntimeError):
    pass


@pytest.mark.parametrize('phase', [
    'partial_copy', 'before_marker', 'after_marker',
    'after_old_rename', 'after_install', 'before_backup_cleanup',
])
def test_directory_publish_recovers_at_each_phase(tmp_path, monkeypatch, phase):
    src, dst = tmp_path / 'source', tmp_path / 'model'
    src.mkdir()
    dst.mkdir()
    (src / 'weights.dat').write_text('complete new weights')
    (dst / 'weights.dat').write_text('previous verified weights')
    aid = 'abcdef12abcdef12abcdef12abcdef12'
    fresh = tmp_path / '.model.publishing-abcdef12'
    marker = tmp_path / '.model.publishing-abcdef12.complete'
    old = tmp_path / '.model.replaced-abcdef12'
    copytree, rmtree, rename, touch = shutil.copytree, shutil.rmtree, Path.rename, Path.touch

    def copying(source, target, *args, **kwargs):
        if phase == 'partial_copy' and Path(target) == fresh:
            fresh.mkdir()
            (fresh / 'weights.dat').write_text('truncated')
            raise SimulatedCrash(phase)
        return copytree(source, target, *args, **kwargs)

    def touching(path, *args, **kwargs):
        if path == marker and phase == 'before_marker':
            raise SimulatedCrash(phase)
        result = touch(path, *args, **kwargs)
        if path == marker and phase == 'after_marker':
            raise SimulatedCrash(phase)
        return result

    def renaming(path, target):
        result = rename(path, target)
        if phase == 'after_old_rename' and path == dst and Path(target) == old:
            raise SimulatedCrash(phase)
        if phase == 'after_install' and path == fresh and Path(target) == dst:
            raise SimulatedCrash(phase)
        return result

    def removing(path, *args, **kwargs):
        if phase == 'before_backup_cleanup' and Path(path) == old:
            raise SimulatedCrash(phase)
        return rmtree(path, *args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr(shutil, 'copytree', copying)
        m.setattr(shutil, 'rmtree', removing)
        m.setattr(Path, 'rename', renaming)
        m.setattr(Path, 'touch', touching)
        with pytest.raises(SimulatedCrash):
            Runner._replace_dir(src, dst, aid)
    Runner._replace_dir(src, dst, aid)
    assert (dst / 'weights.dat').read_text() == 'complete new weights'
    assert not fresh.exists() and not old.exists() and not marker.exists()


def test_failed_cleanup_cannot_publish_incomplete_staging_as_verified(tmp_path, monkeypatch):
    (tmp_path / 'model').mkdir()
    (tmp_path / 'model/weights.dat').write_text('previous verified weights')
    (tmp_path / 'train.py').write_text(
        'import json, os\nfrom pathlib import Path\n'
        "Path('model').mkdir()\nPath('model/weights.dat').write_text('complete new weights')\n"
        "Path('RUN.json').write_text(json.dumps({'provenance': {'attempt_id': os.environ['FARMKIT_ATTEMPT_ID']}, 'metric': 1}))\n")

    def model_ok(attempt, artifacts):
        return Verdict((artifacts['model'] / 'weights.dat').read_text() == 'complete new weights')

    steps = Steps.from_dict({'step': {'train': {
        'run': shlex.quote(sys.executable) + ' train.py', 'snapshot': ['*.py'],
        'outputs': ['RUN.json', 'model'], 'finite': ['metric'], 'verify': 'model_ok',
    }}}, verifiers={'model_ok': model_ok})
    ledger = Ledger(tmp_path / 'attempts')
    runner = Runner(Site.minimal(), ledger, steps, workspace=tmp_path, sacct_row=lambda _: None)

    def partial_copy(source, target, *args, **kwargs):
        Path(target).mkdir()
        (Path(target) / 'weights.dat').write_text('truncated')
        raise OSError('injected interruption while copying model')

    with monkeypatch.context() as m:
        m.setattr(shutil, 'copytree', partial_copy)
        with pytest.raises(OSError, match='injected interruption'):
            runner.tick()
    rec = ledger.latest('train')
    fresh = tmp_path / f".model.publishing-{rec['attempt_id'][:8]}"
    marker = fresh.with_name(fresh.name + '.complete')
    assert fresh.exists() and not marker.exists()
    assert (tmp_path / 'model/weights.dat').read_text() == 'previous verified weights'
    actual_rmtree = shutil.rmtree

    def failed_cleanup(path, *args, **kwargs):
        if Path(path) == fresh:
            # rmtree(ignore_errors=True) returns without removing a tree when deletion
            # fails (e.g. directory permissions or an IO error). Model that exact state.
            if kwargs.get('ignore_errors'):
                return None
            raise PermissionError('injected inability to remove incomplete staging')
        return actual_rmtree(path, *args, **kwargs)

    def must_not_rerun(*args, **kwargs):
        raise AssertionError('recovery must not execute the completed scientific command again')

    resumed_ledger = Ledger(ledger.root)
    resumed = Runner(Site.minimal(), resumed_ledger, steps, workspace=tmp_path,
                     local_run=must_not_rerun, sacct_row=lambda _: None)
    with monkeypatch.context() as m:
        m.setattr(shutil, 'rmtree', failed_cleanup)
        try:
            result = resumed.tick()
        except (OSError, StepsError):
            assert (tmp_path / 'model/weights.dat').read_text() == 'previous verified weights'
            assert not resumed_ledger.ok('train')
            return
    if result.receipt_status != 'SUBMITTED':
        assert (tmp_path / 'model/weights.dat').read_text() == 'previous verified weights'
        return
    assert (tmp_path / 'model/weights.dat').read_text() == 'complete new weights', (
        'SUBMITTED with a successful project verifier, but the published model is the incomplete staging copy')
