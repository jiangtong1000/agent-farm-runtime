"""Immutable access maintenance is independent of task execution epochs."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import os

import pytest

from test_access import farm, fails, contents
from agent_farm_runtime.access.contract import AccessError
from agent_farm_runtime.access.registry import _directory


def test_permission_error_reports_observed_and_required_mode(tmp_path):
    path = tmp_path / "private-registry"
    path.mkdir()
    path.chmod(0o2770)
    with pytest.raises(AccessError) as caught:
        _directory(path, create=True)
    assert caught.value.state == "AUTH_REQUIRED"
    assert "2770" in str(caught.value) and "0700" in str(caught.value)
    assert "getfacl" in str(caught.value) and "group/other write" in str(caught.value)
    assert path.stat().st_mode & 0o7777 == 0o2770


def test_same_epoch_rotation_retains_legacy_record_and_farm(farm):
    old = farm.publish()
    before = contents(farm.paths.root)
    legacy = (farm.directory / "epochs/initial.json").read_bytes()
    del farm.obs.windows["main"]
    fails("SESSION_MISSING", lambda: farm.access.verify(farm.paths, "cluster/study-a"))
    rotated = farm.publish(default_window=None, generation="access-2", expected_current=old["record_sha256"])
    assert rotated["schema_version"] == 2
    assert rotated["record"]["execution_epoch"] == "initial"
    assert rotated["record"]["previous_record_sha256"] == old["record_sha256"]
    assert rotated["record"]["control"]["window_id"] is None
    assert rotated["attachment"]["argv"][6] == "$1"
    assert farm.access.resolve("cluster/study-a")["record"] == rotated["record"]
    assert (farm.directory / "epochs/initial.json").read_bytes() == legacy
    assert contents(farm.paths.root) == before
    fails("STALE_REGISTRY", lambda: farm.access.verify(farm.paths, "cluster/study-a", expected_record=old["record_sha256"]))


def test_generation_retry_is_immutable_and_stale_writer_cannot_restore_previous(farm):
    a = farm.publish(default_window=None, generation="a", expected_current="none")
    path = farm.directory / "generations/a.json"
    original = path.read_bytes(), path.stat().st_mtime_ns
    assert farm.publish(default_window=None, generation="a", expected_current="none")["record"] == a["record"]
    assert (path.read_bytes(), path.stat().st_mtime_ns) == original
    farm.publish(default_window=None, generation="b", expected_current=a["record_sha256"])
    fails("STALE_REGISTRY", lambda: farm.publish(default_window=None, generation="a", expected_current="none"))
    fails("CONFLICT", lambda: farm.publish(default_window=None), "generation_required")


def test_concurrent_generations_share_cas_and_only_one_advances(farm):
    old = farm.publish(default_window=None)
    def publish(generation):
        try:
            return farm.publish(default_window=None, generation=generation, expected_current=old["record_sha256"])
        except AccessError as exc:
            return {"state": exc.state}
    with ThreadPoolExecutor(max_workers=2) as pool:
        values = list(pool.map(publish, ["a", "b"]))
    assert sorted(v["state"] for v in values) == ["STALE_REGISTRY", "VERIFIED"]
    assert len(list((farm.directory / "generations").glob("*.json"))) == 1


def test_generation_crash_can_finish_own_orphan_but_never_overwrite_new_current(farm, monkeypatch):
    old = farm.publish(default_window=None)
    commit = farm.registry.commit
    def crash(*args):
        raise OSError("injected")
    with monkeypatch.context() as fault:
        fault.setattr(farm.registry, "commit", crash)
        with pytest.raises(OSError):
            farm.publish(default_window=None, generation="a", expected_current=old["record_sha256"])
    orphan = (farm.directory / "generations/a.json").read_bytes()
    assert farm.access.resolve("cluster/study-a")["record_sha256"] == old["record_sha256"]
    farm.publish(default_window=None, generation="b", expected_current=old["record_sha256"])
    fails("STALE_REGISTRY", lambda: farm.publish(default_window=None, generation="a", expected_current=old["record_sha256"]))
    assert (farm.directory / "generations/a.json").read_bytes() == orphan


def test_current_pointer_selects_generation_exactly_and_never_falls_back(farm):
    farm.publish(default_window=None, generation="a", expected_current="none")
    (farm.directory / "generations/a.json").unlink()
    fails("STALE_REGISTRY", lambda: farm.access.resolve("cluster/study-a"))


def test_generation_cannot_rebind_farm_or_root(farm):
    old = farm.publish(default_window=None)
    binding = farm.directory / "binding.json"
    value = json.loads(binding.read_text())
    value["farm_id"] = "farm-" + "a" * 64
    binding.write_text(json.dumps(value))
    fails("CONFLICT", lambda: farm.publish(default_window=None, generation="a", expected_current=old["record_sha256"]), "target_binding")


@pytest.mark.parametrize("options", [{"generation": "a"}, {"expected_current": "none"},
                                    {"generation": "../bad", "expected_current": "none"},
                                    {"generation": "a", "expected_current": "bad"}])
def test_generation_requires_explicit_valid_preconditions(farm, options):
    fails("CONFLICT", lambda: farm.publish(**options))
    assert not farm.obs.calls
