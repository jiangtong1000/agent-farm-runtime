"""tools/release.py: exported release digest matches the running identity; rendered wrapper works."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
TOOL = REPO / "tools" / "release.py"
# Optional compatibility check against a wrapper supplied by the test operator.
# No test should discover or depend on a contributor's private deployment.
REFERENCE_WRAPPER = os.environ.get("FARM_TEST_REFERENCE_WRAPPER")

SITE = """
[site]
name = "testsite"
hostname_pattern = ".*"
[scheduler]
squeue = "squeue"
sacct = "sacct"
[python]
runtime_interpreter = "{python}"
[farm_defaults]
executor = "codex-tmux"
interval = 30
grace_seconds = 120
max_auto_restarts = 3
[executor.codex]
cmd = "codex exec --skip-git-repo-check --sandbox danger-full-access -m gpt-6-astra -c model_reasoning_effort=medium"
path_prelude = "export PATH=/opt/node/bin:$PATH"
[executor.claude]
cmd = "claude -p --dangerously-skip-permissions --output-format json"
"""


def _run(*args, **kw):
    return subprocess.run([sys.executable, str(TOOL), *args], text=True, capture_output=True, **kw)


@pytest.fixture(scope="module")
def release(tmp_path_factory):
    root = tmp_path_factory.mktemp("releases")
    proc = _run("export", "--src", str(REPO / "src"), "--tag", "test", "--releases-root", str(root),
                "--allow-dirty", "--writable")
    assert proc.returncode == 0, proc.stderr
    info = json.loads(proc.stdout)
    return Path(info["release_dir"]), info


def test_export_digest_matches_runtime_identity(release):
    release_dir, info = release
    from agent_farm_runtime.provenance import runtime_identity
    assert info["source_sha256"] == runtime_identity()["source_sha256"]
    assert (release_dir / "src" / "agent_farm_runtime" / "protocol" / "__init__.py").exists()
    assert (release_dir / "src" / "farmkit").is_dir()
    assert json.loads((release_dir / "RELEASE.json").read_text())["protocol_version"] == 4


def test_export_refuses_duplicate(release, tmp_path):
    release_dir, _ = release
    proc = _run("export", "--src", str(REPO / "src"), "--tag", "test",
                "--releases-root", str(release_dir.parent), "--allow-dirty", "--writable")
    assert proc.returncode != 0 and "already exists" in proc.stderr


def test_rendered_wrapper_runs_read_only_commands_and_pins_source(release, tmp_path):
    release_dir, info = release
    site = tmp_path / "testsite.toml"; site.write_text(SITE.format(python=sys.executable))
    farm = tmp_path / "myfarm"; farm.mkdir()
    proc = _run("wrapper", "--site", str(site), "--release", str(release_dir), "--out", str(tmp_path / "bin"),
                "--farm", str(farm))
    assert proc.returncode == 0, proc.stderr
    wrapper = tmp_path / "bin" / "farm"
    text = wrapper.read_text()
    assert info["source_sha256"] in text and str(release_dir / "src") in text
    assert "-B -I -S" in text and 'FARM_ACTUATION_ALLOWED' in text
    # version is read-only and reports the pinned source
    out = subprocess.run([str(wrapper), "version"], text=True, capture_output=True, env={**os.environ, "PYTHONPATH": ""})
    assert out.returncode == 0, out.stderr
    ident = json.loads(out.stdout)
    assert ident["source_sha256"] == info["source_sha256"] and ident["source_root"].startswith(str(release_dir))
    # reconcile without activation is refused before anything happens
    out = subprocess.run([str(wrapper), "--project", str(farm), "reconcile"], text=True, capture_output=True)
    assert out.returncode == 78 and "activation is OFF" in out.stderr
    # run_reconciler.sh carries the site's executor settings and the farm's own session/socket
    script = (farm / "run_reconciler.sh").read_text()
    assert "--session myfarm --tmux-socket myfarm" in script
    assert "FARM_CODEX_CMD=" in script and "gpt-6-astra" in script and "FARM_CLAUDE_CMD=" in script
    assert "--max-auto-restarts 3" in script and str(wrapper) in script
    assert f"--log {farm / 'reconciler.log'}" in script
    assert "RECONCILER_EXITED" in script


def test_access_readers_bypass_only_wrapper_write_pin(release, tmp_path):
    release_dir, info = release
    site = tmp_path / "testsite.toml"
    site.write_text(SITE.format(python=sys.executable))
    proc = _run("wrapper", "--site", str(site), "--release", str(release_dir), "--out", str(tmp_path / "bin"))
    assert proc.returncode == 0, proc.stderr
    wrapper = tmp_path / "bin" / "farm"
    wrapper.write_text(wrapper.read_text().replace(info["source_sha256"], "0" * 64))
    env = {k: v for k, v in os.environ.items() if k != "FARM_ACCESS_REGISTRY"}
    for action in ("resolve", "verify", "init", "publish"):
        args = ["access", action]
        if action != "init":
            args += ["--target", "primary"]
        if action == "publish":
            args += ["--job-id", "123", "--control-session", "master"]
        out = subprocess.run([str(wrapper), *args], text=True, capture_output=True, env=env)
        if action in {"resolve", "verify"}:
            assert out.returncode == 1, out.stderr
            assert json.loads(out.stdout)["state"] == "UNPUBLISHED"
        else:
            assert out.returncode == 78 and "source" in out.stderr.lower(), out.stdout + out.stderr


def test_site_model_home_and_log_settings_survive_rendered_shell(release, tmp_path):
    release_dir, _ = release
    farm = tmp_path / "farm $literal; space"
    farm.mkdir()
    site = tmp_path / "site.toml"
    home = str(tmp_path / "worker home $literal")
    log = str(tmp_path / "logs" / "custom $literal.log")
    site.write_text('[site]\nname="test"\n[python]\nruntime_interpreter=' + json.dumps(sys.executable)
                    + '\n[farm_defaults]\nlog=' + json.dumps(log)
                    + '\n[executor.codex]\nmodel="example-model"\neffort="high"\nhome=' + json.dumps(home)
                    + '\n[executor.claude]\nmodel="example-claude"\neffort="medium"\nhome=' + json.dumps(home + "-claude") + '\n')
    out_dir = tmp_path / "bin $literal"
    proc = _run("wrapper", "--site", str(site), "--release", str(release_dir), "--out", str(out_dir),
                "--farm", str(farm))
    assert proc.returncode == 0, proc.stderr
    wrapper = out_dir / "farm"
    # Replace only this disposable wrapper with an argv/env recorder; no real daemon.
    wrapper.write_text(f"#!{sys.executable}\nimport json, os, sys\n"
                       "print(json.dumps({'argv':sys.argv[1:], 'env':{k:v for k,v in os.environ.items() "
                       "if k.startswith(('FARM_CODEX_', 'FARM_CLAUDE_'))}}))\nsys.exit(7)\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FARM_CODEX_", "FARM_CLAUDE_"))}
    proc = subprocess.run([str(farm / "run_reconciler.sh")], env=env, text=True, capture_output=True)
    assert proc.returncode == 7 and "RECONCILER_EXITED rc=7" in proc.stderr
    observed = json.loads(proc.stdout)
    assert observed["env"] == {"FARM_CODEX_MODEL": "example-model", "FARM_CODEX_EFFORT": "high",
                               "FARM_CODEX_HOME": home, "FARM_CLAUDE_MODEL": "example-claude",
                               "FARM_CLAUDE_EFFORT": "medium", "FARM_CLAUDE_HOME": home + "-claude"}
    argv = observed["argv"]
    assert argv[argv.index("--project") + 1] == str(farm)
    assert argv[argv.index("--session") + 1] == farm.name
    assert argv[argv.index("--log") + 1] == log


def test_wrapper_quotes_release_paths_as_literal_shell_values(release, tmp_path):
    release_dir, info = release
    relocated = tmp_path / "release $literal; spaces"
    shutil.copytree(release_dir, relocated)
    site = tmp_path / "site.toml"
    site.write_text(SITE.format(python=sys.executable))
    proc = _run("wrapper", "--site", str(site), "--release", str(relocated), "--out", str(tmp_path / "bin"))
    assert proc.returncode == 0, proc.stderr
    proc = subprocess.run([str(tmp_path / "bin" / "farm"), "version"], text=True, capture_output=True)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["source_sha256"] == info["source_sha256"]


@pytest.mark.skipif(not REFERENCE_WRAPPER, reason="set FARM_TEST_REFERENCE_WRAPPER for an optional local comparison")
def test_rendered_wrapper_differs_from_reference_only_in_pins(release, tmp_path):
    release_dir, _ = release
    site = tmp_path / "s.toml"
    site.write_text(SITE.format(python=os.environ.get("FARM_TEST_REFERENCE_PYTHON", sys.executable)))
    _run("wrapper", "--site", str(site), "--release", str(release_dir), "--out", str(tmp_path))
    rendered = (tmp_path / "farm").read_text().splitlines()
    reference = Path(REFERENCE_WRAPPER).read_text().splitlines()

    def core(lines):
        keep = []
        for line in lines:
            if line.startswith("# Release:") or line.startswith("# Rendered by") or line.startswith("export PYTHONPATH=") \
                    or line.startswith("export PATH=") or line.startswith("exec ") or 'expected = "' in line or "args.command not in" in line:
                continue
            keep.append(line)
        return keep
    assert core(rendered) == core(reference)
