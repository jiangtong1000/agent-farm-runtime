"""Public CLI + actual isolated tmux; scheduler and Master work are synthetic."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from test_access_tmux_canary import private_tmux, pytestmark
from agent_farm_runtime.provenance import runtime_identity, farm_identity, deployment_stamp
from agent_farm_runtime.procutil import proc_starttime
from agent_farm_runtime.store import FarmPaths


def test_public_cli_adopt_attach_reconnect_and_rotate_existing_master(private_tmux):
    socket, run_argv, tmux = private_tmux
    repository = Path(__file__).resolve().parents[1]
    # Access intentionally rejects /tmp durable storage. This disposable directory
    # lives in the checkout, never in a user's existing farm or registry.
    with tempfile.TemporaryDirectory(prefix=".access-canary-", dir=repository) as work:
        work = Path(work)
        project, registry = work / "project", work / "registry"
        paths = FarmPaths(project / ".farm")
        paths.ensure()
        now = datetime.now(timezone.utc).isoformat()
        identity = runtime_identity()
        manifest = {**identity, **farm_identity(str(paths.root)), "execution_epoch": "initial",
                    "pid": os.getpid(), "pid_starttime": proc_starttime(os.getpid()),
                    "started_at": now, "loop": True, "interval": 120,
                    "executor": "codex-tmux", "session": "workers", "tmux_socket": "unused-worker-socket"}
        manifest["scheduler_attestation"] = {"kind": "slurm", "job_id": "123", "scheduler_node": "node-a",
            "allocation_started_at": "2026-01-01T00:00:00", "owner_uid": os.getuid(),
            "runtime_host": identity["host"], "execution_epoch": "initial"}
        (paths.runtime / "deployment.json").write_text(json.dumps(manifest))
        (paths.runtime / "last_tick.json").write_text(json.dumps({"ts": now, "deployment": deployment_stamp(manifest)}))
        bindir = work / "bin"
        bindir.mkdir()
        scheduler = bindir / "scontrol"
        scheduler.write_text("#!/bin/sh\nif [ \"$1\" = show ]; then\n  echo node-a\nelse\n"
            f"  echo 'JobId=123 JobState=RUNNING UserId=canary({os.getuid()}) NodeList=node-a StartTime=2026-01-01T00:00:00'\nfi\n")
        scheduler.chmod(0o700)
        env = {**os.environ, "PATH": str(bindir) + os.pathsep + os.environ["PATH"], "PYTHONPATH": str(repository / "src")}
        for key in ("TMUX", "SLURM_JOB_ID", "SLURM_JOBID", "SLURMD_NODENAME"):
            env.pop(key, None)

        def cli(action, *args, success=True):
            argv = [sys.executable, "-m", "agent_farm_runtime.cli", "--project", str(project),
                    "access", action, "--registry", str(registry)]
            if action != "init":
                argv += ["--target", "canary/study"]
            out = subprocess.run([*argv, *args, "--json"], capture_output=True, text=True, env=env, timeout=20)
            assert out.returncode == (0 if success else 1), (out.stdout, out.stderr)
            return json.loads(out.stdout)

        assert cli("init", "--attest-shared-storage")["state"] == "INITIALIZED"
        master_pid = int(tmux("display-message", "-p", "-t", "=master:main", "#{pane_pid}").stdout)
        common = ["--job-id", "123", "--control-socket", socket, "--control-session", "master", "--role-pid", str(master_pid)]
        adopted = cli("adopt", *common, "--generation", "master-1", "--expected-current", "none")
        assert adopted["record"]["endpoint_role"]["pid"] == master_pid
        resolved = cli("resolve", "--role", "interactive")
        assert resolved["record_sha256"] == adopted["record_sha256"]
        for _ in range(2):
            verified = cli("verify", "--role", "interactive", "--expected-record", resolved["record_sha256"])
            argv = verified["attachment"]["argv"]
            out = run_argv([argv[0], "-C", *argv[1:]], input="display-message -p '#{session_id}'\ndetach-client\n")
            assert out.returncode == 0, (out.stdout, out.stderr)
            assert "%session-changed" in out.stdout, out.stdout
            assert adopted["record"]["control"]["session_id"] in out.stdout
        assert int(tmux("display-message", "-p", "-t", "=master:main", "#{pane_pid}").stdout) == master_pid
        assert tmux("kill-window", "-t", "=master:logs").returncode == 0
        assert cli("verify", "--role", "interactive")["record_sha256"] == adopted["record_sha256"]

        # Exact-window binding deliberately fails after its optional window closes.
        assert tmux("new-window", "-t", "=master:", "-n", "optional", "exec sleep 120").returncode == 0
        exact = cli("adopt", *common, "--default-window", "optional", "--generation", "master-2",
                    "--expected-current", adopted["record_sha256"])
        assert tmux("kill-window", "-t", "=master:optional").returncode == 0
        stale = cli("verify", "--role", "interactive", success=False)
        assert stale["state"] == "SESSION_MISSING" and stale["reason"] == "window_missing"
        assert "--generation" in stale["detail"]
        rotated = cli("adopt", *common, "--generation", "master-3", "--expected-current", exact["record_sha256"])
        assert rotated["record"]["execution_epoch"] == "initial"
        assert len(list((registry / "targets/canary/study/generations").glob("*.json"))) == 3
        rejected = cli("adopt", *common, "--generation", "stale-writer", "--expected-current", exact["record_sha256"], success=False)
        assert rejected["reason"] == "expected_current"
        assert cli("verify", "--role", "interactive", "--expected-record", exact["record_sha256"], success=False)["state"] == "STALE_REGISTRY"
        assert cli("verify", "--role", "interactive")["record_sha256"] == rotated["record_sha256"]

        # A separate daemon server never replaces the interactive default.
        daemon_socket = str(Path(socket).parent / "daemon.sock")
        def daemon(*args):
            return run_argv(["tmux", "-f", "/dev/null", "-S", daemon_socket, *args])
        try:
            assert daemon("new-session", "-d", "-s", "daemon", "exec sleep 120").returncode == 0
            daemon_pid = int(daemon("display-message", "-p", "-t", "=daemon:", "#{pane_pid}").stdout)
            endpoint = cli("adopt", "--job-id", "123", "--control-socket", daemon_socket,
                           "--control-session", "daemon", "--role", "daemon", "--role-pid", str(daemon_pid),
                           "--generation", "daemon-1", "--expected-current", "none")
            assert cli("resolve", "--role", "daemon")["record_sha256"] == endpoint["record_sha256"]
            assert cli("resolve")["record_sha256"] == rotated["record_sha256"]
        finally:
            daemon("kill-server")
