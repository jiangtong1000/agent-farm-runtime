"""Opt-in native SSH transport using private loopback sshd and synthetic protocol responses.

No existing SSH configuration, keys, known_hosts, remote hosts or tmux servers are
used. Public access/tmux behavior is exercised separately by the lifecycle canary.
"""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time

import pytest

from test_access import farm  # noqa: F401
from test_access_roles import adopted  # noqa: F401
from test_connect import endpoint  # noqa: F401


SSHD = shutil.which("sshd") or ("/usr/sbin/sshd" if Path("/usr/sbin/sshd").is_file() else None)
pytestmark = pytest.mark.skipif(
    os.name != "posix" or os.environ.get("FARM_RUN_SSH_CANARY") != "1"
    or not SSHD or not shutil.which("ssh") or not shutil.which("ssh-keygen"),
    reason="requires opt-in disposable native SSH canary and OpenSSH client/server",
)


@pytest.fixture
def private_ssh(tmp_path, endpoint):
    import pwd

    root = tmp_path / "ssh"
    root.mkdir(mode=0o700)
    for name in ("host", "client", "other-host"):
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(root / name)],
                       check=True, timeout=10)
    (root / "authorized_keys").write_bytes((root / "client.pub").read_bytes())
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    server_config = root / "sshd_config"
    server_config.write_text("\n".join([
        f"Port {port}", "ListenAddress 127.0.0.1", f"HostKey {root / 'host'}",
        f"PidFile {root / 'pid'}", f"AuthorizedKeysFile {root / 'authorized_keys'}",
        "StrictModes no", "UsePAM no", "PasswordAuthentication no", "KbdInteractiveAuthentication no",
        "PubkeyAuthentication yes", "PermitRootLogin yes", "PrintMotd no", "LogLevel ERROR",
    ]) + "\n")
    public_key = " ".join((root / "host.pub").read_text().split()[:2])
    known = root / "known_hosts"
    known.write_text(f"login-key {public_key}\ncompute-key {public_key}\n")
    config = root / "client_config"
    config.write_text("\n".join([
        "Host login-canary", "  HostKeyAlias login-key",
        "Host node-a.example", "  HostKeyAlias compute-key",
        "Host *", "  HostName 127.0.0.1", f"  Port {port}",
        f"  User {pwd.getpwuid(os.getuid()).pw_name}", f"  IdentityFile {root / 'client'}",
        "  IdentitiesOnly yes", "  IdentityAgent none", f"  UserKnownHostsFile {known}",
        "  GlobalKnownHostsFile /dev/null", "  PasswordAuthentication no",
        "  KbdInteractiveAuthentication no", "  ConnectTimeout 5", "  LogLevel ERROR",
    ]) + "\n")
    access_farm, original_profile, published = endpoint
    responses = root / "responses.json"
    responses.write_text(json.dumps({"resolve": access_farm.access.resolve("cluster/study-a", role="interactive"),
                                     "verify": published}))
    visits = root / "visits"
    wrapper = root / "remote-wrapper"
    wrapper.write_text(f"#!{sys.executable}\n"
                       "import json, os, pathlib, sys\n"
                       "action = next(a for a in sys.argv if a in ('resolve', 'verify', 'connect-attach'))\n"
                       f"with open({str(visits)!r}, 'a') as handle: handle.write(action + '\\n')\n"
                       "if action == 'connect-attach':\n"
                       "    assert os.isatty(0), 'native SSH did not allocate the requested terminal'\n"
                       "    print('PRIVATE_SSH_ATTACHED')\n"
                       "else:\n"
                       f"    data = json.loads(pathlib.Path({str(responses)!r}).read_text())\n"
                       "    print(json.dumps(data[action]))\n")
    wrapper.chmod(0o700)
    profile = {key: value for key, value in original_profile.items()
               if key not in {"jump_hosts", "control_path", "control_persist"}}
    profile.update(login_host="login-canary", ssh_config=str(config), farm_wrapper=str(wrapper))
    client_config = root / "connect.toml"
    client_config.write_text('version = 1\n[targets."cluster/study-a"]\n' +
                            "\n".join(f"{key} = {json.dumps(value)}" for key, value in profile.items()) + "\n")
    with (root / "server.log").open("w+") as log:
        server = subprocess.Popen([SSHD, "-D", "-e", "-f", str(server_config)], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 5
            while server.poll() is None and time.monotonic() < deadline:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.05)
            assert server.poll() is None, (root / "server.log").read_text()
            yield root, client_config, visits
        finally:
            server.terminate()
            server.wait(timeout=5)


def run_client(config):
    import pty

    master, terminal = pty.openpty()
    try:
        return subprocess.run([sys.executable, "-m", "agent_farm_runtime.cli", "connect", "cluster/study-a",
                               "--config", str(config)], stdin=terminal, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True, timeout=30)
    finally:
        os.close(master)
        os.close(terminal)


def test_native_ssh_cli_resolves_jumps_verifies_and_reconnects(private_ssh):
    _, config, visits = private_ssh
    for _ in range(2):
        proc = run_client(config)
        assert proc.returncode == 0, proc.stderr
        assert "PRIVATE_SSH_ATTACHED" in proc.stdout
    assert visits.read_text().splitlines() == ["resolve", "verify", "connect-attach"] * 2


@pytest.mark.parametrize("failure", ["unknown-login", "unknown-compute", "changed-compute"])
def test_native_ssh_rejects_untrusted_keys_before_attachment(private_ssh, failure):
    root, config, visits = private_ssh
    known = root / "known_hosts"
    lines = known.read_text().splitlines()
    if failure == "unknown-login":
        known.write_text(lines[1] + "\n")
    elif failure == "unknown-compute":
        known.write_text(lines[0] + "\n")
    else:
        other_key = " ".join((root / "other-host.pub").read_text().split()[:2])
        known.write_text(lines[0] + f"\ncompute-key {other_key}\n")
    before = known.read_bytes()
    proc = run_client(config)
    assert proc.returncode != 0
    assert "PRIVATE_SSH_ATTACHED" not in proc.stdout
    observed = visits.read_text().splitlines() if visits.exists() else []
    assert observed == ([] if failure == "unknown-login" else ["resolve"])
    assert known.read_bytes() == before
