# Connect from a terminal

`farm connect SITE/FARM` uses native OpenSSH and the published
[access protocol](ACCESS_PROTOCOL.md). It resolves the current endpoint on a login
host, verifies that record on its compute host, then asks the same pinned remote
wrapper to verify the same record again immediately before attaching tmux.
The default role is the explicitly adopted interactive Master. Use `--role daemon`
only to inspect an independently adopted daemon endpoint.

Install the package with Python 3.11 or newer on your local computer and make
native `ssh` available. The remote release must also provide `connect-attach`.
An ordinary terminal handles SSH password, key and MFA prompts; no Python SSH
library or site shell function is needed. Provision trusted host keys for every
login, jump and compute host through your normal site procedure before connecting.
Unknown or changed keys fail; the client never accepts them automatically.

Create `~/.config/agent-farm-runtime/connect.toml` locally:

```toml
version = 1

[targets."site-a/farm-a"]
login_host = "researcher@login.example.org"
registry = "/shared/researcher/access-registry"
farm_root = "/shared/researcher/farm-a/.farm"
farm_id = "farm-REPLACE_WITH_THE_VERIFIED_FARM_ID"
farm_wrapper = "/shared/researcher/releases/reviewed-release/farm"

# Optional jump hosts before the login host. Native SSH config supplies ports,
# identities and known-hosts policy; user@host:port is also accepted for jumps.
jump_hosts = ["gateway.example.org"]

# Optional; by default the client includes ~/.ssh/config when it exists.
ssh_config = "~/.ssh/config"

# Optional native SSH multiplexing; provision this private directory yourself.
control_path = "~/.ssh/control/farm-%C"
control_persist = "10m"
```

Copy the canonical farm root and matching `farm_id` from the deployment/access
record after checking its identity through a trusted channel. The configuration
pins this exact target to that farm and one reviewed wrapper path. It contains no
passwords, tokens, private keys or current compute-node name. Paths referring to
the remote farm use absolute POSIX syntax. Optional SSH configuration and control
paths are absolute paths on the local computer (a leading `~` is expanded).

The SSH chain is `jump_hosts → login_host` for resolution and
`jump_hosts → login_host → recorded compute host` for verification/attachment.
Configure the compute-node username in native SSH configuration when it differs
from your local username; the user in `login_host` applies only to that host.
The registry and farm root must be readable on the login host. The wrapper must
refer to the same reviewed runtime on login and compute hosts.

```bash
farm connect site-a/farm-a
farm connect site-a/farm-a --config /path/to/connect.toml
farm connect site-a/farm-a --role daemon
```

The client passes a temporary `-F` configuration with `StrictHostKeyChecking yes`
before including your normal SSH configuration. OpenSSH's first-set rule keeps
strict checking enabled; native ProxyJump subprocesses inherit this configuration.
Authentication remains interactive. Optional `ControlMaster auto`, `ControlPath`
and `ControlPersist` let OpenSSH reuse its own connections. This connection reuse
does not cache registry records or authorize attachment to an old compute node.

Before attachment the client checks the record schema, target, farm identity/root,
digest and process role. It reconstructs the exact attachment host and tmux argv
using the access protocol implementation and rejects any difference. The internal
`connect-attach` command repeats remote verification with the expected digest,
role, host and farm identity, then executes that same native argv. The version-2
tmux command also guards the server PID, native session ID and published session
markers before attaching, refusing a replaced or mismatched server/session. It does not
launch a Master, start tmux servers, publish records or mutate farm task state.

A changed publication, missing role, stopped allocation, changed host key or
failed attachment ends the invocation. Resolve again by rerunning `farm connect`
after addressing the reported problem. There is no retry against older records,
session-name search or substitute endpoint. As with the underlying protocol, a
process can exit or a publication can change after the final observation. No
turnover lock is held for the life of the terminal; verification remains a
point-in-time observation, and native attachment failure ends the attempt.

Tests in `tests/test_connect.py` combine simulated SSH transport with the real
access verifier and isolated synthetic records. They cover command quoting,
strict configuration and jump construction, role/farm/digest/argv rejection,
interactive stream inheritance and rotation between verification and attachment.
These tests make no live SSH connections or production changes. The separate
optional localhost SSH canary checks native transport against an isolated sshd;
it does not certify site MFA, routing or host-key provisioning.
