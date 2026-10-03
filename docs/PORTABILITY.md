# Portability boundaries

Keep four concerns separate:

| Layer | Responsibility |
| --- | --- |
| Core | Task lifecycle, leases, revisions and decision evidence |
| Adapters | Filesystem durability, process/transport identity, scheduler observations |
| Project harness | Methods, acceptance criteria, scientific checks and governance |
| Deployment | Installation paths, environment, accounts and supported host policy |

The repository must not contain a particular user's mount paths, live farm state,
campaign instructions or credentials. Filled profiles belong outside it.
The [templates](../templates/BRIEF.md) are neutral starting points, not automatically
loaded configuration. Actor/outcome strings are project metadata, not authentication.

## Supported capabilities

- Read-only task inspection and model/CLI imports do not require POSIX locks.
- Durable writes currently require the POSIX backend: cooperative flock, atomic
  replacement, file fsync and directory fsync. Unsupported backends fail closed.
- Real process observation requires Linux host/boot/process-namespace/PID/start-time identity.
  Codex-tmux additionally requires bash and tmux. This is not a Windows executor.
- A different host, missing identity, inaccessible process information, or a
  dispatch without a matching invocation ID is UNKNOWN, not a dead worker.
- Network filesystems require site validation. Lock acquisition has a bounded
  retry window, but Python cannot guarantee a deadline for a kernel-stalled NFS
  syscall. Host checks and receipt leases are not distributed filesystem fencing.
- Independent users/projects can use separate installations and farms. Shared
  writes by multiple OS users need suitable permissions and a verified backend;
  this release does not provide tenant isolation or change ACLs.

The CI portability matrix covers inspection/contracts on Linux/macOS/Windows,
not full native execution equivalence. A configured matrix is not a completed CI run.

## Version and deployment policy

Protocol 4 adds durable planned drain/release/claim and optional receipt
`rotation_id`/`checkpoint` fields. The lifecycle is unchanged. The version bump is
intentional: protocol 3 writers ignore drain and must reject the new manifest.
All writers must use the fixed new build before enabling turnover; protocol
checks cannot fence direct JSON edits or an already-running old binary.
Upgrade at a stopped-writer boundary.
Legacy worker identities remain unverified; never fabricate their host/boot or
invocation fields to make a health check pass.

`--writer-policy compatible` checks protocol when a daemon manifest exists and
permits offline operation without one. It does not prove that no old writer exists.
`--writer-policy pinned-host` additionally requires exact source and host matching
for an existing farm. Installation path and Python path are diagnostic, not universal
identity requirements. Source hashes use relative file names and survive relocation.
Neither policy authorizes cross-host worker takeover.
Reconciler startup applies the same policy before replacing its manifest. Its
explicit `--upgrade-from-source` option permits only a reviewed same-host,
same-protocol source change, matched to the prior digest; it is not a protocol
migration or general override. See the stopped-writer procedure in
[operations](OPERATIONS.md#execution-failure-and-upgrade).

Offline `recover` additionally provides an explicit protocol 2/3/4 stopped-farm
migration. It archives operator-supplied evidence and retires old leases into the
existing BLOCKED state; it does not import legacy executor identities or resume a
remote session. Source/host/boot matching applies to an interrupted recovery's
target environment. Shutdown proof, scheduler commands, access policy and approvals
remain deployment responsibilities, not hardcoded cluster/user conventions.
See [offline recovery](OPERATIONS.md#offline-hostprotocol-recovery).

Planned handoff requires a shared durable farm root at the same resolved path,
accessible workspaces/inputs, a different exact target hostname, and the same
runtime source/protocol and executor configuration. Task mutations and dispatch
are serialized with drain; released farms reject ordinary writes. Claim binds the
destination and a single-use execution epoch. These are cooperative controls, not
multi-host consensus or fencing of arbitrary external dispatchers. Provisioning,
node-expiry policy and scientific job ownership remain outside the runtime.

[Control access schemas 1 and 2](ACCESS_PROTOCOL.md) are independent of runtime
protocol 4. Schema 1 binds a separate Master tmux endpoint to the execution epoch
and a completed daemon tick; schema 2 adds immutable endpoint generations and
explicit process-role adoption. Publication and verification require Linux process identity,
an exact Slurm allocation, launcher-attested scheduler node, owned tmux socket
and the deployed source. The runtime hostname may be an FQDN while Slurm's
NodeName is short; these are separate exact facts, never normalized or guessed.
The access commands support tmux 2.7 without `-N`; observations and the returned
attach wrapper cannot start a server. A target remains bound to one farm/root.
Registry and farm storage must be persistent/shared at identical canonical paths;
the UID must be consistent across execution nodes. A storage attestation is
required; a path name cannot establish network filesystem durability. Site
validation may proceed in stages on the actual farm under the Owner's authorization,
with cross-node checks at planned turnover; a separate disposable canary is optional.
See the [staged validation procedure](ACCESS_PROTOCOL.md#deployment-and-staged-validation).
Read-only resolution uses no locks or live remote probes and may run on a host
that can read those paths. The [portable connection client](CONNECT.md) uses native
OpenSSH from an ordinary terminal; remote execution still requires Linux/tmux.
The access feature has no native Mac/Windows execution or real-site validation
claim. It never changes task compatibility or treats an unreachable node as dead.
Cross-cluster or cross-storage relocation requires explicit migration/recovery
and destination registry provisioning; access resolution cannot perform it.

Prompt size is an executor constraint, not a scientific rule or core decision limit.
Codex-tmux's default transport ceiling is 98304 UTF-8 bytes, configurable through
`FARM_CODEX_MAX_PROMPT_BYTES`; it is a ceiling, not a target. Do not raise it beyond
the platform's actual command limit. See [operations](OPERATIONS.md).

## Worker configuration and application state

Set `model` and `effort` under `[executor.codex]` or `[executor.claude]` in the
site profile to extend the shipped agent command without copying it. The release
renderer exports `FARM_CODEX_MODEL`/`FARM_CODEX_EFFORT` (or `FARM_CLAUDE_*`), and
the adapter appends CLI arguments on launch and resume. `cmd` remains an optional
complete command override; avoid duplicating model/effort flags there when using
the separate fields. A model and effort must be supported by the installed CLI
and account. Claude's inherited `CLAUDE_CODE_EFFORT_LEVEL`, if set, takes precedence
over its `--effort` flag; clear it in the worker prelude when pinning effort this
way. See [Claude's CLI reference](https://code.claude.com/docs/en/cli-reference)
and [environment precedence](https://code.claude.com/docs/en/env-vars).

Use `[farm_defaults]` for `executor`, `interval`, `grace_seconds`,
`max_auto_restarts`, and optional `log`. The older sample's `[worker]` model,
effort, and rotation fields were inert; they never configured the runtime.
Move model/effort pins to the appropriate executor table. Rotation remains an
explicit runtime control, not a site-profile threshold.

`[executor.codex] home = "/local/path/to/farm-codex-home"` exports
`FARM_CODEX_HOME` from the deployment script and `CODEX_HOME` inside each worker.
Provision a dedicated directory before starting workers. Codex keeps configuration
and local state under `CODEX_HOME`, defaulting to the interactive user's
`~/.codex`; see [OpenAI's configuration documentation](https://learn.chatgpt.com/docs/config-file/config-advanced).
A separate worker home avoids inheriting unrelated interactive configuration and
keeps worker state separate from the owner's review sessions.

For file-based authentication, perform a separate one-time login in that home,
for example `CODEX_HOME=/local/path/to/farm-codex-home codex login --device-auth`.
Follow the installed CLI's login flow if device login is unavailable. Do not
symlink the interactive `auth.json` or maintain parallel copies of that active
login cache for workers: token refresh updates cached credentials, and this
deployment needs independently managed worker authentication. Official guidance
does allow copying a cache as a headless-login fallback; it does not establish
that concurrently refreshed copies are independent credentials. Credentials may
instead use an OS store, subject to managed settings; choosing `CODEX_HOME` alone
does not prove credential isolation. See [OpenAI's authentication documentation](https://learn.chatgpt.com/docs/auth).
The runtime neither provisions credentials nor copies application state.

Use a local filesystem for Codex's SQLite state. SQLite WAL requires same-host
shared memory and does not support network filesystems; merely moving a home
to a farm directory on NFS does not fix that storage constraint. See
[SQLite's WAL requirements](https://www.sqlite.org/wal.html).
Plan node turnover explicitly: local application session state does not move
with a shared farm root. Preserve required session state through a verified
site procedure or use fresh-session checkpoints when resuming on a new node.

`[executor.claude] home` similarly selects `CLAUDE_CONFIG_DIR`; see
[Claude's environment variables](https://code.claude.com/docs/en/env-vars).
Both adapters export `FARM_WORKER_ID`, `FARM_TASK_ID`, `FARM_LEASE_ID`,
`FARM_RECEIPT_PATH`, and `FARM_TASK_PATH` before the application home and
`path_prelude`. A prelude can therefore use the worker identity to select caches
or override a per-farm application home. `home` is a literal path; shell expansion
belongs in `path_prelude`. Codex and Claude preludes are independent.

## Reconciler diagnostics

Generated startup scripts pass `--log <farm>/reconciler.log`; override it with
`[farm_defaults] log`. The daemon mirrors Python stdout/stderr to that file and
records the resolved path as `reconcile_log` in `deployment.json` and status.
Log rotation uses the standard library: the first write after UTC midnight
rotates the file and keeps 14 dated backups. Tick JSON lines include an ISO UTC
`ts` field. Diagnostics are not task authority and do not replace the event log.
Logging begins after the daemon lock is acquired, so wrapper activation failures
and a duplicate daemon's lock refusal remain terminal diagnostics.

The startup script reports `RECONCILER_EXITED` and preserves the exit code. Run
it from an existing tmux shell to return to that shell after exit. A tmux window
whose initial command is the script can still close when the script finishes;
the persisted log remains available, or the operator can set tmux `remain-on-exit`.

The reduction pass removes three unused Python symbols: `adapters.tmux.TmuxAdapter`,
`adapters.base.ComputeObserver`, and `transitions.with_metadata`. They had no
in-repository callers; external imports must be checked before upgrading. CLI
commands, persisted schemas and the frozen lifecycle are unchanged by this pass.
