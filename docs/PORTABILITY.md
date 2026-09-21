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

[Control access schema 1](ACCESS_PROTOCOL.md) is additive to runtime protocol 4.
It binds a separate Master tmux endpoint to the existing execution epoch and a
completed daemon tick. Publication and verification require Linux process identity,
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
that can read those paths. Local SSH/Ghostty setup belongs to a later client.
The access feature has no native Mac/Windows execution or real-site validation
claim. It never changes task compatibility or treats an unreachable node as dead.
Cross-cluster or cross-storage relocation requires explicit migration/recovery
and destination registry provisioning; access resolution cannot perform it.

Prompt size is an executor constraint, not a scientific rule or core decision limit.
Codex-tmux's default transport ceiling is 98304 UTF-8 bytes, configurable through
`FARM_CODEX_MAX_PROMPT_BYTES`; it is a ceiling, not a target. Do not raise it beyond
the platform's actual command limit. See [operations](OPERATIONS.md).

The reduction pass removes three unused Python symbols: `adapters.tmux.TmuxAdapter`,
`adapters.base.ComputeObserver`, and `transitions.with_metadata`. They had no
in-repository callers; external imports must be checked before upgrading. CLI
commands, persisted schemas and the frozen lifecycle are unchanged by this pass.
