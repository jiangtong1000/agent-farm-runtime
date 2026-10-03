# Runtime architecture

This is the current maintainer contract for a single-owner research farm. Use
[operations](OPERATIONS.md) for commands and recovery procedures. Runtime
task/receipt protocol **4** and interactive access schema **2** are separate
versioned contracts; see [compatibility](COMPATIBILITY.md).

## Components and authority

| Component | Responsibility |
|---|---|
| Task Store | Authoritative current task state, execution lease and recorded decisions. |
| Worker Registry | Observed executor state; repaired from task ownership and process observations. |
| Event log | Append-only audit history; current task state is not reconstructed from it. |
| Reconciler | Mechanical dispatch, receipt handling, wait observation and recovery. |
| Master and reviewer | Task contracts, investigation, scientific decisions and acceptance under the Owner's authority. |
| farmkit | Per-workspace attempts, code snapshots, submission intents, verification and publication. |
| farmboard | Views of runtime state and workspace evidence; actions use the runtime CLI. |

The runtime owns task execution; farmkit owns the record of computational
attempts within a task. Neither a board view nor an agent conversation is another
task database. External Slurm jobs run independently of the orchestration
processes. Interactive Master sessions are explicitly adopted access endpoints,
not a process the reconciler must create to keep the farm alive.

## Invariants

These identifiers are retained for source comments and regression discussions.

- **INV-1 — Durable authority.** Task records hold current lifecycle state and
  ownership. Session memory, worker observations and audit events cannot override
  them. Project artifacts and farmkit attempts have their own documented roles.
- **INV-2 — Reconstructable orchestration.** A replacement Master or reconciler
  reads durable state and resumes inspection. Pending commits and uncertain
  external effects must be reconciled; restarting does not justify repeating an
  action whose outcome is unknown.
- **INV-3 — Independent compute.** Losing a Master, worker session or reconciler
  does not by itself cancel external computation. Scheduler references and
  submission intents must survive so subsequent execution can inspect that work.
- **INV-4 — Explicit state.** Control uses declared state and observed conditions,
  not file modification times or inferred message consumption. Clocks may govern
  grace periods, observation freshness and timeouts; silence does not prove death.
- **INV-5 — Fenced ownership.** A task has at most one current execution lease.
  Receipts and execution-scoped transitions must match that lease. The Worker
  Registry does not grant execution ownership.
- **INV-6 — Serialized reconciliation.** Supported daemon entrypoints acquire the
  exclusive reconciler lock. Task writers additionally share a short mutation
  lock, including dispatch admission. These are cooperative filesystem locks,
  not a distributed fencing service.
- **INV-7 — Validated mutations.** Supported task writers use transition validation
  and the Task Store commit boundary for lifecycle and ownership changes.
  Acceptance requires recorded evidence. Workers request changes through receipts;
  agents must not edit authoritative JSON directly.
- **INV-8 — Recoverable audit.** Each committed task mutation carries a unique
  event ID. Recovery completes its state/audit transaction without appending the
  same event again. A damaged or conflicting journal requires inspection rather
  than reconstructing guessed state from the log.
- **INV-9 — Persist before dispatch.** Ownership and issued-worker identity are
  recorded before external launch. Recovery never blindly replays the dispatch
  callback. A failed invocation may already have acted and remains subject to
  positive observation and the executor's invocation guards.
- **INV-10 — Positive death before replacement.** Automatic replacement needs a
  positive dead observation and the grace gate, with no applicable receipt taking
  precedence. UNKNOWN identity or an uncertain dispatch remains unresolved even
  after grace. Automatic restarts have a bounded budget.
- **INV-11 — Stable process identity.** Liveness and signalling use recorded
  process identity, including PID start time and the relevant host context.
  A bare PID, a recycled PID, a zombie or a workspace-wide process-name match is
  insufficient. Resume must track the current invocation.
- **INV-12 — Exclusive mutable workspaces.** Within one farm, READY, RUNNING,
  WAITING and SUBMITTED tasks reserve their canonical workspace path. Path aliases
  do not permit overlapping reservations. This does not fence external programs
  or detect reservations in another farm.

## Task lifecycle

State describes lifecycle; metadata describes the wait, evidence and reason.

| State | Meaning | Legal next states |
|---|---|---|
| READY | Eligible for execution. | RUNNING, FAILED |
| RUNNING | Leased execution is active. | WAITING, SUBMITTED, BLOCKED, FAILED |
| WAITING | A named normal-path condition must clear. | RUNNING, BLOCKED, FAILED |
| SUBMITTED | Final deliverable awaits acceptance. | RUNNING, DONE, FAILED |
| DONE | Recorded acceptance passed. | None |
| BLOCKED | Intervention is needed to establish how to continue. | READY, RUNNING, FAILED |
| FAILED | Terminal failure. | None |

The transition graph is necessary but does not alone authorize an operation.
CLI decisions add revision, state and evidence guards. For example, a ruling
resumes an eligible WAITING task; it releases a BLOCKED task only when that task
is explicitly held by offline recovery. Acceptance requires SUBMITTED. Rework
requests execution from SUBMITTED; DONE and FAILED are terminal.

`waiting_on` identifies a condition such as `job:<id>`, `task:<id>`,
`artifact:<path>` or `ruling:<name>`. Job submission is RUNNING → WAITING, not
SUBMITTED. The job condition is satisfied only by a positive terminal scheduler
observation. An explicit resume request or supported legacy master-note trigger
can also wake a worker to re-evaluate without proving that the job finished.
Status exposes active waits only for WAITING tasks and retained history as
`last_wait` for other states. Receipt status `AWAITING` maps to task WAITING;
workers cannot report DONE.

## Persistence and external effects

Task updates compare the expected snapshot, enforce workspace reservations,
advance the revision and prepare one state/event journal under the mutation lock.
Recovery installs the after-image and its audit event before clearing the journal.
The pending journal is a recovery mechanism, not a second source of task state.
Read-only inspection does not finish pending writes. See
[`store.py`](../src/agent_farm_runtime/store.py) and
[`transitions.py`](../src/agent_farm_runtime/transitions.py).

Dispatch follows the durable commit while admission remains serialized with
drain. Invocation identity and guards constrain repeat launches, but filesystem
transactions cannot make an external command exactly once. A timeout or lost
response may leave a real process or Slurm job running. Preserve the recorded
intent and inspect before retrying; UNKNOWN is not a license to launch or signal.

farmkit records submission intent before `sbatch`, associates attempts with job
identity, and verifies completed execution before publishing declared outputs.
Attempts execute from their code snapshot; only files selected by `snapshot`
are frozen. Linked inputs and other workspace entries may remain mutable.
Publication must preserve the previous verified result through failed attempts
and interrupted copies. Chain successors read producer-bound run-directory
outputs; the workspace may still contain the earlier published result. The
[worker contract](../skills/farm-execution.md) defines these path semantics.

## Deployment and access

Writer policy checks source, protocol and host provenance. Ordinary restart,
same-host source upgrade, planned host turnover and offline recovery are different
operations with different evidence requirements. An unreachable old host is not
proof that its writers stopped. Preserve locks and state; never remove a held lock
or invent process identity to force progress. Procedures belong in
[operations](OPERATIONS.md), rather than a second implementation plan here.

Execution epochs fence deployment changes. Access generations rotate interactive
or daemon endpoints independently within an epoch, using immutable records and
compare-and-swap publication. Role selection, explicit tmux sockets and fresh
identity verification constrain attachment. Role names do not create OS-level
authorization, and a verified attachment is a point-in-time observation. See the
[access protocol](ACCESS_PROTOCOL.md) and [connection guide](CONNECT.md).

The supported model assumes cooperative writers and filesystem semantics verified
for the deployment. It does not protect against arbitrary manual JSON edits,
provide multi-user authorization or guarantee cross-node lock exclusion from a
single-host probe. Shared storage and actual scheduler turnover need site checks;
see [portability](PORTABILITY.md).

## Maintaining the contract

Trace a proposed change to the failure it addresses and preserve regression
coverage at that boundary. Lifecycle, stale leases, crashes, uncertain executor
effects, turnover and artifact publication need behavioral evidence; matching a
private helper's implementation is insufficient. Current regression rationale is
recorded in [lessons](LESSONS.md).

Prefer existing helpers, the standard library and native platform facilities.
Keep scientific acceptance outside mechanical reconciliation. A schema change,
new writer model or new authority boundary requires an explicit compatibility
decision; adding another agent does not require another authoritative state store.
