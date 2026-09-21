# Runtime operations

## Owner interaction

The Owner uses natural language with the Master agent. The Master translates the
authorized request into the commands below, including site setup, release export,
daemon startup, supervision, stop/restart and task decisions. "Owner-authorized"
describes who decides; it does not require the Owner to type shell commands.
The Master executes within the agreed scope and asks for decisions in conversation
when that scope must change. An independent reviewer reports to the Owner; the
Owner communicates acceptance, rework or the next research round to the Master.

Commands in this document are the Master's execution reference. The runtime does
not host the interactive Master or grant authority based on an actor name. See the
[Master entrypoint](../templates/RUNTIME_MASTER.md).

Use one installed, fixed runtime version per deployment. Keep project/site paths
outside this repository. Read [portability](PORTABILITY.md) before selecting a backend.

## Inspect first

```bash
farm --project /path/to/project version
farm --project /path/to/project status
farm --project /path/to/project task-list
farm --project /path/to/project task-show T-example --summary
farm --project /path/to/project doctor
```

These commands are read-only; they do not start workers or recover transactions.
`status` reports lifecycle counts, not health or scientific progress. `doctor`
checks durable records and startup provenance, not current daemon liveness.
Use process/log evidence separately. Observed heartbeat is not proof of useful work.

`task-show --summary` omits large brief/context/evidence bodies and may truncate
previews. Plain `task-show` retains its full JSON output for compatibility. Read the
full effective contract and evidence before accepting or changing a task. The summary
is a view of Task Store, not another database. Keep historical logs on disk and fetch
only relevant portions; do not repeatedly feed every task and ledger into a model.

## Task and decision contracts

```bash
farm --project /path/to/project task-create --id T-example \
  --objective '...' --deliverable '...' --acceptance 'Project-defined checks' \
  --workspace /path/to/workspace --brief-file /path/to/brief.md \
  --context-file /path/to/selected-method.md
```

Start with the short [brief](../templates/BRIEF.md) and optional
[workspace entrypoint](../templates/WORKSPACE_AGENTS.md). Only select needed context.
Selected text/hashes are snapshotted, not executable tools or dependencies; pin those
separately. Each evidence/context file is nonempty UTF-8, at most 64 KiB; selected
context totals at most 128 KiB. Backend transport may impose a smaller limit.
For codex-tmux the default limit is 96 KiB for the complete rendered prompt,
including brief, context, effective contract and wrapper overhead. There is no
universal 88 KiB allowance; select short method excerpts and leave headroom.
Oversize preflight errors report actual rendered UTF-8 bytes and the backend limit.

READY/RUNNING/WAITING/SUBMITTED reserve canonical workspace paths within a farm.
Use distinct mutable workspaces; aliases count as the same directory. This does not
fence external programs or detect conflicts between separate farms.

Read the current revision, then record a short evidence note and one decision:

```bash
farm --project /path/to/project task-accept T-example --expected-revision 3 \
  --actor reviewer-1 --evidence-file /path/to/review.md --outcome approved
farm --project /path/to/project task-ruling T-example --expected-revision 3 \
  --actor reviewer-1 --evidence-file /path/to/instruction.md
farm --project /path/to/project task-amend T-example --expected-revision 3 \
  --actor reviewer-1 --evidence-file /path/to/reason.md --acceptance-file /path/to/criteria.md
farm --project /path/to/project task-rework T-example --expected-revision 3 \
  --actor reviewer-1 --evidence-file /path/to/rework.md
```

These are independent examples, not a sequence. Acceptance requires SUBMITTED;
DONE is terminal. Outcome labels are project-defined, default `accepted`. Ruling
requests a WAITING task's resume; rework requests new execution of a SUBMITTED task.
A ruling can also release a BLOCKED task explicitly held by offline recovery (below),
moving it to READY for fresh execution. It does not release unrelated BLOCKED tasks.
Amendments require a non-running, non-terminal boundary and retain the old contract.
The runtime records decisions, never decides scientific validity or grants permission.
Stale revision means reread/review, not a blind retry. Do not hand-edit task JSON or
append an acceptance event separately. The next supported writer recovers a pending
state/event transaction; damaged records fail closed and must be preserved.

## Execution, failure and upgrade

`reconcile` actuates, even without `--loop`; never use it as a read-only probe.
Select the executor and preserve the deployment's session/socket/settings explicitly.
Codex-tmux uses `FARM_CODEX_CMD` and `FARM_CODEX_PATH_PRELUDE`; no site environment is
loaded implicitly. The shipped command includes `danger-full-access`: it is not a
security boundary; operators must deliberately choose appropriate sandbox/OS limits.
Codex resumes an exact saved session, not an arbitrary latest session
([CLI reference](https://learn.chatgpt.com/docs/non-interactive-mode)).

Tmux commands have a finite timeout (`FARM_TMUX_TIMEOUT_SECONDS`, default 10 s) and
checked results. A failed command might already have acted. Pending invocation
identity survives restart, and each generated invocation has a one-shot guard.
Missing/stale PID identity or foreign-host state stays UNKNOWN regardless of grace;
the runtime does not launch a replacement, signal an unverified pane, or delete locks.
Previous receipt content is excluded on resume until a new receipt is observed.
If an attempt-bound local PID is known but start-time capture lost a race with
process exit, an absent process is confirmed dead; a still-present process with
unknown start-time remains UNKNOWN. This does not make legacy identity-less
workers safe to adopt. The runtime receipt helper is refreshed at launch and at
a verified-dead resume boundary, not while a worker is alive or unverified.

Confirmed-dead workers without a usable receipt can be replaced after grace, at most
`--max-auto-restarts` times per task (default 3). The limit parks the task on
`ruling:runtime-restart-limit`. Only an explicit recorded ruling resets that budget;
an unrelated MASTER file cannot. Rework starts a fresh budget. Review the cause first.
Normal waits do not consume this failure-restart budget. `--no-auto-unblock` also
disables explicit ruling resumes until the operator deliberately enables them.

Before upgrade: pause decisions, record exact process/host/executor/options, preserve
current code and recoverable state, and stop only verified runtime writers. Retain
workers, artifacts and external compute. Confirm old writers have exited and locks
are released; a scheduler CANCELLED label alone is not a cross-host fencing proof.
Never rename a held lock to obtain a second lock inode. If old-host/process ownership
cannot be established, remain stopped and request operator/site assistance.

All CLI task writers, including reconciler startup, enforce `--writer-policy`.
Startup checks happen under the single-reconciler lock, before replacing the
deployment manifest or initializing the executor. A normal pinned-host restart
requires the recorded source, protocol and host to match.
This refers to where the CLI executes, not where the master's chat lives. A remote
master must execute writes in the farm-node environment through the deployment's
approved remote-execution mechanism; a login-node CLI is not the farm host.

For an operator-approved **same-host, same-protocol source upgrade**, preserve the
old manifest and pass `reconcile --upgrade-from-source OLD_SOURCE_SHA256` with
`--writer-policy pinned-host`, alongside the verified executor/session/options.
The value is the old manifest's `source_sha256`, not a Git commit. It must match
exactly; a missing manifest, different host/protocol, or pending task transaction
blocks the upgrade. Stop all task writers first and resolve pending transactions
with their original version; the flag is operator intent, not proof of shutdown.
The new startup manifest records `upgraded_from_source`. Preserve the upgrade
approval/evidence in deployment records; this manifest is not a historical ledger.
Remove the one-time flag for subsequent restarts. If startup fails after updating
the manifest, inspect state before retrying; do not blindly refresh the expected
digest. Never remove a manifest or weaken writer policy to force an upgrade.

This option does not migrate protocols or legacy worker identities. Use the separate
offline recovery procedure below when old execution cannot be safely resumed.
With the fixed installation and verified settings, compare `version` and `doctor`,
inspect actual process/log, then observe a normal receipt boundary.
Legacy identities or a runtime_error require explicit inspection, not invented
identity files or silent task re-creation. Keep rollback code/state recoverable;
never overwrite newer decisions with an old snapshot.

## Stop, restart, structured status (release refactor/farmkit-era)

```bash
farm --project /path/to/project status --json          # deployment identity, pid liveness, last tick, task counts, observed job waits
farm --project /path/to/project events --after 0       # audit events after a cursor; returns the next cursor as a string
farm --project /path/to/project stop --drain --actor m # withhold dispatch, ask lease holders to check out; --loop exits by itself
farm --project /path/to/project stop --now   --actor m # SIGTERM the daemon between two commits (holds task-mutation.lock)
farm --project /path/to/project restart --to /new/release/src --actor m [--plan-only]
```

`stop --drain` reuses the drain control; release/claim refuse it (it is not a node
handoff). `restart --to` computes the candidate's identity in a clean interpreter,
refuses when `src/agent_farm_runtime/protocol/` differs (retire the farm and start a
new one instead), stops the daemon and prints the exact `reconcile
--upgrade-from-source` command to run through the NEW release's wrapper. The daemon
writes `runtime/last_tick.json` after every pass; health checks read it, not the log.
In `--loop` mode only acting ticks and an hourly heartbeat are printed; the loop prints one
`drain_complete` line when a stop or drain lets it exit. A finished `stop` control is
cleared the next time a daemon starts on this farm (event `FARM_STOP_CLEARED`); a node
handoff control is never cleared that way.

Tasks may name their backend: `task-create --executor codex-tmux|claude-tmux|local-process`
(default: the farm's `--executor`). `--resume-mode fresh` starts a new agent session
on every wake instead of resuming the saved one; durable state then lives in the
workspace (attempts/ ledger, CHECKPOINT.md), which is what `farmkit tick` reads.

Worker-side execution (submit intent, job waits, verification, evidence) is the
`farmkit` package; master-side waiting is `farmkit watch`, health is `farmkit health`.
`farmkit watch` pages through the whole audit backlog before sleeping, so a page of
uninteresting events never hides a later one; the cursor it prints is
`<events cursor>[+<state hit id>]`, and `--ack` stores it as printed.
`stop --now` and `status --json` judge the daemon by host, boot id, pid namespace and
the pid's start time recorded at startup, never by the pid alone. A manifest written by
a release before this one has no start time: status reports the daemon as UNKNOWN and
`stop --now` refuses; stop that daemon by hand once, and the next start records its
identity. `status --json` reports the scheduler states the daemon itself saw on its last
pass (`observed_jobs[].source == "daemon"`) and queries Slurm only for jobs the daemon
has not reported or when the daemon is stale. `events --last N` reads the tail of the
audit log without scanning it; the board uses it.
See `skills/farm-execution.md` and `examples/five_arm_study/`.

## Human-facing control access

The Master publishes its own control tmux endpoint using `farm --project PROJECT
access publish`; deployment `session`/`tmux_socket` remain the worker executor's
endpoint. `farm access resolve` reads an explicitly configured shared registry;
`farm --project PROJECT access verify` checks the exact current endpoint on its
recorded host. Neither read command attaches, repairs state or chooses a fallback.

Each target permanently names one farm/root, for example `cluster/study-a` and
`cluster/study-b` for two farms. Node turnover preserves that binding. Movable
local aliases and cross-cluster/storage migration are outside remote resolution.
For access-enabled Slurm deployments, launch `reconcile` with `SLURM_JOB_ID`
(or `SLURM_JOBID`) and the local `SLURMD_NODENAME`, or the validated explicit
pair `--slurm-job-id ID --slurm-node NODE`. Startup records a scheduler attestation
with the exact node, allocation StartTime, UID, runtime host and epoch; inspect it
through `status --json`. An FQDN runtime host need not equal Slurm's NodeName.
Claim/recovery clear the previous attestation, so the destination must capture
fresh launcher inputs. Do not copy old environment values or edit manifest JSON.

Follow the [access runbook](ACCESS_PROTOCOL.md#master-runbook) for bootstrap and
planned turnover. Claim plus a RUNNING allocation is insufficient: the replacement
needs a live reconciler's completed tick, a separately created control session,
and verified publication before current changes. Use commands, never manual edits
to registry JSON or markers. Source upgrades and recreated control sessions can
invalidate immutable access records; plan the required epoch transition before
changing an access-enabled deployment.

## Board (farmboard)

`farmboard` is a read-only view built from `farm status --json`, `farm events`,
`task-show --summary` and each task's workspace (`attempts/` ledger, REVIEW.md,
CHECKPOINT.md, FAILURE.md). It never writes `.farm/`.

```bash
farmboard --project /path/to/farm --farm-wrapper /path/to/farm-wrapper          # Textual TUI
farmboard --project /path/to/farm --once                                          # one text screen
farmboard --project /path/to/farm --html /path/to/board.html                      # static page
farmkit board --project /path/to/farm --once                                      # same thing
```

The TUI needs the optional `board` extra (`pip install 'agent-farm-runtime[board]'`);
`--once` and `--html` work without it. Keys `r a w m t n` (ruling, accept, rework, amend,
rotate, new-from) build the exact `farm task-*` command with `--expected-revision`, open
`$EDITOR` on the evidence note, show the command and run it only after `y`. An action that
does not apply to the selected task says why. The "in-tok" column is the worker's last
model-call input tokens read from its codex session log; ▲ marks the rotation threshold.

## Planned context and node turnover

The owner may request this in chat; the master translates the request into the
following bounded operations under the project's standing authority. This is not
automatic node provisioning or context-size prediction. Put the procedure in the
master's short project entrypoint; the runtime does not make a chat model obey it.

**Master context only:** write a short project checkpoint with task IDs, holds,
next decisions and outstanding rotation/handoff IDs. The next master reads that
checkpoint, `task-show --summary` and `handoff status`, then selected evidence.
No drain, task ruling, worker restart or runtime memory service is needed.

**Worker context:** read its revision and submit one stable request ID:

```bash
farm --project /path/to/project task-rotate T-example --expected-revision 7 \
  --request-id context-001 --actor master-1
```

The running worker checks `python .farm_receipt.py control` at normal safe
boundaries. This reads the existing Task and returns only the lease-bound request
ID and checkpoint requirement, not the brief/history. The worker checkpoints
artifacts/hashes, external job IDs, exact holds/wait, checks/risks and next action,
then calls `AWAITING --rotation-id context-001 --checkpoint /path/to/checkpoint.md`
through the same helper and exits. Include the real `--waiting-on` condition if
there is one; omitting it means only a context boundary (`rotation:context-001`).
Check after each completed logical step and before starting the next long tool
call/job submission, not just at the end of an agent turn. Include this in the
task brief; a long turn may contain many such boundaries. Long tool calls may
still delay cooperation; there is no forced interrupt or guaranteed response time.

The reconciler snapshots the checkpoint (existing 64 KiB evidence limit), retains
the old lease through WAITING, and releases it only after positive process exit.
It records a certified leaseless WAITING task, then launches a fresh worker/lease
when eligible. Existing owner/job/artifact waits remain parked. Infrastructure
rotation never sets `resume_requested`, substitutes for a ruling, or spends/resets
the crash restart budget. An explicit restart-limit ruling still resets that budget.
`--no-auto-unblock` keeps even cleanly surrendered tasks parked until re-enabled.

For an already WAITING worker, reuse its last invocation's snapshotted checkpoint.
If absent, the master may add `--checkpoint-file /path/to/reviewed-checkpoint.md`
to the same request at the current revision. This does not wake the worker or lift
its wait. A previous invocation's checkpoint is not silently reused after resume.
New context receives the effective contract, latest instruction and short checkpoint,
not the entire previous conversation. Checkpoint adequacy remains harness judgment.
Local-process workers receive `FARM_TASK_PATH` and the same receipt fields; they
must implement cooperative checking/yielding in their own command.

**Farm/node:** first arrange the target node, fixed release, shared paths, environment
and permissions through the site workflow. Do not change runtime build mid-handoff.
The master then follows this checklist; these are internal commands, not an owner UI:

During drain, normal CAS/evidence-guarded master writes remain allowed until
release seals the farm. Read-only inspection and attaching a missing WAITING
checkpoint are the normal operations. Acceptance/amendment with independent
justification is permitted; avoid new tasks, rework and rulings solely to advance
handoff. Those decisions persist and can cause immediate execution after claim.
Drain is not scientific authorization, and it does not lift owner holds. Once
released, ordinary task writes are rejected until the destination claims the farm.

Dispatch holds the task-mutation lock through the external launch/resume call so
drain cannot race an admitted invocation. A launch may hold the lock for several
seconds; master writes can wait or reach the lock timeout. On contention, inspect
current state and retry the same intent; on revision conflict, reread/review first.
Do not bypass the lock or treat a timeout as proof that no dispatch occurred.

The old allocation must remain alive through successful release. Obtain the new
allocation and exact hostname first; allow overlap for worker checkpoints and
positive exit checks. A queued allocation request without an assigned node is not
yet a handoff target. Do not cancel the old allocation to make room for its successor.

1. On the old farm node: `farm --project /path/to/project handoff drain
   --request-id node-001 --target-host EXACT_DESTINATION_HOST --actor master-1`.
   This serializes with in-flight dispatch, persists a launch/resume/restart gate,
   and lets the reconciler request checkpoints from every remaining lease-holder.
   It still observes workers/receipts. It never cancels external compute.
2. Inspect `handoff status`, task summaries and normal tick errors. Supply missing
   WAITING checkpoints without issuing owner rulings. Once leases are surrendered,
   `reconcile --loop` exits normally. An UNKNOWN worker or missing clean receipt
   remains a blocker; do not fabricate identity or remove locks to force turnover.
3. On the old node: `farm --project /path/to/project handoff release
   --request-id node-001 --actor master-1`. Under both existing locks it verifies
   all issued worker invocations, including unleased SUBMITTED/DONE workers and
   uncertain queued dispatches. Live/UNKNOWN refuses release. Retry after positive
   exit. Successful release seals task revisions/checkpoints and deployment in
   `runtime/handoffs/node-001.json`; subsequent ordinary writes remain disabled.
4. On the target node: `farm --project /path/to/project handoff claim
   --request-id node-001 --actor master-2`. It validates the exact target, same
   source/protocol, archive hash and unchanged task snapshots under both locks,
   consumes this handoff and transfers host ownership. It does not start workers.
   The old node need not remain accessible after a sealed release.
5. Start `reconcile` on the target with the unchanged reviewed executor settings;
   inspect its ticks and first receipts. READY tasks may launch immediately.
   Cleanly parked tasks get fresh context only when their condition/authorization
   permits; owner-held tasks need their normal ruling, not a blanket recovery ruling.

Repeated requests with the same ID are idempotent; a new master should inspect
and continue that request, not invent a second ID. Deployment changes embed a small
pending audit event in `deployment.json`; ordinary writes stay blocked until a retry
finishes its durable audit. Do not delete the marker. The per-task `dispatches`
metadata records every worker issued in the current execution epoch before dispatch;
the observed registry is not the release authority. Old attempts under a retained
worker ID must already be positively dead before resume, and dispatch is one-shot.
Runtime-owned metadata is not a project editing surface.

If the old plane disappears **before** a sealed release, this procedure cannot
claim success. Use offline recovery only with positive external shutdown/fencing
evidence. Inaccessibility by itself is not death. No new task lifecycle states,
scientific PASS/FAIL rules, scheduling service or automatic migration policy are added.

Release scope is frozen at protocol 4: fix one source revision before deployment
and do not keep changing it during cutover or turnover. After it passes validation, non-blocking
suggestions go to later maintenance rather than reopening this release. Reopen
only for concrete safety, durability, correctness or deployment-blocking defects.

## Offline host/protocol recovery

`recover` is an operator-mediated, stopped-farm procedure for known protocol 2/3/4
stores, not automatic failover. It supports a new deployment host/source and legacy
workers **without pretending UNKNOWN means dead**. No processes, sessions or jobs
are launched, probed, signalled or cancelled by this command.

1. Stop ALL old farm writers and workers, including orphan workers with no current
   lease, transport queues and keepers that could dispatch later. Preserve code,
   the complete farm state and workspace checkpoints. External scientific compute
   stays independent: inspect it, do not cancel it to satisfy a runtime recovery.
   Use site-specific positive shutdown/fencing evidence; connection failure or a
   scheduler terminal label alone is insufficient. If this cannot be established,
   stop here and seek operator/site assistance.
2. On the intended target host, using the fixed candidate installation, run the
   read-only preview. It creates no locks or files:

   ```bash
   farm --project /path/to/project recover
   ```

   Review its source/host/protocol, every lease to retire, unchanged tasks and plan
   SHA-256. The hash binds the full manifest, all task snapshots, farm root and target
   runtime identity (excluding the ephemeral CLI PID/namespace, since recovery
   does not execute workers). READY tasks remain READY;
   they can launch when a daemon is later started. Resolve any pre-existing pending
   task transaction with its original writer before beginning migration.
3. Write a short UTF-8 evidence note identifying who authorized recovery, exact old
   writers/worker identities, how all dispatch sources were stopped/fenced, evidence
   commands/results and checkpoint locations. On explicit operator approval:

   ```bash
   farm --project /path/to/project recover --apply --expected-plan REVIEWED_SHA256 \
     --actor operator-1 --evidence-file /path/to/shutdown-evidence.md --attest-stopped
   ```

   The flag is an **operator attestation**, not a machine-certified death test or
   authentication. The runtime enforces snapshot matching, both existing writer
   locks, durable evidence and audit. All participating writers must use this build;
   it cannot fence old binaries, direct JSON edits or an unreliable filesystem.
4. Leased RUNNING/WAITING tasks become BLOCKED with `metadata.recovery_hold`, retaining
   their contracts and original `waiting_on`. Their old leases are released via the
   existing transition/TaskStore boundary. Unleased tasks (including historical DONE
   and existing BLOCKED tasks) and all worker/session/receipt files are untouched.
   The archive `runtime/recoveries/<id>.json` holds the reviewed snapshots, target,
   operator and evidence. Per-task `RECOVERY_HELD` events and one `FARM_RECOVERED`
   event (`task_id="*"`, farm-scoped) retain the audit trail. The new manifest records
   `pid=null, started_at=null`: recovery completion is not daemon startup.
5. The master reviews each recovery-held task, preserves owner holds and reads its
   current revision. When continuation is actually authorized, use `task-ruling`
   with an instruction note specifying what to reuse, what to inspect, and what must
   not be recalculated/resubmitted. This permits the existing BLOCKED → READY →
   RUNNING path with a **new worker and lease**, not cross-host session resume. A
   formerly WAITING task may need a short new invocation to check its old condition
   and report AWAITING again. Recovery never bypasses the WAITING liveness guard.
   Restart the daemon separately with reviewed executor/session/options and owner
   authorization; no new ruling is implied merely by finishing migration.

   If an owner hold remains active, do not release that recovery-held task to READY
   merely to copy the hold into an instruction: READY is launchable. Already cleanly
   surrendered leaseless WAITING tasks remain unchanged by recovery.

An interrupted operation leaves `runtime/pending-recovery.json`. Ordinary task
writes, reconciler startup and reconciliation refuse to proceed. Preview shows the
original plan; rerun apply with that same hash, actor, evidence contents and target
installation/host/boot. Per-task recovery reuses the normal state/event journal;
retries do not duplicate revisions/events. Changed records or damaged audit fail
closed. Do not delete journals, substitute identities, or rerun an already completed
old plan against later decisions. Archives are intent/evidence records; completion
requires the matching manifest/event and no pending journal. An interrupted recovery
whose target host has also been lost needs separate reviewed operator handling;
this command deliberately does not add a recursive takeover bypass.

## Durability and interpretation

Audit appends and atomic receipt replacements synchronize the file and its
parent directory. Recovery atomically rewrites the task even when the new state
is already visible, and re-synchronizes an already-visible event before removing
its journal. Repeated task-directory sync failures keep the journal for retry.
A sync error propagates to the caller; already-visible
output is not rolled back and may still require inspection/recovery. These checks
require durably provisioned parent directories and a filesystem honoring fsync/rename;
process-crash tests do not certify power-loss durability on every storage system.

Job waits need positive terminal observations: scheduler errors/unknown states keep
waiting. Artifact existence is not validated content; task DONE is not a universal
scientific PASS. A wake only requests re-evaluation. Project methods, budgets, owner
holds and verification requirements belong in selected project context.
