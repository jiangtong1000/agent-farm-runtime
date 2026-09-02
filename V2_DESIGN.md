# V2_DESIGN.md — durable-state-driven farm runtime (v0.3)

Status: CONTRACT ALIGNED TO IMPLEMENTATION. The minimal actuation layer is now
implemented in `src/agent_farm_runtime/` and validated by real-codex canaries
(launch, crash-adoption, resume; see §10). v0.2 was DESIGN ONLY; v0.3 pins the
representations that implementation settled and RATIFIES the actuation semantics
that v0.2 deferred (lease rotation, grace-based death detection, crash-consistent
ordering, the structured receipt primitive). The next phase is NOT further
feature development but running an isolated canary farm on this contract.

Derived from the empirical review of the 2026-08-31 → 09-02 farm operation:
every failure was in the ephemeral coordination layer (tmux, nudger, master
session memory); durable on-disk state and decoupled SLURM compute never failed.
V2 makes that separation deliberate: the coordination layer itself derives from
durable state, so any actor (reconciler, master, worker) is reconstructable and
replaceable.

Each section is marked **[FROZEN]** (semantics we commit to), **[PINNED v0.3]**
(a representation implementation settled) or **[PROVISIONAL]** (still free to
change without reopening the frozen contract). §10 maps every invariant and
mechanism to the code that realizes it and lists what remains deferred.

---

## 1. Core primitives [FROZEN]

- **Task Store** — the single authoritative current state. Not event-sourced;
  current state is read directly, never reconstructed by replaying events.
- **Worker Registry** — disposable executors: identity, session handle,
  heartbeat (liveness only), current lease, state.
- **Event Log** — append-only audit/history. Currently audit only, NOT an
  authority and NOT yet a signal bus.
- **Reconciler** — the single mechanical control loop. NO scientific judgment.
  Computes desired-vs-actual from durable state and acts, idempotently.
- **Claude Master** — decision function over durable state: task
  decomposition, priority, scientific decisions, acceptance, synthesis. Holds
  NO authoritative state in session memory; retires/hands off freely.

Role boundary [FROZEN]: mechanical reconciliation is not Claude's job — it is
a stateless script. Claude Master is invoked only for the decision subset.

---

## 2. Invariants [FROZEN]

Each is anchored to a real failure mode from the reviewed operation.

- **INV-1 Durable authority.** The Task Store is the single authoritative
  current state. No authoritative state lives only in a Claude/master session
  or a worker context. *(master memory authoritative; forgot to re-arm watcher)*

- **INV-2 Reconstructable orchestrators.** Reconciler and Master are stateless
  over durable state; killing and restarting either resumes correct behavior
  with no lost or duplicated task progress. *(master outage x2; tmux crash)*

- **INV-3 Compute decoupled.** Long-running compute (e.g. SLURM jobs) is
  decoupled from orchestration liveness; an orchestration-layer crash never
  kills or loses compute. *(tmux crashed, all SLURM jobs survived — preserve
  this deliberately)*

- **INV-4 State, not clock.** Control decisions depend on explicit durable
  state and explicit observed conditions, never on file mtime or timestamp
  ordering. If/when event consumption is introduced, consumers use durable
  cursors rather than time. (Liveness death-detection may compare a heartbeat
  clock — that is a mechanism, not a control signal.) V2.0 does NOT require a
  cursor primitive. *(nudger timestamp edge race; stale-file deadlock)*

- **INV-5 Single valid executor (fencing lease).** A task has at most one
  valid execution lease at a time; only the current lease-holder may mutate
  that task's authoritative state; a write bearing a stale lease is rejected
  (fencing token). *(stale worker revives after reassignment)*

- **INV-6 Serialized idempotent reconcile.** A single reconciler holds a lock;
  reconcile actions are idempotent (desired==actual ⇒ no-op); two reconcilers
  cannot act concurrently. *(double daemon / duplicate reconciler)*

- **INV-7 Transitions are the only mutation path.** Task lifecycle changes only
  through defined transitions, and every authoritative Task mutation goes
  through ONE unified transition path/API. No worker, master, or shell script
  may write the Task Store directly. DONE is set only by passing recorded
  acceptance. *(closed E's window; "done" was a belief; brief-error mutating
  state; ad-hoc file edits)*

- **INV-8 Append-only, idempotent events.** The Event Log is append-only; each
  event carries a unique id; redelivery/duplicate of an id is a no-op for any
  consumer. *(duplicate event)* (Global monotonic SEQUENCE is deferred — see
  §5, to avoid inventing a new coordination race before events become a
  signal bus.)

The following were introduced when actuation was implemented (v0.3); each is
anchored to a concrete correctness risk found in review or canary.

- **INV-9 Persist desired state before external actuation.** The authoritative
  RUNNING+lease is committed to the Task Store BEFORE any worker is launched,
  and on adoption the NEW ownership is committed BEFORE the stale generation is
  stopped. A reconciler crash between the durable write and the external side
  effect therefore never double-actuates and never orphans a task. Launch is
  idempotent for a given lease. *(review: crash between actuation and store
  write could relaunch work)*

- **INV-10 Durable death before revocation (grace).** A lease is rotated off a
  worker only when that worker is DURABLY dead: not observed alive AND no valid
  receipt AND no heartbeat within a grace window. A single transient liveness
  miss never revokes a live worker's lease (guards INV-5 from the observer
  side). *(review: a transient pgrep/proc miss must not revoke a live worker)*

- **INV-11 Stable worker identity.** A worker's liveness is judged by a stable
  process identity — `(pid, starttime)` — recorded at launch AND refreshed
  identically on resume, never by a workspace-wide process scan. A recycled pid
  or a sibling process is never mistaken for the worker; a resumed worker is
  tracked as its current process, not its dead original. *(review: resume left
  liveness pointing at the dead original → false adoption; codex runs as
  `node`, so name-based identity is wrong)*

- **INV-12 One active task per workspace.** A workspace holds one agent's mutable
  state (LEDGER, session id, master notes); at most one task in an active state
  (RUNNING/WAITING) may reference a given workspace. `doctor` FAILs otherwise.
  *(two workers in one workspace would corrupt each other's state)*

---

## 3. Task lifecycle & state semantics [FROZEN]

State represents LIFECYCLE; metadata represents WHY. "Awaiting a ruling",
"awaiting review", "waiting on a SLURM job", "waiting on another task's
artifact" are all WAITING + metadata.waiting_on — never new lifecycle states.

States:
- **READY** — eligible to run; no live executor yet.
- **RUNNING** — a leased worker is actively executing/reasoning.
- **WAITING** — has a specific, representable normal-path unblock condition:
  a job completes, an upstream task completes, an artifact appears, a ruling
  arrives. metadata.waiting_on names it (e.g. `job:43624081`,
  `task:X#deliverable`, `ruling:MR_009`, `artifact:/path`). Note: the unblock
  condition being "a master issues a ruling" is still WAITING —
  `waiting_on: ruling:<id>` — because the condition is explicit and expected
  in normal flow.
- **SUBMITTED** — the worker has produced its FINAL deliverable and is waiting
  for ACCEPTANCE. (Reserved strictly for the acceptance handoff — NOT for
  waiting on SLURM.)
- **DONE** — deliverable passed recorded acceptance.
- **BLOCKED** — has NO clear normal-path unblock condition; needs extra
  intervention or clarification before it is even known how to continue
  (distinct from WAITING, whose unblock condition is explicit and named).
- **FAILED** — unrecoverable.

Transitions [FROZEN]:
```
READY   → RUNNING       reconciler grants a lease, worker starts
RUNNING ↔ WAITING       worker yields on an external dependency (RUNNING→WAITING);
                        the dependency clears (WAITING→RUNNING)
RUNNING → SUBMITTED      worker submits its final deliverable for acceptance
SUBMITTED → RUNNING      acceptance requests revision (rework)
SUBMITTED → DONE         acceptance passes
RUNNING/WAITING → BLOCKED   an intervention decision is required
BLOCKED → READY/RUNNING     the intervention lands
any → FAILED             unrecoverable
```
Notes:
- The common "agent submits a SLURM job then exits" pattern is
  `RUNNING → WAITING (waiting_on: job:<id>)`, and `WAITING → RUNNING` when the
  job ends. SUBMITTED is NOT involved there.
- WAITING vs BLOCKED: WAITING has an explicit, named, normal-path unblock
  condition (job/task/artifact/ruling); BLOCKED has none — you must first
  clarify/intervene to even know what would unblock it. "Awaiting a ruling"
  is WAITING (the condition is named), NOT BLOCKED.

---

## 4. Authority boundaries [FROZEN]

- Only the reconciler and the acceptance step trigger transitions, via the
  single transition API (INV-7). Workers and masters REQUEST transitions
  (by emitting an intent/event or a ruling); they do not write Task state.
- Only the current lease-holder's requests are honored for a given task
  (INV-5, fencing).
- The reconciler makes no scientific judgment; acceptance and rulings are the
  master's; the reconciler only moves tasks whose preconditions are met.
- Compute lives outside the runtime (INV-3); the runtime records references
  (metadata.waiting_on: job:<id>) but never owns the job's lifecycle.

- **Single source of truth for execution ownership [FROZEN].**
  `Task.lease` is the ONE authoritative record of execution ownership.
  The Worker Registry's `lease`/`current_task` is OBSERVED/operational state
  and may be transiently inconsistent (e.g. after a crash). On any
  disagreement, the reconciler treats `Task.lease` + actual process state as
  ground truth and REPAIRS the Worker Registry to match — never the reverse.
  This prevents the Task Store and Worker Registry from becoming two sources
  of truth.

---

## 5. Provisional details [PROVISIONAL]

These are expected to change during implementation without reopening §1–§4.

- **Concrete field sets** (below in §6) — minimal now; add only when an
  invariant demands it.
- **File layout / storage** — **[PINNED v0.3]** project-local `.farm/`:
  `tasks/<id>.json` (authoritative, atomic write via temp+fsync+rename),
  `workers/<id>.json` (observed), `events/log.ndjson` (append-only),
  `runtime/` (reconcile lock, per-worker executor state, receipts). INV-6
  locking is a non-blocking `flock` on `runtime/reconcile.lock`. Not sqlite; a
  single reconciler + atomic per-file writes suffice at farm scale.
- **Heartbeat / grace** — **[PINNED v0.3]** death is declared only after a grace
  window with no heartbeat and no receipt (INV-10); default 60s, operator-set
  via `--grace-seconds`. **Reconciler cadence** — operator-set via
  `reconcile --loop --interval` (no fixed number baked in).
- **Structured receipt primitive** — **[PINNED v0.3]** the worker→runtime signal
  deferred in v0.2 (§8/§9) is now real: a worker writes a fenced JSON
  `Receipt {worker_id, task_id, lease_id, status: RUNNING|AWAITING|SUBMITTED|
  FAILED, ts, note, waiting_on}` atomically; the reconciler applies it ONLY if
  `lease_id` matches the task's authoritative lease (fencing), mapping
  AWAITING→WAITING, SUBMITTED→SUBMITTED, FAILED→FAILED. A worker never asserts
  DONE (INV-7). It is a structured receipt/event, not a free-text ledger.
- **Event representation** and, specifically, **global monotonic sequence
  allocation** — DEFERRED. Events currently carry only a unique id and are
  audit-only. A global monotonic sequence + cursor bus is designed only when
  events become the signaling substrate (V2.1), so we do not create a new
  sequence-allocation coordination race prematurely.
- **owner / decision_owner** — DEFERRED. Tasks are NOT bound to a specific
  master session (master must be replaceable). If durable multi-master routing
  is later required, add a `decision_owner` field then — not now.
- **rev / CAS enforcement** — deferred (single-reconciler-lock suffices; lease
  fencing is the must-have).

---

## 6. Minimal schemas [PROVISIONAL fields; the ROLE of each is FROZEN]

Task (authoritative current state):
```
id           unique
objective    what to achieve (master-authored)
deliverable  path/spec that counts as output
acceptance   criteria, or a ref to a registration
state        READY | RUNNING | WAITING | SUBMITTED | DONE | BLOCKED | FAILED
lease        {worker_id, lease_id, expiry} | null      # current valid executor (INV-5)
metadata     free-form: waiting_on / priority / workspace / receipts / notes
```
(No `owner` — deferred, §5. No `rev` — deferred, §5.)

Worker (disposable executor) — OBSERVED/operational state, NOT authoritative;
reconciled against Task.lease + actual process state (see §4):
```
id             session-independent worker identity
session_handle resumable executor handle (e.g. codex session id) | null
heartbeat      last-seen timestamp — liveness/death-detection ONLY, not a control signal
lease          {task_id, lease_id, expiry} | null      # observed; may lag Task.lease after a crash
state          IDLE | BUSY | DEAD
```

Event (append-only audit):
```
id       unique                       # unique only; global monotonic sequence deferred (§5)
task_id  the task it concerns
type     transition | ruling | note | error
actor    reconciler | master | worker:<id>
payload  details (old→new state, ruling ref, receipt sha, ...)
ts       wall clock (audit only)
```

---

## 7. Regression scenarios (V2 must pass) [FROZEN intent; harness PROVISIONAL]

| Scenario | Invariant | Required behavior |
|---|---|---|
| kill master | INV-1,2 | new master reads Task Store, sees same state, can issue the pending ruling; no task lost, no duplicate ruling |
| kill worker | INV-2,5 | reconciler detects stale heartbeat → marks DEAD → new lease → new worker; task resumes; exactly one lease valid |
| kill tmux / host | INV-2,3 | SLURM jobs keep running; reconciler restart reconciles (task WAITING on a live job → no-op); zero compute lost |
| duplicate reconciler start | INV-6 | only the lock-holder acts; the other no-ops/exits; no double worker start |
| stale worker after lease reassignment wakes | INV-5 | old lease L1 write rejected (fencing); only new lease L2 writes apply |
| duplicate event | INV-8 | consumed once; redelivery is a no-op |
| reconciler restart mid-action | INV-2,4 | recompute desired-vs-actual from durable state; idempotent completion, no bad side effect |
| direct Task-Store write attempt (worker/shell) | INV-7 | rejected; only the transition API mutates state |

---

## 8. Minimal shadow V2 (read-only; proves the model before any control) [PROVISIONAL]

Status note (v0.3): shadow was the planned way to de-risk the model before any
control. In practice the model was validated directly by ISOLATED canaries — a
dedicated tmux server + a separate session, live farm untouched — exercising real
launch, crash-adoption, and resume (§10). Shadow (`farm shadow`) remains
available and useful as a read-only differential tool against a running farm, but
is no longer a prerequisite gate. The sections below are retained as the shadow
contract for that tool.

Goal: a READ-ONLY reconciler that derives task state from existing artifacts
and LOGS what it WOULD do, compared event-by-event against the live nudger.
Zero risk; current farm untouched.

Minimal to run shadow:
- Task: `id, state, workspace, lease(recorded only), metadata{waiting_on}` +
  thin refs for objective/deliverable/acceptance (shadow does not evaluate
  acceptance).
- Worker: `id, session_handle, heartbeat, state`.
- Event: `id, task_id, type, actor` (unique id; no sequence).
- States exercised: READY / RUNNING / WAITING / DONE (WAITING is the critical
  one — it covers the SLURM-job waits where nudger's mtime race lived; SUBMITTED
  and BLOCKED can be observed but need not be enforced in shadow).

Deferred to canary / V2.1: lease-fencing ENFORCEMENT (shadow only records
leases), acceptance auto-evaluation, event-as-signal-bus + monotonic sequence,
rev/CAS, executor/acceptance adapter abstraction, priority scheduling,
decision_owner / multi-master routing, and an explicit receipt/ack primitive
(see §9 — needed to observe master-message consumption; must be a structured
receipt/event, not a free-text ledger convention).

Shadow success criterion [FROZEN as DIFFERENTIAL, not reproduction]:
the live nudger is NOT ground truth (it has known bugs). Shadow runs as a
differential test: every disagreement between the state-derived V2 decision
and the live nudger action is CLASSIFIED (§9.5), and success is that every
disagreement is explainable — with the "V2-correct" ones landing exactly on
the nudger's known failure modes (timestamp edge race, stale-file deadlock,
phantom await). Shadow does not aim to reproduce nudger behavior; it aims to
explain current behavior and correctly flag the old system's errors.

---

## 9. Shadow v0 — observation contract [PROVISIONAL]

Strictly READ-ONLY and ZERO added instrumentation. Shadow uses only facts
that ALREADY exist; it never modifies worker behavior, never adds an ACK/
receipt protocol to live workers, and never uses mtime or guessing to infer
state.

### 9.1 Facts shadow may read each cycle (existing, explicit only)
- **Process liveness**: `pgrep -x codex` + `/proc/<pid>/cwd` → which workspace
  has a live executor. (Ground truth for Worker liveness.)
- **SLURM state**: `squeue` / `sacct` for jobs referenced by a workspace.
- **Explicit awaiting condition**: a workspace's `.awaiting` file IF it exists
  (an already-present explicit fact) — the job/artifact it names.
- **Artifact / ruling refs that already exist**: presence of a named
  deliverable artifact; presence of a `MASTER_REPLY_*.md` file that an
  ESCALATION explicitly references by name.
- **Live nudger actions**: `nudger.log` entries this cycle (the differential
  comparison side).
NOT read for control inference: file mtimes, timestamp ordering, or any guess
about "unread" master messages.

### 9.2 Deriving observed Task / Worker state (from the above only)
- Worker.observed: live codex proc cwd here → BUSY; else IDLE/absent.
- Task.observed_state:
  - live executor present → RUNNING;
  - no executor + a referenced SLURM job still running → WAITING(job:<id>);
  - no executor + `.awaiting` names a satisfied job/artifact condition → the
    unblock condition is met (candidate for WAKE);
  - deliverable artifact present and its acceptance ref is satisfiable by an
    existing explicit fact → DONE;
  - none of the above determinable from existing explicit facts → do NOT
    guess: emit **UNKNOWN / OBSERVABILITY_GAP** (e.g.
    `master_message_consumption_not_explicit` when a MASTER_REPLY exists but
    there is no explicit record that the worker consumed it).

### 9.3 Allowed outputs (shadow log only; never executed)
`WOULD_WAKE <worker>` / `WOULD_WAIT <worker> (on X)` /
`WOULD_REASSIGN <task>` / `WOULD_NOOP` / `UNKNOWN(<gap-reason>)`.

### 9.4 Disagreement record (per workspace per cycle)
`{workspace, observed_facts, nudger_action(from nudger.log),
  v2_would, agree:bool}`; `agree=false` (or any UNKNOWN vs a nudger action)
goes to `disagreement.log` with BOTH sides' explicit reasons.

### 9.5 Adjudication categories (each disagreement gets exactly one)
- **nudger-correct** — V2 missed an existing fact → fix observation/derivation.
- **V2-correct** — a nudger known bug (validates V2's value).
- **ambiguous** — both defensible → human judgment.
- **observability-or-model-gap** — cannot decide from existing explicit facts
  (e.g. master_message_consumption_not_explicit) → a genuine finding that
  canary/V2 needs an explicit receipt/ack primitive; that primitive is
  designed at canary, structured (receipt/event), not free-text ledger.

### 9.6 Scope of shadow v0 (do NOT force full coverage)
Cover only paths observable WITHOUT new instrumentation: process liveness,
SLURM state, explicit `.awaiting` conditions, and artifact/ruling refs that
already exist. Wake cases that depend on "did the worker read this master
message" are expected to surface as observability-gaps, not forced coverage —
that gap is itself the deliverable pointing to the canary receipt primitive.

---

## 10. Implementation status (v0.3) [maps contract → code]

The minimal actuation layer is implemented and validated by real-codex canaries
(isolated tmux server, live farm untouched). This section is the authoritative
map from contract to code; it does not add new semantics.

### 10.1 Invariant → realization
| Invariant | Realized by |
|---|---|
| INV-1 durable authority | `store.TaskStore` (per-task JSON is the only authoritative read) |
| INV-2 reconstructable orchestrators | reconciler + master hold no state; all state in `.farm/` |
| INV-3 compute decoupled | runtime records `waiting_on: job:<id>`; never submits/cancels SLURM |
| INV-4 state, not clock | control reads task state; only death-detection compares a heartbeat clock (a mechanism, §INV-10) |
| INV-5 fencing lease | `transitions.validate_transition` rejects a stale `lease_id`; receipts fenced by `lease_id` |
| INV-6 serialized reconcile | `locking.single_reconciler` (flock); `reconcile_once` is idempotent |
| INV-7 one mutation path | `Reconciler._commit` (durable write + event) for BOTH transitions and lease acquire/rotate/release; DONE needs recorded acceptance |
| INV-8 idempotent events | `events.EventLog` append-only, unique ids |
| INV-9 persist-before-actuate | `_start` commits RUNNING+lease before `executor.launch`; commits new ownership before stopping the stale generation; launch idempotent per lease |
| INV-10 durable death (grace) | `_durably_dead` (no heartbeat within `grace_seconds` + no receipt) gates `rotate_lease` |
| INV-11 stable worker identity | `procutil` `(pid, starttime)` recorded at launch AND resume via one shared shell block; liveness never a workspace pgrep |
| INV-12 one active task per workspace | `doctor` FAILs on two RUNNING/WAITING tasks sharing `metadata.workspace` |

### 10.2 Ratified from v0.2 deferrals
- **Lease rotation** (acquire/rotate/release) is now a first-class authoritative
  mutation through `_commit` (was flagged in v0.2 as "beyond the frozen graph").
  `rotate_lease` requires a `reason` establishing death and is guarded to never
  rotate off a possibly-live holder.
- **Structured receipt/ack primitive** (v0.2 §8/§9 "observability gap") is
  implemented and fenced (§5, PINNED).
- **INV-6 lock** is a real flock (v0.2 listed it as TBD).

### 10.3 Executors & session handling
- `adapters/base.WorkerExecutor` = `launch / resume / poll / stop`.
- `adapters/local_process.LocalProcessExecutor` — real subprocesses; identity via
  `(pid, starttime)`; reaps zombies.
- `adapters/codex.CodexTmuxExecutor` — codex workers in a tmux session on a
  DEDICATED tmux server (`-L <socket>`), a session separate from any live
  `farm`; never kills/moves windows; pane identity = worker id; cluster
  specifics in `CodexClusterConfig` (no personal paths in the module). The codex
  session id is captured from the worker's OWN log (`session id: <uuid>`),
  race-free; resume reuses it under the same lease.

### 10.4 Canary evidence (real codex, isolated)
- Launch happy-path: READY→…→SUBMITTED→DONE, doctor PASS, live farm untouched.
- Crash-adoption: worker killed mid-run → `LEASE_ROTATED`+`WORKER_ADOPTED` (new
  lease), task state survived; successor recovered from durable workspace state.
- Resume identity: worker AWAITING→ends (pid dead) → resume refreshes the
  `(pid,starttime)` identity to the live process → stays alive across grace, no
  false adoption → SUBMITTED→DONE.

### 10.5 Still deferred (unchanged from v0.2 unless noted)
- WAITING-condition OBSERVERS (`job:`/`artifact:`/`task:`/`ruling:` auto-unblock)
  — the reconciler resumes on an injected predicate; the observers that evaluate
  those conditions are the next build item (post-merge).
- Event-as-signal-bus + global monotonic sequence/cursors (V2.1).
- `rev`/CAS (single-reconciler lock + fencing suffice).
- `decision_owner` / durable multi-master routing.
- Priority scheduling; SLURM submit/cancel; enforcement against non-cooperative
  (receipt-less) workers.
