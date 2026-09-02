# agent-farm-runtime

A small, portable runtime for durable-state-driven research agent farms.

The runtime separates **scientific decisions** from **mechanical orchestration**:

- **Claude Master**: decomposes tasks, sets acceptance criteria, makes scientific rulings, synthesizes results.
- **Reconciler**: deterministic control loop; no scientific judgment.
- **Workers**: disposable executors (Codex first, other backends later).
- **Task Store**: the single authoritative current state.
- **Worker Registry**: observed operational state; repairable, not authoritative.
- **Event Log**: append-only audit history.

The design is derived from real failure modes in a live SSH/tmux/SLURM agent farm. The frozen contract lives in [`V2_DESIGN.md`](V2_DESIGN.md).

## Current status

**v0.3 — actuation implemented and validated by real-codex canaries.** The
contract in [`V2_DESIGN.md`](V2_DESIGN.md) is aligned to the code (§10 maps every
invariant to its implementation). The next phase is running an isolated canary
farm, not more feature development.

Foundation:

- task / worker / event models; atomic durable task storage;
- legal lifecycle validation; lease/fencing validation;
- invariant checks (`farm doctor`); read-only shadow observations (`farm shadow`);
- cluster/project templates; regression tests for the invariants.

Actuation — the control loop that drives disposable workers:

- `Reconciler.reconcile_once` — one serialized, idempotent pass (INV-6) that
  actuates `READY` tasks, applies fenced worker receipts, adopts crashed
  workers, resumes unblocked `WAITING` tasks, and repairs the observed registry;
- **crash-consistency** (INV-9): the authoritative RUNNING+lease is persisted
  BEFORE any launch, and on adoption new ownership is committed BEFORE the stale
  generation is stopped — so a reconciler crash never double-actuates or orphans
  a task; launch is idempotent per lease;
- **grace policy** (INV-10): a lease is rotated off a worker only when it is
  *durably* dead (no receipt and no heartbeat within `--grace-seconds`) — a
  single transient liveness miss never revokes a live worker's lease (INV-5);
- **stable worker identity** (INV-11): liveness is a recorded `(pid, starttime)`
  set at launch AND refreshed identically on resume — never a workspace-wide
  process scan; a recycled pid or a resumed worker's dead original is never
  mistaken for the live worker;
- **single-reconciler lock** (`locking.single_reconciler`, flock, INV-6);
- one authoritative-mutation boundary (INV-7): every state transition AND lease
  acquire/rotate/release goes through a durable-write + event;
- structured, atomically-written **receipt** primitive
  (`RUNNING`/`AWAITING`/`SUBMITTED`/`FAILED`), fenced by `lease_id`;
- **lease rotation** (`rotate_lease`) — the "adopt a stopped task" primitive,
  ratified into the v0.3 contract; requires a `reason` establishing death and
  can never rotate off a possibly-live holder;
- **one active task per workspace** (INV-12): `doctor` FAILs on two active tasks
  sharing a workspace;
- `WorkerExecutor` backends: `FakeExecutor` (tests), `LocalProcessExecutor` (real
  subprocesses), and **`CodexTmuxExecutor`** (real codex workers in a dedicated,
  isolated tmux server; cluster specifics in `CodexClusterConfig`);
- `farm reconcile [--executor codex-tmux] [--loop]` CLI.

A worker never drives `DONE`: `DONE` still requires recorded acceptance, a
master/harness judgment (Layer-1/Layer-2 boundary).

## Design rule

> Task state is durable; agents and sessions are disposable.

The intended lifecycle is:

```text
READY → RUNNING ↔ WAITING → SUBMITTED → DONE
                  │              │
                  └→ BLOCKED     └→ RUNNING (revision)

any → FAILED
```

`WAITING` means there is a named normal-path unblock condition (`job:...`, `task:...`, `artifact:...`, `ruling:...`). `SUBMITTED` is reserved for the final acceptance handoff.

## Quick start

Requires Python 3.11+ and no runtime third-party dependencies.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .

# initialize a project-local durable state directory
cd /path/to/your/research-project
farm init

# inspect state and invariants
farm status
farm doctor

# create a task contract
farm task-create \
  --objective "Task A objective" \
  --deliverable "artifacts/task-a/result.md" \
  --acceptance "Result is reproducible and passes registered checks"

farm task-list
```

### Read-only shadow observation

On a Linux cluster, shadow mode can inspect existing workspaces without modifying them:

```bash
farm shadow /path/to/existing/workspaces
```

It only uses explicit facts that already exist: process liveness, `.awaiting` contents, referenced artifacts, and SLURM status when available. It never uses file mtimes to infer control state.

## Project-local layout

`farm init` creates:

```text
.farm/
├── tasks/       # authoritative task JSON
├── workers/     # observed worker JSON
├── events/      # append-only audit JSONL
├── decisions/   # durable rulings/acceptance records (representation provisional)
└── runtime/     # locks / local runtime metadata
```

Runtime code belongs here; scientific project context remains in the project repository.

## Portability model

```text
agent-farm-runtime
       │
       ├── project A/.farm
       ├── project B/.farm
       └── project C/.farm
```

Cluster-specific behavior belongs behind adapters. The core task semantics should not know whether it is running on Harvard FASRC, Anvil, Delta, or another SLURM cluster.

## What is deliberately deferred

- WAITING-condition observers (`job:`/`artifact:`/`task:`/`ruling:` predicates):
  the reconciler resumes on an injected `unblock` predicate; the observers that
  evaluate those conditions are the next build item;
- enforcement of leases against live *non-cooperative* workers (fencing today
  assumes workers echo their `lease_id`);
- SLURM job submission/cancellation;
- automatic scientific acceptance (DONE stays a master/harness judgment);
- priority scheduling;
- multi-master routing (`decision_owner`);
- event stream as a signal bus; global event sequence/cursors.

These are introduced only when canary evidence requires them (see V2_DESIGN §10.5).

## Actuation quick start

```bash
farm --project P init
farm --project P task-create --id T-1 \
  --objective "..." --deliverable "..." --acceptance "..." \
  --command "python my_worker.py"     # worker echoes FARM_LEASE_ID in its receipt
farm --project P reconcile            # one pass: launch -> observe -> advance
farm --project P doctor               # invariant check
```

The worker reads `FARM_RECEIPT_PATH`, `FARM_WORKER_ID`, `FARM_TASK_ID`,
`FARM_LEASE_ID` from its environment and writes a JSON `Receipt` to
`FARM_RECEIPT_PATH` when it reaches a wait boundary, submits, or fails.

For real codex workers, create the task with `--workspace`/`--brief`(`--brief-file`)
and drive it with the codex-tmux executor on a dedicated, isolated tmux server
(never the live `farm` session):

```bash
farm --project P task-create --id T-1 \
  --objective "..." --deliverable "..." --acceptance "..." \
  --workspace /abs/workspace --brief-file BRIEF.md
farm --project P reconcile --executor codex-tmux --session farm2 --tmux-socket canary
```

The executor drops a `.farm_receipt.py` helper into the workspace; the agent
records `AWAITING`/`SUBMITTED`/`FAILED` with it (the helper echoes the lease id
for fencing). Cluster specifics (PATH prelude, codex command) come from
`CodexClusterConfig` / `FARM_CODEX_*` env, not the module.

## Tests

```bash
pip install -e ".[dev]"
python -m pytest -q
```

The regression suite turns historical orchestration failures (and each review finding) into permanent invariants.

## License

No license has been chosen yet. Add one intentionally before public distribution.
