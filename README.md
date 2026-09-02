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

**Minimal actuator landed (canary stage). Codex/tmux backend still pending.**

Safe foundation (unchanged):

- task / worker / event models;
- atomic durable task storage;
- legal lifecycle validation;
- lease/fencing validation helpers;
- invariant checks (`farm doctor`);
- read-only shadow observations (`farm shadow`);
- cluster/project templates;
- regression tests for the frozen invariants.

Actuation (new — the control loop that drives disposable workers):

- `Reconciler.reconcile_once` — one serialized, idempotent pass (INV-6) that
  actuates `READY` tasks, applies fenced worker receipts, adopts crashed
  workers, resumes unblocked `WAITING` tasks, and repairs the observed registry;
- structured **receipt** primitive (`RUNNING`/`AWAITING`/`SUBMITTED`/`FAILED`),
  fenced by `lease_id` so a superseded worker generation cannot advance a task;
- **lease rotation** (`rotate_lease`) — releases a lease from a *proven-dead*
  holder so the task can be re-actuated with state preserved. This is the
  "adopt a stopped task" primitive and is a **design delta beyond frozen v0.2**
  (state graph froze before actuation), pending ratification into v0.3;
- `WorkerExecutor` backends: `FakeExecutor` (tests) and `LocalProcessExecutor`
  (drives real OS subprocesses — the honest bridge before a codex/tmux backend);
- `farm reconcile [--loop]` CLI.

A worker never drives `DONE`: `DONE` still requires recorded acceptance, which
is a master/harness judgment (Layer-1/Layer-2 boundary).

Still deferred: the **codex/tmux executor backend** (real farm workers), SLURM
job mutation, WAITING-condition observers (`job:`/`artifact:`/`task:`/`ruling:`),
and enforcement against live (non-cooperative) workers.

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

- codex/tmux executor backend (real farm workers; `LocalProcessExecutor` is the
  current real backend and the pattern to specialize);
- enforcement of leases against live *non-cooperative* workers (fencing today
  assumes workers echo their `lease_id`);
- WAITING-condition observers (`job:`/`artifact:`/`task:`/`ruling:` predicates);
- SLURM job submission/cancellation;
- automatic scientific acceptance (DONE stays a master/harness judgment);
- priority scheduling;
- multi-master routing;
- event stream as a signal bus;
- global event sequence/cursors.

These should be introduced only when shadow/canary evidence requires them.

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

## Tests

```bash
python -m unittest discover -s tests -v
```

The regression suite is intended to turn historical orchestration failures into permanent invariants.

## License

No license has been chosen yet. Add one intentionally before public distribution.
