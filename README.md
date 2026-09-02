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

**Shadow-first prototype. No production actuator is enabled.**

This repository intentionally implements only the safe foundation:

- task / worker / event models;
- atomic durable task storage;
- legal lifecycle validation;
- lease/fencing validation helpers;
- invariant checks (`farm doctor`);
- read-only shadow observations (`farm shadow`);
- cluster/project templates;
- regression tests for the frozen invariants.

It does **not** yet replace a production nudger/watcher, start or kill workers, mutate SLURM jobs, or add a receipt/ACK protocol to existing workers.

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

- production worker launching/restarting;
- enforcement of leases against live workers;
- structured receipt/ACK protocol;
- automatic scientific acceptance;
- priority scheduling;
- multi-master routing;
- event stream as a signal bus;
- global event sequence/cursors;
- generic backend plugin framework beyond thin adapters.

These should be introduced only when shadow/canary evidence requires them.

## Tests

```bash
python -m unittest discover -s tests -v
```

The regression suite is intended to turn historical orchestration failures into permanent invariants.

## License

No license has been chosen yet. Add one intentionally before public distribution.
