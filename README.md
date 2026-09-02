# agent-farm-runtime

A small, portable runtime for durable-state-driven multi-agent research farms.

The core idea is simple:

- **Task state is durable and authoritative.**
- **Claude/master sessions and worker agents are replaceable.**
- **A deterministic reconciler handles mechanical liveness and recovery.**
- **Scientific decisions and acceptance remain explicit decision-layer actions.**
- **Shadow-first migration is preferred over rewriting a live farm in place.**

This repository is intentionally small. It is meant to provide the orchestration substrate, not project-specific research knowledge.

## Current status

V2 is in **design/shadow-first** stage. Production actuation (worker restart/kill, automated receipt protocol, priority scheduling, multi-master routing) is deliberately deferred.

## Layout

```text
src/agent_farm_runtime/   core runtime and adapters
roles/                    reusable worker role prompts
examples/                 minimal project example
templates/                cluster/project/task templates
tests/                    regression tests for runtime invariants
V2_DESIGN.md              frozen architecture contract
```

## Quick start

```bash
python -m pip install -e .
farm init .farm
farm doctor .farm
farm status .farm
```

See `V2_DESIGN.md` for the architecture and invariants.
