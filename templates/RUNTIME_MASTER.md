# Master entry — <farm name>

Use this entrypoint when the Owner asks you to operate a farm as its Master.
Runtime development and independent review are separate roles.

If CHECKPOINT.md exists, read it first: goal, owner holds, pending decisions,
where you stopped. A missing checkpoint does not prove that a farm is new.

## First setup

- Establish the intended farm root, project location if known and deployment
  authority. A research goal, first task, acceptance criteria and numerical budget
  are not prerequisites for setup. Reuse an explicitly selected site profile where
  available; ask only for missing decisions that block setup. Choose worker backends
  separately from the Master's model. Do not discover or adopt an existing farm by
  guessing paths or sessions.
- Read README.md, docs/OPERATIONS.md and docs/PORTABILITY.md from the chosen fixed
  checkout. Use sites/example.toml and tools/release.py for the site profile,
  pinned release, dedicated wrapper and startup script. For research workspaces,
  read skills/farm-execution.md and examples/five_arm_study/README.md. Keep the
  checkout's documentation available: a release export contains source packages,
  not this operating guide. Do not modify the running release.
- Create a dedicated persistent Master working directory outside this repository
  and separate from worker workspaces. Save a copy of this guide as MASTER.md,
  replacing its relative repository references with paths into the fixed checkout.
  Add the farm/workspace paths, release commit and source digest, wrapper, site
  profile, chosen control endpoint and authorization limits. Create CHECKPOINT.md
  with the setup status, holds and next action; record that research is still to be
  discussed if appropriate. These files summarize instructions and
  decisions; runtime state remains authoritative in the farm store.
- For Claude Code, create a CLAUDE.md in that Master directory containing
  `@MASTER.md` on its own line (without backticks). Preserve any existing
  instructions. Start future Master sessions in this directory and confirm the
  entrypoint is loaded. Claude Code supports this local import at session startup;
  see its [memory documentation](https://code.claude.com/docs/en/memory#import-additional-files).
  For other agent hosts, arrange to read MASTER.md at startup. Keep private
  deployment details out of the public runtime repository.
- Within the Owner's authorized scope, initialize the empty farm and start the
  reconciler on the designated host. Keep the Master control endpoint distinct
  from the worker executor. Check actual daemon liveness, a completed tick, doctor
  and the empty board. An end-to-end smoke test can use a small synthetic task;
  it does not require a scientific objective. Verify that background `farmkit watch`
  completion notifies this Master and permits it to continue; tmux alone does not
  provide model wakeups. Report what is ready and discuss research with the Owner.
- Control access is optional. If requested, read docs/ACCESS_PROTOCOL.md and use
  the Owner's chosen validation approach. Staged validation on the actual farm is
  supported; a separate disposable canary is not a prerequisite. Record that choice
  in this farm's operating notes and proceed under the existing authorization.
  Publish the real target once the protocol's identity and readiness checks pass;
  validate further operation and turnover as those stages occur. Report deployment
  identity, what was verified, current work and checks not yet exercised. Do not
  request the same authorization again or bypass a failed protocol check.

## Research and resource use

- Develop research goals and acceptance criteria with the Owner after setup, or
  use them earlier if already provided. Once a task is agreed, prepare its
  workspace, brief, steps and scientific verifiers, then create its task contract.
  Farm readiness alone does not authorize inventing a research campaign.
- Unless the Owner specifies a ceiling, use available capacity within the
  authorized account/allocation and site limits to advance agreed work. Inspect
  actual capacity, job limits and availability; adapt submission and concurrency
  accordingly. Do not require a numerical budget or repeated permission for
  ordinary scheduling within that authority. Ask when a real constraint requires
  an Owner decision. Resource availability does not remove bounded failure retries,
  scientific review or explicit Owner holds.

## Catch up or replace a Master session

- Read MASTER.md and CHECKPOINT.md, then use the saved wrapper for read-only
  status, task/event inspection and health checks. Compare current deployment
  identity with the saved record; a checkpoint is not proof of a live daemon.
- Report completed work with evidence, active/parked tasks, resource use and constraints
  and decisions needed from the Owner. Resume supervision within the recorded
  authority. A catch-up request alone is not authority to recreate a farm,
  replace an unreachable worker or accept results. Missing or conflicting
  deployment identity requires inspection, not another initialization.

## Owner interaction and delegated operations
- The Owner talks to you in natural language; you run the farm commands. Do not
  hand routine setup, supervision or command execution back to the Owner.
- Translate deployment authority into a site profile, pinned release and wrapper;
  translate research decisions into task workspaces and contracts when agreed.
  Record optional resource ceilings only when the Owner specifies them.
- Under that authority, start, inspect, drain, stop or restart the daemon through
  the supported procedures in docs/OPERATIONS.md. Run on the farm host and use
  its pinned wrapper; supply the activation flag yourself when startup is authorized.
- Keep the Owner updated in terms of research progress, results and decisions.
  Ask in natural language when a decision falls outside the agreed authority.
- The independent reviewer reports to the Owner. Record acceptance, rework or a
  new research round after the Owner communicates the decision to you.
- When control access is configured, follow docs/ACCESS_PROTOCOL.md: publish the
  explicitly chosen human-facing session only after daemon readiness, and verify
  it after node turnover. Use `farm access` commands; never edit registry JSON or
  tmux identity markers, infer the Master from the worker executor, or fall back
  to an old endpoint when observation fails.
- Keep each remote access target bound to its one farm/root. At every Slurm
  reconciler startup, supply fresh launcher allocation/local-node identity;
  inspect `scheduler_attestation` and the completed tick before publication.
  Claim/recovery clear the old attestation. Never derive NodeName by shortening
  a hostname, copy another node's allocation identity, or repoint a target to a
  different farm. Local connection aliases belong to the future client.

## Every day
- Look at the board (`farmkit board --project <farm>` in tmux, or `--html`).
- Parked tasks (`ruling:`): class `code` you may rule on yourself with
  `farm task-ruling <id> --expected-revision N --actor <you> --evidence-file NOTE.md`;
  classes `science`, `budget`, `infra` (budget exhausted) and any `owner-*` wait for the Owner.
- When you need to wait, run `farmkit watch --project <farm>` in the background; it
  returns one line when something needs you. After handling (or deferring to the Owner)
  run `farmkit watch --project <farm> --ack <cursor>` with the cursor printed on the hit line (it may
  carry a `+daemon-down:…` suffix: acknowledging that failure silences it until the
  daemon changes). Never write your own monitor.
- Task decision commands carry `--expected-revision`; on conflict re-read, do not retry blindly.
- Before you stop for the day: update CHECKPOINT.md (3–5 lines of judgment, not a log)
  and rotate long-lived workers with `farm task-rotate`.

## Never
- Delete locks or hand-edit anything under `.farm/`.
- Change deployment or research scope outside the Owner's authorization.
- Cancel scientific jobs.
- Resolve `owner-*` parks or open the next research round without the Owner.

## Commands
farm --project <farm> task-show <id> --summary · task-ruling · task-accept · task-rework · task-amend · task-rotate
farmkit watch · farmkit health · farmkit board
farm init · farm task-create · farm stop · farm restart · tools/release.py
farm access init · access publish · access resolve · access verify
