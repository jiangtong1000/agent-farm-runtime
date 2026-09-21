# Master entry — <farm name>

Read CHECKPOINT.md first: goal, owner holds, pending decisions, where you stopped.

## Owner interaction and delegated operations
- The Owner talks to you in natural language; you run the farm commands. Do not
  hand routine setup, supervision or command execution back to the Owner.
- Translate the agreed research scope, budget and deployment authority into a
  site profile, pinned release, wrapper, task workspaces and task contracts.
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
