# <TASK-ID> — <one-line title>
<!-- template: copy into the workspace; `farmkit brief lint` passes once the referenced files exist there -->

## Goal
<Three lines at most: what must exist when this task is accepted, and the one
question it answers. Science contract details live in PROTOCOL.md, not here.>

## Boundaries
- Write only inside <workspace>. Read-only: <list of directories>.
- Holds: <e.g. "extended runs not authorised"; "no budget changes">.
- Never cancel external jobs; never edit .farm/ files.

## Read first
1. steps.toml — the step table (what runs, in what order, how it is verified)
2. PROTOCOL.md — the frozen scientific contract
3. NOTES.md / TRAPS.md — known pitfalls (read before touching code)

## Method
On every wake run `farmkit tick --workspace <workspace> --checkpoint`, read its
summary, then execute the receipt command it prints, verbatim. Do not write
polling loops or your own monitor. If tick parks a step on `ruling:`, add 3–5
lines of judgment to CHECKPOINT.md below the marker and exit. Trust the ledger
over your memory; if they disagree, the ledger is right.
