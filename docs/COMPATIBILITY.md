# Compatibility

| runtime protocol | runtime release | farmkit | receipt fields accepted by the daemon | notes |
|---|---|---|---|---|
| 4 | 91b616e (baseline) | — | worker_id, task_id, lease_id, status, ts, note, waiting_on, rotation_id, checkpoint | baseline |
| 4 | refactor/farmkit-era | 0.1 | same | same protocol: `farm restart --to` applies; executor state files from 91b616e are read (legacy `previous_receipt_sha256` honoured) |
| 4 | control-access-v1 capability | 0.1 | same | additive access schema 1, manifest identity and heartbeat stamp; requires upgraded daemon before publication |

A change under `src/agent_farm_runtime/protocol/` bumps the protocol and means
"retire the farm, start a new one" (D5). `farm restart --to` refuses such a release.

Access records have an independent schema version; this feature does not change
the task/receipt protocol. Old readers may ignore the added deployment identity
and tick stamp, but old daemons cannot establish access readiness. Publisher and
verifier require the deployed source digest even when protocol 4 is compatible.
Once an epoch is published, changing its source or control endpoint is a conflict;
`restart --to` alone does not create a new access generation. See the
[access compatibility and upgrade constraints](ACCESS_PROTOCOL.md#compatibility-and-client-boundary).

## Shell

Worker launch scripts need bash 4.4 or newer: they `wait` on the pid of the process
substitution that mirrors the agent's log, so the session-id capture reads a finished
log. Check the installed bash version on each execution host.
