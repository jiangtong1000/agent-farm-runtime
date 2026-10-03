# Compatibility

| runtime protocol | runtime release | farmkit | receipt fields accepted by the daemon | notes |
|---|---|---|---|---|
| 4 | 91b616e (baseline) | — | worker_id, task_id, lease_id, status, ts, note, waiting_on, rotation_id, checkpoint | baseline |
| 4 | refactor/farmkit-era | 0.1 | same | same protocol: `farm restart --to` applies; executor state files from 91b616e are read (legacy `previous_receipt_sha256` honoured) |
| 4 | control-access-v1 capability | 0.1 | same | additive access schema 1, manifest identity and heartbeat stamp; requires upgraded daemon before publication |
| 4 | access generations and operational fixes | 0.1 | same | schema 2 adds explicit process roles and immutable endpoint generations; schema 1 records remain readable |

A change under `src/agent_farm_runtime/protocol/` bumps the protocol and means
"retire the farm, start a new one" (D5). `farm restart --to` refuses such a release.

Access records have an independent schema version; this feature does not change
the task/receipt protocol. Old readers may ignore the added deployment identity
and tick stamp, but old daemons cannot establish access readiness. Publisher and
verifier require the deployed source digest even when protocol 4 is compatible.
Schema 1 binds one immutable endpoint per execution epoch. Schema 2 permits an
explicit generation change under the same epoch, using `--generation` and
`--expected-current` compare-and-swap against the prior record digest. It preserves
the farm/root binding and all old records. Old schema-1-only readers reject the
new pointer, so upgrade access readers together. `restart --to` alone does not
publish an access generation; adopt/publish the reviewed endpoint afterward. See the
[access compatibility and upgrade constraints](ACCESS_PROTOCOL.md#compatibility-and-client-boundary).

Access schema 1 requires `scheduler.scheduler_node`, an epoch-bound
deployment `scheduler_attestation`, and host/node/allocation-start tmux markers.
This corrects schema 1 directly; there is no access-record migration or automatic
repair. Runtime protocol stays 4. Claim/recovery clear the old attestation;
reconciler startup captures and validates fresh launcher input before dispatch.
Access observation and attachment support tmux 2.7 without `-N`.

Structured status now uses `source_matches: null` before any daemon manifest
exists; `false` means an actual comparison failed. Summary/status `waiting_on`
is populated only for WAITING tasks; `last_wait` exposes retained metadata on
other states. Reconciler output adds a UTC `ts` field; the durable task/receipt
and event payload schemas are unchanged.

## Shell

Worker launch scripts need bash 4.4 or newer: they `wait` on the pid of the process
substitution that mirrors the agent's log, so the session-id capture reads a finished
log. Check the installed bash version on each execution host.
