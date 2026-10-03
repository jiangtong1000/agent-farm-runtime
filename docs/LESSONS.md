# Lessons

Failure mechanisms that explain current safeguards and regression tests. Use
[architecture](ARCHITECTURE.md) for contracts and [operations](OPERATIONS.md) for
procedures. Detailed review chronology remains in Git history. Keep private
project details and deployment evidence outside the repository.

## Execution, verification and publication are separate

A process finishing does not establish a verified result. After a worker crash,
completed but unfinalized attempts must be verified without rerunning computation.
A recorded retry decision must also survive a crash before the next submission.
A declared project verifier must be loaded; silently skipping it cannot certify
an output. Parsed JSON must have the expected shape, not merely be valid JSON.

Regression coverage: [runner recovery and verification](../tests/farmkit/review/test_regressions_6b38783.py).

## Preserve the last verified result

Jobs execute snapshotted code in their attempt directory. Declared outputs must
never link back to the workspace's published copy: a failed attempt could overwrite
it before verification. Reject workspace-root outputs, unsafe paths and input/output
overlap before execution and again before publication. Merge declared input
folders without exposing output paths through those links.

Directory publication must recover at every copy/swap/cleanup boundary. Retain the
old result until a complete staging copy can be installed. A completion marker
separates a reusable complete copy from an interrupted one; an incomplete copy
that cannot be removed must prevent publication. NFS may temporarily report an
emptied directory as nonempty, so a best-effort removal is not proof of removal.

Regression coverage: [output isolation](../tests/farmkit/review/test_regressions_605bc3d.py),
[path boundaries](../tests/farmkit/review/test_regressions_1057b53.py),
[interrupted publication](../tests/farmkit/review/test_regressions_eb9558e.py) and
[publication crash matrix](../tests/farmkit/review/test_regressions_15ffc14.py).

## Bind evidence to the attempt that produced it

An `afterok` successor can start before the worker verifies its predecessor. Its
relative input paths must resolve to that producer attempt; the workspace still
contains the previous verified result. Record input identity from the run directory.
Preserve identities established at submission and reject later changes instead of
rewriting provenance to describe new bytes. Only producer-bound inputs unavailable
at submission acquire their initial identity during verification.

The [execution guide](../skills/farm-execution.md) describes the worker contract.
Regression coverage: [chain inputs](../tests/farmkit/review/test_regressions_81eb4e3.py)
and [input identity](../tests/farmkit/review/test_regressions_eb9558e.py).

## Uncertain identity cannot authorize process control

A pid may be recycled; a zombie still has a `/proc` entry. Use recorded process
start time and the applicable host/boot/namespace checks. Missing identity is
UNKNOWN, not proof of death. Refresh identity on resume, and never signal an
unverified process, the caller or its parent. A transport timeout may follow a
successful launch, so uncertainty must not trigger duplicate dispatch.

Agent session identifiers may arrive only at the end of a long turn. Wait for the
log-mirroring process to drain before parsing the completed log. Test rendered shell
commands as well as their Python inputs; quoting bugs appear at that boundary.

Regression coverage: [runtime safety](../tests/test_runtime_safety.py),
[daemon lifecycle](../tests/test_lifecycle.py) and
[process/log boundaries](../tests/farmkit/review/test_regressions_605bc3d.py).

## Observe current state and acknowledge explicit evidence

A failed or empty `squeue` query does not establish job completion. Consult `sacct`
when it can provide the answer; preserve UNKNOWN when neither source can. Cache
observations for readers without treating a stale or foreign-epoch cache as truth.
An in-flight job is submitted and not terminal. Wait on the job that produces an
output, rather than polling for its artifact.

Scanning events and acknowledging notifications are separate operations. Advance
the scan past quiet pages without discarding acknowledged faults. A task's retained
wait metadata is active only while it is WAITING. Parse ruling permissions in one
place; a task name containing `code` must not grant authority over a science ruling.

Regression coverage: [scheduler vectors](../tests/vectors/README.md),
[notification recovery](../tests/farmkit/review/test_regressions_6b38783.py),
[watch behavior](../tests/farmkit/unit/test_watch_health_adopt_cli.py) and
[current wait/cache reporting](../tests/test_issue_regressions.py).

## Recovery must preserve decisions and durable writes

Adopting an existing job carries its prior retry usage; recomputing only the new
ledger's failures must not reset its budget. A ruling releases exactly one more
attempt while retaining the failure class. Ordinary stop controls are cleared when
the daemon restarts; node handoff controls have a separate lifecycle.

Atomic rename alone is not the full persistence contract. Sync the file and its
containing directory, and retain recovery intent if either fails. Exercise the real
wrapper when validating generated restart commands: injected flags can conflict
with an otherwise plausible command assembled by the CLI.

Regression coverage: [budget and retry recovery](../tests/farmkit/review/test_regressions_6b38783.py),
[stop/restart](../tests/test_lifecycle.py), [durable commits](../tests/test_review_regressions.py)
and [directory-sync recovery](../tests/test_reduction_regressions.py).

## Keep observation cheap and environments explicit

Use the daemon's fresh scheduler observations instead of querying Slurm for each
board reader. Tail audit logs instead of replaying their entire history; cache
ledger reads within a tick instead of rereading all attempts for every lookup.
Implement reader methods on the concrete adapter, not just its typing Protocol.
Project verifier modules must not shadow standard-library names such as `select`.

Local tests and synthetic canaries establish specific behaviors. Validate shared
storage, real scheduler access, authentication and node turnover at the deployment
site using the [portability guidance](PORTABILITY.md). A prior successful canary is
not evidence that every agent backend and later deployment has been exercised.
