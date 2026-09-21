# Lessons

Maintainer notes: failure mechanism → fix → verification. Newest first. Historical
entries explain why a regression test or design rule exists; later entries may
refine earlier fixes. Use [operations](OPERATIONS.md) for current behavior.
Keep project names, private paths, hostnames and research results outside this file.

## 2026-09-20 — refactor/farmkit-era

- **Sixth review of 15ffc14: 1 finding, fixed** (`tests/farmkit/review/test_regressions_15ffc14.py`,
  which also keeps the reviewer's six-phase crash matrix). `rmtree(ignore_errors=True)` can
  return with the tree still there; recovery then skipped the copy and installed the
  truncated staging directory as the verified result. If an incomplete staging copy cannot be
  removed, publishing now refuses with a reason and the previous version stays in place.
  Running the reviewer's six-phase crash matrix on this cluster's NFS (the reviewer ran it
  on local disk) showed a second thing: an emptied backup directory can report "not empty"
  for a moment, so `rmtree(ignore_errors=True)` leaves it behind. Tree removal now retries
  with a short back-off; leftovers of the previous version are best effort, leftovers of an
  incomplete staging copy must be gone or publishing refuses.
- **Fifth review of eb9558e: 2 findings, both fixed** (`tests/farmkit/review/test_regressions_eb9558e.py`).
  Recovery of an interrupted directory publish deleted the complete staging copy and the old
  backup before copying again, so a failed recovery copy left nothing; the staging copy now
  carries a completion marker, a complete copy is never copied again, and the old version is
  removed only after the new one is installed. Verification re-described inputs and
  overwrote the submission digest with whatever the file held later; an input that existed
  at submission keeps that identity, a producer-bound input that did not exist yet is
  recorded at verification, and an input whose bytes differ between the two fails the
  attempt instead of having its provenance rewritten.
- **Fourth review of 81eb4e3: 2 findings, both fixed** (`tests/farmkit/review/test_regressions_81eb4e3.py`).
  A crash between the directory swap and the cleanup left a `.<name>.replaced-*` folder that
  made every retry fail with FileExistsError; an existing leftover of the same attempt is
  removed before the next swap, and the unfinalized-attempt path re-publishes without
  re-running. Input provenance was hashed from the workspace at submission, so a chain
  consumer's ledger described the previous round's file; inputs are now described through
  the run directory (resolved links) and recorded again at verification, so the digest is of
  the bytes the attempt actually read.
- **Third review of 1057b53: 5 findings, all reproduced; 4 fixed as reported, 1 fixed under a
  stated contract** (`tests/farmkit/review/test_regressions_1057b53.py`). The artifact model
  is now written down in one place (skills/farm-execution.md): *who produced it* (the
  attempt), *when a successor may read it* (a chain successor's run directory binds the
  predecessor's declared outputs to the producer attempt, so afterok's exit-code release is
  enough), *when the workspace changes* (after the worker's ok verdict), *what may overlap*
  (an input may not name or sit inside an output; outputs are normalised relative paths
  outside attempts/). The reviewer's chain test read the predecessor through
  `FARMKIT_WORKSPACE`; under this contract that is the previous verified result by
  definition, so the repository copy of the test reads the relative path and a companion
  test asserts the workspace copy stays old until publication.
- **A directory publish must never be able to point at the workspace.** `outputs = ["."]`
  deleted a whole test workspace. Declarations are validated before execution and again
  before the destructive step; directories are replaced by copy-then-swap, never by
  deleting the only copy first.
- **Merging, not skipping, when an input directory is also an output parent.** Declared
  input directories are linked child by child into an existing run-directory folder;
  declared outputs are skipped inside them.
- **A method on a Protocol class is not an implementation.** `events_last` had landed on
  the `RuntimeReader` Protocol, so the real CLI reader never used `events --last`. Caught
  by a test that records the argv the reader issues.
- **Follow-up review of 605bc3d: 8 findings, all reproduced, all fixed** (regressions in
  `tests/farmkit/review/test_regressions_605bc3d.py`). Four were the same boundaries
  drawn only half-way. *Identity vs presence*: a legacy manifest without a start time fell
  back to "pid exists", and a zombie with the right start time counted as alive; a bare pid
  is now UNKNOWN (stop refuses) and identity requires a runnable process. *CLI exit vs log
  written*: `wait` on the CLI returned before tee had drained the pipe; tee now runs on an
  explicit descriptor whose pid is waited for after the descriptor is closed. *Reading an
  old output vs writing a new one*: an existing top-level output was linked into the run
  directory, so a failing attempt overwrote the published result; outputs are never linked,
  inputs are linked individually, directory outputs are published with copytree. *Acking an
  event vs acking a fault*: an event ack rebuilt the cursor without the acknowledged state
  ids; they are carried through.
- **One observer for the scheduler.** `status --json` queried Slurm per WAITING job on every
  call from watch and the board. The daemon now records the states it saw on each pass in
  `last_tick.json`; status serves them while the tick is fresh and queries only the rest.
- **Reading the tail of a log is not paging through it.** `events --last N` seeks from the
  end; the board no longer replays the whole audit log per refresh.
- **Independent review of 6b38783 (2026-09-20): 12 findings, all reproduced, all fixed.**
  The regressions are kept in `tests/farmkit/review/test_regressions_6b38783.py`.
  The pattern behind most of them: two states that had been treated as one.
  *Execution ended* vs *verified*: a crash after a local step finished lost the
  verification forever; the ledger now exposes `unfinalized()` and tick verifies without
  re-running. *Retry decided* vs *retry submitted*: a crash before the new `sbatch` parked
  the step as no-progress; a `retried` failure with no later attempt now makes the step
  ready again. *Snapshot recorded* vs *code executed*: jobs ran in the workspace and
  imported whatever `train.py` was there at start time; attempts now run inside their
  snapshot directory with the rest of the workspace linked in, and top-level outputs are
  published after verification. *Scanned* vs *acknowledged*: watch re-read the same page
  every poll, so 1000 quiet events hid the next SUBMITTED; the scan position now advances
  page by page while the ack cursor stays the master's, and status-derived hits carry an
  id the master can acknowledge.
- **A check that never ran certified a run.** A declared project verifier without a
  loaded module was skipped silently; `verifier_for` now raises and tick refuses to start.
  JSON `null` in RUN.json passed every check because "parsed as None" looked like "not
  parsed"; non-object payloads are now a failure with a reason.
- **Two parsers for one grammar disagreed.** watch scanned the whole ruling name for a
  class word, so task `code-study` made a science park look master-resolvable; the board
  parsed differently. One `classify.parse_ruling` now strips the exact task prefix first;
  anything that does not parse is owner-only.
- **A pid is not an identity.** `stop --now` would have signalled whatever process held
  the manifest's pid after a reboot. The manifest records the daemon's pid start time and
  `daemon_alive()` (shared by stop and status) checks host, boot id, namespace and start.
- **The session id may arrive at the end of the turn.** Claude's JSON output prints it in
  the final result; the 45 s startup capture missed long first turns. A second capture
  after `wait` covers both CLIs.
- **Adopted budget is a baseline, not a value.** `_finalize` recomputed `used` from this
  ledger's failures and overwrote the budget an adopted job brought along; `prior_used` is
  now added to the count (D34).
- **A stop control outlived the stop.** After `farm stop --drain` the manifest kept
  `phase: draining`; the restarted canary daemon ran one pass and exited, and
  `restart --to` would have been refused. Fix: reconcile startup clears a `kind: stop`
  control (event `FARM_STOP_CLEARED`) before the writer checks; node handoffs are not
  touched. Verified on the canary: rc1 → `stop --drain` → `restart --to rc2` → rc2 daemon
  ticking with `upgraded_from_source` recorded → `stop --now` exited it.
- **The restart plan duplicated a flag the wrapper injects.** `next_command` carried
  `--writer-policy pinned-host`; the pinned wrapper adds the same global option, so the
  copy after the subcommand was "unrecognized arguments". Only running the printed
  command through the real wrapper showed it; the unit test checked the tokens, not
  the wrapper's parse.
- **Canary before production (D5 SOP, 2026-09-20).** Release `0.5.0-rc1` exported from
  the merged branch, wrapper rendered from a real site profile, a scratch farm with one
  local-process worker running `farmkit tick` against two real `test`-partition jobs:
  launched → AWAITING on `job:` → auto-unblock on job end → SUBMITTED in about two
  minutes; `sacct` rows carried `attempt:<id>` in Comment; `farmkit watch --until
  task:` returned on SUBMITTED; a board-generated `task-accept` moved the task to DONE;
  `farm stop --drain` ended the loop. Nothing was rehearsed in a production farm.
- **The board's "in flight" test was wrong until a real job ran.** A job observed as
  RUNNING has a state but is not finished; the board dropped it from the jobs column.
  Rule: in flight = submitted and not `is_terminal(state)`; shared with farmkit, not a
  second definition. Caught by `farmboard --once` on the canary, then pinned in a test.
- **Workspace modules must not shadow stdlib names.** farmkit puts the workspace on
  `sys.path` for project verifiers; a `select.py` there broke `subprocess`. Documented
  in the example README; keep project scripts under descriptive names.
- **Ledger reads were O(attempts²) on NFS.** Every `latest()` re-read every record; a
  19-attempt tick took about 8 s. Reads are cached per Ledger instance until the next
  save (a workspace has one writer). About 1 s now.
- **A parked step needs an explicit release.** Without `farmkit release --step … --ruling …`
  a code-class failure could never run again after the master's ruling. One release
  grants one more attempt; the failure keeps its class so the budget still counts it.
- **A manifest written by a CLI carries the CLI's pid.** `runtime_identity()` includes
  `os.getpid()`; `farm stop --now` on such a manifest would have signalled the caller.
  Fix: refuse to signal the current pid or its parent. Verified by
  `test_stop_never_signals_the_cli_itself`.
- **A killed child is a zombie until reaped, and `/proc/<pid>` still exists.** Liveness
  must read the state field of `/proc/<pid>/stat` (Z/X = gone). Verified by
  `test_stop_now_terminates_recorded_pid_between_commits`.
- **squeue gating hid finished jobs.** `slurm_state()` only asked `sacct` after a
  successful `squeue`; a host without `squeue`, or a transient `squeue` error, kept a
  finished job UNKNOWN forever. Fix: consult `sacct` whenever `squeue` gives no single
  answer (D6). Verified by the shared vectors (`tests/vectors/slurm_states.jsonl`).
- **f-string brace doubling applied to an interpolated expression corrupts regexes.**
  The session-id grep pattern rendered as `{{36}}` in the shell script. Fix: quote
  the pattern with `shlex.quote`, no manual doubling. Caught by asserting the rendered
  script text, not by a unit test of the pattern.
- **`rmtree` on NFS can fail with "Directory not empty" on freshly written trees.**
  The release exporter no longer stages and renames; it computes the digest from the
  source, copies straight to the final directory, and re-verifies.
- **Workers waited on output files instead of jobs.** An operational review found
  repeated artifact waits for unfinished computation because the worker instructions
  did not explain the scheduler wait contract. The rule is now enforced in farmkit
  (`wait_reference` never returns `artifact:` for a job output) and in the brief lint.
