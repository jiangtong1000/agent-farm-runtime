# Skill: executing a farm task with farmkit

Use when you are a worker under the agent-farm runtime (FARM_* variables set) and
the task has a steps.toml.

**The loop.** Each time you are launched or woken: run
`farmkit tick --workspace <ws> --checkpoint`. Read the ≤30-line summary: current
steps, in-flight jobs, last verdicts, next action, changed files. Then run the
receipt command it printed, exactly. Exit. You never sleep, poll, or loop.

**What tick does for you.** Snapshots the step's code per attempt, writes the
submit intent before `sbatch`, tags the job with the attempt id, records the job
id, chooses the wait (`job:<id>`; never an output file), observes finished jobs,
verifies outputs (produced by this attempt, required files present, declared
metrics finite, optional task checks), classifies failures, writes FAILURE.md,
resubmits `infra` failures once, and parks the rest on `ruling:` — but only after
all other work that can proceed has been started.

**Where a step runs.** Inside its own run directory, `attempts/<id>/code/`, which is
the job's working directory (`FARMKIT_WORKSPACE` and `FARMKIT_RUN_DIR` are exported).
Three kinds of path, one rule each: *code* matched by the step's `snapshot` globs is a
frozen copy (it never changes after submission); *declared `inputs`* are linked from the
workspace at the same relative path; *declared `outputs`* are never linked, their parent
directories exist in the run directory, the step writes there, and farmkit copies them
to the workspace only after the verdict is ok, so a failed attempt cannot overwrite an
earlier result. Other top-level workspace entries are linked as a convenience. Code a
step imports must be covered by `snapshot`, or it runs from the mutable link.

**Chains.** A segment released by `afterok` runs before any worker has verified its
predecessor, so it must read the predecessor's outputs at their relative path in its own
run directory: farmkit binds those paths to the *producer attempt*. The workspace copy
(`$FARMKIT_WORKSPACE/...`) is the verified, published result and may still be the previous
round's; never use it for in-chain handoff. An input may not name an output or sit inside
one; outputs may not be `.`, absolute, contain `..`, or point into `attempts/` (`farmkit
steps check` reports these, `farmkit tick` refuses to start).

**What is yours.** Judgment. When tick parks a step, read the FAILURE.md it names
and add 3–5 lines under the marker in CHECKPOINT.md: what you think happened,
what you would try, what you need decided. When new files appear in the summary's
"changed" list, read them.

**Never.** Wait on an artifact for a job's output (`artifact:` is for files a human
places). Report `FAILED` for anything recoverable (`FAILED` is terminal; parking is
not). Edit `.farm/`. Re-run a step by hand outside tick. Cancel jobs.

**Failure classes.** `infra` (node failure, preemption): one automatic resubmit.
`code` (traceback, missing output, wrong attempt): park; the master may rule.
`science` (non-finite metric, a scientific check failed) and `budget` (time cap):
park; the Owner decides. Thresholds are never loosened to pass.

**Memory.** The ledger (attempts/) is the truth about what ran. CHECKPOINT.md is the
truth about what you were thinking. Your conversation is neither; it may be
compacted or replaced by a fresh session at any wake.
