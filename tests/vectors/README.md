# Shared scheduler test vectors

`slurm_states.jsonl`: one scenario per line. `squeue`/`sacct` describe what the two
commands would return for a single job id; `expect` is the state the runtime's
`slurm_state()` must return and how `slurm_job_terminal` / `slurm_job_active` must
classify it. Entries with `expect_pre_d6` document the behaviour before decision D6
(sacct consulted even when squeue is missing or fails); tests select which column
applies to the code under test.

Both the runtime (`agent_farm_runtime.adapters.slurm`) and farmkit (`farmkit.observe`)
read this file, so the two packages cannot drift in how they read the scheduler.
