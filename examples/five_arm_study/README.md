# five_arm_study — a complete farmkit project

This directory is the reference shape of a project that runs under the farm. It is
also the fixture of `tests/integration/test_five_arm_study.py`, which drives it end
to end with the real runtime, the real farmkit and a fake Slurm.

| file | role |
|---|---|
| `steps.toml` | the experiment as data: three matrix arms in group `round1`, a local `select`, two second-round arms, two-segment `afterok` chains per arm with `concurrency = 2` |
| `verifiers.py` | project checks farmkit calls after its own defaults (attempt match, required files, finite metrics) |
| `train.sbatch`, `segment.sbatch`, `select_arm.py`, `train.py` | the science scripts; farmkit copies them into `attempts/<id>/code/` per attempt. Workspace modules must not shadow stdlib names (a `select.py` breaks `subprocess`), because farmkit puts the workspace on `sys.path` for verifiers |
| `BRIEF.md` | the ≤2 KB prompt a worker gets (four sections: Goal, Boundaries, Read first, Method) |

Each attempt runs inside its snapshot directory (`attempts/<id>/code/`, the job's
working directory). `train.py` and the `.sbatch` files are frozen copies; declared
inputs are linked in; outputs (`runs/…`, `afqmc/…`, `selection.json`) are written in
the run directory and copied to the workspace once verified, so a failed attempt never
replaces an earlier result. Steps that read earlier outputs should declare them as
`inputs` (undeclared top-level directories are linked as a convenience only). A chain
segment reads its predecessor's `SEGMENT_END.json` at the relative path in its run
directory, which is bound to the predecessor's attempt, not to the workspace copy. Declared project verifiers must resolve: `farmkit tick` refuses to start
when `verify = "…"` names a function the `--verifiers` module does not provide.

To copy this for a real project:

1. Copy the directory, replace the scripts with yours, and make every output JSON
   carry `{"provenance": {"attempt_id": "$FARMKIT_ATTEMPT_ID"}}` (farmkit exports
   that variable into every job and local step).
2. Edit `steps.toml`: step names, matrix values, `outputs`, `finite`, groups,
   `after`, chains. Run `farmkit steps check --verifiers verifiers`.
3. Write the verifiers you actually need in `verifiers.py` (often none beyond the
   defaults). Keep science judgment for the review at SUBMITTED.
4. `farmkit brief lint BRIEF.md`, then register the task with the runtime:
   `farm task-create --workspace <dir> --brief-file BRIEF.md ...`.
5. The worker's whole job is: `farmkit tick --checkpoint --verifiers verifiers`,
   then run the receipt command tick printed, then exit.
