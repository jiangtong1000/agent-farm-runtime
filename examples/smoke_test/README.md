# Synthetic runtime smoke test

One local step and one five-minute Slurm step each write a result with current
attempt provenance and metric `1.0`. The task exercises dispatch, receipts,
`job:` waiting, automatic resume, verification, submission, watch and acceptance.
No model or scientific inputs are needed.

Copy this directory into a fresh workspace. Replace `PARTITION` and `ACCOUNT` in
`smoke.sbatch`, activate the installed runtime Python environment, and configure
the site's `FARMKIT_SITE` profile as described in [operations](../../docs/OPERATIONS.md).
The same environment's `python` must be available on the compute node.

Use absolute paths for these shell variables:

```bash
repo=/path/to/agent-farm-runtime
ws=/shared/workspaces/runtime-smoke
project=/shared/farms/runtime-smoke
farmkit brief lint "$ws/BRIEF.md"
farmkit steps check --workspace "$ws"
farm --project "$project" init
farm --project "$project" task-create --id T-SMOKE \
  --objective 'Exercise one local and one Slurm attempt' \
  --deliverable 'results/local.json and results/slurm.json' \
  --acceptance 'Both current attempts verify; each metric equals 1.0' \
  --workspace "$ws" --cwd "$ws" --brief-file "$ws/BRIEF.md" \
  --executor local-process \
  --command "python '$repo/tools/local_worker.py' --workspace '$ws'"
farm --project "$project" reconcile --executor local-process --loop --interval 5
```

Run the reconciler in its own terminal on the farm host. In the Master's terminal,
`farmkit watch --project "$project" --until task:T-SMOKE` reports submission.
Review both results and their attempt verdicts. Write an acceptance note outside
`.farm/`, get the current revision using `farm task-show T-SMOKE --summary` with
the same `--project`, then call `farm task-accept T-SMOKE --expected-revision REV
--actor master --evidence-file /path/to/note.md` with that `--project`.
The expected final state is `DONE`. Stop this smoke farm's reconciler using the
normal `farm stop --now --actor master` command with the same `--project`.

`tools/local_worker.py` runs exactly one tick and its printed receipt command per
wake. It installs the runtime's receipt helper and leaves `worker_last_tick.out`
and `worker_last_receipt.out` for debugging. Failures park for review; this worker does
not interpret or apply scientific rulings.

The repository test runs this exact worker and batch script with fake scheduler
commands, in temporary directories, without a Slurm allocation:

```bash
python -m pytest tests/integration/test_smoke_example.py -q
```
