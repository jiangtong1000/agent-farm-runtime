# T-SMOKE — runtime plumbing check

## Goal
Verify one local step and one Slurm step. Both results must carry their own
attempt provenance and a finite metric equal to 1.0.

## Boundaries
Write only in this workspace. Never cancel external jobs or edit .farm/.
Use the configured five-minute CPU allocation; do not expand the budget.

## Read first
steps.toml and write_result.py; CHECKPOINT.md if it exists.

## Method
On every wake run `farmkit tick --checkpoint`, read its summary, execute the
receipt command it prints, and exit. CHECKPOINT.md and attempts/ hold progress.
The Master reviews both verified results before accepting the task.
