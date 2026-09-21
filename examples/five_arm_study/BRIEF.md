# T-EXAMPLE five-arm study

## Goal
Train three arms (lam0, lam0.1, lam1) at seed 1, select the arm with the lowest
finite validation metric, repeat lam0 and the winner at seed 2, and run a
two-segment AFQMC chain for every trained arm. Deliver the runs, runs2 and afqmc
directories plus the selection file, each with its verdict in the ledger.

## Boundaries
Write only inside this workspace. Do not edit steps.toml or verifiers.py during
the run; a change is a new task. Never cancel a Slurm job. Budgets and metrics in
steps.toml are the contract; do not loosen them.

## Read first
steps.toml (what the steps are), verifiers.py (what counts as a valid output), and
the notes file if an earlier session left one. Read the protocol only when a
verifier reason points at it.

## Method
On every wake run `farmkit tick --checkpoint --verifiers verifiers` and report with
EXACTLY the receipt command it prints. Do not write loops or monitors. If a step is
parked on a `ruling:`, describe its failure evidence in your checkpoint and continue
with whatever else tick lets you do. When the master's ruling tells you to release a
step, run the `farmkit release` line it gives, then tick again. Append 3-5 lines of
judgment below the marker in the checkpoint before you exit.
