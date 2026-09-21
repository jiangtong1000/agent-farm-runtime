"""farmkit: the worker-side execution layer of the agent farm.

Everything between "the agent decides to run a step" and "the agent reports to the
runtime": intent + submit, wait selection, scheduler observation, attempt ledger,
code snapshots, default verification, failure evidence, and the step runner.

Boundaries (DECISIONS D1, D35, D41):
  * never imports agent_farm_runtime; never reads or writes a farm's .farm/ directory
  * writes only inside the task workspace (attempts/, STATUS.md, CHECKPOINT.md)
  * runtime state, when needed (watch/health/board), comes from the read-only farm CLI
"""
__version__ = "0.1.0.dev0"
