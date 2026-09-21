"""The on-disk protocol of a farm: the only part of the runtime whose change means
"retire this farm and start a new one" instead of "restart the daemon" (D5).

Anything here is read or written by every cooperating writer of a live farm:
the daemon, the CLI, and the receipt helper already copied into worker
workspaces. Changing it under a running farm mixes incompatible writers.
Everything else in the package (policies, observers, executors, CLI output)
can change freely and is picked up by a restart.

Contents are re-exported from their historical modules so imports stay stable.
"""
from ..models import Lease, Receipt, ReceiptStatus, Task, TaskState, Worker, WorkerState  # noqa: F401
from ..transitions import LEGAL_TRANSITIONS  # noqa: F401

PROTOCOL_VERSION = 4

# Files under <farm>/.farm/ whose layout is part of the protocol.
PROTOCOL_PATHS = (
    "tasks/<task_id>.json",          # authoritative Task (models.Task)
    "workers/<worker_id>.json",      # observed Worker (models.Worker)
    "events/log.ndjson",             # append-only Event records (models.Event)
    "decisions/",                    # reserved
    "runtime/deployment.json",       # daemon manifest, handoff control
    "runtime/pending-task-commit.json",
    "runtime/pending-recovery.json",
    "runtime/receipts/<worker_id>.json",  # Receipt written by .farm_receipt.py
)

# Receipt fields the worker-side helper writes; the daemon must accept a receipt
# carrying only these (older helpers stay valid across a same-protocol restart).
RECEIPT_REQUIRED_FIELDS = ("worker_id", "task_id", "lease_id", "status", "ts")
RECEIPT_OPTIONAL_FIELDS = ("note", "waiting_on", "rotation_id", "checkpoint")

__all__ = [
    "PROTOCOL_VERSION", "PROTOCOL_PATHS", "RECEIPT_REQUIRED_FIELDS", "RECEIPT_OPTIONAL_FIELDS",
    "Lease", "Receipt", "ReceiptStatus", "Task", "TaskState", "Worker", "WorkerState", "LEGAL_TRANSITIONS",
]
