from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

from .doctor import run_doctor
from .models import Task
from .shadow import codex_cwds, inspect_workspace
from .store import FarmPaths, TaskStore


def farm_root(project: Path) -> Path:
    return project.resolve() / ".farm"


def cmd_init(args: argparse.Namespace) -> int:
    root = farm_root(Path(args.project))
    paths = FarmPaths(root)
    paths.ensure()
    print(f"initialized {root}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    paths = FarmPaths(farm_root(Path(args.project)))
    paths.ensure()
    tasks = TaskStore(paths).list()
    counts: dict[str, int] = {}
    for task in tasks:
        counts[task.state.value] = counts.get(task.state.value, 0) + 1
    if not tasks:
        print("no tasks")
        return 0
    for state in sorted(counts):
        print(f"{state:10s} {counts[state]}")
    return 0


def cmd_task_create(args: argparse.Namespace) -> int:
    paths = FarmPaths(farm_root(Path(args.project)))
    paths.ensure()
    task_id = args.id or f"T-{uuid.uuid4().hex[:8]}"
    metadata: dict = {}
    if args.command:
        metadata["command"] = args.command
    if args.cwd:
        metadata["cwd"] = args.cwd
    task = Task(
        id=task_id,
        objective=args.objective,
        deliverable=args.deliverable,
        acceptance=args.acceptance,
        metadata=metadata,
    )
    TaskStore(paths).create(task)
    print(task_id)
    return 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    import time

    from .adapters.local_process import LocalProcessExecutor
    from .reconciler import Reconciler

    paths = FarmPaths(farm_root(Path(args.project)))
    paths.ensure()
    executor = LocalProcessExecutor(paths.runtime)
    reconciler = Reconciler(paths, executor)

    def one_pass() -> None:
        rep = reconciler.reconcile_once()
        print(json.dumps({
            "launched": rep.launched,
            "adopted": rep.adopted,
            "advanced": rep.advanced,
            "resumed": rep.resumed,
            "heartbeats": rep.heartbeats,
            "ignored_stale": rep.ignored_stale,
        }, sort_keys=True))

    if not args.loop:
        one_pass()
        return 0
    while True:
        one_pass()
        time.sleep(args.interval)


def cmd_task_list(args: argparse.Namespace) -> int:
    paths = FarmPaths(farm_root(Path(args.project)))
    paths.ensure()
    for task in TaskStore(paths).list():
        print(f"{task.id}\t{task.state.value}\t{task.objective}")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    paths = FarmPaths(farm_root(Path(args.project)))
    paths.ensure()
    checks = run_doctor(paths)
    for check in checks:
        print(f"{check.level:4s}  {check.message}")
    return 1 if any(check.level == "FAIL" for check in checks) else 0


def cmd_shadow(args: argparse.Namespace) -> int:
    root = Path(args.workspaces).resolve()
    live = codex_cwds()
    workspaces = [p for p in sorted(root.iterdir()) if p.is_dir()]
    records = [inspect_workspace(p, live) for p in workspaces]
    for record in records:
        print(json.dumps(record.to_dict(), sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="farm")
    parser.add_argument("--project", default=".", help="research project containing .farm (default: .)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="initialize project-local durable farm state")
    p.set_defaults(func=cmd_init)
    p = sub.add_parser("status", help="summarize authoritative task state")
    p.set_defaults(func=cmd_status)
    p = sub.add_parser("task-create", help="create a READY task contract")
    p.add_argument("--id")
    p.add_argument("--objective", required=True)
    p.add_argument("--deliverable", required=True)
    p.add_argument("--acceptance", required=True)
    p.add_argument("--command", help="worker command (metadata.command) for the local-process executor")
    p.add_argument("--cwd", help="working directory for the worker command")
    p.set_defaults(func=cmd_task_create)
    p = sub.add_parser("task-list", help="list task contracts")
    p.set_defaults(func=cmd_task_list)
    p = sub.add_parser("doctor", help="check durable-state invariants")
    p.set_defaults(func=cmd_doctor)
    p = sub.add_parser("shadow", help="read-only shadow observation of existing workspaces")
    p.add_argument("workspaces")
    p.set_defaults(func=cmd_shadow)
    p = sub.add_parser("reconcile", help="run the actuating control loop (local-process executor)")
    p.add_argument("--loop", action="store_true", help="run continuously instead of one pass")
    p.add_argument("--interval", type=float, default=10.0, help="seconds between passes in --loop")
    p.set_defaults(func=cmd_reconcile)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
