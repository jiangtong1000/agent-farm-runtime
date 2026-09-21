from __future__ import annotations

import argparse
import json
import os
import uuid
from dataclasses import asdict
from pathlib import Path

from .adapters.filesystem import FileLockBusy, FilesystemCapabilityError
from .doctor import run_doctor
from .locking import ReconcilerBusy
from .models import Task
from .shadow import codex_cwds, inspect_workspace
from .store import FarmPaths, StoreError, TaskStore, atomic_write_json


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
    if getattr(args, "json", False):
        from .status import farm_status
        print(json.dumps(farm_status(paths), indent=2, sort_keys=True))
        return 0
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


def cmd_events(args: argparse.Namespace) -> int:
    from .status import events_after
    paths = FarmPaths(farm_root(Path(args.project)))
    try:
        cursor = int(args.after)
    except (TypeError, ValueError):
        raise StoreError("--after expects the integer cursor returned by a previous call")
    if args.last:
        from .status import events_tail
        result = events_tail(paths, args.last)
    else:
        result = events_after(paths, cursor, limit=args.limit)
    result["cursor"] = str(result["cursor"])
    print(json.dumps(result, sort_keys=True))
    return 0


def cmd_task_create(args: argparse.Namespace) -> int:
    from .provenance import require_compatible_writer
    paths = FarmPaths(farm_root(Path(args.project)))
    require_compatible_writer(paths, policy=args.writer_policy)
    paths.ensure()
    task_id = args.id or f"T-{uuid.uuid4().hex[:8]}"
    metadata: dict = {}
    if args.command:
        metadata["command"] = args.command
    if args.cwd:
        metadata["cwd"] = args.cwd
    if args.workspace:
        workspace = Path(args.workspace).resolve(strict=True)
        if not workspace.is_dir():
            raise StoreError("workspace must be an existing directory")
        metadata["workspace"] = str(workspace)
    brief = args.brief
    if args.brief_file:
        from .master import snapshot_file
        metadata["brief_source"] = snapshot_file(args.brief_file)
        brief = metadata["brief_source"]["text"]
    if args.context_file:
        from .master import snapshot_file
        contexts = [snapshot_file(path) for path in args.context_file]
        if sum(len(c["text"].encode()) for c in contexts) > 131072:
            raise StoreError("selected context exceeds 128 KiB; select only required methods")
        metadata["context_manifest"] = contexts
        brief = (brief or "") + "\n\nPINNED TASK CONTEXT (master-selected; snapshot at creation):\n"
        for context in contexts:
            brief += f"\nSource: {context['path']}\n{context['text']}\n"
    if brief:
        metadata["brief"] = brief
    if args.agent_label:
        metadata["agent_label"] = args.agent_label
    if args.executor:
        if args.executor not in EXECUTOR_NAMES:
            raise StoreError(f"unknown executor {args.executor!r}; choose from {', '.join(EXECUTOR_NAMES)}")
        metadata["executor"] = args.executor
    if args.resume_mode:
        metadata["resume_mode"] = args.resume_mode
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


def cmd_task_show(args: argparse.Namespace) -> int:
    store = TaskStore(FarmPaths(farm_root(Path(args.project))))
    task = store.get(args.id)
    payload = task_summary(task) if getattr(args, "summary", False) else task.to_dict()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def task_summary(task: Task) -> dict:
    """A bounded read view, not another state store or acceptance decision."""
    def preview(value, limit=320):
        if value is None:
            return None
        text = str(value)
        return text if len(text) <= limit else text[:limit] + "… [truncated; read full task]"

    def reference(record):
        if not isinstance(record, dict):
            return {"preview": preview(record)} if record else None
        return {k: preview(record.get(k)) for k in ("path", "actor", "ts") if record.get(k)}

    meta = task.metadata
    contexts = meta.get("context_manifest") or []
    instruction = meta.get("latest_master_instruction")
    return {
        "id": task.id, "state": task.state.value, "revision": meta.get("revision", 0),
        "objective": preview(task.objective), "deliverable": preview(task.deliverable),
        "acceptance_preview": preview(task.acceptance, 640),
        "workspace": preview(meta.get("workspace") or meta.get("cwd")),
        "lease": task.lease.__dict__ if task.lease else None,
        "waiting_on": preview(meta.get("waiting_on")), "outcome": preview(meta.get("outcome")),
        "last_receipt_note": preview(meta.get("last_receipt_note")),
        "runtime_error": preview(meta.get("runtime_error")),
        "recovery_hold": preview(meta.get("recovery_hold")),
        "rotation_request": {k: meta["rotation_request"].get(k) for k in ("id", "actor", "lease_id")}
                            if meta.get("rotation_request") else None,
        "continuation_checkpoint": reference((meta.get("clean_surrender") or {}).get("checkpoint")),
        "latest_instruction": reference(instruction),
        "instruction_preview": preview(instruction.get("text")) if isinstance(instruction, dict) else None,
        "acceptance_evidence": reference(meta.get("acceptance_receipt")),
        "dispatches": {"workers": list((meta.get("dispatches") or {}).get("workers") or [])},
        "context_references": [reference(item) for item in contexts[:8]],
        "context_references_omitted": max(0, len(contexts) - 8),
        "full_text_omitted": True,
        "detail": f"task-show {task.id} (without --summary); review the full contract before deciding",
    }


def cmd_decision(args: argparse.Namespace) -> int:
    from .master import record_decision
    from .provenance import require_compatible_writer
    store = TaskStore(FarmPaths(farm_root(Path(args.project))))
    require_compatible_writer(store.paths, policy=args.writer_policy)
    saved = record_decision(store, args.id, expected_revision=args.expected_revision,
                            action=args.action, evidence_file=args.evidence_file,
                            actor=args.actor, outcome=getattr(args, "outcome", None),
                            acceptance_file=getattr(args, "acceptance_file", None))
    print(json.dumps({"id": saved.id, "state": saved.state.value,
                      "revision": saved.metadata["revision"]}, sort_keys=True))
    return 0


def cmd_version(args: argparse.Namespace) -> int:
    from .provenance import runtime_identity
    print(json.dumps(runtime_identity(), indent=2, sort_keys=True))
    return 0


def cmd_recover(args: argparse.Namespace) -> int:
    from .recovery import recover_farm
    paths = FarmPaths(farm_root(Path(args.project)))
    result = recover_farm(paths, apply=args.apply, expected_plan=args.expected_plan,
                          actor=args.actor, evidence_file=args.evidence_file,
                          attest_stopped=args.attest_stopped)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def cmd_rotate(args: argparse.Namespace) -> int:
    from .provenance import require_compatible_writer
    from .turnover import request_rotation
    paths = FarmPaths(farm_root(Path(args.project)))
    require_compatible_writer(paths, policy=args.writer_policy)
    task = request_rotation(TaskStore(paths), args.id, expected_revision=args.expected_revision,
                            request_id=args.request_id, actor=args.actor, checkpoint_file=args.checkpoint_file)
    print(json.dumps(task_summary(task), sort_keys=True))
    return 0


EXECUTOR_NAMES = ("local-process", "codex-tmux", "claude-tmux")


def _executor_factories(paths: FarmPaths, session: str, tmux_socket: str | None) -> dict:
    def codex():
        from .adapters.codex import CodexTmuxExecutor
        return CodexTmuxExecutor(paths.runtime, session=session, tmux_socket=tmux_socket)

    def claude():
        from .adapters.claude import ClaudeTmuxExecutor
        return ClaudeTmuxExecutor(paths.runtime, session=session, tmux_socket=tmux_socket)

    def local():
        from .adapters.local_process import LocalProcessExecutor
        return LocalProcessExecutor(paths.runtime)

    return {"codex-tmux": codex, "claude-tmux": claude, "local-process": local}


def _executor(paths: FarmPaths, name: str, session: str, tmux_socket: str | None):
    """The farm's executor: routes per task (metadata.executor), defaulting to `name`."""
    if name not in EXECUTOR_NAMES:
        raise StoreError("unknown recorded executor; inspect deployment")
    from .adapters.multi import MultiExecutor
    return MultiExecutor(name, _executor_factories(paths, session, tmux_socket), TaskStore(paths))


def cmd_handoff(args: argparse.Namespace) -> int:
    from .turnover import claim_farm, deployment, drain_farm, handoff_status, release_farm
    paths = FarmPaths(farm_root(Path(args.project)))
    if args.action == "status":
        result = handoff_status(paths)
    elif args.action == "drain":
        result = drain_farm(paths, request_id=args.request_id, target_host=args.target_host, actor=args.actor)
    elif args.action == "release":
        manifest = deployment(paths)
        executor = _executor(paths, manifest.get("executor"), manifest.get("session", "farm2"),
                             manifest.get("tmux_socket"))
        result = release_farm(paths, executor, request_id=args.request_id, actor=args.actor)
    else:
        result = claim_farm(paths, request_id=args.request_id, actor=args.actor)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    from .lifecycle import stop_drain, stop_now
    from .provenance import require_compatible_writer
    paths = FarmPaths(farm_root(Path(args.project)))
    require_compatible_writer(paths, policy=args.writer_policy)
    if args.now:
        result = stop_now(paths, actor=args.actor)
    else:
        result = stop_drain(paths, actor=args.actor)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def cmd_restart(args: argparse.Namespace) -> int:
    from .lifecycle import restart_plan, stop_now
    from .provenance import require_compatible_writer
    paths = FarmPaths(farm_root(Path(args.project)))
    require_compatible_writer(paths, policy=args.writer_policy)
    plan = restart_plan(paths, target_src=Path(args.to), actor=args.actor)
    if not args.plan_only:
        plan["stop"] = stop_now(paths, actor=args.actor)
    print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


def tick_acted(report: dict) -> bool:
    """Did this pass change anything worth a log line? A RUNNING worker's heartbeat is
    routine observation, not an action; counting it would print every tick (D15)."""
    return any(v for k, v in report.items() if k != "heartbeats")


def cmd_reconcile(args: argparse.Namespace) -> int:
    import time
    import math

    if not math.isfinite(args.interval) or args.interval <= 0:
        raise ValueError("reconcile interval must be finite and positive")

    from .locking import ReconcilerBusy, single_reconciler
    from .reconciler import Reconciler

    paths = FarmPaths(farm_root(Path(args.project)))
    paths.ensure()

    from datetime import datetime, timezone
    from .status import write_last_tick
    heartbeat_every = 3600.0
    last_print = [0.0]

    def one_pass() -> None:
        observed_this_pass.clear()
        rep = reconciler.reconcile_once()
        report = asdict(rep)
        write_last_tick(paths, report, observed_jobs=dict(observed_this_pass), deployment=started_deployment)
        # Quiet log in --loop mode (D15): print only ticks that did something, plus an
        # hourly heartbeat. A single explicit pass always prints its full report.
        acted = tick_acted(report)
        now = time.monotonic()
        if not args.loop or acted or now - last_print[0] >= heartbeat_every:
            line = report if (acted or not args.loop) else {"heartbeat": datetime.now(timezone.utc).isoformat()}
            print(json.dumps(line, sort_keys=True), flush=True)
            last_print[0] = now

    # INV-6: at most one reconciler mutates a farm at a time.
    try:
        with single_reconciler(paths.runtime):
            from datetime import datetime, timezone
            from .provenance import farm_identity, require_compatible_writer, require_local_executor_host, runtime_identity
            upgrade = getattr(args, "upgrade_from_source", None)
            from .locking import task_mutation_lock
            from .turnover import deployment, execution_epoch, finish_deployment_event, require_task_writer
            with task_mutation_lock(paths.runtime):
                # Serialize startup checks/manifest publication with drain and
                # master commits, before initializing any executor.
                require_compatible_writer(paths, policy=args.writer_policy, upgrade_from_source=upgrade)
                require_local_executor_host(paths)
                previous = finish_deployment_event(paths)
                from .lifecycle import clear_stop_control
                cleared = clear_stop_control(paths, actor="reconciler")   # a finished stop is over once we start again
                if cleared:
                    previous = deployment(paths)
                require_task_writer(paths, upgrade_from_source=upgrade)
                # A restart cannot erase a drain/release or change its backend.
                if previous.get("handoff") and any(previous.get(k) != getattr(args, k)
                                                   for k in ("executor", "session", "tmux_socket")):
                    raise StoreError("handoff requires unchanged executor/session/socket configuration")
                executor = _executor(paths, args.executor, args.session, args.tmux_socket)
                unblock = None
                observed_this_pass: dict[str, str | None] = {}
                if args.auto_unblock:
                    from .adapters.slurm import slurm_state
                    from .observers import make_unblock

                    def recording_slurm(job_id: str, _q=slurm_state):
                        state = _q(job_id)
                        observed_this_pass[str(job_id)] = state
                        return state
                    unblock = make_unblock(paths, slurm=recording_slurm)
                reconciler = Reconciler(paths, executor, grace_seconds=args.grace_seconds, unblock=unblock,
                                        max_auto_restarts=args.max_auto_restarts)
                from .procutil import proc_starttime
                started_deployment = {
                    **previous, **runtime_identity(), "started_at": datetime.now(timezone.utc).isoformat(),
                    **farm_identity(str(paths.root.resolve())), "execution_epoch": execution_epoch(previous),
                    "pid_starttime": proc_starttime(os.getpid()),
                    **({"upgraded_from_source": upgrade} if upgrade is not None else {}),
                    "executor": args.executor, "session": args.session,
                    "tmux_socket": args.tmux_socket, "auto_unblock": args.auto_unblock,
                    "loop": args.loop, "writer_policy": args.writer_policy,
                    "grace_seconds": args.grace_seconds, "interval": args.interval,
                    "max_auto_restarts": args.max_auto_restarts,
                }
                atomic_write_json(paths.runtime / "deployment.json", started_deployment)
            if not args.loop:
                one_pass()
                return 0
            while True:
                one_pass()
                control = deployment(paths).get("handoff", {})
                if control.get("phase") == "draining" and not any(t.lease for t in TaskStore(paths).list()):
                    print(json.dumps({"drain_complete": datetime.now(timezone.utc).isoformat(), "control": control.get("id"),
                                      "kind": control.get("kind"), "exiting": True}, sort_keys=True), flush=True)
                    return 0  # release verifies unleased/late-exiting invocations separately
                time.sleep(args.interval)
    except ReconcilerBusy as exc:
        print(f"reconciler busy: {exc}")
        return 1


def cmd_task_list(args: argparse.Namespace) -> int:
    paths = FarmPaths(farm_root(Path(args.project)))
    for task in TaskStore(paths).list():
        print(f"{task.id}\t{task.state.value}\t{task.objective}")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    paths = FarmPaths(farm_root(Path(args.project)))
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
    parser.add_argument("--writer-policy", choices=["compatible", "pinned-host"], default="compatible",
                        help="protocol compatibility, or opt-in same-host/source pinning for a deployment")
    sub = parser.add_subparsers(dest="command", required=True)

    from .access.cli import add_parser as add_access_parser
    add_access_parser(sub)

    p = sub.add_parser("init", help="initialize project-local durable farm state")
    p.set_defaults(func=cmd_init)
    p = sub.add_parser("status", help="summarize authoritative task state")
    p.add_argument("--json", action="store_true",
                   help="structured read-only status: deployment identity, daemon liveness, last tick, task counts")
    p.set_defaults(func=cmd_status)
    p = sub.add_parser("events", help="read-only: audit events after a cursor (byte offset into events/log.ndjson)")
    p.add_argument("--after", default="0", help="cursor returned by a previous call (default: from the start)")
    p.add_argument("--limit", type=int, default=1000, help="maximum events to return")
    p.add_argument("--last", type=int, help="read-only: the last N events from the end of the log (no full scan)")
    p.set_defaults(func=cmd_events)
    p = sub.add_parser("task-create", help="create a READY task contract")
    p.add_argument("--id")
    p.add_argument("--objective", required=True)
    p.add_argument("--deliverable", required=True)
    p.add_argument("--acceptance", required=True)
    p.add_argument("--command", help="worker command (metadata.command) for the local-process executor")
    p.add_argument("--cwd", help="working directory for the worker command")
    p.add_argument("--workspace", help="abs workspace dir (metadata.workspace) for the codex-tmux executor")
    p.add_argument("--brief", help="agent-facing brief (metadata.brief) for the codex-tmux executor")
    p.add_argument("--brief-file", help="read the agent brief from this file")
    p.add_argument("--context-file", action="append", default=[],
                   help="snapshot and inject one selected harness/context file; repeat as needed")
    p.add_argument("--agent-label", help="tmux window name for the codex-tmux worker")
    p.add_argument("--executor", choices=list(EXECUTOR_NAMES),
                   help="worker backend for this task (default: the farm's --executor)")
    p.add_argument("--resume-mode", choices=["resume", "fresh"],
                   help="resume the saved agent session (default) or start a fresh session on every wake")
    p.set_defaults(func=cmd_task_create)
    p = sub.add_parser("task-list", help="list task contracts")
    p.set_defaults(func=cmd_task_list)
    p = sub.add_parser("task-show", help="read full authoritative task, including metadata.revision")
    p.add_argument("id")
    p.add_argument("--summary", action="store_true", help="bounded read view; omit full brief/context/evidence text")
    p.set_defaults(func=cmd_task_show)
    p = sub.add_parser("version", help="show imported runtime path, source digest, and protocol")
    p.set_defaults(func=cmd_version)
    p = sub.add_parser("recover", help="preview offline host/protocol recovery; never launches or signals workers")
    p.add_argument("--apply", action="store_true", help="apply the reviewed plan under both writer locks")
    p.add_argument("--expected-plan", metavar="SHA256", help="exact digest returned by the read-only preview")
    p.add_argument("--actor", help="operator audit label, not authentication")
    p.add_argument("--evidence-file", help="nonempty UTF-8 shutdown/fencing evidence, snapshotted before changes")
    p.add_argument("--attest-stopped", action="store_true",
                   help="attest ALL old farm writers/workers and queued dispatches are stopped/fenced; not external compute")
    p.set_defaults(func=cmd_recover)
    p = sub.add_parser("task-rotate", help="request a checkpoint and fresh context without lifting task holds")
    p.add_argument("id")
    p.add_argument("--expected-revision", type=int, required=True)
    p.add_argument("--request-id", required=True, help="stable unique ID; reuse after master context turnover")
    p.add_argument("--actor", required=True)
    p.add_argument("--checkpoint-file", help="attach/reuse checkpoint for an already WAITING worker only")
    p.set_defaults(func=cmd_rotate)
    p = sub.add_parser("handoff", help="planned node turnover; no job allocation or external compute cancellation")
    actions = p.add_subparsers(dest="action", required=True)
    for action in ("status", "drain", "release", "claim"):
        command = actions.add_parser(action)
        if action != "status":
            command.add_argument("--request-id", required=True)
            command.add_argument("--actor", required=True)
        if action == "drain":
            command.add_argument("--target-host", required=True)
        command.set_defaults(func=cmd_handoff)
    for action in ("accept", "ruling", "amend", "rework"):
        p = sub.add_parser(f"task-{action}", help=f"record a master {action} with evidence and optimistic locking")
        p.add_argument("id")
        p.add_argument("--expected-revision", type=int, required=True)
        p.add_argument("--actor", required=True, help="decision author identity, e.g. reviewer-1 (audit label, not authentication)")
        p.add_argument("--evidence-file", required=True, help="nonempty UTF-8 decision/verification note (snapshotted)")
        if action == "accept":
            p.add_argument("--outcome", default="accepted", help="project-defined acceptance label (default: accepted)")
        if action == "amend":
            p.add_argument("--acceptance-file", required=True, help="complete replacement effective acceptance contract")
        p.set_defaults(func=cmd_decision, action=action)
    p = sub.add_parser("doctor", help="check durable-state invariants")
    p.set_defaults(func=cmd_doctor)
    p = sub.add_parser("shadow", help="read-only shadow observation of existing workspaces")
    p.add_argument("workspaces")
    p.set_defaults(func=cmd_shadow)
    p = sub.add_parser("stop", help="stop the daemon: --drain (withhold dispatch, workers check point, loop exits) or --now (SIGTERM between commits)")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--drain", action="store_true")
    mode.add_argument("--now", action="store_true")
    p.add_argument("--actor", required=True)
    p.set_defaults(func=cmd_stop)
    p = sub.add_parser("restart", help="validate a same-protocol release, stop the daemon, print the start command")
    p.add_argument("--to", required=True, metavar="SRC_DIR", help="candidate release's src/ directory")
    p.add_argument("--actor", required=True)
    p.add_argument("--plan-only", action="store_true", help="validate and print the command without stopping")
    p.set_defaults(func=cmd_restart)
    p = sub.add_parser("reconcile", help="run the actuating control loop")
    p.add_argument("--upgrade-from-source", metavar="SHA256",
                   help="explicit same-host/protocol source upgrade from this recorded digest; requires pinned-host and stopped writers")
    p.add_argument("--executor", choices=list(EXECUTOR_NAMES),
                   default="local-process", help="default worker backend; tasks may name another via metadata.executor")
    p.add_argument("--session", default="farm2", help="tmux session for codex-tmux (never the live `farm`)")
    p.add_argument("--tmux-socket", default=None, help="dedicated tmux server socket (-L) for isolation")
    p.add_argument("--grace-seconds", type=float, default=60.0,
                   help="minimum heartbeat/launch age before replacing a positively observed dead worker")
    p.add_argument("--max-auto-restarts", type=int, default=3,
                   help="automatic replacements after confirmed death; then wait for an explicit ruling")
    p.add_argument("--auto-unblock", dest="auto_unblock", action="store_true", default=True,
                   help="resume WAITING tasks whose named job/artifact/task condition is met (default on)")
    p.add_argument("--no-auto-unblock", dest="auto_unblock", action="store_false",
                   help="do not auto-resume WAITING tasks (master resumes them)")
    p.add_argument("--loop", action="store_true", help="run continuously instead of one pass")
    p.add_argument("--interval", type=float, default=10.0, help="seconds between passes in --loop")
    p.set_defaults(func=cmd_reconcile)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except (StoreError, FileLockBusy, ReconcilerBusy, FilesystemCapabilityError, ValueError, OSError) as exc:
        parser.exit(1, f"farm: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
