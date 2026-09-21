"""farmkit command line.

  farmkit tick [--checkpoint] [--workspace DIR]     one idempotent runner pass; prints the summary
                                                     and the exact receipt command (never runs it)
  farmkit verify --step ID [--workspace DIR]        re-run verification of a step's latest attempt
  farmkit release --step ID --ruling TEXT           lift a parked failure after a ruling (one more attempt)
  farmkit reconcile-intents [--workspace DIR]       resolve submissions whose job id was never saved
  farmkit adopt --step ID --job JOBID --inputs ... --script-version TEXT [--budget-used N]
  farmkit steps check [--workspace DIR]
  farmkit brief lint BRIEF.md
  farmkit watch --project FARM [--until job:ID|file:/abs|task:ID] [--timeout S] [--ack CURSOR]
  farmkit health --project FARM

Runtime state is read only through the pinned `farm` wrapper (site profile `paths.farm_wrapper`).
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from pathlib import Path

from . import adopt as adopt_mod
from . import brief, health, intent, observe, watch as watch_mod
from .ledger import Ledger
from .runner import Runner
from .runtime_reader import CliRuntimeReader
from .site import Site, SiteError
from .steps import Steps


def _workspace(args) -> Path:
    return Path(args.workspace or os.getcwd()).resolve()


def _verifiers(ws: Path, name: str | None):
    if not name:
        return None
    sys.path.insert(0, str(ws))
    return importlib.import_module(name)


def _steps(ws: Path, args) -> Steps:
    return Steps.load(ws / args.steps, verifiers=_verifiers(ws, args.verifiers))


def _site(args) -> Site:
    try:
        return Site.detect()
    except SiteError:
        if getattr(args, "allow_no_site", False):
            return Site.minimal("unknown")
        raise


def cmd_tick(args) -> int:
    ws = _workspace(args)
    steps = _steps(ws, args)
    site = _site(args)
    runner = Runner(site, Ledger(ws / "attempts"), steps, workspace=ws,
                    task_id=os.environ.get("FARM_TASK_ID"), lease_id=os.environ.get("FARM_LEASE_ID"),
                    accounting=intent.sacct_lookup_factory() if site.scheduler.get("accounting_stores_comment", True) else None)
    result = runner.tick(checkpoint=args.checkpoint)
    print(result.summary)
    print()
    print("REPORT WITH EXACTLY THIS COMMAND (farmkit never runs it for you):")
    print("  " + result.receipt_command)
    return 0


def cmd_verify(args) -> int:
    ws = _workspace(args)
    steps = _steps(ws, args)
    ledger = Ledger(ws / "attempts")
    runner = Runner(_site(args), ledger, steps, workspace=ws)
    spec = steps.by_id.get(args.step)
    if spec is None:
        print(f"unknown step {args.step}", file=sys.stderr)
        return 2
    rec = ledger.latest(args.step)
    if rec is None:
        print(f"{args.step}: no attempt yet", file=sys.stderr)
        return 2
    verdict = runner._verify(rec, spec)
    if not args.dry_run:
        ledger.save(rec)
    print(json.dumps({"step": args.step, "attempt": rec["attempt_id"], "ok": verdict.ok, "reasons": verdict.reasons,
                      "checked": verdict.checked}, indent=1))
    return 0 if verdict.ok else 1


def cmd_release(args) -> int:
    ws = _workspace(args)
    rec = Ledger(ws / "attempts").release(args.step, args.ruling)
    print(json.dumps({"step": args.step, "attempt": rec["attempt_id"], "class": rec["failure"]["class"],
                      "released": rec["failure"]["released"]}, indent=1))
    return 0


def cmd_reconcile(args) -> int:
    ws = _workspace(args)
    result = intent.reconcile_all(Ledger(ws / "attempts"), intent.sacct_lookup_factory())
    print(json.dumps(result, indent=1))
    return 0


def cmd_adopt(args) -> int:
    ws = _workspace(args)
    rec = adopt_mod.adopt(Ledger(ws / "attempts"), step=args.step, job_id=args.job, inputs=args.inputs or [],
                          script_version=args.script_version, budget_used=args.budget_used,
                          task_id=os.environ.get("FARM_TASK_ID"), note=args.note or "")
    print(json.dumps({"attempt_id": rec["attempt_id"], "step": rec["step"], "job_id": args.job}, indent=1))
    return 0


def cmd_steps_check(args) -> int:
    ws = _workspace(args)
    problems = _steps(ws, args).check(ws)
    for p in problems:
        print(p)
    print("steps.toml: ok" if not problems else f"steps.toml: {len(problems)} problem(s)")
    return 0 if not problems else 1


def cmd_brief_lint(args) -> int:
    problems = brief.lint(Path(args.brief))
    for p in problems:
        print(p)
    print("brief: ok" if not problems else f"brief: {len(problems)} problem(s)")
    return 0 if not problems else 1


def _reader(args):
    site = _site(args)
    wrapper = args.farm_wrapper or site.paths.get("farm_wrapper")
    if not wrapper:
        print("no farm wrapper: pass --farm-wrapper or set paths.farm_wrapper in the site profile", file=sys.stderr)
        sys.exit(2)
    return CliRuntimeReader(wrapper, args.project)


def cmd_watch(args) -> int:
    cursor_file = Path(args.cursor_file or (Path.cwd() / ".farmkit" / f"watch_cursor_{Path(args.project).name}"))
    if args.ack:
        watch_mod.ack(cursor_file, args.ack)
        print(json.dumps({"acked": args.ack, "cursor_file": str(cursor_file)}))
        return 0
    result = watch_mod.watch(_reader(args), cursor_file, until=args.until, timeout_s=args.timeout, poll_s=args.poll)
    print(watch_mod.format_hits(result))
    return 0 if result["hits"] else 3        # 3 = timeout, nothing happened


def cmd_health(args) -> int:
    status = _reader(args).status()
    found = health.findings(status)
    for f in found:
        print(f"{f['level']:8s} {f['check']}: {f['detail']}")
    return 0 if health.worst(found) in {"ok", "warn"} else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="farmkit")
    sub = p.add_subparsers(dest="command", required=True)

    def ws_opts(q):
        q.add_argument("--workspace")
        q.add_argument("--steps", default="steps.toml")
        q.add_argument("--verifiers", help="python module in the workspace exporting verifier functions")
        q.add_argument("--allow-no-site", action="store_true", help="run without a site profile (tests)")

    q = sub.add_parser("tick"); ws_opts(q); q.add_argument("--checkpoint", action="store_true"); q.set_defaults(func=cmd_tick)
    q = sub.add_parser("verify"); ws_opts(q); q.add_argument("--step", required=True); q.add_argument("--dry-run", action="store_true"); q.set_defaults(func=cmd_verify)
    q = sub.add_parser("release"); ws_opts(q); q.add_argument("--step", required=True); q.add_argument("--ruling", required=True); q.set_defaults(func=cmd_release)
    q = sub.add_parser("reconcile-intents"); ws_opts(q); q.set_defaults(func=cmd_reconcile)
    q = sub.add_parser("adopt"); ws_opts(q)
    q.add_argument("--step", required=True); q.add_argument("--job", required=True)
    q.add_argument("--inputs", nargs="*"); q.add_argument("--script-version", required=True)
    q.add_argument("--budget-used", type=int, default=0); q.add_argument("--note"); q.set_defaults(func=cmd_adopt)
    q = sub.add_parser("steps"); s2 = q.add_subparsers(dest="steps_cmd", required=True)
    c = s2.add_parser("check"); ws_opts(c); c.set_defaults(func=cmd_steps_check)
    q = sub.add_parser("brief"); s3 = q.add_subparsers(dest="brief_cmd", required=True)
    c = s3.add_parser("lint"); c.add_argument("brief"); c.set_defaults(func=cmd_brief_lint)
    for name, fn in (("watch", cmd_watch), ("health", cmd_health)):
        q = sub.add_parser(name)
        q.add_argument("--project", required=True)
        q.add_argument("--farm-wrapper")
        q.add_argument("--allow-no-site", action="store_true")
        if name == "watch":
            q.add_argument("--until"); q.add_argument("--timeout", type=float, default=watch_mod.DEFAULT_TIMEOUT_S)
            q.add_argument("--poll", type=float, default=15.0); q.add_argument("--ack"); q.add_argument("--cursor-file")
        q.set_defaults(func=fn)
    return p


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["board"]:                      # `farmkit board ...` == `farmboard ...`
        from farmboard.cli import main as board_main
        return board_main(argv[1:])
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (SiteError, ValueError, OSError) as exc:
        print(f"farmkit: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
