#!/usr/bin/env python3
"""Release tooling: export a pinned source tree and render a deployment's entry scripts.

Two subcommands, both stdlib-only:

  export   copy src/ (runtime, farmkit, farmboard) into
           <releases_root>/agent-farm-<tag>-<sha8>/, compute the source digest with the
           exported code itself in a clean interpreter, write RELEASE.json, make the tree
           read-only. The digest is the same one the runtime records in deployment.json.

  wrapper  render `farm` (the pinned, sha-checked entry point) and `run_reconciler.sh`
           for ONE deployment from a site profile (your own toml, never in the repo) and a
           release directory. The rendered `farm` keeps the production policy: -I -S, an
           explicit pinned PYTHONPATH, activation OFF by default for `reconcile`, and a
           refusal to write when the pinned source digest changed.

Site profile keys used here (see sites/example.toml):
  [python] runtime_interpreter
  [farm_defaults] executor, interval, grace_seconds, max_auto_restarts   (optional)
  [executor.codex] cmd, path_prelude      [executor.claude] cmd, path_prelude   (optional)
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import stat
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path

READ_ONLY_COMMANDS = ("version", "status", "task-list", "task-show", "doctor", "shadow", "events")

WRAPPER = r'''#!/usr/bin/env bash
# Local deployment policy, not a requirement on other runtime users.
# Release: {tag} ({commit}; protocol {protocol}; exported {exported}).
# Rendered by tools/release.py from site {site_name}; do not edit by hand.
set -euo pipefail
export PATH="{python_dir}:$PATH"
export PYTHONPATH="{src}"
# Parse with the CLI itself so help stays read-only and abbreviated options
# cannot accidentally bypass the activation check. Never pass this flag to workers.
# Runtime is stdlib-only. Ignore cwd/Python search-path injection and skip site
# initialization (including editable .pth); explicitly load only this pinned src.
exec {python} -B -I -S -c '
import os
import sys
sys.path.insert(0, os.environ["PYTHONPATH"])
from agent_farm_runtime import cli
from agent_farm_runtime.provenance import runtime_identity

args = cli.build_parser().parse_args(["--writer-policy", "pinned-host", *sys.argv[1:]])
activation = os.environ.pop("FARM_ACTUATION_ALLOWED", None)
if args.writer_policy != "pinned-host":
    sys.exit("farm: this local entrypoint requires pinned-host writer policy")
if args.command == "reconcile" and activation != "1":
    print("farm: activation is OFF; owner-authorized recovery requires FARM_ACTUATION_ALLOWED=1", file=sys.stderr)
    sys.exit(78)
access_read = args.command == "access" and args.access_action in ("resolve", "verify")
if args.command not in {read_only} and not access_read:
    expected = "{sha}"
    if runtime_identity()["source_sha256"] != expected:
        print("farm: pinned source changed; refusing writes until release is reviewed", file=sys.stderr)
        sys.exit(78)
sys.argv[1:1] = ["--writer-policy", "pinned-host"]
raise SystemExit(cli.main())
' "$@"
'''

RUN_RECONCILER = '''#!/bin/bash
# Owner-authorized startup only; the pinned wrapper defaults to activation OFF.
# Dedicated {farm} socket/session; never operate the master or another farm's socket.
# Rendered by tools/release.py from site {site_name} for release {tag}; do not edit by hand.
set -euo pipefail
export PATH={path_export}
{codex_exports}{claude_exports}exec {wrapper} \\
  --project {project} reconcile \\
  --executor {executor} --session {farm} --tmux-socket {farm} \\
  --loop --interval {interval} --grace-seconds {grace} --auto-unblock --max-auto-restarts {restarts}
'''


def _identity_of(src: Path, python: str) -> dict:
    code = ("import json, sys; sys.path.insert(0, sys.argv[1]); "
            "from agent_farm_runtime.provenance import runtime_identity; print(json.dumps(runtime_identity()))")
    proc = subprocess.run([python, "-B", "-I", "-S", "-c", code, str(src)], text=True, capture_output=True, timeout=120)
    if proc.returncode != 0:
        raise SystemExit(f"release: cannot import exported source: {proc.stderr.strip()[:400]}")
    return json.loads(proc.stdout)


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(["git", "-C", str(repo), *args], text=True, capture_output=True)
    return proc.stdout.strip() if proc.returncode == 0 else ""


def cmd_export(args: argparse.Namespace) -> int:
    src = Path(args.src).resolve()
    if not (src / "agent_farm_runtime").is_dir():
        raise SystemExit(f"release: {src} does not contain agent_farm_runtime/")
    repo = src.parent
    commit = _git(repo, "rev-parse", "--short", "HEAD") or "unknown"
    dirty = bool(_git(repo, "status", "--porcelain", "--", "src"))
    if dirty and not args.allow_dirty:
        raise SystemExit("release: src/ has uncommitted changes; commit first or pass --allow-dirty")
    python = args.python or sys.executable
    identity = _identity_of(src, python)          # digest depends on file names + bytes only
    sha8 = identity["source_sha256"][:8]
    final = Path(args.releases_root).resolve() / f"agent-farm-{args.tag}-{sha8}"
    if final.exists():
        raise SystemExit(f"release: {final} already exists")
    final.mkdir(parents=True)
    shutil.copytree(src, final / "src", ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache"))
    exported = _identity_of(final / "src", python)
    if exported["source_sha256"] != identity["source_sha256"]:
        raise SystemExit("release: exported copy does not reproduce the source digest; aborting")
    record = {"tag": args.tag, "git_commit": commit, "git_dirty": dirty,
              "protocol_version": identity["protocol_version"], "source_sha256": identity["source_sha256"],
              "exported_at": datetime.now(timezone.utc).isoformat(), "exported_from": str(src)}
    (final / "RELEASE.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    if not args.writable:
        for path in final.rglob("*"):
            path.chmod(path.stat().st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
        final.chmod(final.stat().st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
    print(json.dumps({"release_dir": str(final), **record}, indent=2, sort_keys=True))
    return 0


def _load_site(path: Path) -> dict:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def render_wrapper(site: dict, release_dir: Path, site_name: str) -> str:
    release = json.loads((release_dir / "RELEASE.json").read_text())
    python = site["python"]["runtime_interpreter"]
    return WRAPPER.format(
        tag=release["tag"], commit=release["git_commit"], protocol=release["protocol_version"],
        exported=release["exported_at"][:10], site_name=site_name,
        python=python, python_dir=str(Path(python).parent), src=str(release_dir / "src"),
        read_only="{" + ", ".join(f'"{c}"' for c in READ_ONLY_COMMANDS) + "}", sha=release["source_sha256"])


def render_run_reconciler(site: dict, release_dir: Path, site_name: str, farm_dir: Path, wrapper: Path) -> str:
    release = json.loads((release_dir / "RELEASE.json").read_text())
    defaults = site.get("farm_defaults", {})
    executors = site.get("executor", {})
    path_parts = [str(Path(site["python"]["runtime_interpreter"]).parent)]
    exports = {"codex": "", "claude": ""}
    for name, env_prefix in (("codex", "FARM_CODEX"), ("claude", "FARM_CLAUDE")):
        cfg = executors.get(name)
        if not cfg:
            continue
        lines = ""
        if cfg.get("path_prelude"):
            lines += f"export {env_prefix}_PATH_PRELUDE={shlex.quote(cfg['path_prelude'])}\n"
        if cfg.get("cmd"):
            lines += f"export {env_prefix}_CMD={shlex.quote(cfg['cmd'])}\n"
        if cfg.get("bin_dir"):
            path_parts.append(cfg["bin_dir"])
        exports[name] = lines
    return RUN_RECONCILER.format(
        farm=farm_dir.name, site_name=site_name, tag=release["tag"],
        path_export=":".join(path_parts) + ":$PATH",
        codex_exports=exports["codex"], claude_exports=exports["claude"],
        wrapper=str(wrapper), project=str(farm_dir),
        executor=defaults.get("executor", "codex-tmux"), interval=defaults.get("interval", 30),
        grace=defaults.get("grace_seconds", 120), restarts=defaults.get("max_auto_restarts", 3))


def cmd_wrapper(args: argparse.Namespace) -> int:
    site_path = Path(args.site).resolve()
    site = _load_site(site_path)
    release_dir = Path(args.release).resolve()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    wrapper = out / "farm"
    wrapper.write_text(render_wrapper(site, release_dir, site_path.stem))
    wrapper.chmod(0o755)
    written = [str(wrapper)]
    if args.farm:
        farm_dir = Path(args.farm).resolve()
        script = farm_dir / "run_reconciler.sh"
        if script.exists() and not args.force:
            raise SystemExit(f"release: {script} exists; pass --force to overwrite")
        script.write_text(render_run_reconciler(site, release_dir, site_path.stem, farm_dir, wrapper))
        script.chmod(0o755)
        written.append(str(script))
    print(json.dumps({"written": written}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="release.py")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("export", help="copy src/ into a read-only release directory with RELEASE.json")
    p.add_argument("--src", required=True, help="the repository's src/ directory")
    p.add_argument("--tag", required=True, help="release tag, e.g. 0.5.0")
    p.add_argument("--releases-root", required=True)
    p.add_argument("--python", help="interpreter used to compute the digest (default: this one)")
    p.add_argument("--allow-dirty", action="store_true")
    p.add_argument("--writable", action="store_true", help="do not strip write permission (tests)")
    p.set_defaults(func=cmd_export)
    p = sub.add_parser("wrapper", help="render farm (and run_reconciler.sh) for one deployment")
    p.add_argument("--site", required=True, help="your site profile toml (kept outside the repository)")
    p.add_argument("--release", required=True, help="release directory produced by export")
    p.add_argument("--out", required=True, help="directory receiving the `farm` wrapper")
    p.add_argument("--farm", help="farm project directory; also renders <farm>/run_reconciler.sh")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_wrapper)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
