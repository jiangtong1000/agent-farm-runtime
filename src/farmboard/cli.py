"""farmboard command line.

  farmboard --project FARM [--farm-wrapper PATH] [--actor NAME]   Textual TUI (default)
  farmboard --project FARM --once                                  one text screen
  farmboard --project FARM --html out.html                         static page
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from farmkit.runtime_reader import CliRuntimeReader
from farmkit.site import Site, SiteError

from .model import build
from .render import render_html, render_text


def _wrapper(args) -> str:
    if args.farm_wrapper:
        return args.farm_wrapper
    try:
        wrapper = Site.detect().paths.get("farm_wrapper")
    except SiteError:
        wrapper = None
    if not wrapper:
        sys.exit("farmboard: pass --farm-wrapper or set paths.farm_wrapper in your site profile")
    return wrapper


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="farmboard")
    p.add_argument("--project", required=True, help="farm project directory (contains .farm/)")
    p.add_argument("--farm-wrapper", help="pinned farm entry point used for read-only queries and actions")
    p.add_argument("--actor", default="master", help="actor recorded on actions")
    p.add_argument("--once", action="store_true", help="print one text screen and exit")
    p.add_argument("--html", metavar="OUT", help="write a static HTML page and exit")
    p.add_argument("--refresh", type=float, default=10.0)
    p.add_argument("--sessions-root", help="codex sessions directory (default ~/.codex/sessions)")
    args = p.parse_args(argv)
    wrapper = _wrapper(args)
    reader = CliRuntimeReader(wrapper, args.project)
    sessions = Path(args.sessions_root) if args.sessions_root else None
    farm = Path(args.project).resolve().name
    if args.once or args.html:
        board = build(reader, farm=farm, sessions_root=sessions)
        if args.html:
            Path(args.html).write_text(render_html(board), encoding="utf-8")
            print(args.html)
        if args.once:
            print(render_text(board))
        return 0
    from .tui import run_tui
    return run_tui(reader, farm=farm, farm_wrapper=wrapper, project=args.project, actor=args.actor,
                   refresh_s=args.refresh, sessions_root=sessions)


if __name__ == "__main__":
    raise SystemExit(main())
