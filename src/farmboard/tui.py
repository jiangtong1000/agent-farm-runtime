"""Textual TUI for farmboard (D28). Textual is an optional dependency; the module
imports lazily so `farmboard --once` and `--html` never need it.

Keys: r ruling · a accept · w rework · m amend · t rotate · n new-from · enter detail · q quit.
Every action shows the exact command and asks y/n before running it through the wrapper.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from . import actions
from .model import Board, TaskCard, build
from .render import header_lines, render_attempts


def run_tui(reader, *, farm: str, farm_wrapper: str, project: str, actor: str, refresh_s: float = 10.0,
            evidence_dir: Path | None = None, sessions_root: Path | None = None) -> int:
    try:
        from textual.app import App, ComposeResult
        from textual.binding import Binding
        from textual.containers import Vertical
        from textual.widgets import DataTable, Footer, Header, Static
    except ImportError:
        print("farmboard: the TUI needs Textual (pip install 'agent-farm-runtime[board]'); "
              "use `farmboard --once` for a text screen or `--html out.html` for a page.")
        return 2

    evidence_dir = evidence_dir or Path.cwd() / ".farmboard"

    class FarmBoard(App):
        BINDINGS = [Binding("r", "act('ruling')", "ruling"), Binding("a", "act('accept')", "accept"),
                    Binding("w", "act('rework')", "rework"), Binding("m", "act('amend')", "amend"),
                    Binding("t", "act('rotate')", "rotate"), Binding("n", "act('new-from')", "new-from"),
                    Binding("enter", "detail", "detail"), Binding("q", "quit", "quit")]

        def __init__(self):
            super().__init__()
            self.board: Board | None = None
            self.cards: list[TaskCard] = []

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            with Vertical():
                yield Static(id="top")
                yield DataTable(id="tasks", cursor_type="row")
                yield Static(id="detail")
                yield Static(id="events")
            yield Footer()

        async def on_mount(self) -> None:
            table = self.query_one("#tasks", DataTable)
            table.add_columns("id", "state", "waiting_on", "jobs", "worker (gen)", "in-tok", "needs you", "note")
            await self.refresh_board()
            self.set_interval(refresh_s, lambda: asyncio.create_task(self.refresh_board()))

        async def refresh_board(self) -> None:
            loop = asyncio.get_running_loop()
            try:
                board = await asyncio.wait_for(loop.run_in_executor(
                    None, lambda: build(reader, farm=farm, sessions_root=sessions_root)), timeout=refresh_s * 2)
            except Exception as exc:  # a slow or failing read must not freeze the keyboard
                self.query_one("#top", Static).update(f"[red]read failed: {exc}[/red]")
                return
            self.board = board
            now = datetime.now(timezone.utc)
            self.query_one("#top", Static).update("\n".join(header_lines(board, now)))
            table = self.query_one("#tasks", DataTable)
            table.clear()
            self.cards = board.needs_you + board.live + board.history
            for c in self.cards:
                tok = "-" if c.input_tokens_last is None else f"{c.input_tokens_last // 1000}k" + (" ▲" if c.input_tokens_last >= board.token_budget_over else "")
                table.add_row(c.id, c.state, c.waiting_on or "-", ",".join(c.in_flight_jobs) or "-",
                              f"{c.worker_id} ({c.generations})" if c.worker_id else "-", tok, c.needs_you or "", (c.last_note or "")[:60], key=c.id)
            ev = "\n".join(f"{str(e.get('ts',''))[11:19]} {e.get('type',''):<22} {e.get('task_id','')}" for e in board.events)
            self.query_one("#events", Static).update(ev)

        def current(self) -> TaskCard | None:
            table = self.query_one("#tasks", DataTable)
            if not self.cards or table.cursor_row is None:
                return None
            return self.cards[table.cursor_row]

        def action_detail(self) -> None:
            c = self.current()
            if not c:
                return
            text = [f"{c.id} rev {c.revision} · {c.state} · {c.waiting_on or ''}", f"workspace {c.workspace}",
                    f"objective: {c.objective}", "", render_attempts(c) if c.attempts else "(no attempts)"]
            for label, path in (("evidence", c.evidence), ("review", c.review), ("checkpoint", c.checkpoint)):
                if path:
                    text.append(f"{label}: {path}")
            text.append("actions: " + " ".join(c.actions) + "  disabled: " + "; ".join(f"{k}: {v}" for k, v in c.reasons_disabled.items()))
            self.query_one("#detail", Static).update("\n".join(text))

        def action_act(self, name: str) -> None:
            c = self.current()
            if not c:
                return
            reason = actions.precondition(c, name)
            if reason:
                self.query_one("#detail", Static).update(f"{name} not available for {c.id}: {reason}")
                return
            extra: dict = {}
            evidence = None
            if actions.ACTIONS[name][1]:
                evidence = str(self._edit(evidence_dir / f"{name}_{c.id}_rev{c.revision}.md",
                                          f"# {name} — {c.id} (rev {c.revision})\n\n{actions.ACTIONS[name][1]}\n"))
            if name == "amend":                 # amend replaces the acceptance contract: a second file
                extra["acceptance_file"] = str(self._edit(evidence_dir / f"acceptance_{c.id}_rev{c.revision}.md",
                                                          "# Acceptance (complete replacement)\n\n<what must be true for this task to be accepted>\n"))
            if name == "new-from":              # a successor task: one contract file, then a brief file from it
                new_id = f"{c.id}-next"
                contract_path = self._edit(evidence_dir / f"contract_{new_id}.md",
                                           actions.CONTRACT_TEMPLATE.format(new_id=new_id, old_id=c.id,
                                                                            read_first=c.review or c.checkpoint or ""))
                contract = actions.parse_contract(contract_path.read_text())
                brief_path = evidence_dir / f"BRIEF_{new_id}.md"
                brief_path.write_text((contract.get("brief") or "") + "\n")
                extra.update(new_task_id=new_id, brief_file=str(brief_path), objective=contract.get("objective"),
                             deliverable=contract.get("deliverable"), acceptance=contract.get("acceptance"))
            try:
                argv = actions.command(c, name, farm_wrapper=farm_wrapper, project=project, actor=actor,
                                       evidence_file=evidence, **extra)
            except ValueError as exc:
                self.query_one("#detail", Static).update(str(exc))
                return
            self.pending = argv
            self.query_one("#detail", Static).update(f"About to run:\n{actions.describe(argv)}\n\npress y to confirm, any other key to cancel")

        def _edit(self, path: Path, template: str) -> Path:
            """Open $EDITOR with the terminal handed back to the editor (Textual suspend)."""
            suspend = getattr(self, "suspend", None)
            if suspend is None:
                return actions.edit_evidence(path, template)
            with suspend():
                return actions.edit_evidence(path, template)

        def on_key(self, event) -> None:
            pending = getattr(self, "pending", None)
            if not pending:
                return
            self.pending = None
            if event.key != "y":
                self.query_one("#detail", Static).update("cancelled")
                return
            proc = actions.run(pending)
            out = (proc.stdout or "") + (proc.stderr or "")
            self.query_one("#detail", Static).update(f"exit {proc.returncode}\n{out[-1500:]}")
            asyncio.create_task(self.refresh_board())

    FarmBoard().run()
    return 0
