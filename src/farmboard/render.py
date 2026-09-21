"""Plain-text and static-HTML renderings of a Board (D28). No Textual needed."""
from __future__ import annotations

import html
from datetime import datetime, timezone

from .model import Board, TaskCard


def _age(ts: str | None, now: datetime) -> str:
    if not ts:
        return "-"
    try:
        delta = (now - datetime.fromisoformat(ts)).total_seconds()
    except ValueError:
        return "?"
    if delta < 90:
        return f"{int(delta)} s"
    if delta < 5400:
        return f"{int(delta // 60)} m"
    if delta < 172800:
        return f"{delta / 3600:.1f} h"
    return f"{delta / 86400:.1f} d"


def header_lines(board: Board, now: datetime) -> list[str]:
    s = board.status
    alive = s.get("pid_alive")
    daemon = {True: f"ALIVE pid {s.get('pid')}", False: f"DEAD (pid {s.get('pid')} gone)", None: "UNKNOWN (no manifest or other host)"}[alive]
    tick = s.get("last_tick") or {}
    tick_age = f"{_age(tick.get('ts'), now)} ago" if tick else "never"
    source = "source ok" if s.get("source_matches") else "SOURCE MISMATCH: daemon code differs from CLI"
    handoff = s.get("handoff") or {}
    extra = f" · {handoff.get('kind') or 'handoff'} {handoff.get('phase')}" if handoff else ""
    pending = [k for k in ("pending_task_commit", "pending_recovery", "pending_deployment_event") if s.get(k)]
    extra += f" · PENDING {' '.join(pending)}" if pending else ""
    counts = s.get("task_counts") or {}
    counts_txt = " ".join(f"{k}:{v}" for k, v in counts.items() if v)
    return [f"{board.farm} · daemon {daemon} · last tick {tick_age} · {source}{extra}",
            f"tasks {counts_txt or 'none'} · observed {board.observed_at[:19]}"]


def _tokens(card: TaskCard, over: int) -> str:
    if card.input_tokens_last is None:
        return "-"
    flag = " ▲" if card.input_tokens_last >= over else ""
    return f"{card.input_tokens_last // 1000}k{flag}"


def render_text(board: Board, now: datetime | None = None, width: int = 160) -> str:
    now = now or datetime.now(timezone.utc)
    out = header_lines(board, now)
    out.append("─" * width)
    out.append("NEEDS YOU")
    if not board.needs_you:
        out.append("  (nothing)")
    for c in board.needs_you:
        what = "SUBMITTED, awaiting acceptance" if c.needs_you == "submitted" else f"parked: {c.waiting_on}"
        ev = f"  evidence {c.evidence}" if c.evidence else ""
        rv = f"  review {c.review}" if c.review else ""
        out.append(f"  {c.id:<28} {what}  rev {c.revision}  [{' '.join(c.actions)}]{ev}{rv}")
    out.append("")
    out.append("LIVE")
    out.append(f"  {'id':<28} {'state':<9} {'waiting_on':<38} {'jobs':<12} {'worker(gen)':<26} {'in-tok':<7} note")
    for c in board.live:
        jobs = ",".join(c.in_flight_jobs)[:12] or "-"
        worker = f"{c.worker_id} ({c.generations})" if c.worker_id else "-"
        note = (c.last_note or "")[:max(24, width - 128)]
        out.append(f"  {c.id:<28} {c.state:<9} {(c.waiting_on or '-')[:38]:<38} {jobs:<12} {worker:<26} {_tokens(c, board.token_budget_over):<7} {note}")
    if not board.live:
        out.append("  (none)")
    out.append("")
    hist = " · ".join(f"{c.id} {c.state} rev {c.revision}" for c in board.history) or "(none)"
    out.append(f"HISTORY  {hist}")
    out.append("")
    out.append("EVENTS (latest)")
    for e in board.events:
        p = e.get("payload") or {}
        detail = p.get("status") or p.get("to") or p.get("worker_id") or ""
        out.append(f"  {str(e.get('ts', ''))[11:19]} {e.get('type', ''):<22} {e.get('task_id', ''):<28} {detail}")
    return "\n".join(out)


def render_attempts(card: TaskCard) -> str:
    lines = [f"{'step':<28} {'n':>2} {'job':<10} {'state':<12} {'verdict':<8} class"]
    for a in card.attempts:
        lines.append(f"{a.step:<28} {a.n:>2} {(a.job_id or '-'):<10} {(a.state or '-'):<12} {a.verdict:<8} {a.failure_class or ''}")
    return "\n".join(lines)


HTML_HEAD = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>farmboard · {farm}</title>
<style>
:root{--bg:#F6F8FA;--paper:#fff;--ink:#1B2A3A;--ink2:#4A5B70;--rule:#D8DFE8;--accent:#0F766E;--blue:#3B5BA5;--amber:#B7791F;--red:#B23A48}
@media (prefers-color-scheme:dark){:root{--bg:#0F151C;--paper:#151D26;--ink:#E6ECF2;--ink2:#A6B3C2;--rule:#2A3644;--accent:#3FB8AB;--blue:#8DA6E6;--amber:#E0A43C;--red:#E27585}}
body{background:var(--bg);color:var(--ink);font:15px/1.5 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:0;padding:16px}
h1{font-size:20px;margin:0 0 4px}h2{font-size:14px;letter-spacing:.08em;text-transform:uppercase;color:var(--ink2);margin:24px 0 8px}
.top{font-family:ui-monospace,Menlo,monospace;font-size:13px;color:var(--ink2)}
table{border-collapse:collapse;width:100%;font-size:14px;background:var(--paper);border:1px solid var(--rule)}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--rule);vertical-align:top}th{font-size:12px;color:var(--ink2)}
.mono{font-family:ui-monospace,Menlo,monospace;font-size:13px}.warn{color:var(--amber)}.bad{color:var(--red)}.ok{color:var(--accent)}
.card{background:var(--paper);border:1px solid var(--rule);border-radius:8px;padding:10px 12px;margin:8px 0}
.muted{color:var(--ink2)} .wrap{overflow-x:auto}
</style></head><body>"""


def render_html(board: Board, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    h = html.escape
    lines = header_lines(board, now)
    parts = [HTML_HEAD.replace("{farm}", h(board.farm)), f"<h1>{h(board.farm)}</h1>", f"<div class='top'>{h(lines[0])}<br>{h(lines[1])}</div>"]
    parts.append("<h2>Needs you</h2>")
    if not board.needs_you:
        parts.append("<p class='muted'>nothing</p>")
    for c in board.needs_you:
        what = "SUBMITTED, awaiting acceptance" if c.needs_you == "submitted" else f"parked: <span class='mono'>{h(c.waiting_on or '')}</span>"
        links = "".join(f" · <a href='file://{h(p)}'>{h(n)}</a>" for n, p in (("FAILURE.md", c.evidence), ("REVIEW.md", c.review), ("CHECKPOINT.md", c.checkpoint)) if p)
        parts.append(f"<div class='card'><b>{h(c.id)}</b> <span class='muted'>rev {c.revision}</span> — {what}{links}"
                     f"<br><span class='muted'>actions: {h(' '.join(c.actions))}</span></div>")
    parts.append("<h2>Live</h2><div class='wrap'><table><tr><th>id</th><th>state</th><th>waiting_on</th><th>jobs</th><th>worker (gen)</th><th>in-tok</th><th>note</th></tr>")
    for c in board.live:
        tok = _tokens(c, board.token_budget_over)
        cls = " class='warn'" if "▲" in tok else ""
        parts.append(f"<tr><td class='mono'>{h(c.id)}</td><td>{h(c.state)}</td><td class='mono'>{h(c.waiting_on or '-')}</td>"
                     f"<td class='mono'>{h(','.join(c.in_flight_jobs) or '-')}</td><td class='mono'>{h(c.worker_id or '-')} ({c.generations})</td>"
                     f"<td{cls}>{h(tok)}</td><td>{h(c.last_note or '')}</td></tr>")
    parts.append("</table></div>")
    for c in board.live + board.needs_you:
        if c.attempts:
            parts.append(f"<div class='card'><b>{h(c.id)}</b> attempts<pre class='mono'>{h(render_attempts(c))}</pre></div>")
    parts.append("<h2>History</h2><p class='mono'>" + (" · ".join(h(f"{c.id} {c.state} rev {c.revision}") for c in board.history) or "none") + "</p>")
    parts.append("<h2>Events</h2><div class='wrap'><table><tr><th>time</th><th>type</th><th>task</th><th>detail</th></tr>")
    for e in board.events:
        p = e.get("payload") or {}
        detail = p.get("status") or p.get("to") or p.get("worker_id") or ""
        parts.append(f"<tr><td class='mono'>{h(str(e.get('ts', ''))[:19])}</td><td>{h(e.get('type', ''))}</td><td class='mono'>{h(e.get('task_id', ''))}</td><td>{h(str(detail))}</td></tr>")
    parts.append("</table></div></body></html>")
    return "\n".join(parts)
