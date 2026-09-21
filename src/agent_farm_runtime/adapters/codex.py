from __future__ import annotations

import json
import hashlib
import math
import os
import shlex
import subprocess
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ..models import Lease, Receipt, Task
from ..procutil import host_identity, observe_pidfile
from .base import ExecutorUnavailable, LaunchHandle, WorkerObservation
from .filesystem import atomic_write_json
from .prompts import worker_contract

# --- the receipt protocol the codex agent must honor ------------------------

RECEIPT_HELPER = '''\
#!/usr/bin/env python3
"""Write a fenced runtime receipt. Usage:
  python .farm_receipt.py AWAITING --waiting-on job:12345 --note "..."
  python .farm_receipt.py SUBMITTED --note "deliverable ready"
  python .farm_receipt.py FAILED --note "why"
The worker_id/task_id/lease_id are read from the environment the runtime set."""
import argparse, json, os, sys
from datetime import datetime, timezone

ap = argparse.ArgumentParser()
ap.add_argument("status", choices=["control", "RUNNING", "AWAITING", "SUBMITTED", "FAILED"])
ap.add_argument("--note", default="")
ap.add_argument("--waiting-on", default=None)
ap.add_argument("--rotation-id")
ap.add_argument("--checkpoint")
a = ap.parse_args()
try:
    if a.status == "control":
        with open(os.environ["FARM_TASK_PATH"]) as fh:
            task = json.load(fh)
        lease = task.get("lease") or {}
        if (task["id"] != os.environ["FARM_TASK_ID"]
                or lease.get("worker_id") != os.environ["FARM_WORKER_ID"]
                or lease.get("lease_id") != os.environ["FARM_LEASE_ID"]):
            sys.exit("stale worker lease: stop and report; do not write task state")
        request = task["metadata"].get("rotation_request")
        print(json.dumps({"rotation_id": request["id"] if request else None,
                          "checkpoint_required": bool(request)}))
        sys.exit(0)
    if a.rotation_id and (a.status != "AWAITING" or not a.checkpoint):
        sys.exit("rotation requires AWAITING and --checkpoint")
    path = os.environ["FARM_RECEIPT_PATH"]
    receipt = {
        "worker_id": os.environ["FARM_WORKER_ID"],
        "task_id": os.environ["FARM_TASK_ID"],
        "lease_id": os.environ["FARM_LEASE_ID"],  # echo verbatim: fencing
        "status": a.status,
        "ts": datetime.now(timezone.utc).isoformat(),
        "note": a.note,
        "waiting_on": a.waiting_on,
    }
    if a.rotation_id:
        receipt["rotation_id"] = a.rotation_id
    if a.checkpoint:
        receipt["checkpoint"] = os.path.abspath(a.checkpoint)
except KeyError as e:
    sys.exit(f"runtime env missing: {e}")
# atomic write: the reconciler must never read a half-written receipt
tmp = f"{path}.tmp.{os.getpid()}"
with open(tmp, "w") as fh:
    json.dump(receipt, fh)
    fh.flush()
    os.fsync(fh.fileno())
os.replace(tmp, path)
# The rename is not durable until the containing directory is synchronized.
directory_fd = os.open(os.path.dirname(path) or ".", os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory_fd)
finally:
    os.close(directory_fd)
print(f"receipt {a.status} -> {path}")
'''


def receipt_instruction(receipt_path: str) -> str:
    return (
        "RUNTIME RECEIPT PROTOCOL (mandatory). You run under the durable farm "
        "runtime; env vars FARM_RECEIPT_PATH, FARM_WORKER_ID, FARM_TASK_ID, "
        "FARM_LEASE_ID are set. Before you END any run you MUST record a receipt "
        "by calling `python .farm_receipt.py <STATUS> [--waiting-on ...] [--note ...]` "
        "in your workspace: AWAITING when you stop to wait on a job/file/ruling "
        "(pass --waiting-on job:ID | artifact:/abs/path | task:ID | ruling:NAME), "
        "SUBMITTED when the deliverable is ready for the master's acceptance, "
        "FAILED on an unrecoverable error. The helper echoes FARM_LEASE_ID for "
        "you (fencing). Never claim DONE — acceptance is the master's decision. "
        "At normal safe boundaries, query `python .farm_receipt.py control` (small read-only response). "
        "If rotation_id is present, finish the current safe step, write a short checkpoint of artifacts, "
        "external job IDs, holds and next action, then emit AWAITING --rotation-id ID --checkpoint PATH "
        "and exit cleanly. Preserve any real --waiting-on condition; omit it only for a context-only yield. "
        "Do not cancel external compute. Include --checkpoint PATH at ordinary AWAITING boundaries too. "
        f"Your receipt path is {receipt_path}."
    )


# --- cluster configuration (no cluster/user specifics baked into the adapter) -

@dataclass(frozen=True)
class CodexClusterConfig:
    """Cluster/user-specific knobs pushed OUT of the generic adapter.

    Defaults are generic ($HOME-based, codex assumed on PATH). A site supplies
    its specifics via `from_env()` (FARM_CODEX_* vars) or an explicit instance,
    so the module contains no hard-coded personal paths.
    """

    codex_cmd: str = "codex exec --skip-git-repo-check --sandbox danger-full-access"
    # shell line(s) to prepare PATH inside the tmux window (e.g. an nvm export);
    # empty means "codex is already on the login shell PATH".
    path_prelude: str = ""
    session_id_capture_delay: int = 45
    # Limit for this backend's argv transport, not a core Task Store constraint.
    max_prompt_bytes: int = 98304
    command_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        if not math.isfinite(self.command_timeout_seconds) or self.command_timeout_seconds <= 0:
            raise ValueError("tmux command timeout must be finite and positive")

    @classmethod
    def from_env(cls) -> "CodexClusterConfig":
        d = cls()
        return cls(
            codex_cmd=os.environ.get("FARM_CODEX_CMD", d.codex_cmd),
            path_prelude=os.environ.get("FARM_CODEX_PATH_PRELUDE", d.path_prelude),
            session_id_capture_delay=int(
                os.environ.get("FARM_CODEX_SID_DELAY", d.session_id_capture_delay)
            ),
            max_prompt_bytes=int(os.environ.get("FARM_CODEX_MAX_PROMPT_BYTES", d.max_prompt_bytes)),
            command_timeout_seconds=float(os.environ.get("FARM_TMUX_TIMEOUT_SECONDS", d.command_timeout_seconds)),
        )


# --- script rendering (pure; unit-tested) -----------------------------------

def _env_exports(worker_id: str, task_id: str, lease_id: str, receipt_path: str) -> str:
    return "\n".join(
        f"export {k}={shlex.quote(v)}"
        for k, v in {
            "FARM_WORKER_ID": worker_id,
            "FARM_TASK_ID": task_id,
            "FARM_LEASE_ID": lease_id,
            "FARM_RECEIPT_PATH": receipt_path,
            "FARM_TASK_PATH": str(Path(receipt_path).parent.parent.parent / "tasks" / f"{task_id}.json"),
        }.items()
    )


def _prelude(cfg: CodexClusterConfig) -> str:
    return (cfg.path_prelude + "\n") if cfg.path_prelude else ""


def validate_prompt(cfg: CodexClusterConfig, text: str) -> None:
    size = len(text.encode("utf-8"))
    if cfg.max_prompt_bytes <= 0 or size > cfg.max_prompt_bytes:
        raise ValueError(f"codex-tmux prompt exceeds backend limit ({size} rendered bytes; limit {cfg.max_prompt_bytes} bytes); "
                         "shorten context/instruction or configure this backend's transport limit")


CODEX_SID_PATTERN = "session id: [0-9a-f-]{36}"


def _run_and_record(
    *, invocation: str, log_name: str, pid_file: str, sid_path: str | None,
    append_log: bool, delay: int, attempt_id: str = "", sid_pattern: str = CODEX_SID_PATTERN,
) -> str:
    """Shared tail used by BOTH launch and resume: background the codex
    invocation, record THIS invocation's (pid, starttime) identity to `pid_file`,
    optionally capture the session id from this worker's own log, then wait.

    Because launch and resume share this block, a resumed worker's pid identity is
    refreshed exactly like a launched one -- so poll() liveness always tracks the
    CURRENT codex process, never a resumed worker's dead original invocation.
    """
    # The log is still mirrored to the pane through tee, but tee is opened on an explicit
    # descriptor whose pid we keep: after the CLI exits we close the descriptor and WAIT
    # for tee, so the late session-id capture reads a finished log (F02). Waiting for the
    # CLI alone was not a completion boundary for the log.
    tee = f"tee -a {shlex.quote(log_name)}" if append_log else f"tee {shlex.quote(log_name)}"
    sid_capture = ""
    if sid_path is not None:  # only launch needs to discover the id; resume reuses it
        sid_capture = f"""# race-free session identity: the agent CLI prints it into THIS worker's own log at startup
( for _ in $(seq 1 {delay}); do
    sid=$(grep -oiE {shlex.quote(sid_pattern)} {shlex.quote(log_name)} 2>/dev/null | grep -oE '[0-9a-f-]{{36}}' | head -1)
    [ -n "$sid" ] && {{ echo "$sid" > {shlex.quote(sid_path)}; break; }}
    sleep 1
  done ) &
"""
    late_capture = ""
    if sid_path is not None:  # some CLIs print the id only in the final result (claude --output-format json)
        late_capture = f"""[ -s {shlex.quote(sid_path)} ] || {{ sid=$(grep -oiE {shlex.quote(sid_pattern)} {shlex.quote(log_name)} 2>/dev/null | grep -oE '[0-9a-f-]{{36}}' | head -1); [ -n "$sid" ] && echo "$sid" > {shlex.quote(sid_path)}; }}
"""
    return f"""exec 3> >({tee})
_TEEPID=$!
{invocation} >&3 2>&1 &
_CPID=$!
_ST=$(awk '{{s=substr($0,index($0,") ")+2); n=split(s,a," "); print a[20]}}' /proc/$_CPID/stat 2>/dev/null)
_ST=${{_ST:-unknown}}
echo "$_CPID $_ST {attempt_id}" > {shlex.quote(pid_file)}
{sid_capture}wait $_CPID
_RC=$?
exec 3>&-
wait $_TEEPID 2>/dev/null || true
{late_capture}exit $_RC
"""


def render_launch_script(
    *, cfg: CodexClusterConfig, workspace: str, worker_id: str, task_id: str,
    lease_id: str, receipt_path: str, brief: str, log_name: str, sid_path: str,
    pid_file: str, attempt_id: str = "", invocation: Callable[[str], str] | None = None,
    sid_pattern: str = CODEX_SID_PATTERN,
) -> str:
    """Headless agent launch mirroring the proven RESTART_HEADLESS.sh, plus the
    runtime env + receipt protocol. Records (pid,starttime) identity and captures
    the session id from this worker's own log (see _run_and_record).

    `invocation(prompt)` renders the agent CLI command line; the default is codex."""
    full_brief = brief.rstrip() + "\n\n" + receipt_instruction(receipt_path)
    validate_prompt(cfg, full_brief)
    render = invocation or (lambda prompt: f"{cfg.codex_cmd} {shlex.quote(prompt)}")
    body = _run_and_record(
        invocation=render(full_brief),
        log_name=log_name, pid_file=pid_file, sid_path=sid_path,
        append_log=False, delay=cfg.session_id_capture_delay, attempt_id=attempt_id,
        sid_pattern=sid_pattern,
    )
    guard = f"mkdir {shlex.quote(pid_file + '.dispatch-' + attempt_id)} || exit 4\n" if attempt_id else ""
    return f"""#!/bin/bash
{guard}cd {shlex.quote(workspace)} || exit 3
{_prelude(cfg)}{_env_exports(worker_id, task_id, lease_id, receipt_path)}
rm -f {shlex.quote(receipt_path)}
{body}"""


def render_resume_script(
    *, cfg: CodexClusterConfig, workspace: str, worker_id: str, task_id: str,
    lease_id: str, receipt_path: str, sid_path: str, log_name: str, wake_msg: str,
    pid_file: str, attempt_id: str = "", invocation: Callable[[str], str] | None = None,
) -> str:
    """Resume an existing agent session (mirrors WAKE.sh) under the same lease.

    Uses the SAME _run_and_record tail as launch, so the resumed process's
    (pid,starttime) identity is written to `pid_file`; poll() then sees the live
    resumed process instead of the dead original (the bug this fixes).

    `invocation(prompt)` may reference the shell variable $SID; the default is codex."""
    full_msg = wake_msg.rstrip() + "\n\n" + receipt_instruction(receipt_path)
    validate_prompt(cfg, full_msg)
    render = invocation or (lambda prompt: f'{cfg.codex_cmd} resume "$SID" {shlex.quote(prompt)}')
    body = _run_and_record(
        invocation=render(full_msg),
        log_name=log_name, pid_file=pid_file, sid_path=None,  # id unchanged on resume
        append_log=True, delay=cfg.session_id_capture_delay, attempt_id=attempt_id,
    )
    guard = f"mkdir {shlex.quote(pid_file + '.dispatch-' + attempt_id)} || exit 4\n" if attempt_id else ""
    return f"""#!/bin/bash
{guard}cd {shlex.quote(workspace)} || exit 3
{_prelude(cfg)}{_env_exports(worker_id, task_id, lease_id, receipt_path)}
rm -f {shlex.quote(receipt_path)}
SID=$(cat {shlex.quote(sid_path)} 2>/dev/null)
if [ -z "$SID" ]; then echo "no session id at {sid_path}; cannot resume" >&2; exit 3; fi
{body}"""


def _same_as_previous(raw: bytes, state: dict) -> bool:
    """Is this receipt the one that was already applied before the current attempt?

    Compared by content. State files written by earlier releases carry only a
    digest of the previous receipt; honour it so an upgrade never re-applies one.
    """
    if state.get("previous_receipt") is not None:
        return raw.decode("utf-8", "replace") == state["previous_receipt"]
    legacy = state.get("previous_receipt_sha256")
    return legacy is not None and hashlib.sha256(raw).hexdigest() == legacy


# --- liveness (default; injectable) -----------------------------------------

def _default_codex_alive(state: dict) -> bool | None:
    """Per-worker liveness: is the SPECIFIC codex process we launched still the
    live process at its recorded (pid, starttime)?

    This deliberately does NOT scan for any codex whose cwd matches the workspace:
    a workspace-level pgrep cannot tell two workers (e.g. a dead prior generation
    and its live successor) apart in the same workspace. Identity is the launched
    pid plus its /proc start-time, so a recycled pid is not mistaken for a worker.
    """
    pid_file = state.get("pid_file")
    if not pid_file:
        return None
    return observe_pidfile(pid_file, state, state.get("attempt_id"))


def _default_run(cmd: list[str], *, timeout: float = 10.0) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                          env={**os.environ, "LC_ALL": "C"})


class CodexTmuxExecutor:
    """Real farm backend: disposable codex workers in a tmux session.

    Faithful translation of the live farm's launch (RESTART_HEADLESS.sh) and
    wake (WAKE.sh), wrapped in the runtime's lease/receipt contract.

    Safety (given a prior tmux-server crash from live window surgery):
      * a dedicated tmux SERVER (`-L <socket>`) fully isolates these windows from
        a live farm on the same host;
      * targets a configurable SESSION, defaulting to one separate from `farm`;
      * NEVER kills or moves windows — only create-if-absent + send-keys; `stop`
        only C-c's the pane;
      * launch is IDEMPOTENT per lease/worker (won't start a second codex if the
        worker is already alive);
      * cluster/user specifics live in CodexClusterConfig, not in this module;
      * all tmux/pgrep calls are injectable -> unit-tested without real tmux/codex.

    Task metadata: metadata["workspace"] (abs dir, required), metadata["brief"]
    (agent brief, required), metadata["agent_label"] (tmux window; default
    per-worker).
    """

    def __init__(
        self,
        runtime_dir: Path,
        *,
        session: str = "farm2",
        tmux_socket: str | None = None,
        config: CodexClusterConfig | None = None,
        run: Callable[[list[str]], subprocess.CompletedProcess] | None = None,
        is_alive: Callable[[dict], bool | None] = _default_codex_alive,
    ):
        self.session = session
        self.tmux_socket = tmux_socket
        self.config = config or CodexClusterConfig.from_env()
        self.run = run or (lambda cmd: _default_run(cmd, timeout=self.config.command_timeout_seconds))
        self.is_alive = is_alive
        self.state_dir = Path(runtime_dir) / "codex_workers"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.receipts_dir = Path(runtime_dir) / "receipts"
        self.receipts_dir.mkdir(parents=True, exist_ok=True)

    # -- per-worker persisted mapping -----------------------------------------

    def _state_path(self, worker_id: str) -> Path:
        return self.state_dir / f"{worker_id}.json"

    def _save_state(self, worker_id: str, data: dict) -> None:
        atomic_write_json(self._state_path(worker_id), data)

    def _load_state(self, worker_id: str) -> dict | None:
        p = self._state_path(worker_id)
        try:
            data = json.loads(p.read_text())
            if not isinstance(data, dict):
                raise ValueError("not an object")
            required = {"workspace", "window", "receipt_path", "sid_path", "log_name", "task_id", "lease_id", "pid_file"}
            if any(not isinstance(data.get(key), str) or not data[key] for key in required):
                raise ValueError("incomplete worker identity")
            if data["window"] != worker_id:
                raise ValueError("worker/pane identity mismatch")
            return data
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            raise ExecutorUnavailable(f"unreadable worker identity {p}; inspect without dispatch") from exc

    def _alive(self, state: dict) -> bool | None:
        if state.get("host") != host_identity()["host"]:
            return None
        return self.is_alive(state)

    def _prepare_attempt(self, worker_id: str, state: dict) -> dict:
        """Persist uncertainty BEFORE dispatch, and fence the previous receipt."""
        try:
            prior = Path(state["receipt_path"]).read_text(encoding="utf-8")
        except FileNotFoundError:
            prior = None
        prepared = {**{k: v for k, v in state.items() if k != "previous_receipt_sha256"},
                    **host_identity(), "attempt_id": uuid.uuid4().hex, "previous_receipt": prior}
        self._save_state(worker_id, prepared)
        return prepared

    def _receipt_path(self, worker_id: str) -> Path:
        return self.receipts_dir / f"{worker_id}.json"

    # -- tmux helpers (create-if-absent + send-keys only) --------------------

    def _tmux(self, *args: str) -> list[str]:
        base = ["tmux"]
        if self.tmux_socket:
            base += ["-L", self.tmux_socket]
        return base + list(args)

    def _command(self, *args: str, allow_missing: bool = False) -> subprocess.CompletedProcess:
        try:
            result = self.run(self._tmux(*args))
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ExecutorUnavailable(f"tmux {args[0]} unavailable/timed out; execution outcome unverified") from exc
        if result.returncode:
            diagnostic = (result.stderr or "").strip()
            missing = result.returncode == 1 and (
                "no server running on" in diagnostic or "can't find session:" in diagnostic
                or "session not found:" in diagnostic
                or ("No such file or directory" in diagnostic and
                    ("error connecting to" in diagnostic or "failed to connect to server" in diagnostic)))
            if not (allow_missing and missing):
                raise ExecutorUnavailable(f"tmux {args[0]} failed ({result.returncode}): {diagnostic[:400]}")
        return result

    def _ensure_window(self, window: str, cwd: str) -> None:
        if self._command("has-session", "-t", self.session, allow_missing=True).returncode != 0:
            self._command("new-session", "-d", "-s", self.session, "-n", window, "-c", cwd)
            return
        listed = self._command("list-windows", "-t", self.session, "-F", "#{window_name}")
        if window not in (listed.stdout or "").split():
            self._command("new-window", "-t", self.session, "-n", window, "-c", cwd)

    def _send(self, window: str, line: str) -> None:
        self._command("send-keys", "-t", f"{self.session}:{window}", line, "Enter")

    # -- agent CLI hooks (overridden by other agent executors) -----------------

    sid_pattern = CODEX_SID_PATTERN
    agent_name = "codex"

    def _launch_invocation(self, prompt: str) -> str:
        return f"{self.config.codex_cmd} {shlex.quote(prompt)}"

    def _resume_invocation(self, prompt: str) -> str:
        return f'{self.config.codex_cmd} resume "$SID" {shlex.quote(prompt)}'

    # -- WorkerExecutor protocol ---------------------------------------------

    def validate_task(self, task: Task) -> None:
        ws, brief = task.metadata.get("workspace"), task.metadata.get("brief")
        if not isinstance(ws, str) or not ws or not Path(ws).is_absolute() or not Path(ws).is_dir():
            raise ValueError("codex-tmux requires an existing absolute workspace directory")
        if not isinstance(brief, str) or not brief.strip():
            raise ValueError("codex-tmux requires a nonempty brief")
        validate_prompt(self.config, brief + worker_contract(task) + receipt_instruction(
            str(self.receipts_dir / ("W-" + task.id + "-000000.json"))))

    def launch(self, task: Task, lease: Lease) -> LaunchHandle:
        ws = task.metadata.get("workspace")
        brief = task.metadata.get("brief")
        if not ws or not brief:
            raise ValueError(f"task {task.id} needs metadata.workspace and metadata.brief")
        brief += worker_contract(task)
        wid = lease.worker_id
        # tmux pane identity is the WORKER id, not a human agent_label: each worker
        # generation gets its own unambiguous pane, so generations never collide.
        window = wid
        workspace = str(Path(ws).resolve())

        # idempotent per lease: never start a second codex for THIS live worker
        # (judged by this worker's own pid identity, not any codex in the workspace)
        st = self._load_state(wid)
        if st is not None:
            if st["task_id"] != task.id or st["lease_id"] != lease.lease_id:
                raise ExecutorUnavailable(f"{wid}: persisted lease/task mismatch; launch withheld")
            observed = self._alive(st)
            if observed is True:
                return LaunchHandle(worker_id=wid, session_handle=f"{self.session}:{window}")
            if observed is None:
                raise ExecutorUnavailable(f"{wid}: prior dispatch/process identity unknown; launch withheld")

        receipt_path = str(self._receipt_path(wid))
        pid_file = str(self.state_dir / f"{wid}.pid")
        sid_path = str(Path(workspace) / f".session_id_{wid}")
        log_name = f"agent_{window}_{wid}.log"
        launch_sh = Path(workspace) / f".farm_launch_{wid}.sh"

        st = self._prepare_attempt(wid, {
            "workspace": workspace, "window": window, "receipt_path": receipt_path,
            "sid_path": sid_path, "log_name": log_name, "task_id": task.id,
            "lease_id": lease.lease_id, "pid_file": pid_file,
        })

        (Path(workspace) / ".farm_receipt.py").write_text(RECEIPT_HELPER)
        launch_sh.write_text(render_launch_script(
            cfg=self.config, workspace=workspace, worker_id=wid, task_id=task.id,
            lease_id=lease.lease_id, receipt_path=receipt_path, brief=brief,
            log_name=log_name, sid_path=sid_path, pid_file=pid_file, attempt_id=st["attempt_id"],
            invocation=self._launch_invocation, sid_pattern=self.sid_pattern,
        ))
        launch_sh.chmod(0o755)
        self._ensure_window(window, workspace)
        self._send(window, f"bash {shlex.quote(str(launch_sh))}")
        return LaunchHandle(worker_id=wid, session_handle=f"{self.session}:{window}")

    def resume(self, task: Task, worker_id: str, lease: Lease) -> None:
        st = self._load_state(worker_id)
        if st is None:
            raise ExecutorUnavailable(f"{worker_id}: missing prior identity; cannot safely resume")
        if st["task_id"] != task.id or st["lease_id"] != lease.lease_id:
            raise ExecutorUnavailable(f"{worker_id}: persisted lease/task mismatch; resume withheld")
        alive = self._alive(st)
        if alive is True:
            return  # repeated resume must never inject a second invocation
        if alive is None:
            raise ExecutorUnavailable(f"{worker_id}: prior process identity unknown; resume withheld")
        st = self._prepare_attempt(worker_id, st)
        # A stopped worker may resume after a runtime upgrade. Refresh only the
        # runtime-owned helper, never a live process's files or task governance.
        (Path(st["workspace"]) / ".farm_receipt.py").write_text(RECEIPT_HELPER)
        wake = ("Wake-up: a condition you were awaiting has fired or a new "
                "MASTER_*.md landed. Re-scan your workspace, verify the awaited "
                "state, and follow the task's instructions." + worker_contract(task))
        if task.metadata.get("resume_mode") == "fresh":
            # D18/D6: a new agent session under the SAME lease and worker id. It gets the
            # brief, the effective contract and the checkpoint; durable state lives in
            # the workspace (ledger, CHECKPOINT.md), not in the previous conversation.
            brief = (task.metadata.get("brief") or "").rstrip()
            fresh_sh = Path(st["workspace"]) / f".farm_resume_{worker_id}.sh"
            fresh_sh.write_text(render_launch_script(
                cfg=self.config, workspace=st["workspace"], worker_id=worker_id, task_id=task.id,
                lease_id=lease.lease_id, receipt_path=st["receipt_path"],
                brief=brief + "\n\nFRESH SESSION: you are continuing this task in a new session. "
                      "Read CHECKPOINT.md and the attempts/ ledger in your workspace before acting.\n" + wake,
                log_name=st["log_name"], sid_path=st["sid_path"], pid_file=st["pid_file"],
                attempt_id=st["attempt_id"], invocation=self._launch_invocation, sid_pattern=self.sid_pattern,
            ))
            fresh_sh.chmod(0o755)
            self._ensure_window(st["window"], st["workspace"])
            self._send(st["window"], f"bash {shlex.quote(str(fresh_sh))}")
            return
        resume_sh = Path(st["workspace"]) / f".farm_resume_{worker_id}.sh"
        resume_sh.write_text(render_resume_script(
            cfg=self.config, workspace=st["workspace"], worker_id=worker_id, task_id=task.id,
            lease_id=lease.lease_id, receipt_path=st["receipt_path"],
            sid_path=st["sid_path"], log_name=st["log_name"], pid_file=st["pid_file"],
            attempt_id=st["attempt_id"], wake_msg=wake, invocation=self._resume_invocation,
        ))
        resume_sh.chmod(0o755)
        self._ensure_window(st["window"], st["workspace"])
        self._send(st["window"], f"bash {shlex.quote(str(resume_sh))}")

    def poll(self, worker_id: str) -> WorkerObservation:
        st = self._load_state(worker_id)
        if st is None:
            return WorkerObservation(worker_id=worker_id, alive=None, detail="missing executor identity")
        receipt = None
        rp = Path(st["receipt_path"])
        if rp.exists():
            try:
                raw = rp.read_bytes()
                if not _same_as_previous(raw, st):
                    receipt = Receipt.from_dict(json.loads(raw))
            except (ValueError, KeyError):
                receipt = None
        alive = self._alive(st)
        return WorkerObservation(worker_id=worker_id, alive=alive, receipt=receipt,
                                 detail="dispatch, host or process identity unverified" if alive is None else None)

    def stop(self, worker_id: str) -> None:
        """Interrupt codex in the worker's window; leaves the window intact."""
        st = self._load_state(worker_id)
        if st is None:
            return
        alive = self._alive(st)
        if alive is None:
            raise ExecutorUnavailable(f"{worker_id}: cannot verify signal target; stop withheld")
        if alive is True:
            self._command("send-keys", "-t", f"{self.session}:{st['window']}", "C-c")
