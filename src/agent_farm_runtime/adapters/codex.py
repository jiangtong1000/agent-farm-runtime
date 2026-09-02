from __future__ import annotations

import json
import os
import shlex
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ..models import Lease, Receipt, Task
from ..procutil import pid_identity_alive, read_pidfile
from .base import LaunchHandle, WorkerObservation

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
ap.add_argument("status", choices=["RUNNING", "AWAITING", "SUBMITTED", "FAILED"])
ap.add_argument("--note", default="")
ap.add_argument("--waiting-on", default=None)
a = ap.parse_args()
try:
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
except KeyError as e:
    sys.exit(f"runtime env missing: {e}")
# atomic write: the reconciler must never read a half-written receipt
tmp = f"{path}.tmp.{os.getpid()}"
with open(tmp, "w") as fh:
    json.dump(receipt, fh)
    fh.flush()
    os.fsync(fh.fileno())
os.replace(tmp, path)
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

    @classmethod
    def from_env(cls) -> "CodexClusterConfig":
        d = cls()
        return cls(
            codex_cmd=os.environ.get("FARM_CODEX_CMD", d.codex_cmd),
            path_prelude=os.environ.get("FARM_CODEX_PATH_PRELUDE", d.path_prelude),
            session_id_capture_delay=int(
                os.environ.get("FARM_CODEX_SID_DELAY", d.session_id_capture_delay)
            ),
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
        }.items()
    )


def _prelude(cfg: CodexClusterConfig) -> str:
    return (cfg.path_prelude + "\n") if cfg.path_prelude else ""


def _run_and_record(
    *, invocation: str, log_name: str, pid_file: str, sid_path: str | None,
    append_log: bool, delay: int,
) -> str:
    """Shared tail used by BOTH launch and resume: background the codex
    invocation, record THIS invocation's (pid, starttime) identity to `pid_file`,
    optionally capture the session id from this worker's own log, then wait.

    Because launch and resume share this block, a resumed worker's pid identity is
    refreshed exactly like a launched one -- so poll() liveness always tracks the
    CURRENT codex process, never a resumed worker's dead original invocation.
    """
    tee = f"tee -a {shlex.quote(log_name)}" if append_log else f"tee {shlex.quote(log_name)}"
    sid_capture = ""
    if sid_path is not None:  # only launch needs to discover the id; resume reuses it
        sid_capture = f"""# race-free session id: codex prints it into THIS worker's own log at startup
( for _ in $(seq 1 {delay}); do
    sid=$(grep -oiE 'session id: [0-9a-f-]{{36}}' {shlex.quote(log_name)} 2>/dev/null | grep -oE '[0-9a-f-]{{36}}' | head -1)
    [ -n "$sid" ] && {{ echo "$sid" > {shlex.quote(sid_path)}; break; }}
    sleep 1
  done ) &
"""
    return f"""{invocation} > >({tee}) 2>&1 &
_CPID=$!
_ST=$(awk '{{s=substr($0,index($0,") ")+2); n=split(s,a," "); print a[20]}}' /proc/$_CPID/stat 2>/dev/null)
echo "$_CPID $_ST" > {shlex.quote(pid_file)}
{sid_capture}wait $_CPID
"""


def render_launch_script(
    *, cfg: CodexClusterConfig, workspace: str, worker_id: str, task_id: str,
    lease_id: str, receipt_path: str, brief: str, log_name: str, sid_path: str,
    pid_file: str,
) -> str:
    """Headless-codex launch mirroring the proven RESTART_HEADLESS.sh, plus the
    runtime env + receipt protocol. Records (pid,starttime) identity and captures
    the session id from this worker's own log (see _run_and_record)."""
    full_brief = brief.rstrip() + "\n\n" + receipt_instruction(receipt_path)
    body = _run_and_record(
        invocation=f"{cfg.codex_cmd} {shlex.quote(full_brief)}",
        log_name=log_name, pid_file=pid_file, sid_path=sid_path,
        append_log=False, delay=cfg.session_id_capture_delay,
    )
    return f"""#!/bin/bash
cd {shlex.quote(workspace)}
{_prelude(cfg)}{_env_exports(worker_id, task_id, lease_id, receipt_path)}
rm -f {shlex.quote(receipt_path)}
{body}"""


def render_resume_script(
    *, cfg: CodexClusterConfig, workspace: str, worker_id: str, task_id: str,
    lease_id: str, receipt_path: str, sid_path: str, log_name: str, wake_msg: str,
    pid_file: str,
) -> str:
    """Resume an existing codex session (mirrors WAKE.sh) under the same lease.

    Uses the SAME _run_and_record tail as launch, so the resumed process's
    (pid,starttime) identity is written to `pid_file`; poll() then sees the live
    resumed process instead of the dead original (the bug this fixes)."""
    full_msg = wake_msg.rstrip() + "\n\n" + receipt_instruction(receipt_path)
    body = _run_and_record(
        invocation=f'{cfg.codex_cmd} resume "$SID" {shlex.quote(full_msg)}',
        log_name=log_name, pid_file=pid_file, sid_path=None,  # id unchanged on resume
        append_log=True, delay=cfg.session_id_capture_delay,
    )
    return f"""#!/bin/bash
cd {shlex.quote(workspace)}
{_prelude(cfg)}{_env_exports(worker_id, task_id, lease_id, receipt_path)}
rm -f {shlex.quote(receipt_path)}
SID=$(cat {shlex.quote(sid_path)} 2>/dev/null)
if [ -z "$SID" ]; then echo "no session id at {sid_path}; cannot resume" >&2; exit 3; fi
{body}"""


# --- liveness (default; injectable) -----------------------------------------

def _default_codex_alive(state: dict) -> bool:
    """Per-worker liveness: is the SPECIFIC codex process we launched still the
    live process at its recorded (pid, starttime)?

    This deliberately does NOT scan for any codex whose cwd matches the workspace:
    a workspace-level pgrep cannot tell two workers (e.g. a dead prior generation
    and its live successor) apart in the same workspace. Identity is the launched
    pid plus its /proc start-time, so a recycled pid is not mistaken for a worker.
    """
    pid_file = state.get("pid_file")
    if not pid_file:
        return False
    pid, start = read_pidfile(pid_file)
    return pid_identity_alive(pid, start)


def _default_run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


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
        run: Callable[[list[str]], subprocess.CompletedProcess] = _default_run,
        is_alive: Callable[[dict], bool] = _default_codex_alive,
    ):
        self.session = session
        self.tmux_socket = tmux_socket
        self.config = config or CodexClusterConfig.from_env()
        self.run = run
        self.is_alive = is_alive
        self.state_dir = Path(runtime_dir) / "codex_workers"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.receipts_dir = Path(runtime_dir) / "receipts"
        self.receipts_dir.mkdir(parents=True, exist_ok=True)

    # -- per-worker persisted mapping -----------------------------------------

    def _state_path(self, worker_id: str) -> Path:
        return self.state_dir / f"{worker_id}.json"

    def _save_state(self, worker_id: str, data: dict) -> None:
        self._state_path(worker_id).write_text(json.dumps(data, indent=2))

    def _load_state(self, worker_id: str) -> dict | None:
        p = self._state_path(worker_id)
        return json.loads(p.read_text()) if p.exists() else None

    def _receipt_path(self, worker_id: str) -> Path:
        return self.receipts_dir / f"{worker_id}.json"

    # -- tmux helpers (create-if-absent + send-keys only) --------------------

    def _tmux(self, *args: str) -> list[str]:
        base = ["tmux"]
        if self.tmux_socket:
            base += ["-L", self.tmux_socket]
        return base + list(args)

    def _ensure_window(self, window: str, cwd: str) -> None:
        if self.run(self._tmux("has-session", "-t", self.session)).returncode != 0:
            self.run(self._tmux("new-session", "-d", "-s", self.session, "-n", window, "-c", cwd))
            return
        listed = self.run(self._tmux("list-windows", "-t", self.session, "-F", "#{window_name}"))
        if window not in (listed.stdout or "").split():
            self.run(self._tmux("new-window", "-t", self.session, "-n", window, "-c", cwd))

    def _send(self, window: str, line: str) -> None:
        self.run(self._tmux("send-keys", "-t", f"{self.session}:{window}", line, "Enter"))

    # -- WorkerExecutor protocol ---------------------------------------------

    def launch(self, task: Task, lease: Lease) -> LaunchHandle:
        ws = task.metadata.get("workspace")
        brief = task.metadata.get("brief")
        if not ws or not brief:
            raise ValueError(f"task {task.id} needs metadata.workspace and metadata.brief")
        wid = lease.worker_id
        # tmux pane identity is the WORKER id, not a human agent_label: each worker
        # generation gets its own unambiguous pane, so generations never collide.
        window = wid
        workspace = str(Path(ws).resolve())

        # idempotent per lease: never start a second codex for THIS live worker
        # (judged by this worker's own pid identity, not any codex in the workspace)
        st = self._load_state(wid)
        if st is not None and self.is_alive(st):
            return LaunchHandle(worker_id=wid, session_handle=f"{self.session}:{window}")

        receipt_path = str(self._receipt_path(wid))
        pid_file = str(self.state_dir / f"{wid}.pid")
        sid_path = str(Path(workspace) / f".session_id_{wid}")
        log_name = f"agent_{window}_{wid}.log"
        launch_sh = Path(workspace) / f".farm_launch_{wid}.sh"

        (Path(workspace) / ".farm_receipt.py").write_text(RECEIPT_HELPER)
        launch_sh.write_text(render_launch_script(
            cfg=self.config, workspace=workspace, worker_id=wid, task_id=task.id,
            lease_id=lease.lease_id, receipt_path=receipt_path, brief=brief,
            log_name=log_name, sid_path=sid_path, pid_file=pid_file,
        ))
        launch_sh.chmod(0o755)
        self._save_state(wid, {
            "workspace": workspace, "window": window, "receipt_path": receipt_path,
            "sid_path": sid_path, "log_name": log_name, "task_id": task.id,
            "lease_id": lease.lease_id, "pid_file": pid_file,
        })
        self._ensure_window(window, workspace)
        self._send(window, f"bash {shlex.quote(str(launch_sh))}")
        return LaunchHandle(worker_id=wid, session_handle=f"{self.session}:{window}")

    def resume(self, task: Task, worker_id: str, lease: Lease) -> None:
        st = self._load_state(worker_id)
        if st is None:
            self.launch(task, lease)  # nothing to resume; fresh launch
            return
        resume_sh = Path(st["workspace"]) / f".farm_resume_{worker_id}.sh"
        resume_sh.write_text(render_resume_script(
            cfg=self.config, workspace=st["workspace"], worker_id=worker_id, task_id=task.id,
            lease_id=lease.lease_id, receipt_path=st["receipt_path"],
            sid_path=st["sid_path"], log_name=st["log_name"], pid_file=st["pid_file"],
            wake_msg=("Wake-up: a condition you were awaiting has fired or a new "
                      "MASTER_*.md landed. Re-scan your workspace, verify the awaited "
                      "state, acknowledge in LEDGER, and continue."),
        ))
        resume_sh.chmod(0o755)
        self._ensure_window(st["window"], st["workspace"])
        self._send(st["window"], f"bash {shlex.quote(str(resume_sh))}")

    def poll(self, worker_id: str) -> WorkerObservation:
        st = self._load_state(worker_id)
        if st is None:
            return WorkerObservation(worker_id=worker_id, alive=False, receipt=None)
        receipt = None
        rp = Path(st["receipt_path"])
        if rp.exists():
            try:
                receipt = Receipt.from_dict(json.loads(rp.read_text()))
            except (ValueError, KeyError):
                receipt = None
        return WorkerObservation(worker_id=worker_id, alive=self.is_alive(st), receipt=receipt)

    def stop(self, worker_id: str) -> None:
        """Interrupt codex in the worker's window; leaves the window intact."""
        st = self._load_state(worker_id)
        if st is None:
            return
        self.run(self._tmux("send-keys", "-t", f"{self.session}:{st['window']}", "C-c"))
