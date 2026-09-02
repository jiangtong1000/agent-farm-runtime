from __future__ import annotations

import json
import os
import shlex
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ..models import Lease, Receipt, Task
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
with open(path, "w") as fh:
    json.dump(receipt, fh)
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
    # directory (a shell expression is allowed) where codex writes rollout-*.jsonl
    sessions_dir: str = "$HOME/.codex/sessions/$(date +%Y/%m/%d)"
    session_id_capture_delay: int = 45

    @classmethod
    def from_env(cls) -> "CodexClusterConfig":
        d = cls()
        return cls(
            codex_cmd=os.environ.get("FARM_CODEX_CMD", d.codex_cmd),
            path_prelude=os.environ.get("FARM_CODEX_PATH_PRELUDE", d.path_prelude),
            sessions_dir=os.environ.get("FARM_CODEX_SESSIONS_DIR", d.sessions_dir),
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


def render_launch_script(
    *, cfg: CodexClusterConfig, workspace: str, worker_id: str, task_id: str,
    lease_id: str, receipt_path: str, brief: str, log_name: str, sid_path: str,
) -> str:
    """Headless-codex launch mirroring the proven RESTART_HEADLESS.sh, plus the
    runtime env + receipt protocol. Session-id capture diffs the rollout set
    against a pre-launch snapshot (rather than blindly taking the newest file),
    which is robust to pre-existing rollouts; concurrent starts within the
    capture window remain ambiguous and are serialized by the reconciler."""
    full_brief = brief.rstrip() + "\n\n" + receipt_instruction(receipt_path)
    sd = cfg.sessions_dir
    return f"""#!/bin/bash
cd {shlex.quote(workspace)}
{_prelude(cfg)}{_env_exports(worker_id, task_id, lease_id, receipt_path)}
rm -f {shlex.quote(receipt_path)}
# capture the NEW codex session id (rollout that appears after launch)
_PRE=$(mktemp); ls -1 {sd}/rollout-*.jsonl 2>/dev/null > "$_PRE" || true
( sleep {cfg.session_id_capture_delay}; \
  f=$(ls -t {sd}/rollout-*.jsonl 2>/dev/null | grep -vxF -f "$_PRE" | head -1); \
  [ -n "$f" ] && echo "$f" | grep -oE '[0-9a-f-]{{36}}' > {shlex.quote(sid_path)}; \
  rm -f "$_PRE" ) &
{cfg.codex_cmd} {shlex.quote(full_brief)} 2>&1 | tee {shlex.quote(log_name)}
"""


def render_resume_script(
    *, cfg: CodexClusterConfig, workspace: str, worker_id: str, task_id: str,
    lease_id: str, receipt_path: str, sid_path: str, log_name: str, wake_msg: str,
) -> str:
    """Resume an existing codex session (mirrors WAKE.sh) under the same lease."""
    full_msg = wake_msg.rstrip() + "\n\n" + receipt_instruction(receipt_path)
    return f"""#!/bin/bash
cd {shlex.quote(workspace)}
{_prelude(cfg)}{_env_exports(worker_id, task_id, lease_id, receipt_path)}
rm -f {shlex.quote(receipt_path)}
SID=$(cat {shlex.quote(sid_path)} 2>/dev/null)
if [ -z "$SID" ]; then echo "no session id at {sid_path}; cannot resume" >&2; exit 3; fi
{cfg.codex_cmd} resume "$SID" {shlex.quote(full_msg)} 2>&1 | tee -a {shlex.quote(log_name)}
"""


# --- liveness (default; injectable) -----------------------------------------

def _default_is_alive(workspace: str) -> bool:
    """Live iff a codex process has this workspace as cwd (the farm's own test).

    NOTE: this is a best-effort signal; the reconciler's grace policy — not this
    function — is what protects a live worker from a transient miss."""
    try:
        pids = subprocess.run(["pgrep", "-u", str(os.getuid()), "-x", "codex"],
                              capture_output=True, text=True).stdout.split()
    except FileNotFoundError:
        return False
    target = str(Path(workspace).resolve())
    for pid in pids:
        try:
            if str(Path(f"/proc/{pid}/cwd").resolve()) == target:
                return True
        except (OSError, RuntimeError):
            continue
    return False


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
        is_alive: Callable[[str], bool] = _default_is_alive,
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
        window = task.metadata.get("agent_label") or wid
        workspace = str(Path(ws).resolve())

        # idempotent per lease: never start a second codex for a live worker
        st = self._load_state(wid)
        if st is not None and self.is_alive(workspace):
            return LaunchHandle(worker_id=wid, session_handle=f"{self.session}:{window}")

        receipt_path = str(self._receipt_path(wid))
        sid_path = str(Path(workspace) / f".session_id_{wid}")
        log_name = f"agent_{window}_{wid}.log"
        launch_sh = Path(workspace) / f".farm_launch_{wid}.sh"

        (Path(workspace) / ".farm_receipt.py").write_text(RECEIPT_HELPER)
        launch_sh.write_text(render_launch_script(
            cfg=self.config, workspace=workspace, worker_id=wid, task_id=task.id,
            lease_id=lease.lease_id, receipt_path=receipt_path, brief=brief,
            log_name=log_name, sid_path=sid_path,
        ))
        launch_sh.chmod(0o755)
        self._save_state(wid, {
            "workspace": workspace, "window": window, "receipt_path": receipt_path,
            "sid_path": sid_path, "log_name": log_name, "task_id": task.id,
            "lease_id": lease.lease_id,
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
            sid_path=st["sid_path"], log_name=st["log_name"],
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
        return WorkerObservation(worker_id=worker_id, alive=self.is_alive(st["workspace"]), receipt=receipt)

    def stop(self, worker_id: str) -> None:
        """Interrupt codex in the worker's window; leaves the window intact."""
        st = self._load_state(worker_id)
        if st is None:
            return
        self.run(self._tmux("send-keys", "-t", f"{self.session}:{st['window']}", "C-c"))
