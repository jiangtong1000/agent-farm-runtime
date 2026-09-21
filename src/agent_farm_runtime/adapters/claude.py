"""Claude Code CLI worker backend (D16).

Same tmux isolation, identity, receipt and resume contract as CodexTmuxExecutor;
only the agent command line and the session-id capture differ:

  launch:  claude -p <prompt> --dangerously-skip-permissions --output-format json
           (the JSON result carries "session_id": "<uuid>", captured from the log)
  resume:  claude --resume <session_id> -p <prompt> --dangerously-skip-permissions --output-format json

Cluster specifics come from FARM_CLAUDE_CMD / FARM_CLAUDE_PATH_PRELUDE, never from
this module.
"""
from __future__ import annotations

import os
import shlex
from dataclasses import dataclass, replace

from .codex import CodexClusterConfig, CodexTmuxExecutor

CLAUDE_SID_PATTERN = '"session_id": *"[0-9a-f-]{36}"'


@dataclass(frozen=True)
class ClaudeClusterConfig(CodexClusterConfig):
    """`codex_cmd` holds the claude command line; the field name is inherited."""

    codex_cmd: str = "claude -p --dangerously-skip-permissions --output-format json"

    @classmethod
    def from_env(cls) -> "ClaudeClusterConfig":
        base = CodexClusterConfig.from_env()
        return cls(codex_cmd=os.environ.get("FARM_CLAUDE_CMD", cls.codex_cmd),
                   path_prelude=os.environ.get("FARM_CLAUDE_PATH_PRELUDE", base.path_prelude),
                   session_id_capture_delay=base.session_id_capture_delay,
                   max_prompt_bytes=base.max_prompt_bytes,
                   command_timeout_seconds=base.command_timeout_seconds)


class ClaudeTmuxExecutor(CodexTmuxExecutor):
    sid_pattern = CLAUDE_SID_PATTERN
    agent_name = "claude"

    def __init__(self, runtime_dir, *, session: str = "farm2", tmux_socket: str | None = None,
                 config: ClaudeClusterConfig | None = None, run=None, is_alive=None):
        kwargs = {"session": session, "tmux_socket": tmux_socket, "config": config or ClaudeClusterConfig.from_env()}
        if run is not None:
            kwargs["run"] = run
        if is_alive is not None:
            kwargs["is_alive"] = is_alive
        super().__init__(runtime_dir, **kwargs)

    def _launch_invocation(self, prompt: str) -> str:
        # `claude -p` takes the prompt as the argument that follows -p; keep the flags after it.
        cmd, _, rest = self.config.codex_cmd.partition(" -p")
        return f"{cmd} -p {shlex.quote(prompt)}{rest}"

    def _resume_invocation(self, prompt: str) -> str:
        cmd, _, rest = self.config.codex_cmd.partition(" -p")
        return f'{cmd} --resume "$SID" -p {shlex.quote(prompt)}{rest}'
