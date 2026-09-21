"""Site profile: everything that differs between clusters, kept out of code (D3).

The repository ships only sites/example.toml. A user keeps their own profile at
$FARMKIT_SITE or under ~/.config/agent-farm/sites/*.toml; the profile whose
[site].hostname_pattern matches this host is used.
"""
from __future__ import annotations

import os
import re
import socket
import tomllib
from pathlib import Path


class SiteError(RuntimeError):
    pass


DEFAULTS = {
    "scheduler": {"kind": "slurm", "squeue": "squeue", "sacct": "sacct", "sbatch": "sbatch",
                  "accounting_stores_comment": True, "dependency_kill_invalid": True,
                  "barrier_partition": None, "barrier_time": "0:05:00", "poll_interval_s": 30},
    "python": {}, "executor": {}, "worker": {"rotate_when_input_tokens_over": 120000,
                                            "rotate_after_wakes": 12}, "paths": {},
}


class Site:
    def __init__(self, data: dict, source: Path | None = None):
        if "site" not in data or "name" not in data["site"]:
            raise SiteError("site profile needs [site] name")
        self.data = {k: {**DEFAULTS.get(k, {}), **(data.get(k) or {})} for k in
                     set(DEFAULTS) | set(data)}
        self.source = source

    @property
    def name(self) -> str:
        return self.data["site"]["name"]

    @property
    def scheduler(self) -> dict:
        return self.data["scheduler"]

    @property
    def worker(self) -> dict:
        return self.data["worker"]

    @property
    def paths(self) -> dict:
        return self.data["paths"]

    def executor(self, name: str) -> dict:
        return (self.data.get("executor") or {}).get(name, {})

    def matches(self, hostname: str) -> bool:
        pattern = self.data["site"].get("hostname_pattern")
        return bool(pattern) and re.search(pattern, hostname) is not None

    @classmethod
    def load(cls, path: Path) -> "Site":
        with open(path, "rb") as fh:
            return cls(tomllib.load(fh), Path(path))

    @classmethod
    def minimal(cls, name: str = "test", **scheduler) -> "Site":
        return cls({"site": {"name": name, "hostname_pattern": ".*"}, "scheduler": scheduler})

    @classmethod
    def detect(cls, hostname: str | None = None, env: dict | None = None) -> "Site":
        env = os.environ if env is None else env
        hostname = hostname or socket.gethostname()
        if env.get("FARMKIT_SITE"):
            return cls.load(Path(env["FARMKIT_SITE"]))
        root = Path(env.get("XDG_CONFIG_HOME") or Path(env.get("HOME", "~")).expanduser() / ".config") / "agent-farm" / "sites"
        candidates = sorted(root.glob("*.toml")) if root.exists() else []
        for path in candidates:
            site = cls.load(path)
            if site.matches(hostname):
                return site
        raise SiteError(f"no site profile matches host {hostname!r}: set FARMKIT_SITE or add one under {root} "
                        "(see sites/example.toml)")
