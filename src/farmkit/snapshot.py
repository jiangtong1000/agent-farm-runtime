"""Provenance by copying, not by hashing prose (D8).

* Scripts a step runs are COPIED into attempts/<id>/code/ and submitted from there,
  so a queued job never follows later edits. Their digests are recorded for machine
  comparison against a gate record only.
* Large inputs are never re-hashed: we record path, size and, when the input
  directory carries a MANIFEST.json, that manifest's digest.
* The workspace git commit (if any) is the human-facing version.
"""
from __future__ import annotations

import functools
import hashlib
import os
import importlib.metadata
import shutil
import subprocess
import sys
from pathlib import Path

LARGE_INPUT_BYTES = 64 * 1024 * 1024


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def snapshot_code(workspace: Path, attempt_dir: Path, globs: list[str]) -> dict[str, str]:
    """Copy matching files (relative paths preserved) into attempt_dir/code/; return {rel: sha}."""
    workspace, code_dir = Path(workspace), Path(attempt_dir) / "code"
    out: dict[str, str] = {}
    for pattern in globs:
        for src in sorted(workspace.glob(pattern)):
            if not src.is_file() or "attempts" in src.relative_to(workspace).parts:
                continue
            rel = src.relative_to(workspace).as_posix()
            dst = code_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            out[rel] = sha256_file(dst)
    return out


def describe_inputs(workspace: Path, inputs: list[str]) -> dict[str, dict]:
    """Identity of inputs without repeated hashing of big data."""
    out: dict[str, dict] = {}
    for rel in inputs:
        path = (Path(workspace) / rel) if not Path(rel).is_absolute() else Path(rel)
        entry: dict = {"path": str(path), "exists": path.exists()}
        if path.is_dir():
            manifest = path / "MANIFEST.json"
            entry["kind"] = "directory"
            entry["manifest"] = str(manifest) if manifest.exists() else None
            if manifest.exists():
                entry["manifest_sha256"] = sha256_file(manifest)
        elif path.is_file():
            size = path.stat().st_size
            entry.update(kind="file", size=size)
            if size <= LARGE_INPUT_BYTES:
                entry["sha256"] = sha256_file(path)
            else:
                entry["identity"] = "large: identified by size and manifest, not re-hashed"
        out[rel] = entry
    return out


def git_state(workspace: Path) -> dict | None:
    """{'commit': ..., 'dirty': bool} when the workspace is inside a git repo, else None."""
    try:
        head = subprocess.run(["git", "-C", str(workspace), "rev-parse", "--short=12", "HEAD"],
                              text=True, capture_output=True, timeout=10)
        if head.returncode != 0:
            return None
        status = subprocess.run(["git", "-C", str(workspace), "status", "--porcelain"],
                                text=True, capture_output=True, timeout=10)
        return {"commit": head.stdout.strip(), "dirty": bool(status.stdout.strip())}
    except (OSError, subprocess.TimeoutExpired):
        return None


def env_digest() -> str:
    """Digest of the interpreter path plus the sorted installed-distribution list.

    $FARMKIT_ENV_DIGEST, when set, is recorded verbatim instead: for tests and offline
    replays, where walking the environment costs seconds per worker generation.
    """
    pinned = os.environ.get("FARMKIT_ENV_DIGEST")
    if pinned:
        return pinned
    return _computed_env_digest()


@functools.lru_cache(maxsize=1)
def _computed_env_digest() -> str:
    names = sorted(f"{d.metadata['Name']}=={d.version}" for d in importlib.metadata.distributions()
                   if d.metadata and d.metadata.get("Name"))
    payload = sys.executable + "\n" + "\n".join(names)
    return hashlib.sha256(payload.encode()).hexdigest()
