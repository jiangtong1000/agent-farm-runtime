"""`farmkit brief lint`: keep the frozen prompt small and pointing at real files (D13)."""
from __future__ import annotations

import re
from pathlib import Path

MAX_BYTES = 2048
HEADINGS = {
    "goal": ("目标", "goal"), "boundaries": ("边界", "boundar"), "read": ("先读", "read first"),
    "method": ("做法", "method"),
}
HEX64 = re.compile(r"\b[0-9a-f]{64}\b")
PATH = re.compile(r"(?<![\w/])(/[\w./+-]+|[\w.-]+/[\w./+-]*|[\w.-]+\.(?:toml|md|json|py|sbatch))")
OPTIONAL_AFTER = re.compile(r"^[`\s\]\)\"']*(?:\(\s*)?(?:if (?:it exists|present|exists)|when present)\b", re.I)
RUNTIME_FILES = {"CHECKPOINT.md", ".farm_receipt.py"}
RUNTIME_DIRS = {".farm", "attempts"}


def _runtime_path(ref: str) -> bool:
    """Generated workspace files and runtime directories are not required inputs."""
    parts = Path(ref).parts
    return bool(parts and (parts[0] in RUNTIME_DIRS or len(parts) == 1 and parts[0] in RUNTIME_FILES))


def lint(path: Path) -> list[str]:
    path = Path(path)
    problems: list[str] = []
    raw = path.read_bytes()
    if len(raw) > MAX_BYTES:
        problems.append(f"brief is {len(raw)} bytes; keep it under {MAX_BYTES} (put detail in workspace files)")
    text = raw.decode("utf-8", errors="replace")
    lower = text.lower()
    for key, needles in HEADINGS.items():
        if not any(n in lower for n in needles):
            problems.append(f"missing section: {key} ({' / '.join(needles)})")
    if HEX64.search(text):
        problems.append("contains a 64-hex digest; name the file or version instead")
    if "farmkit tick" not in lower:
        problems.append("does not tell the worker to run `farmkit tick` on each wake")
    for line in text.splitlines():
        # URLs describe remote resources, not local prerequisites.
        line = re.sub(r"https?://\S+", "", line)
        for m in PATH.finditer(line):
            # A sentence-ending period is not part of a file or directory name.
            ref = m.group(1).rstrip(".")
            if _runtime_path(ref) or OPTIONAL_AFTER.match(line[m.end():]):
                continue
            candidate = Path(ref) if ref.startswith("/") else path.parent / ref
            if not candidate.exists() and not any(ch in ref for ch in "*<>{}"):
                problems.append(f"referenced path does not exist: {ref}")
    return problems
