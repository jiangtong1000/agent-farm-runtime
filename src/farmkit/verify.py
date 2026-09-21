"""Default verification (D11): reject broken outputs, never judge the science.

Three generic checks: the output belongs to THIS attempt, required files exist, and
every declared metric is a finite number. Optionally the code that ran must match the
code a gate (smoke) certified. Project-specific checks are ordinary callables.
Reasons name files and fields; they never print digests (D8).
"""
from __future__ import annotations

import json
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Verdict:
    ok: bool
    reasons: list[str] = field(default_factory=list)
    checked: list[str] = field(default_factory=list)

    def merge(self, other: "Verdict") -> "Verdict":
        return Verdict(self.ok and other.ok, self.reasons + other.reasons, self.checked + other.checked)


Verifier = Callable[[dict, dict], Verdict]   # (attempt record, {name: Path}) -> Verdict


def resolve(payload, dotted: str):
    cur = payload
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
            cur = cur[int(part)]
        else:
            return None, False
    return cur, True


def default_checks(attempt: dict, artifacts: dict[str, Path], *, required: list[str] = (),
                   finite: list[str] = (), gate: dict | None = None, main: str | None = None) -> Verdict:
    v = Verdict(True)
    for name in required:
        v.checked.append(f"required:{name}")
        path = artifacts.get(name)
        if path is None or not Path(path).exists():
            v.ok = False
            v.reasons.append(f"required output missing: {name}")
    main_name = main or (list(artifacts)[0] if artifacts else None)
    payload, parsed = None, False
    if main_name and artifacts.get(main_name) and Path(artifacts[main_name]).exists():
        try:
            payload = json.loads(Path(artifacts[main_name]).read_text())
            parsed = True
        except (ValueError, OSError) as exc:
            v.ok = False
            v.reasons.append(f"{main_name} is not readable JSON: {exc}")
    if parsed and not isinstance(payload, dict):       # null, [], 3, "x": a file, but not an artifact
        v.ok = False
        v.checked.append("attempt-match")
        v.reasons.append(f"{main_name} is not a JSON object (got {type(payload).__name__}); attempt and metric checks cannot run")
        payload = None
    if payload is not None:
        v.checked.append("attempt-match")
        prov = payload.get("provenance")
        seen = prov.get("attempt_id") if isinstance(prov, dict) else None
        if seen != attempt["attempt_id"]:
            v.ok = False
            v.reasons.append(f"{main_name} belongs to a different attempt (expected this run, found {'none' if not seen else 'an earlier one'})")
        for key in finite:
            v.checked.append(f"finite:{key}")
            value, found = resolve(payload, key)
            if not found or isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                v.ok = False
                v.reasons.append(f"{key} is missing or not a finite number in {main_name}")
    if gate:
        v.checked.append("gate")
        certified = gate.get("code") or {}
        ran = attempt.get("code") or {}
        differing = sorted(f for f in set(certified) | set(ran) if certified.get(f) != ran.get(f))
        if differing:
            v.ok = False
            v.reasons.append("code differs from the certified gate: " + ", ".join(differing))
    return v


def run_verifier(fn: Verifier, attempt: dict, artifacts: dict[str, Path]) -> Verdict:
    try:
        result = fn(attempt, artifacts)
    except Exception as exc:  # a crashing verifier is a failed check, not a pass
        return Verdict(False, [f"verifier {getattr(fn, '__name__', 'callable')} raised {type(exc).__name__}: {exc}"], ["verifier"])
    if not isinstance(result, Verdict):
        return Verdict(bool(result), [] if result else ["verifier returned false"], ["verifier"])
    return result
