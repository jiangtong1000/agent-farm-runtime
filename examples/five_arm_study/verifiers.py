"""Project verifiers for the five-arm study (D11: reject broken outputs, never judge science).

Each function receives the attempt record and {output name: Path} and returns a
farmkit Verdict. farmkit has already checked that the main output belongs to this
attempt, that required files exist and that declared metrics are finite.
"""
from __future__ import annotations

import json
from pathlib import Path

from farmkit.verify import Verdict

DECLARED_STOPS = {"max_updates", "plateau", "stage_wall_cap", "budget_exhausted"}


def _main(artifacts: dict) -> dict:
    return json.loads(Path(next(iter(artifacts.values()))).read_text())


def verify_train(attempt: dict, artifacts: dict) -> Verdict:
    run = _main(artifacts)
    reasons = []
    if run.get("stop_reason") not in DECLARED_STOPS:
        reasons.append(f"stop_reason {run.get('stop_reason')!r} is not one of the declared stop reasons")
    if run.get("stationary_certified") and run.get("grad_inf", 1.0) > 1e-5:
        reasons.append("stationary_certified claimed but grad_inf exceeds 1e-5")
    return Verdict(not reasons, reasons, ["stop_reason", "stationarity"])


def verify_segment(attempt: dict, artifacts: dict) -> Verdict:
    end = _main(artifacts)
    ok = end.get("status") == "PASS"
    return Verdict(ok, [] if ok else [f"segment ended with status {end.get('status')!r}"], ["segment_end"])
