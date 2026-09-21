"""steps.toml: the project's declarative step table (SRC_REFACTOR_PROPOSAL §5, D12/D13).

    [defaults]
    retry = { infra = 1 }
    snapshot = ["*.py", "*.sbatch"]
    finite = ["selected.validation.S_val"]

    [step."train:{arm}"]
    matrix.arm = ["lam0", "lam0.1", "lam1"]          # or matrix = [{arm="lam0", lam="0"}, ...]
    run = "train.sbatch"                              # *.sbatch -> sbatch; anything else -> local command
    env = { SLT_LAMBDA = "{lam}" }
    inputs = ["theta_init.npz", "minor_cache_mo"]
    outputs = ["run_{arm}/RUN.json"]                  # first is the main (JSON) artifact
    group = "round1"                                  # wait = "earliest" (default) | "barrier"
    after = ["group:round1", "smoke:{arm}"]
    verify = "verify_train"                           # optional project function
    gate = "gate4"                                    # optional: code must equal that step's certified code
    segments = 4   chain = "afterok"   concurrency = 2

Everything a worker needs to decide "what now" is derivable from this file plus the ledger.
"""
from __future__ import annotations

import itertools
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from .ledger import Ledger

KNOWN_STEP_KEYS = {"matrix", "run", "env", "inputs", "outputs", "group", "after", "verify", "gate",
                   "segments", "chain", "concurrency", "wait", "finite", "retry", "snapshot", "timeout_s"}
KNOWN_DEFAULT_KEYS = {"retry", "snapshot", "finite", "wait", "timeout_s"}


class StepsError(ValueError):
    pass


_VAR = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _fmt(text: str, vars_: dict, *, strict: bool = True) -> str:
    """Substitute {var} placeholders. strict: an unknown placeholder is an error
    (ids, outputs, inputs, after, group). Non-strict (run, env): unknown braces are
    left alone so shell/JSON snippets survive."""
    def sub(m):
        name = m.group(1)
        if name in vars_:
            return str(vars_[name])
        if strict:
            raise StepsError(f"unknown variable {{{name}}}")
        return m.group(0)
    return _VAR.sub(sub, text)


@dataclass
class StepSpec:
    id: str
    template: str
    vars: dict = field(default_factory=dict)
    run: str = ""
    kind: str = "local"          # "sbatch" | "local"
    env: dict = field(default_factory=dict)
    inputs: list = field(default_factory=list)
    outputs: list = field(default_factory=list)
    group: str | None = None
    after: list = field(default_factory=list)
    verify: str | None = None
    gate: str | None = None
    segments: int = 1
    segment: int | None = None   # 1-based when expanded from a chain
    chain: str = "afterok"
    concurrency: int | None = None
    wait: str = "earliest"
    finite: list = field(default_factory=list)
    retry: dict = field(default_factory=dict)
    snapshot: list = field(default_factory=list)
    timeout_s: float | None = None

    @property
    def main_output(self) -> str | None:
        return self.outputs[0] if self.outputs else None

    @property
    def pool(self) -> str:
        return self.template

    @property
    def chain_root(self) -> str:
        return self.id.split("#", 1)[0]


def _rows(matrix) -> list[dict]:
    if matrix is None:
        return [{}]
    if isinstance(matrix, list):
        return [dict(r) for r in matrix]
    keys = list(matrix)
    return [dict(zip(keys, combo)) for combo in itertools.product(*(matrix[k] for k in keys))]


class Steps:
    def __init__(self, specs: list[StepSpec], defaults: dict, source: Path | None = None,
                 verifiers=None, problems: list[str] | None = None):
        self.specs = specs
        self.by_id = {s.id: s for s in specs}
        self.defaults = defaults
        self.source = source
        self.verifiers = verifiers
        self._problems = problems or []

    # -- loading --------------------------------------------------------------
    @classmethod
    def load(cls, path: Path, verifiers=None) -> "Steps":
        path = Path(path)
        with open(path, "rb") as fh:
            data = tomllib.load(fh)
        return cls.from_dict(data, source=path, verifiers=verifiers)

    @classmethod
    def from_dict(cls, data: dict, *, source: Path | None = None, verifiers=None) -> "Steps":
        problems: list[str] = []
        defaults = dict(data.get("defaults") or {})
        for key in defaults:
            if key not in KNOWN_DEFAULT_KEYS:
                problems.append(f"defaults.{key}: unknown key")
        specs: list[StepSpec] = []
        for template, body in (data.get("step") or {}).items():
            for key in body:
                if key not in KNOWN_STEP_KEYS:
                    problems.append(f'step."{template}".{key}: unknown key')
            for row in _rows(body.get("matrix")):
                try:
                    specs.extend(cls._expand(template, body, row, defaults))
                except StepsError as exc:
                    problems.append(f'step."{template}": {exc}')
        ids = [s.id for s in specs]
        for dup in sorted({i for i in ids if ids.count(i) > 1}):
            problems.append(f"duplicate step id {dup}")
        return cls(specs, defaults, source, verifiers, problems)

    @staticmethod
    def _expand(template: str, body: dict, row: dict, defaults: dict) -> list[StepSpec]:
        vars_ = dict(row)
        sid = _fmt(template, vars_)
        run = _fmt(body.get("run", ""), vars_, strict=False)
        base = StepSpec(
            id=sid, template=template, vars=vars_, run=run,
            kind="sbatch" if run.endswith(".sbatch") or run.startswith("sbatch ") else "local",
            env={k: _fmt(str(v), vars_, strict=False) for k, v in (body.get("env") or {}).items()},
            inputs=[_fmt(x, vars_) for x in body.get("inputs", [])],
            outputs=[_fmt(x, {**vars_, "seg": "{seg}"}) for x in body.get("outputs", [])],
            group=_fmt(body["group"], vars_) if body.get("group") else None,
            after=[_fmt(x, vars_) for x in body.get("after", [])],
            verify=body.get("verify"), gate=body.get("gate"),
            segments=int(body.get("segments", 1)), chain=body.get("chain", "afterok"),
            concurrency=body.get("concurrency"),
            wait=body.get("wait", defaults.get("wait", "earliest")),
            finite=list(body.get("finite", defaults.get("finite", []))),
            retry=dict(defaults.get("retry", {}), **body.get("retry", {})),
            snapshot=list(body.get("snapshot", defaults.get("snapshot", []))),
            timeout_s=body.get("timeout_s", defaults.get("timeout_s")),
        )
        if base.segments <= 1:
            return [base]
        out = []
        for k in range(1, base.segments + 1):
            seg = StepSpec(**{**base.__dict__, "id": f"{sid}#{k}", "segment": k,
                              "outputs": [o.replace("{seg}", str(k)) for o in base.outputs],
                              "env": {**base.env, "SEGMENT": str(k)},
                              "after": list(base.after) if k == 1 else [f"chain:{sid}#{k-1}"]})
            out.append(seg)
        return out

    # -- validation --------------------------------------------------------------
    def check(self, workspace: Path | None = None) -> list[str]:
        problems = list(self._problems)
        groups = self.groups()
        for s in self.specs:
            for dep in s.after:
                kind, _, rest = dep.partition(":")
                if kind == "group" and rest not in groups:
                    problems.append(f"{s.id}: after refers to unknown group {rest}")
                elif kind == "chain" and rest not in self.by_id:
                    problems.append(f"{s.id}: chain refers to unknown segment {rest}")
                elif kind not in {"group", "chain"} and dep not in self.by_id:
                    problems.append(f"{s.id}: after refers to unknown step {dep}")
            if s.gate and s.gate not in self.by_id:
                problems.append(f"{s.id}: gate refers to unknown step {s.gate}")
            if s.verify:
                try:
                    self.verifier_for(s)
                except StepsError as exc:
                    problems.append(str(exc))
            if s.wait not in {"earliest", "barrier"}:
                problems.append(f"{s.id}: wait must be earliest or barrier")
            if workspace is not None and s.kind == "sbatch" and not (Path(workspace) / s.run).exists():
                problems.append(f"{s.id}: run script {s.run} not found in workspace")
            if s.kind == "sbatch" and not s.outputs:
                problems.append(f"{s.id}: sbatch step declares no outputs; nothing could be verified")
            problems.extend(f"{s.id}: {p}" for p in path_problems(s))
        return problems

    # -- queries -------------------------------------------------------------------
    def groups(self) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for s in self.specs:
            if s.group:
                out.setdefault(s.group, []).append(s.id)
        return out

    def verifier_for(self, spec: StepSpec):
        """The project verifier a step declares. Declaring one is optional; once declared it
        must resolve, otherwise the run would be certified by a check that never ran."""
        if not spec.verify:
            return None
        if self.verifiers is None:
            raise StepsError(f"{spec.id}: verifier {spec.verify} declared but no verifier module was loaded (pass --verifiers)")
        fn = self.verifiers.get(spec.verify) if isinstance(self.verifiers, dict) else getattr(self.verifiers, spec.verify, None)
        if not callable(fn):
            raise StepsError(f"{spec.id}: verifier {spec.verify} not found in the verifier module")
        return fn

    def ok(self, ledger: Ledger, step_id: str) -> bool:
        return ledger.ok(step_id)

    def unresolved(self, ledger: Ledger, step_id: str) -> bool:
        """Latest attempt exists but has neither verdict ok nor a parked failure."""
        latest = ledger.latest(step_id)
        if latest is None:
            return False
        if (latest.get("verdict") or {}).get("ok"):
            return False
        failure = latest.get("failure") or {}
        if failure.get("parked") or failure.get("released"):
            return False           # parked: waits for a ruling; released: ready for one more attempt
        if failure.get("retried") and not failure.get("parked"):
            return False           # retry decided, next attempt never recorded (crash): submit it now
        return True

    def dependency_met(self, ledger: Ledger, dep: str) -> bool:
        kind, _, rest = dep.partition(":")
        if kind == "group":
            return all(ledger.ok(i) for i in self.groups().get(rest, [])) and bool(self.groups().get(rest))
        if kind == "chain":
            prev = ledger.latest(rest)
            return bool(prev and (prev.get("submit") or {}).get("job_id")) and not (prev.get("failure") or {}).get("parked")
        return ledger.ok(dep)

    def active_in_pool(self, ledger: Ledger, spec: StepSpec) -> int:
        roots = set()
        for other in self.specs:
            if other.pool == spec.pool and other.segment is not None and self.unresolved(ledger, other.id):
                roots.add(other.chain_root)
        return len(roots)

    def ready(self, ledger: Ledger) -> list[StepSpec]:
        out = []
        for s in self.specs:
            if ledger.ok(s.id) or self.unresolved(ledger, s.id):
                continue
            if (ledger.latest(s.id) or {}).get("failure", {}) and (ledger.latest(s.id) or {}).get("failure", {}).get("parked"):
                continue
            if not all(self.dependency_met(ledger, d) for d in s.after):
                continue
            if s.concurrency and s.segment == 1 and self.active_in_pool(ledger, s) >= s.concurrency:
                continue
            out.append(s)
        return out

    def all_ok(self, ledger: Ledger) -> bool:
        return all(ledger.ok(s.id) for s in self.specs)

    def previous_job(self, ledger: Ledger, spec: StepSpec) -> str | None:
        for dep in spec.after:
            if dep.startswith("chain:"):
                prev = ledger.latest(dep.split(":", 1)[1])
                return (prev or {}).get("submit", {}).get("job_id") if prev else None
        return None


def _relative_ok(rel: str) -> str | None:
    """Why a declared input/output path may not be used inside a workspace, or None."""
    p = Path(rel)
    if rel in ("", ".") or p.is_absolute():
        return "must be a relative path inside the workspace, not '.' or absolute"
    if ".." in p.parts:
        return "must not contain '..'"
    if p.parts[0] == "attempts":
        return "must not point into attempts/ (the ledger)"
    return None


def path_problems(spec: "StepSpec") -> list[str]:
    """Output and input declarations a run directory cannot honour (G02, G04):
    outputs must be strict, normalised paths inside the workspace and outside attempts/;
    an input may not name an output (or lie inside one), because outputs are never linked."""
    out: list[str] = []
    for name in spec.outputs:
        why = _relative_ok(name)
        if why:
            out.append(f"output {name!r} {why}")
    for rel in spec.inputs:
        if Path(rel).is_absolute():
            continue
        why = _relative_ok(rel)
        if why:
            out.append(f"input {rel!r} {why}")
            continue
        for name in spec.outputs:
            if rel == name or Path(rel).parts[:len(Path(name).parts)] == Path(name).parts:
                out.append(f"input {rel!r} overlaps output {name!r}: outputs are never linked into the run "
                           "directory, so a step cannot read and rewrite the same path; declare a different input")
    return out


def sanitize(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", text).strip("-")
