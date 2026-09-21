"""The step runner: one idempotent `tick()` per worker wake (D13, D18, RFC §5).

    reconcile unverified intents -> observe in-flight jobs -> verify/classify what ended
    -> retry within budget or park with evidence -> submit whatever became ready
    -> tell the worker exactly what to report to the runtime.

The runner never writes runtime state: it returns the receipt parameters, and the
worker runs the runtime's own receipt helper. Failures park LAST (RFC §5.5 F1): while
other steps can still progress, the worker waits on their jobs, not on a ruling.
"""
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import classify, evidence, intent, observe, snapshot, waits
from ._fs import atomic_write_json, atomic_write_text, read_json
from .ledger import Ledger
from .site import Site
from .steps import Steps, StepSpec, StepsError, path_problems
from .verify import Verdict, default_checks, run_verifier

JUDGMENT_MARKER = "<!-- judgment: the agent writes 3-5 lines below this line; farmkit rewrites everything above -->"


@dataclass
class TickResult:
    receipt_status: str                 # "AWAITING" | "SUBMITTED"
    waiting_on: str | None
    note: str
    summary: str
    receipt_command: str
    checkpoint_path: str | None = None
    actions: list[str] = field(default_factory=list)
    submitted: list[str] = field(default_factory=list)
    parked: list[str] = field(default_factory=list)
    changed_files: list[str] = field(default_factory=list)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Runner:
    def __init__(self, site: Site, ledger: Ledger, steps: Steps, *, workspace: Path,
                 task_id: str | None = None, lease_id: str | None = None,
                 sbatch: Callable[[list[str]], subprocess.CompletedProcess] = intent.default_sbatch,
                 local_run: Callable[..., subprocess.CompletedProcess] = subprocess.run,
                 states: observe.StateCache | None = None,
                 accounting: intent.AccountingLookup | None = None,
                 sacct_row: Callable[[str], str | None] = observe.sacct_row,
                 receipt_helper: str = "python .farm_receipt.py"):
        self.site, self.ledger, self.steps = site, ledger, steps
        for spec in steps.specs:
            steps.verifier_for(spec)            # raises StepsError: a declared verifier must resolve (R02)
            problems = path_problems(spec)
            if problems:                        # G02/G04: refuse before anything runs
                raise StepsError(f"{spec.id}: " + "; ".join(problems))
        self.workspace = Path(workspace)
        problems = steps.check(self.workspace)
        if problems:
            raise StepsError("steps.toml has problems; fix them before running:\n  " + "\n  ".join(problems))
        self.task_id, self.lease_id = task_id, lease_id
        self.sbatch, self.local_run = sbatch, local_run
        self.states = states or observe.StateCache()
        self.accounting = accounting
        self.sacct_row = sacct_row
        self.receipt_helper = receipt_helper
        self.state_path = self.ledger.root / ".tick_state.json"

    # ------------------------------------------------------------------ tick
    def tick(self, checkpoint: bool = False) -> TickResult:
        actions: list[str] = []
        submitted: list[str] = []
        if self.accounting is not None:
            for aid, status in intent.reconcile_all(self.ledger, self.accounting).items():
                actions.append(f"reconciled intent {aid[:8]}: {status}")
        self._observe_and_finalize(actions, submitted)
        # Launch until nothing new becomes ready (chain segments become ready once the
        # previous segment has a job id). Bounded by the number of steps.
        for _ in range(len(self.steps.specs) * 2 + 1):
            ready = [s for s in self.steps.ready(self.ledger) if s.id not in submitted]
            if not ready:
                break
            spec = ready[0]
            self._launch(spec, actions)
            submitted.append(spec.id)       # attempted this tick, whatever the outcome
        self._barriers(actions, submitted)
        status, waiting_on, parked = self._decide()
        changed = self._changed_files()
        note = self._note(status, waiting_on, submitted, parked)
        checkpoint_path = self._write_checkpoint(status, waiting_on, parked) if checkpoint else None
        summary = self._summary(status, waiting_on, submitted, parked, changed, actions, checkpoint_path)
        cmd = self._receipt_command(status, waiting_on, note, checkpoint_path)
        return TickResult(status, waiting_on, note, summary, cmd, checkpoint_path, actions, submitted, parked, changed)

    # ------------------------------------------------------------- observe
    def _observe_and_finalize(self, actions: list[str], submitted: list[str]) -> None:
        # Execution ended, verification never recorded (crash in between): verify now, do
        # not re-run anything (R09).
        for rec in self.ledger.unfinalized():
            spec = self.steps.by_id.get(rec["step"])
            if spec is None:
                rec["verdict"] = {"ok": True, "reasons": [], "verifier": None, "ts": _now()}
                self.ledger.save(rec)
                continue
            actions.append(f"{spec.id}: execution had ended before a crash; verifying the recorded attempt")
            self._finalize(rec, spec, actions, submitted)
        inflight = self.ledger.in_flight()
        if not inflight:
            return
        ids = [r["submit"]["job_id"] for r in inflight]
        observed = self.states.states(ids)
        for rec in inflight:
            state = observed.get(rec["submit"]["job_id"])
            rec["observed"] = {"state": state, "observed_ts": _now(),
                               **({"end": _now()} if observe.is_terminal(state) else {})}
            if not observe.is_terminal(state):
                self.ledger.save(rec)
                continue
            spec = self.steps.by_id.get(rec["step"])
            if spec is None:                       # barrier or adopted-without-spec: record and move on
                rec["verdict"] = {"ok": True, "reasons": [], "verifier": None, "ts": _now()}
                self.ledger.save(rec)
                continue
            self._finalize(rec, spec, actions, submitted)

    def _artifacts(self, spec: StepSpec, rec: dict | None = None) -> dict[str, Path]:
        """Where this attempt's outputs are read from: the attempt's run directory when it
        wrote there, else the workspace (adopted jobs, outputs reached through a linked dir)."""
        run_dir = Path(rec["run_dir"]) if rec and rec.get("run_dir") else None
        out: dict[str, Path] = {}
        for name in spec.outputs:
            candidate = run_dir / name if run_dir else None
            out[name] = candidate if candidate is not None and candidate.exists() else self.workspace / name
        return out

    def _publish(self, rec: dict, spec: StepSpec) -> None:
        """Copy a verified attempt's outputs from its run directory to the workspace path
        downstream steps and reviewers read. Outputs written through a linked directory
        are already in place."""
        run_dir = rec.get("run_dir")
        if not run_dir:
            return
        published = {}
        ws = self.workspace.resolve()
        for name in spec.outputs:
            src, dst = Path(run_dir) / name, self.workspace / name
            if not src.exists():
                continue
            # Destination bounds, checked again right before the destructive step (G02):
            # strictly inside the workspace, never the workspace itself, never the ledger.
            resolved = dst.resolve()
            if resolved == ws or ws not in resolved.parents or resolved.parts[len(ws.parts):][:1] == ("attempts",):
                raise StepsError(f"{spec.id}: refusing to publish output {name!r} to {resolved}: outside the allowed area")
            try:
                same = src.resolve() == resolved
            except OSError:
                same = False
            if not same:
                dst.parent.mkdir(parents=True, exist_ok=True)
                if src.is_dir():                                   # directory artifact (F05)
                    self._replace_dir(src, dst, rec["attempt_id"])
                else:
                    shutil.copy2(src, dst)
            published[name] = str(dst)
        rec["published"] = published

    @staticmethod
    def _replace_dir(src: Path, dst: Path, attempt_id: str) -> None:
        """Replace a published directory so that a complete copy exists at every instant and
        an interruption at any step is recoverable by calling this again (H01, I01).

        Steps: copy src -> fresh; write the completion marker; dst -> old; fresh -> dst;
        remove old and the marker. On entry, leftovers tell where an earlier run stopped:
          fresh without marker      incomplete copy: discard it and copy again
          fresh with marker         the copy is complete: never copy again, finish the swap
          old and dst, no fresh     the swap finished, only the cleanup did not
        The previous verified version (old) is removed only after the new one is installed.
        """
        fresh = dst.with_name(f".{dst.name}.publishing-{attempt_id[:8]}")
        marker = dst.with_name(f".{dst.name}.publishing-{attempt_id[:8]}.complete")
        old = dst.with_name(f".{dst.name}.replaced-{attempt_id[:8]}")
        if fresh.exists() and not marker.exists():
            if not Runner._remove_tree(fresh):         # rmtree(ignore_errors) can leave the tree in place
                raise StepsError(f"cannot remove the incomplete staging copy {fresh}; refusing to publish "
                                 f"until it is gone (the previous published version is untouched)")
        if not fresh.exists():
            if old.exists() and dst.exists():          # earlier run installed dst; finish the cleanup
                Runner._remove_tree(old)
                marker.unlink(missing_ok=True)
                return
            marker.unlink(missing_ok=True)
            shutil.copytree(src, fresh, symlinks=True)
            marker.touch()
        # from here on `fresh` is a complete copy
        if dst.is_symlink() or dst.is_file():
            dst.unlink()
        elif dst.is_dir():
            if old.exists() and not Runner._remove_tree(old):     # stale backup from an even earlier run
                raise StepsError(f"cannot remove the stale backup {old}; refusing to publish until it is gone")
            dst.rename(old)
        fresh.rename(dst)
        Runner._remove_tree(old)                       # best effort: the new version is installed either way
        marker.unlink(missing_ok=True)

    @staticmethod
    def _remove_tree(path: Path, attempts: int = 6) -> bool:
        """rmtree that copes with NFS: a directory whose files were just unlinked can report
        'not empty' for a moment. Retries with a short back-off; True when the path is gone."""
        for i in range(attempts):
            shutil.rmtree(path, ignore_errors=True)
            if not path.exists():
                return True
            time.sleep(0.05 * (2 ** i))
        return not path.exists()

    def _gate_record(self, spec: StepSpec) -> dict | None:
        if not spec.gate:
            return None
        latest = self.ledger.latest(spec.gate)
        if latest and (latest.get("verdict") or {}).get("ok"):
            return {"code": latest.get("code") or {}}
        return {"code": {}, "missing": spec.gate}

    def _describe_inputs(self, rec: dict, spec: StepSpec) -> dict:
        """Identity of the inputs THIS attempt reads: resolved through its run directory, so a
        producer-bound link is described by the producer's bytes, not the workspace copy."""
        root = Path(rec["run_dir"]) if rec.get("run_dir") else self.workspace
        described = snapshot.describe_inputs(root, spec.inputs)
        for rel, entry in described.items():
            path = root / rel
            if path.is_symlink() or path.exists():
                try:
                    entry["resolved"] = str(path.resolve())
                except OSError:
                    pass
        return described

    @staticmethod
    def _input_identity(entry: dict) -> tuple:
        return (entry.get("sha256"), entry.get("size"), entry.get("manifest_sha256"), entry.get("kind"))

    def _settle_inputs(self, rec: dict, spec: StepSpec) -> Verdict:
        """Input provenance at verification (H02, I02). An input that did not exist at
        submission (a producer-bound link, filled by the predecessor) is recorded now. An
        input that existed at submission keeps its submission identity; if the bytes differ
        now, nobody can tell which version the job read, so the attempt fails instead of the
        ledger being rewritten to match the later file."""
        v = Verdict(True)
        if not rec.get("run_dir"):
            return v
        now = self._describe_inputs(rec, spec)
        settled = dict(rec.get("inputs") or {})
        for rel, entry in now.items():
            before = (rec.get("inputs") or {}).get(rel)
            if not before or not before.get("exists"):
                settled[rel] = {**entry, "recorded_at": "verification"}
                continue
            if entry.get("exists") and self._input_identity(before) != self._input_identity(entry):
                v.ok = False
                v.checked.append(f"input-unchanged:{rel}")
                v.reasons.append(f"input {rel} changed between submission and verification; "
                                 f"the attempt's provenance cannot be established, rerun it")
                settled[rel] = {**before, "changed_after_submit": entry}
            else:
                v.checked.append(f"input-unchanged:{rel}")
        rec["inputs"] = settled
        return v

    def _verify(self, rec: dict, spec: StepSpec) -> Verdict:
        inputs_verdict = self._settle_inputs(rec, spec)
        artifacts = self._artifacts(spec, rec)
        gate = self._gate_record(spec)
        v = default_checks(rec, artifacts, required=spec.outputs, finite=spec.finite,
                           gate=gate if gate and "missing" not in gate else None, main=spec.main_output)
        if gate and "missing" in gate:
            v = v.merge(Verdict(False, [f"gate step {gate['missing']} has not passed; cannot certify this run's code"], ["gate"]))
        v = v.merge(inputs_verdict)
        fn = self.steps.verifier_for(spec)
        if fn is not None:
            v = v.merge(run_verifier(fn, rec, artifacts))
        rec["verdict"] = {"ok": v.ok, "reasons": v.reasons, "checked": v.checked,
                          "verifier": spec.verify, "ts": _now()}
        rec["artifacts"] = {k: str(p) for k, p in artifacts.items() if p.exists()}
        return v

    def _finalize(self, rec: dict, spec: StepSpec, actions: list[str], submitted: list[str]) -> None:
        state = rec["observed"]["state"]
        main = self._artifacts(spec, rec).get(spec.main_output) if spec.main_output else None
        verdict = self._verify(rec, spec) if observe.normalized(state) == "COMPLETED" else None
        cls = classify.classify(state, verdict=verdict, output_present=bool(main and main.exists()),
                                exit_code=rec.get("exit_code"))
        if cls is None:
            rec["failure"] = None
            self._publish(rec, spec)
            self.ledger.save(rec)
            actions.append(f"{spec.id}: verified ok")
            return
        # Budget = failures of this class recorded in this ledger + what was already spent
        # before adoption (D34: budgets do not reset when a farm changes).
        rows = self.ledger.by_step(spec.id)
        prior = sum(int((r.get("budget") or {}).get("prior_used") or 0) for r in rows)
        used = prior + sum(1 for r in rows if (r.get("failure") or {}).get("class") == cls)
        max_auto = int(spec.retry.get(cls, classify.MAX_AUTO.get(cls, 0)))
        rec["failure"] = {"class": cls, "reasons": (verdict.reasons if verdict else []), "ts": _now(),
                          "retried": False, "parked": False}
        rec["budget"] = {"class": cls, "max_auto": max_auto, "used": used,
                         **({"prior_used": rec["budget"]["prior_used"]} if (rec.get("budget") or {}).get("prior_used") else {})}
        if used < max_auto:
            rec["failure"]["retried"] = True
            self.ledger.save(rec)
            self._write_evidence(rec, spec, cls, verdict, ruling=None,
                                 budget_note=f"Automatic retry {used + 1} of {max_auto} for class {cls}; resubmitting.")
            actions.append(f"{spec.id}: {cls} failure, resubmitting ({used + 1}/{max_auto})")
            if self._launch(spec, actions) is not None:
                submitted.append(spec.id)
            return
        ruling = classify.ruling_name(self.task_id, cls, spec.id, rec["attempt_id"])
        rec["failure"]["parked"] = True
        rec["failure"]["ruling"] = ruling
        self.ledger.save(rec)
        self._write_evidence(rec, spec, cls, verdict, ruling=ruling,
                             budget_note=f"Automatic retries for class {cls}: {max_auto}; used {used}.")
        actions.append(f"{spec.id}: {cls} failure, parked as {ruling}")

    def _write_evidence(self, rec: dict, spec: StepSpec, cls: str, verdict: Verdict | None, *,
                        ruling: str | None, budget_note: str) -> None:
        job = (rec.get("submit") or {}).get("job_id")
        row = self.sacct_row(job) if job else None
        stderr = rec.get("stderr_path")
        path = evidence.write_failure(self.ledger.attempt_dir(rec["attempt_id"]), rec, cls=cls, sacct_row=row,
                                      stderr_path=stderr, verdict=verdict, ruling=ruling, budget_note=budget_note)
        rec["failure"]["evidence"] = str(path)
        self.ledger.save(rec)

    # -------------------------------------------------------------- launch
    def _launch(self, spec: StepSpec, actions: list[str]) -> dict | None:
        rec = self.ledger.new(spec.id, task_id=self.task_id, lease_id=self.lease_id, site=self.site.name)
        attempt_dir = self.ledger.attempt_dir(rec["attempt_id"])
        attempt_dir.mkdir(parents=True, exist_ok=True)
        globs = list(spec.snapshot)
        if spec.kind == "sbatch" and spec.run not in globs:
            globs.append(spec.run)
        rec["code"] = snapshot.snapshot_code(self.workspace, attempt_dir, globs)
        rec["code_snapshot"] = str(attempt_dir / "code")
        rec["run_dir"] = str(self._prepare_run_dir(spec, attempt_dir / "code"))
        # Inputs as seen from the run directory at submission. A chain successor's producer-bound
        # inputs do not exist yet; their identity is recorded again at verification (H02).
        rec["inputs"] = self._describe_inputs(rec, spec)
        rec["env_sha256"] = snapshot.env_digest()
        git = snapshot.git_state(self.workspace)
        if git:
            rec["git"] = git
        rec["stderr_path"] = str(attempt_dir / ("slurm.err" if spec.kind == "sbatch" else "local.err"))
        if spec.kind == "sbatch":
            return self._submit_sbatch(rec, spec, attempt_dir, actions)
        return self._run_local(rec, spec, attempt_dir, actions)

    def _prepare_run_dir(self, spec: StepSpec, code_dir: Path) -> Path:
        """The attempt executes INSIDE its run directory (= its code snapshot). Three kinds
        of path, each with one rule (R03, F03, F04):

        * code: files matched by `snapshot` are frozen copies, already in place;
        * inputs: every declared input is linked from the workspace at its own relative
          path (parents created), so `project/input.txt` is visible next to the frozen
          `project/train.py`;
        * outputs: never linked. Their parent directories exist as real directories in
          the run directory, the step writes there, and `_publish` copies verified outputs
          to the workspace. A failed attempt therefore cannot touch an earlier result.

        Anything else at the top of the workspace (data the step reads without declaring
        it) is linked as a convenience, unless it is the top-level component of an output.
        """
        code_dir.mkdir(parents=True, exist_ok=True)
        outputs = {Path(n) for n in spec.outputs}
        output_roots = {p.parts[0] for p in outputs if p.parts}
        for name in spec.outputs:
            (code_dir / name).parent.mkdir(parents=True, exist_ok=True)
        # A chain successor consumes the PRODUCER ATTEMPT's outputs, not the workspace copy
        # (G01): Slurm releases it on the producer's exit code, before any worker has
        # verified or published. Its run directory therefore links the predecessor's
        # declared outputs straight to the predecessor's run directory (dangling until the
        # producer writes them, which afterok guarantees or cancels).
        for pred_rel, pred_run in self._predecessor_outputs(spec):
            dst = code_dir / pred_rel
            if not (dst.exists() or dst.is_symlink()):
                dst.parent.mkdir(parents=True, exist_ok=True)
                self._link(pred_run / pred_rel, dst)
        for rel in spec.inputs:
            if Path(rel).is_absolute():
                continue
            self._link_input(self.workspace / rel, code_dir / rel, outputs, Path(rel))
        for entry in sorted(self.workspace.iterdir()):
            if entry.name == "attempts" or entry.name in output_roots:
                continue
            target = code_dir / entry.name
            if target.exists() or target.is_symlink():
                continue            # snapshotted code, a declared input, or a partial copy under this name
            self._link(entry, target)
        return code_dir

    def _predecessor_outputs(self, spec: StepSpec) -> list[tuple[str, Path]]:
        out = []
        for dep in spec.after:
            if not dep.startswith("chain:"):
                continue
            prev_spec = self.steps.by_id.get(dep.split(":", 1)[1])
            prev = self.ledger.latest(prev_spec.id) if prev_spec else None
            if prev and prev.get("run_dir"):
                out += [(name, Path(prev["run_dir"])) for name in prev_spec.outputs]
        return out

    def _link_input(self, src: Path, dst: Path, outputs: set[Path], rel: Path) -> None:
        """Link a declared input at its relative path. When the destination already exists
        as a real directory (an output parent, snapshotted code, a predecessor's outputs),
        merge: recurse and link the children that are not there yet (G03). Declared outputs
        are never linked, whatever directory they sit in."""
        if rel in outputs:
            return
        if not src.exists():
            return
        if dst.is_symlink() or dst.is_file():
            return
        if dst.is_dir():
            if not src.is_dir():
                return
            for child in sorted(src.iterdir()):
                if child.name == "attempts" and src == self.workspace:
                    continue
                self._link_input(child, dst / child.name, outputs, rel / child.name)
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        self._link(src, dst)

    @staticmethod
    def _link(src: Path, dst: Path) -> None:
        try:
            os.symlink(src, dst)
        except OSError:
            pass

    def _step_env(self, rec: dict, spec: StepSpec) -> dict:
        return {**spec.env, "FARMKIT_ATTEMPT_ID": rec["attempt_id"], "FARMKIT_STEP": spec.id,
                "FARMKIT_WORKSPACE": str(self.workspace), "FARMKIT_RUN_DIR": rec.get("run_dir") or str(self.workspace)}

    def _submit_sbatch(self, rec: dict, spec: StepSpec, attempt_dir: Path, actions: list[str]) -> dict | None:
        script = attempt_dir / "code" / spec.run
        run_dir = rec.get("run_dir") or str(self.workspace)
        extra = [f"--chdir={run_dir}", f"--output={attempt_dir / 'slurm.out'}", f"--error={attempt_dir / 'slurm.err'}"]
        env = self._step_env(rec, spec)
        extra.append("--export=ALL," + ",".join(f"{k}={v}" for k, v in env.items()))
        prev = self.steps.previous_job(self.ledger, spec)
        if prev:
            extra.append(f"--dependency={spec.chain}:{prev}")
        argv = intent.sbatch_argv(rec, str(script), extra=extra)
        rec = intent.submit(self.ledger, rec, argv, run=self.sbatch, cwd=str(self.workspace))
        status = rec["submit"]["status"]
        if status == "submitted":
            actions.append(f"{spec.id}: submitted job {rec['submit']['job_id']}")
            return rec
        if status == "rejected":
            rec["failure"] = {"class": "code", "reasons": [f"sbatch refused: {rec['submit']['error']}"], "ts": _now(),
                              "retried": False, "parked": True,
                              "ruling": classify.ruling_name(self.task_id, "code", spec.id, rec["attempt_id"])}
            self.ledger.save(rec)
            self._write_evidence(rec, spec, "code", None, ruling=rec["failure"]["ruling"], budget_note="sbatch itself refused the job.")
            actions.append(f"{spec.id}: sbatch refused, parked")
            return None
        actions.append(f"{spec.id}: sbatch outcome unknown, intent kept for reconciliation")
        return None

    def _run_local(self, rec: dict, spec: StepSpec, attempt_dir: Path, actions: list[str]) -> dict | None:
        env = {**os.environ, **self._step_env(rec, spec)}
        run_dir = rec.get("run_dir") or str(self.workspace)
        rec["submit"] = {"intent_ts": _now(), "argv": shlex.split(spec.run), "cwd": run_dir,
                         "job_id": None, "status": "local", "error": None}
        self.ledger.save(rec)
        try:
            proc = self.local_run(shlex.split(spec.run), cwd=run_dir, env=env, text=True,
                                  capture_output=True, timeout=spec.timeout_s)
            rc, out, err = proc.returncode, proc.stdout or "", proc.stderr or ""
        except (OSError, subprocess.TimeoutExpired) as exc:
            rc, out, err = 1, "", f"{type(exc).__name__}: {exc}"
        atomic_write_text(attempt_dir / "local.out", out)
        atomic_write_text(attempt_dir / "local.err", err)
        rec["exit_code"] = rc
        rec["observed"] = {"state": "COMPLETED" if rc == 0 else "FAILED", "observed_ts": _now(), "end": _now(), "local": True}
        self.ledger.save(rec)
        self._finalize(rec, spec, actions, [])
        return rec

    def _barriers(self, actions: list[str], submitted: list[str]) -> None:
        """Opt-in per group: one afterany job so a whole group costs one wake."""
        for group, members in self.steps.groups().items():
            specs = [self.steps.by_id[m] for m in members]
            if not any(s.wait == "barrier" for s in specs):
                continue
            jobs = []
            for m in members:
                latest = self.ledger.latest(m)
                job = (latest or {}).get("submit", {}).get("job_id") if latest else None
                if not job or (latest.get("observed") or {}).get("state") and observe.is_terminal(latest["observed"]["state"]):
                    jobs = []
                    break
                jobs.append(job)
            if not jobs or self.ledger.latest(f"barrier:{group}") and self.ledger.in_flight() and any(
                    r["step"] == f"barrier:{group}" for r in self.ledger.in_flight()):
                continue
            rec = self.ledger.new(f"barrier:{group}", task_id=self.task_id, lease_id=self.lease_id, site=self.site.name)
            argv = waits.barrier_argv(group, jobs, partition=self.site.scheduler.get("barrier_partition"),
                                      time_limit=self.site.scheduler.get("barrier_time", "0:05:00"), attempt_id=rec["attempt_id"])
            rec = intent.submit(self.ledger, rec, argv, run=self.sbatch, cwd=str(self.workspace))
            if rec["submit"]["status"] == "submitted":
                actions.append(f"barrier for {group}: job {rec['submit']['job_id']}")
                submitted.append(f"barrier:{group}")

    # -------------------------------------------------------------- decide
    def _decide(self) -> tuple[str, str | None, list[str]]:
        parked = [r["failure"]["ruling"] for r in self.ledger.parked() if r["failure"].get("ruling")]
        inflight = self.ledger.in_flight()
        if inflight:
            barriers = [r for r in inflight if r["step"].startswith("barrier:")]
            if barriers:
                return "AWAITING", f"job:{barriers[0]['submit']['job_id']}", parked
            return "AWAITING", waits.wait_reference([r["submit"]["job_id"] for r in inflight]), parked
        unverified = [r for r in self.ledger.all() if (r.get("submit") or {}).get("status") in {"intent", "unverified"}]
        if unverified:
            r = unverified[0]
            return "AWAITING", classify.ruling_name(self.task_id, "unknown", f"submit-{r['step']}", r["attempt_id"]), parked
        if parked:
            return "AWAITING", parked[0], parked
        if self.steps.all_ok(self.ledger):
            return "SUBMITTED", None, parked
        return "AWAITING", classify.ruling_name(self.task_id, "unknown", "no-progress", "00000000"), parked

    # ------------------------------------------------------------- reports
    def _note(self, status: str, waiting_on: str | None, submitted: list[str], parked: list[str]) -> str:
        done = sum(1 for s in self.steps.specs if self.ledger.ok(s.id))
        bits = [f"{done}/{len(self.steps.specs)} steps ok"]
        if submitted:
            bits.append(f"submitted {', '.join(submitted[:4])}")
        if parked:
            bits.append(f"{len(parked)} parked")
        if status == "SUBMITTED":
            bits.append("all steps verified; deliverable ready")
        return "; ".join(bits)[:200]

    def _receipt_command(self, status: str, waiting_on: str | None, note: str, checkpoint_path: str | None) -> str:
        cmd = f"{self.receipt_helper} {status}"
        if status == "AWAITING" and waiting_on:
            cmd += f" --waiting-on {shlex.quote(waiting_on)}"
        cmd += f" --note {shlex.quote(note)}"
        if checkpoint_path:
            cmd += f" --checkpoint {shlex.quote(str(Path(checkpoint_path).resolve()))}"
        return cmd

    def _summary(self, status, waiting_on, submitted, parked, changed, actions, checkpoint_path) -> str:
        lines = [f"farmkit tick — {self.task_id or self.workspace.name} — {_now()[:19]}Z"]
        lines.append("STEPS")
        for s in self.steps.specs[:12]:
            latest = self.ledger.latest(s.id)
            if latest is None:
                state = "not started" if not all(self.steps.dependency_met(self.ledger, d) for d in s.after) else "ready"
            elif (latest.get("verdict") or {}).get("ok"):
                state = "ok"
            elif (latest.get("failure") or {}).get("parked"):
                state = f"PARKED {latest['failure']['class']}"
            else:
                obs = (latest.get("observed") or {}).get("state")
                job = (latest.get("submit") or {}).get("job_id")
                state = f"job {job} {obs or 'not yet observed'}" if job else (latest.get("submit") or {}).get("status", "?")
            lines.append(f"  {s.id:<28} {state}")
        if len(self.steps.specs) > 12:
            lines.append(f"  … {len(self.steps.specs) - 12} more steps")
        if actions:
            lines.append("THIS TICK")
            lines += [f"  {a}" for a in actions[:6]]
        if parked:
            lines.append("NEEDS A RULING")
            lines += [f"  {p}" for p in parked[:3]]
        if changed:
            lines.append("CHANGED SINCE LAST TICK: " + ", ".join(changed[:6]))
        lines.append(f"NEXT: report {status}" + (f" waiting on {waiting_on}" if waiting_on else ""))
        if checkpoint_path:
            lines.append(f"CHECKPOINT: {checkpoint_path} (append your 3-5 lines of judgment below the marker)")
        return "\n".join(lines[:30])

    def _changed_files(self) -> list[str]:
        prev = read_json(self.state_path) if self.state_path.exists() else {}
        current = {}
        for p in sorted(self.workspace.glob("*")):
            if p.is_file() and not p.name.startswith("."):
                current[p.name] = p.stat().st_mtime
        changed = [name for name, m in current.items() if prev.get("mtimes", {}).get(name) != m]
        atomic_write_json(self.state_path, {"mtimes": current, "ts": _now()})
        return changed if prev else []

    def _write_checkpoint(self, status: str, waiting_on: str | None, parked: list[str]) -> str:
        path = self.workspace / "CHECKPOINT.md"
        judgment = ""
        if path.exists():
            text = path.read_text()
            if JUDGMENT_MARKER in text:
                judgment = text.split(JUDGMENT_MARKER, 1)[1]
        rows = []
        for s in self.steps.specs:
            latest = self.ledger.latest(s.id)
            if latest is None:
                rows.append(f"| {s.id} | – | – | not started |")
                continue
            job = (latest.get("submit") or {}).get("job_id") or "–"
            obs = (latest.get("observed") or {}).get("state") or "–"
            verdict = "ok" if (latest.get("verdict") or {}).get("ok") else (
                f"PARKED {latest['failure']['class']}" if (latest.get("failure") or {}).get("parked") else "pending")
            rows.append(f"| {s.id} | {job} | {obs} | {verdict} |")
        mech = ["# CHECKPOINT (mechanical part written by farmkit)", "",
                f"Task: {self.task_id or '–'} · updated {_now()[:19]}Z", "",
                "| step | job | scheduler | verdict |", "|---|---|---|---|", *rows, "",
                f"Next: report {status}" + (f", waiting on `{waiting_on}`" if waiting_on else ""),
                *( ["", "Parked (need a ruling):", *[f"- {p}" for p in parked]] if parked else []),
                "", JUDGMENT_MARKER]
        atomic_write_text(path, "\n".join(mech) + (judgment if judgment.strip() else "\n\n(judgment: none yet)\n"))
        return str(path)
