"""Fail-closed access publication and read-only verification of a control session.

Task ownership stays in TaskStore. This layer never invokes an executor, repairs
a deployment, or attaches a terminal. Resolution alone never authorizes attach.
"""
from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from pathlib import Path

from ..adapters.filesystem import exclusive_lock
from ..lifecycle import daemon_alive
from ..locking import task_mutation_lock
from ..provenance import deployment_stamp, runtime_identity
from ..store import FarmPaths
from .contract import (AccessError, EPOCH, SCHEMA_VERSION, SHA256, digest, farm_id, identifier,
                       markers, require, result, target_name, timestamp, validate_record)
from .observations import Observations
from .allocation import attested_scheduler, running_allocation
from .registry import Registry, persistent_path, read_json


def manifest_for(paths: FarmPaths) -> dict:
    root = str(persistent_path(paths.root))
    manifest = read_json(paths.runtime / "deployment.json", missing="PENDING")
    require(manifest.get("farm_root") == root and manifest.get("farm_id") == farm_id(root),
            "CONFLICT", "deployment_identity", "Deployment lacks this farm's access identity; restart the reviewed release")
    identifier(manifest.get("execution_epoch"), EPOCH)
    identifier(manifest.get("source_sha256"), SHA256)
    require(type(manifest.get("protocol_version")) is int and manifest["protocol_version"] > 0
            and isinstance(manifest.get("host"), str) and bool(manifest["host"]),
            "CONFLICT", "deployment_schema", "Invalid deployment host/protocol")
    for name in ("pending-recovery.json", "pending-task-commit.json"):
        path = paths.runtime / name
        require(not (path.exists() or path.is_symlink()), "PENDING", "pending_transaction", "Finish the recorded farm transaction")
    require("pending_event" not in manifest, "PENDING", "deployment_audit", "Deployment audit is pending")
    if "handoff" in manifest:
        handoff = manifest["handoff"]
        require(isinstance(handoff, dict) and handoff.get("phase") in {"draining", "released", "claimed"},
                "CONFLICT", "handoff_schema", "Invalid handoff control")
        identifier(handoff.get("id"), EPOCH)
        require(handoff["phase"] == "claimed", "PENDING", "handoff_" + handoff["phase"], "Handoff is not claimed")
        require(handoff.get("kind") != "stop" and handoff["id"] == manifest["execution_epoch"]
                and handoff.get("target_host") == manifest["host"],
                "CONFLICT", "claim_identity", "Claim does not match deployment host/epoch")
    return manifest


def match_manifest(record: dict, manifest: dict) -> None:
    for field, source, state in (("execution_epoch", "execution_epoch", "EPOCH_MISMATCH"),
                                  ("runtime_host", "host", "HOST_MISMATCH"),
                                  ("farm_id", "farm_id", "CONFLICT"),
                                  ("farm_root", "farm_root", "CONFLICT"),
                                  ("source_sha256", "source_sha256", "STALE_REGISTRY"),
                                  ("protocol_version", "protocol_version", "STALE_REGISTRY")):
        require(record[field] == manifest[source], state, field, f"Registry/deployment {field} mismatch")
    require(record["scheduler"] == attested_scheduler(manifest), "STALE_REGISTRY", "scheduler_attestation",
            "Registry differs from the deployment's attested allocation")
    require(record["owner_uid"] == manifest["scheduler_attestation"]["owner_uid"],
            "AUTH_REQUIRED", "attested_owner", "Registry and attested allocation owner differ")


class Access:
    def __init__(self, registry: Registry, *, observations=None, identity=None, alive=None, clock=None):
        self.registry = registry
        self.observations = observations or Observations()
        self.identity = identity or runtime_identity
        self.alive = alive or daemon_alive
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def local(self, manifest: dict) -> dict:
        current = self.identity()
        require(current["host"] == manifest["host"], "HOST_MISMATCH", "verification_host", "Run on the exact deployment host")
        for key in ("protocol_version", "source_sha256"):
            require(current[key] == manifest[key], "STALE_REGISTRY", key, "Use the exact deployed runtime build/protocol")
        return current

    def ready(self, paths: FarmPaths, manifest: dict) -> None:
        require(manifest.get("loop") is True and type(manifest.get("pid")) is int and manifest["pid"] > 0
                and type(manifest.get("pid_starttime")) is int and bool(manifest.get("boot_id"))
                and bool(manifest.get("pid_namespace")) and bool(manifest.get("started_at")),
                "PENDING", "daemon_not_started", "A claimed allocation is not a ready reconciler")
        interval = manifest.get("interval")
        require(type(interval) in (int, float) and math.isfinite(interval) and interval > 0,
                "CONFLICT", "daemon_interval", "Invalid reconciler interval")
        tick = read_json(paths.runtime / "last_tick.json", missing="PENDING")
        require(tick.get("deployment") == deployment_stamp(manifest), "PENDING", "readiness_generation",
                "No completed tick from this exact daemon/epoch")
        when, started = timestamp(tick.get("ts")), timestamp(manifest["started_at"])
        require(when >= started and 0 <= (self.clock() - when).total_seconds() <= 3 * interval,
                "PENDING", "readiness_stale", "Reconciler heartbeat is stale or clock-inconsistent")
        alive = self.alive(manifest)
        require(alive is not None, "UNREACHABLE", "daemon_unknown", "Daemon identity cannot be observed")
        require(alive is True, "PENDING", "daemon_not_running", "The recorded reconciler is not running")

    def live(self, paths: FarmPaths, manifest: dict, record: dict) -> None:
        match_manifest(record, manifest)
        self.local(manifest)
        require(hasattr(os, "getuid") and record["owner_uid"] == os.getuid(),
                "AUTH_REQUIRED", "verification_user", "Verify as the control session's owner")
        self.ready(paths, manifest)
        scheduler = record["scheduler"]
        require(running_allocation(self.observations, scheduler["job_id"], scheduler["scheduler_node"],
                                   record["owner_uid"]) == scheduler,
                "STALE_REGISTRY", "allocation_reused", "Allocation identity changed (including possible job ID reuse)")
        control = record["control"]
        require(self.observations.control(control["socket"], control["session"], control["default_window"]) == control,
                "STALE_REGISTRY", "control_replaced", "Control socket/server/session/window identity changed")
        require(self.observations.environment(record) == markers(record), "CONFLICT", "marker_mismatch", "Control session markers differ")
        require(self.observations.control(control["socket"], control["session"], control["default_window"]) == control,
                "STALE_REGISTRY", "control_replaced", "Control endpoint changed while reading its markers")
        require(manifest_for(paths) == manifest, "PENDING", "deployment_changed", "Deployment changed during verification")

    def resolve(self, target: str) -> dict:
        target_name(target)
        pointer, record = self.registry.current(target)
        paths = FarmPaths(Path(record["farm_root"]))
        manifest = manifest_for(paths)
        match_manifest(record, manifest)
        require(self.registry.current(target) == (pointer, record), "STALE_REGISTRY", "pointer_changed", "Current pointer changed during resolution")
        # A login host can do this step. It cannot certify remote tmux/process state.
        return result("RESOLVED", target, "verification_required", "Candidate only; run verify on the recorded host",
                      record=record, record_sha256=pointer["record_sha256"], verification={
                          "host": record["runtime_host"], "project": str(paths.root.parent),
                          "registry": str(self.registry.root), "target": target,
                          "expected_record": pointer["record_sha256"]})

    def verify(self, paths: FarmPaths, target: str, *, expected_record: str | None = None) -> dict:
        target_name(target)
        pointer, record = self.registry.current(target)
        if expected_record is not None:
            identifier(expected_record, SHA256)
            require(pointer["record_sha256"] == expected_record, "STALE_REGISTRY", "expected_record", "Resolved candidate is no longer current")
        require(str(paths.root.resolve()) == record["farm_root"], "CONFLICT", "project_mismatch", "Target belongs to another project")
        manifest = manifest_for(paths)
        match_manifest(record, manifest)
        self.live(paths, manifest, record)
        require(self.registry.current(target) == (pointer, record), "STALE_REGISTRY", "pointer_changed", "Current pointer changed during verification")
        return self.verified(record)

    @staticmethod
    def verified(record: dict) -> dict:
        control = record["control"]
        target = control["session_id"] + (":" + control["window_id"] if control["window_id"] is not None else "")
        return result("VERIFIED", record["target"], "exact_match", "Point-in-time verification; no attach performed",
                      record=record, record_sha256=digest(record), attachment={
                          "host": record["runtime_host"],
                          # Unlike attach-session, if-shell cannot start a server
                          # in tmux 2.7. -F evaluates a format, never a shell.
                          "argv": ["tmux", "-S", control["socket"], "if-shell", "-F", "-t", target,
                                   "1", f"attach-session -t '{target}'"]})

    def publish(self, paths: FarmPaths, target: str, *, job_id: str, control_session: str,
                control_socket: str | None = None, default_window: str | None = None,
                expected_epoch: str | None = None) -> dict:
        target_name(target)
        identifier(job_id, r"[1-9][0-9]*")
        identifier(control_session)
        if default_window is not None:
            identifier(default_window)
        configuration = self.registry.configuration(writing=True)
        self.local(manifest_for(paths))  # Validate before creating access locks/directories.
        directory = self.registry.target_dir(target, create=True)
        # Registry first, farm second. Turnover/startup hold the same farm mutation lock.
        with exclusive_lock(directory / "publish.lock", blocking=True), task_mutation_lock(paths.runtime):
            manifest = manifest_for(paths)
            self.local(manifest)
            if expected_epoch is not None:
                require(manifest["execution_epoch"] == expected_epoch, "EPOCH_MISMATCH", "expected_epoch", "Deployment epoch changed")
            scheduler = attested_scheduler(manifest)
            require(scheduler["job_id"] == job_id, "CONFLICT", "attested_job",
                    "--job-id contradicts the allocation attested at reconciler startup")
            require(manifest["scheduler_attestation"]["owner_uid"] == configuration["owner_uid"],
                    "AUTH_REQUIRED", "attested_owner", "Publish as the attested allocation owner")
            self.ready(paths, manifest)
            require(running_allocation(self.observations, job_id, scheduler["scheduler_node"],
                                       configuration["owner_uid"]) == scheduler,
                    "STALE_REGISTRY", "allocation_reused", "Allocation changed since reconciler startup")
            socket = control_socket if control_socket is not None else self.observations.default_socket()
            require(Path(socket).is_absolute(), "CONFLICT", "socket_path", "Use an absolute control socket path")
            control = self.observations.control(socket, control_session, default_window)
            record = {"schema_version": SCHEMA_VERSION, "target": target, "farm_id": manifest["farm_id"],
                      "farm_root": str(paths.root.resolve()), "execution_epoch": manifest["execution_epoch"],
                      "runtime_host": manifest["host"], "protocol_version": manifest["protocol_version"],
                      "source_sha256": manifest["source_sha256"], "owner_uid": configuration["owner_uid"],
                      "scheduler": scheduler, "control": control, "published_at": self.clock().isoformat()}
            validate_record(record, target, record["execution_epoch"])
            # Conflicting bindings/generations fail before setting any tmux markers.
            binding = directory / "binding.json"
            if binding.exists() or binding.is_symlink():
                require(read_json(binding) == self.registry.binding(record), "CONFLICT", "target_binding", "Target already belongs to another farm")
            instance = directory / "epochs" / f"{record['execution_epoch']}.json"
            if instance.exists() or instance.is_symlink():
                prior = validate_record(read_json(instance), target, record["execution_epoch"])
                record["published_at"] = prior["published_at"]
                require(record == prior, "CONFLICT", "epoch_publication", "Same epoch has a different immutable endpoint")
            current = directory / "current.json"
            if current.exists() or current.is_symlink():
                self.registry.current(target)  # A broken pointer is never silently repaired.
            self.observations.bind(record)
            self.live(paths, manifest, record)
            record = self.registry.prepare(directory, record)
            # Recheck after the durable write too; a crash here leaves only an orphan.
            self.live(paths, manifest, record)
            self.registry.commit(directory, record)
            return self.verified(record)
