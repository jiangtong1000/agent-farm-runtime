"""Launcher-attested Slurm identity, bound to one runtime host and epoch."""
from __future__ import annotations

import os

from ..adapters.slurm import slurm_job_terminal
from .contract import (AccessError, NODE, SCHEDULER_FIELDS, identifier, require,
                       validate_scheduler)
from .observations import Observations


def attested_scheduler(manifest: dict) -> dict:
    attestation = manifest.get("scheduler_attestation")
    require(attestation is not None, "PENDING", "scheduler_unattested",
            "Start the reconciler with an explicit Slurm job/node identity before publication")
    require(isinstance(attestation, dict) and set(attestation) == SCHEDULER_FIELDS | {
        "execution_epoch", "runtime_host", "owner_uid"},
        "CONFLICT", "scheduler_attestation", "Invalid deployment scheduler attestation")
    require(attestation["execution_epoch"] == manifest["execution_epoch"],
            "EPOCH_MISMATCH", "scheduler_epoch", "Scheduler attestation belongs to another epoch")
    require(attestation["runtime_host"] == manifest["host"],
            "HOST_MISMATCH", "scheduler_runtime_host", "Scheduler attestation belongs to another runtime host")
    require(type(attestation["owner_uid"]) is int and attestation["owner_uid"] >= 0,
            "CONFLICT", "scheduler_owner", "Invalid attested owner UID")
    return validate_scheduler({key: attestation[key] for key in SCHEDULER_FIELDS})


def running_allocation(observations: Observations, job_id: str, node: str, owner_uid: int) -> dict:
    observed = observations.scheduler(job_id)
    require(observed["job_id"] == job_id, "CONFLICT", "job_identity", "Scheduler job identity mismatch")
    require(observed["owner_uid"] == owner_uid, "AUTH_REQUIRED", "allocation_owner", "Allocation belongs to another user")
    state = observed["state"]
    require(not slurm_job_terminal(state), "JOB_EXPIRED", "job_terminal", "Allocation has a positive terminal observation")
    if state in {"PENDING", "CONFIGURING", "SUSPENDED", "COMPLETING", "RESIZING", "REQUEUED", "REQUEUE_HOLD"}:
        raise AccessError("PENDING", "allocation_not_running", f"Allocation is {state}")
    require(state == "RUNNING", "UNREACHABLE", "job_unknown", "Scheduler did not establish RUNNING")
    require(node in observed["nodes"], "HOST_MISMATCH", "allocation_node",
            "Allocation does not include the exact attested scheduler node")
    scheduler = {"kind": "slurm", "job_id": job_id, "scheduler_node": node,
                 "allocation_started_at": observed["allocation_started_at"]}
    try:
        return validate_scheduler(scheduler)
    except AccessError as exc:
        raise AccessError("UNREACHABLE", "allocation_identity", "Invalid scheduler allocation identity") from exc


def capture_allocation(previous: dict, current: dict, epoch: str, *, job_id: str | None = None,
                       node: str | None = None, environ=None, observations=None) -> dict | None:
    """Capture fresh launcher input on every startup; never infer a local NodeName.

    Explicit inputs attest which node this launcher runs on. They must agree with
    any standard Slurm environment values and pass an exact allocation query.
    Absence is allowed for non-Slurm runtimes, which cannot publish Slurm access.
    """
    env = os.environ if environ is None else environ
    require((job_id is None) == (node is None), "CONFLICT", "launcher_pair",
            "Provide both --slurm-job-id and --slurm-node")
    jobs = [env[key] for key in ("SLURM_JOB_ID", "SLURM_JOBID") if key in env]
    nodes = [env["SLURMD_NODENAME"]] if "SLURMD_NODENAME" in env else []
    if job_id is not None:
        jobs.append(job_id)
        nodes.append(node)
    if not jobs and not nodes:
        require(previous.get("scheduler_attestation") is None,
                "PENDING", "scheduler_unattested", "Restart requires fresh launcher job/node identity")
        return None
    require(bool(jobs) and bool(nodes), "CONFLICT", "launcher_incomplete",
            "Slurm startup requires both allocation ID and local SLURMD_NODENAME (or explicit launcher pair)")
    for value in jobs:
        identifier(value, r"[1-9][0-9]*")
    for value in nodes:
        identifier(value, NODE)
    require(len(set(jobs)) == 1 and len(set(nodes)) == 1, "CONFLICT", "launcher_conflict",
            "Explicit launcher inputs and Slurm environment must agree exactly")
    require(hasattr(os, "getuid"), "UNREACHABLE", "unsupported_platform", "Slurm attestation requires POSIX")
    scheduler = running_allocation(observations or Observations(), jobs[0], nodes[0], os.getuid())
    attestation = {**scheduler, "execution_epoch": epoch, "runtime_host": current["host"], "owner_uid": os.getuid()}
    prior = previous.get("scheduler_attestation")
    require(prior is None or prior == attestation, "CONFLICT", "scheduler_epoch_binding",
            "Allocation identity changed within an epoch; use an authorized turnover/recovery")
    return attestation
