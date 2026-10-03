"""Portable terminal client: native SSH transport around the access protocol.

Only local connection policy is configured. Every invocation resolves the current
record and verifies it on the recorded host; no compute endpoint is cached.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
import sys
import tempfile
import tomllib

from .access import Access
from .access.contract import (AccessError, NODE, SHA256, digest, farm_id, identifier,
                              require, target_name, validate_record)
from .access.registry import Registry, _invalid_constant, _unique_object
from .store import FarmPaths

HOST = r"(?:[A-Za-z0-9_][A-Za-z0-9_.-]*@)?[A-Za-z0-9][A-Za-z0-9_.-]*"
PROFILE_FIELDS = {"login_host", "registry", "farm_root", "farm_id", "farm_wrapper",
                  "jump_hosts", "ssh_config", "control_path", "control_persist"}


def _require(condition, detail):
    require(condition, "CONFLICT", "connect_contract", detail)


def _remote_path(value, label):
    _require(isinstance(value, str) and PurePosixPath(value).is_absolute()
             and not any(c in value for c in "\x00\r\n"), f"{label} must be an absolute remote path")
    return value


def load_profile(config: Path, target: str) -> dict:
    target_name(target)
    with Path(config).expanduser().open("rb") as handle:
        data = tomllib.load(handle)
    _require(set(data) == {"version", "targets"} and type(data["version"]) is int and data["version"] == 1
             and isinstance(data["targets"], dict), "Expected version = 1 and a targets table in connect config")
    profile = data["targets"].get(target)
    _require(isinstance(profile, dict) and not set(profile) - PROFILE_FIELDS,
             "Target is not configured, or its configuration contains unknown fields")
    for name in ("registry", "farm_root", "farm_wrapper"):
        _remote_path(profile.get(name), name)
    _require(PurePosixPath(profile["farm_root"]).name == ".farm"
             and profile.get("farm_id") == farm_id(profile["farm_root"]),
             "Configured farm_id must match the pinned canonical .farm root")
    identifier(profile.get("login_host"), HOST)
    jumps = profile.get("jump_hosts", [])
    _require(isinstance(jumps, list), "jump_hosts must be a list of SSH hosts")
    for jump in jumps:
        identifier(jump, HOST + r"(?::[1-9][0-9]{0,4})?")
    for name in ("ssh_config", "control_path"):
        if name in profile:
            value = profile[name]
            _require(isinstance(value, str) and bool(value) and not any(c in value for c in '\x00\r\n"'),
                     f"Invalid {name}")
            _require(Path(value).expanduser().is_absolute(), f"{name} must be an absolute local path")
    if "control_persist" in profile:
        _require("control_path" in profile and isinstance(profile["control_persist"], str)
                 and re.fullmatch(r"[0-9]+[smhd]?", profile["control_persist"]) is not None,
                 "control_persist needs control_path and a duration such as 10m")
    return profile


@contextmanager
def ssh_configuration(profile):
    """-F propagates to native ProxyJump subprocesses, enforcing strict keys there too."""
    lines = ["Host *", "  StrictHostKeyChecking yes", "  BatchMode no"]
    if profile.get("control_path"):
        lines += ["  ControlMaster auto", f'  ControlPath "{Path(profile["control_path"]).expanduser().as_posix()}"',
                  f'  ControlPersist {profile.get("control_persist", "10m")}']
    configured = profile.get("ssh_config")
    included = Path(configured).expanduser() if configured else Path.home() / ".ssh" / "config"
    if configured or included.exists():
        # OpenSSH uses double quotes for paths with spaces; reject quote/newline
        # injection, including a home directory supplied by the environment.
        _require(not any(c in str(included) for c in '\x00\r\n"'), "Invalid SSH configuration path")
        lines += [f'Include "{included.as_posix()}"']
    with tempfile.TemporaryDirectory(prefix="farm-connect-") as directory:
        path = Path(directory) / "ssh_config"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        yield str(path)


def ssh_argv(profile: dict, config: str, host: str, command: list[str], *, compute=False, terminal=False) -> list[str]:
    identifier(host, HOST)
    jumps = [*profile.get("jump_hosts", [])]
    if compute and host != profile["login_host"]:
        jumps.append(profile["login_host"])
    argv = ["ssh", "-F", config, "-o", "StrictHostKeyChecking=yes", "-t" if terminal else "-T"]
    if jumps:
        argv += ["-J", ",".join(jumps)]
    # SSH sends a shell command on the remote end, even when local argv is a list.
    return [*argv, host, shlex.join(command)]


def _read_response(proc, expected_state):
    if proc.returncode:
        messages = {"UNPUBLISHED": "Target has no published endpoint; explicitly adopt the intended process",
                    "PENDING": "Endpoint is not ready; inspect reconciler readiness and turnover state",
                    "STALE_REGISTRY": "Endpoint changed; review the current publication and resolve again",
                    "AUTH_REQUIRED": "Remote access requires the correct authenticated owner",
                    "CONFLICT": "Remote endpoint disagrees with its publication; inspect the exact target",
                    "HOST_MISMATCH": "Verification reached a different host; inspect routing and publication",
                    "EPOCH_MISMATCH": "Execution epoch changed; review publication and resolve again",
                    "SESSION_MISSING": "The published session is missing; explicitly adopt a replacement"}
        try:
            failure = json.loads(proc.stdout) if len(proc.stdout or "") <= 65536 else None
        except (ValueError, TypeError):
            failure = None
        state = failure.get("state") if isinstance(failure, dict) else None
        if isinstance(state, str) and state in messages:
            raise AccessError(state, "remote_refused", messages[state])
        raise AccessError("UNREACHABLE", "ssh_or_verification_failed",
                          "SSH or remote verification failed; check authentication, host keys and current publication")
    try:
        _require(len(proc.stdout) <= 65536, "Remote response exceeds 64 KiB")
        value = json.loads(proc.stdout, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
    except (ValueError, TypeError) as exc:
        raise AccessError("CONFLICT", "invalid_response", "Remote wrapper did not return one valid JSON response") from exc
    _require(isinstance(value, dict) and value.get("state") == expected_state
             and value.get("verified") is (expected_state == "VERIFIED"),
             "Remote wrapper did not return the required access state; no attachment attempted")
    return value


def validate_response(value, profile, target, role, *, expected_record=None, verified=False):
    """Pin all authority-bearing fields; never execute argv supplied without reconstruction."""
    _require(value.get("schema_version") == 2 and value.get("target") == target,
             "An adopted version-2 endpoint for the exact target is required")
    record = value.get("record")
    _require(isinstance(record, dict), "Missing access record")
    validate_record(record, target, record.get("execution_epoch"))
    _require(record.get("schema_version") == 2 and isinstance(record.get("endpoint_role"), dict)
             and record["endpoint_role"].get("kind") == role,
             "Endpoint has a different or unadopted process role")
    _require(record["farm_root"] == profile["farm_root"] and record["farm_id"] == profile["farm_id"],
             "Endpoint does not match the configured farm identity/root")
    identifier(record["runtime_host"], NODE)
    sha = digest(record)
    _require(value.get("record_sha256") == sha and (expected_record is None or sha == expected_record),
             "Access record digest changed or does not match its contents; resolve again")
    if verified:
        _require(value.get("state") == "VERIFIED" and value.get("verified") is True
                 and value.get("attachment") == Access.verified(record)["attachment"],
                 "Verified attachment host/arguments do not match the exact endpoint")
    else:
        _require("attachment" not in value, "Resolution must not supply attachment authority")
    return record


def connect(profile, target, role="interactive", *, run=subprocess.run) -> int:
    _require(role in {"interactive", "daemon"}, "Unknown endpoint role")
    base = [profile["farm_wrapper"]]
    common = ["--registry", profile["registry"], "--target", target, "--role", role]
    with ssh_configuration(profile) as config:
        resolved = _read_response(run(ssh_argv(profile, config, profile["login_host"],
                                               base + ["access", "resolve", *common, "--json"]),
                                      stdout=subprocess.PIPE, text=True), "RESOLVED")
        record = validate_response(resolved, profile, target, role)
        sha, host = resolved["record_sha256"], record["runtime_host"]
        verified = _read_response(run(ssh_argv(profile, config, host,
                                               base + ["--project", str(PurePosixPath(profile["farm_root"]).parent),
                                                       "access", "verify", *common, "--expected-record", sha, "--json"],
                                               compute=True), stdout=subprocess.PIPE, text=True), "VERIFIED")
        validate_response(verified, profile, target, role, expected_record=sha, verified=True)
        attach = base + ["connect-attach", *common, "--expected-record", sha,
                         "--farm-root", profile["farm_root"], "--farm-id", profile["farm_id"], "--expected-host", host]
        # Authentication and the final terminal inherit stdin/stderr. The remote
        # wrapper re-verifies this same digest immediately before native exec.
        return run(ssh_argv(profile, config, host, attach, compute=True, terminal=True)).returncode


def remote_attach(args, *, access=None, execute=os.execvp) -> int:
    target_name(args.target)
    identifier(args.expected_record, SHA256)
    identifier(args.expected_host, NODE)
    _remote_path(args.farm_root, "farm_root")
    _remote_path(args.registry, "registry")
    _require(args.role in {"interactive", "daemon"}, "Unknown endpoint role")
    access = access or Access(Registry(Path(args.registry)))
    value = access.verify(FarmPaths(Path(args.farm_root)), args.target,
                          expected_record=args.expected_record, role=args.role)
    record = validate_response(value, {"farm_root": args.farm_root, "farm_id": args.farm_id},
                               args.target, args.role, expected_record=args.expected_record, verified=True)
    _require(record["runtime_host"] == args.expected_host, "Verification returned a different compute host")
    argv = Access.verified(record)["attachment"]["argv"]
    execute(argv[0], argv)
    return 0


def command(args) -> int:
    try:
        if getattr(args, "remote_attach", False):
            return remote_attach(args)
        return connect(load_profile(Path(args.config), args.target), args.target, args.role)
    except AccessError as exc:
        print(f"farm connect: {exc}", file=sys.stderr)
    except (OSError, ValueError, TypeError, KeyError):
        # Do not echo malformed records, command output or environment values.
        print("farm connect: configuration/transport failed; check the config and native SSH diagnostics", file=sys.stderr)
    return 1


def add_parser(sub):
    parser = sub.add_parser("connect", help="resolve, verify and attach through native SSH")
    parser.add_argument("target", help="exact configured SITE/FARM or NAME target")
    parser.add_argument("--config", default="~/.config/agent-farm-runtime/connect.toml")
    parser.add_argument("--role", choices=("interactive", "daemon"), default="interactive")
    parser.set_defaults(func=command)
    remote = sub.add_parser("connect-attach", help=argparse.SUPPRESS)
    for flag in ("registry", "target", "farm-root", "farm-id", "expected-record", "expected-host"):
        remote.add_argument("--" + flag, required=True)
    remote.add_argument("--role", choices=("interactive", "daemon"), required=True)
    remote.set_defaults(func=command, remote_attach=True)
