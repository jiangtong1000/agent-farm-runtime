"""Machine-readable CLI boundary. Every non-VERIFIED result omits attachment data."""
from __future__ import annotations

import json
import os
from pathlib import Path

from ..adapters.filesystem import FileLockBusy, FilesystemCapabilityError
from ..store import FarmPaths
from . import Access
from .contract import AccessError, require, result
from .registry import Registry


def command(args) -> int:
    target = getattr(args, "target", None)
    try:
        root = args.registry or os.environ.get("FARM_ACCESS_REGISTRY")
        require(bool(root), "UNPUBLISHED", "registry_not_configured",
                "Set --registry or FARM_ACCESS_REGISTRY to the persistent shared registry")
        registry = Registry(Path(root))
        access = Access(registry)
        if args.access_action == "init":
            registry.initialize(attest_shared_storage=args.attest_shared_storage)
            value = result("INITIALIZED", None, "registry_ready", "Registry initialized; no target published")
        elif args.access_action == "resolve":
            value = access.resolve(target)
        else:
            paths = FarmPaths(Path(args.project).resolve() / ".farm")
            if args.access_action == "verify":
                value = access.verify(paths, target, expected_record=args.expected_record)
            else:
                value = access.publish(paths, target, job_id=args.job_id, control_session=args.control_session,
                                       control_socket=args.control_socket, default_window=args.default_window,
                                       expected_epoch=args.expected_epoch)
    except AccessError as exc:
        value = result(exc.state, target, exc.reason, str(exc))
    except PermissionError:
        value = result("AUTH_REQUIRED", target, "storage_permission", "Access storage permission denied")
    except FileLockBusy:
        value = result("PENDING", target, "publisher_busy", "Publisher/turnover lock is busy; no lock was replaced")
    except (OSError, FilesystemCapabilityError):
        value = result("UNREACHABLE", target, "storage_unreachable",
                       "Storage operation failed; preserve records and retry the identical publication if needed")
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0 if value["state"] in {"INITIALIZED", "RESOLVED", "VERIFIED"} else 1


def add_parser(sub) -> None:
    parser = sub.add_parser("access", help="publish/resolve/verify human-facing control endpoints; never attach or provision")
    actions = parser.add_subparsers(dest="access_action", required=True)
    for name in ("init", "publish", "resolve", "verify"):
        action = actions.add_parser(name)
        action.add_argument("--registry", help="absolute persistent shared registry (or FARM_ACCESS_REGISTRY)")
        action.add_argument("--json", action="store_true", help="structured JSON (also the default)")
        if name == "init":
            action.add_argument("--attest-shared-storage", action="store_true",
                                help="attest storage is persistent/shared and supports cooperative locks/fsync/rename")
        else:
            action.add_argument("--target", required=True, help="stable NAME or SITE/NAME, never a discovered session")
        if name == "publish":
            action.add_argument("--job-id", required=True, help="exact numeric Slurm allocation ID")
            action.add_argument("--control-session", required=True, help="exact human-facing tmux session name")
            action.add_argument("--control-socket", help="absolute tmux socket path; omitted means the default server")
            action.add_argument("--default-window", help="exact window name, or numeric window index")
            action.add_argument("--expected-epoch", help="optional execution_epoch precondition")
        if name == "verify":
            action.add_argument("--expected-record", help="record_sha256 returned by resolve; reject a changed candidate")
        action.set_defaults(func=command)
