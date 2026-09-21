"""Placeholder training entry point for the example (never executed by the tests)."""
import argparse, json, os
p = argparse.ArgumentParser(); p.add_argument("--arm"); p.add_argument("--seed"); p.add_argument("--out")
a = p.parse_args()
json.dump({"provenance": {"attempt_id": os.environ.get("FARMKIT_ATTEMPT_ID")},
           "selected": {"metric": 0.5}, "stop_reason": "max_updates"}, open(a.out, "w"))
