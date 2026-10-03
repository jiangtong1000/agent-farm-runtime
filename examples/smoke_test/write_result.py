#!/usr/bin/env python3
"""Write a synthetic result bound to the attempt that actually ran."""
import json
import os
import sys
from pathlib import Path


def main() -> None:
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"provenance": {"attempt_id": os.environ["FARMKIT_ATTEMPT_ID"]},
                                  "metric": 1.0}) + "\n")


if __name__ == "__main__":
    main()
