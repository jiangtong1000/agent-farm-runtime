#!/bin/bash
#SBATCH -J five-arm-train
#SBATCH -t 2:00:00
# Fake science script for the example: a real project puts its training entry here.
# farmkit exports FARMKIT_ATTEMPT_ID and FARMKIT_STEP; the RUN.json this job writes
# must carry {"provenance": {"attempt_id": "$FARMKIT_ATTEMPT_ID"}}.
set -euo pipefail
mkdir -p "runs/${ARM}"
python train.py --arm "${ARM}" --seed "${SEED}" --out "runs/${ARM}/RUN.json"
