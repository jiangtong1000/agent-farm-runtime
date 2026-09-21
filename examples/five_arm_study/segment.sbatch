#!/bin/bash
#SBATCH -J five-arm-afqmc
#SBATCH -t 12:00:00
# Fake AFQMC segment for the example. Writes afqmc*/<arm>/seg<k>/SEGMENT_END.json
# with {"status": "PASS"|"FAIL", "provenance": {"attempt_id": ...}} via a trap on EXIT.
set -euo pipefail
echo "segment ${SEGMENT} of arm ${ARM} (round ${ROUND}) attempt ${FARMKIT_ATTEMPT_ID}"
