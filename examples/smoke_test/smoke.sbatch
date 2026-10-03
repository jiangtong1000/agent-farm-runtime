#!/bin/bash
# Replace PARTITION and ACCOUNT with your site's CPU partition and account.
#SBATCH --job-name=farm-smoke
#SBATCH --partition=PARTITION
#SBATCH --account=ACCOUNT
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=256M
#SBATCH --time=00:05:00
set -euo pipefail
python write_result.py results/slurm.json
