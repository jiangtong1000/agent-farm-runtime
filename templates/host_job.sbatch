#!/bin/bash
# Replace PARTITION and ACCOUNT; tune memory, time and CPUs for your worker count.
#SBATCH --job-name=farm-host
#SBATCH --partition=PARTITION
#SBATCH --account=ACCOUNT
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=12:00:00
#SBATCH --signal=B:TERM@60
# Submit with --export=ALL,FARM_SHARED=/shared/farm/hosts,FARM_SESSION=farm2.
# Submit successors with --exclude=<current-node> and --begin=<overlap-time>
# several hours before expiry; then follow the drain/release/claim runbook.
set -euo pipefail
umask 077
: "${SLURM_JOB_ID:?submit this template with sbatch}"
: "${FARM_SHARED:?set FARM_SHARED to a shared directory for host records}"
FARM_SESSION=${FARM_SESSION:-farm2}
[[ "$FARM_SESSION" =~ ^[A-Za-z0-9_-]+$ ]] || { echo "invalid FARM_SESSION" >&2; exit 1; }

# Stable per-node location detects another allocation using this same session.
# Always pass -L; the runtime's --tmux-socket takes this same socket name.
# Never let a login shell's TMUX or default socket select a server.
export TMUX_TMPDIR="${FARM_NODE_TMP:-/tmp}/farm-tmux-${UID}"
mkdir -p "$TMUX_TMPDIR" "$FARM_SHARED"
socket_name="farm-host-$FARM_SESSION"
socket="$TMUX_TMPDIR/tmux-${UID}/$socket_name"
if tmux -L "$socket_name" has-session -t "=$FARM_SESSION" 2>/dev/null; then
    echo "refusing existing session $FARM_SESSION on socket $socket" >&2
    exit 1
fi
# new-session itself also refuses an existing name, closing the check/start race.
tmux -L "$socket_name" new-session -d -s "$FARM_SESSION" -n master
tmux -L "$socket_name" new-window -t "$FARM_SESSION:" -n w1
tmux -L "$socket_name" new-window -t "$FARM_SESSION:" -n w2
# These are empty shells. The existing Master attaches here; this allocation
# never launches an extra Master, agent, or reconciler.
node=$(hostname -s)
record="$FARM_SHARED/host-${SLURM_JOB_ID}.txt"
{
    printf 'job=%s\nnode=%s\nsocket=%s\nsession=%s\n' "$SLURM_JOB_ID" "$node" "$socket" "$FARM_SESSION"
    printf 'TMUX_TMPDIR=%s\nruntime --tmux-socket=%s\n' "$TMUX_TMPDIR" "$socket_name"
    printf 'attach on this node: tmux -S %q attach-session -t %q\n' "$socket" "$FARM_SESSION"
} > "$record"
cat "$record"
trap 'exit 0' TERM INT
while tmux -L "$socket_name" has-session -t "=$FARM_SESSION" 2>/dev/null; do
    sleep 10
done
