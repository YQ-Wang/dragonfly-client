#!/usr/bin/env bash
# Runs the same workload over RDMA and over the TCP piece server at a range of concurrencies, and
# prints the two side by side.
#
# Both transports read the same seeded pieces on the same parent, apply the same digest, and write
# through the same sink, so the difference between the two columns is the transport. The transports
# alternate at each concurrency rather than running all of one and then all of the other, so a
# drifting machine shows up as noise in both instead of as a win for whichever ran first.
#
#   TASK_ID=... MANIFEST_PIECE=... ./compare.sh
#   CONCURRENCIES="1 4 16" DIGEST=crc32 ./compare.sh
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$HERE/config.sh"

TASK_ID=${TASK_ID:?set TASK_ID to the task_id printed by server.sh}
MANIFEST_PIECE=${MANIFEST_PIECE:?set MANIFEST_PIECE to the manifest_piece printed by server.sh}
CONCURRENCIES=${CONCURRENCIES:-"1 2 4 8 16"}
REPEATS=${REPEATS:-3}
DIGEST=${DIGEST:-crc32}
SINK=${SINK:-pwrite}

# goodput_of runs one transfer and echoes its aggregate goodput in Gbps.
goodput_of() {
  local transport=$1 concurrency=$2

  TRANSPORT="$transport" CONCURRENCY="$concurrency" DIGEST="$DIGEST" SINK="$SINK" \
    "$HERE/client.sh" 2>/dev/null \
    | sed -n 's/.*aggregate_goodput=\([0-9.]*\) Gbps.*/\1/p' \
    | tail -1
}

# best_of takes the fastest of REPEATS runs. The slow runs are the ones that collided with
# something else on the node, so the best run is the one that says the most about the transport.
best_of() {
  local transport=$1 concurrency=$2 best=0 sample

  for _ in $(seq 1 "$REPEATS"); do
    sample=$(goodput_of "$transport" "$concurrency")
    [ -z "$sample" ] && continue
    best=$(awk -v a="$best" -v b="$sample" 'BEGIN { print (b > a) ? b : a }')
  done
  echo "$best"
}

printf 'provider=%s device=%s digest=%s sink=%s chunk_mib=%s inflight=%s repeats=%s\n\n' \
  "$PROVIDER" "$DEVICE" "$DIGEST" "$SINK" "$CHUNK_MIB" "$MAX_INFLIGHT" "$REPEATS"
printf '%-12s %14s %14s %10s\n' streams "rdma Gbps" "tcp Gbps" speedup

for concurrency in $CONCURRENCIES; do
  rdma=$(best_of rdma "$concurrency")
  tcp=$(best_of tcp "$concurrency")
  speedup=$(awk -v r="$rdma" -v t="$tcp" 'BEGIN { print (t > 0) ? sprintf("%.2fx", r / t) : "n/a" }')
  printf '%-12s %14s %14s %10s\n' "$concurrency" "$rdma" "$tcp" "$speedup"
done
