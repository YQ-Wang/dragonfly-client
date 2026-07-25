#!/usr/bin/env bash
# Seeds a directory into the harness storage and starts the RDMA piece server, then prints the
# task_id and manifest_piece that client.sh needs. The TCP piece server starts alongside it, so the
# same seeded task serves both transports.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

FILES_DIR=${FILES_DIR:-$MEMFS/llama-2-13b-chat-hf}
DATA_DIR=${DATA_DIR:-$MEMFS/df-data}

# Done in its own exec so the pattern cannot match the shell running the kill.
kubectl exec "$SRV_POD" -- pkill -f efa_cross_node || true
sleep 1

kubectl exec "$SRV_POD" -- sh -c "
  cd ${BENCH_DIR} && rm -f ${BENCH_DIR}/server.log
  setsid nohup ./bin/efa_cross_node server \
    --provider ${PROVIDER} --device ${DEVICE} --fabric-tag ${FABRIC_TAG} \
    --bind 0.0.0.0 --tcp-port ${TCP_PORT} --rdma-port ${RDMA_PORT} \
    --chunk-mib ${CHUNK_MIB} --max-inflight ${MAX_INFLIGHT} \
    --files-dir ${FILES_DIR} --data-dir ${DATA_DIR} \
    > ${BENCH_DIR}/server.log 2>&1 < /dev/null &
  echo started
"

# Seeding hashes and copies the whole dataset, which takes about a minute for 26 GB.
for _ in $(seq 1 120); do
  if kubectl exec "$SRV_POD" -- grep -q MODEL_SERVER_READY "${BENCH_DIR}/server.log" 2>/dev/null; then
    kubectl exec "$SRV_POD" -- cat "${BENCH_DIR}/server.log"
    exit 0
  fi
  sleep 2
done

echo "server did not become ready:" >&2
kubectl exec "$SRV_POD" -- cat "${BENCH_DIR}/server.log" >&2
exit 1
