#!/usr/bin/env bash
# Seeds a directory into the harness storage and starts the RDMA piece server, then prints the
# task_id and manifest_piece that client.sh needs.
set -euo pipefail

DEVICE=${DEVICE:-rocep25s0}
CHUNK_MIB=${CHUNK_MIB:-4}
MAX_INFLIGHT=${MAX_INFLIGHT:-16}
FILES_DIR=${FILES_DIR:-/mnt/memfs/llama-2-13b-chat-hf}
DATA_DIR=${DATA_DIR:-/mnt/memfs/df-data}

kubectl exec rdma-df-srv -- pkill -f efa_cross_node || true
sleep 1

kubectl exec rdma-df-srv -- sh -c "
  cd /bench && rm -f /bench/server.log
  nohup ./bin/efa_cross_node server \
    --provider verbs --device ${DEVICE} \
    --bind 0.0.0.0 --tcp-port 4001 --rdma-port 4007 \
    --chunk-mib ${CHUNK_MIB} --max-inflight ${MAX_INFLIGHT} \
    --files-dir ${FILES_DIR} --data-dir ${DATA_DIR} \
    > /bench/server.log 2>&1 &
  echo started
"

# Seeding hashes and copies the whole dataset, which takes about a minute for 26 GB.
for _ in $(seq 1 120); do
  if kubectl exec rdma-df-srv -- grep -q MODEL_SERVER_READY /bench/server.log 2>/dev/null; then
    kubectl exec rdma-df-srv -- cat /bench/server.log
    exit 0
  fi
  sleep 2
done

echo "server did not become ready:" >&2
kubectl exec rdma-df-srv -- cat /bench/server.log >&2
exit 1
