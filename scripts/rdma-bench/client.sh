#!/usr/bin/env bash
# Runs one client transfer against the seeded server and prints the goodput plus cost breakdown.
#
#   DIGEST=sha256 ./client.sh   verify every file against the digests the server computed
#   DIGEST=crc32  ./client.sh   the digest dfdaemon actually computes, so the realistic goodput
#   DIGEST=none SINK=null ./client.sh   transport ceiling, with nothing touching the bytes
set -euo pipefail

# hostNetwork means the pod IP is the node IP, so this stays stable across pod recreation.
SRV_IP=${SRV_IP:-$(kubectl get pod rdma-df-srv -o jsonpath='{.status.podIP}')}
DEVICE=${DEVICE:-rocep25s0}
TASK_ID=${TASK_ID:?set TASK_ID to the task_id printed by server.sh}
MANIFEST_PIECE=${MANIFEST_PIECE:?set MANIFEST_PIECE to the manifest_piece printed by server.sh}
CONCURRENCY=${CONCURRENCY:-10}
CHUNK_MIB=${CHUNK_MIB:-4}
MAX_INFLIGHT=${MAX_INFLIGHT:-16}
DIGEST=${DIGEST:-sha256}
SINK=${SINK:-pwrite}
OUT_DIR=${OUT_DIR:-/mnt/memfs/model-out}
DATA_DIR=${DATA_DIR:-/mnt/memfs/df-data-cli}

kubectl exec rdma-df-cli -- sh -c "
  set -e
  rm -rf ${OUT_DIR} && mkdir -p ${OUT_DIR}
  cd /bench
  MODEL_TASK_ID=${TASK_ID} MODEL_MANIFEST_PIECE=${MANIFEST_PIECE} \
  ./bin/efa_cross_node client \
    --provider verbs --device ${DEVICE} \
    --parent-host ${SRV_IP} --tcp-port 4001 --rdma-port 4007 \
    --concurrency ${CONCURRENCY} --chunk-mib ${CHUNK_MIB} --max-inflight ${MAX_INFLIGHT} \
    --digest ${DIGEST} --sink ${SINK} \
    --out-dir ${OUT_DIR} --data-dir ${DATA_DIR} 2>&1 \
  | grep -E 'OK piece|MODEL_TRANSFER_DONE|MODEL_TRANSFER_COST|panic|error'
"
