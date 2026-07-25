#!/usr/bin/env bash
# Runs one client transfer against the seeded server and prints the goodput plus cost breakdown.
#
#   DIGEST=sha256 ./client.sh              verify every file against the digests the server computed
#   DIGEST=crc32  ./client.sh              the digest dfdaemon computes, so the realistic goodput
#   DIGEST=none SINK=null ./client.sh      transport ceiling, with nothing touching the bytes
#   TRANSPORT=tcp ./client.sh              the same workload over the TCP piece server
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

# hostNetwork means the pod IP is the node IP, so this stays stable across pod recreation.
SRV_IP=${SRV_IP:-$(kubectl get pod "$SRV_POD" -o jsonpath='{.status.podIP}')}
TASK_ID=${TASK_ID:?set TASK_ID to the task_id printed by server.sh}
MANIFEST_PIECE=${MANIFEST_PIECE:?set MANIFEST_PIECE to the manifest_piece printed by server.sh}
TRANSPORT=${TRANSPORT:-rdma}
CONCURRENCY=${CONCURRENCY:-10}
DIGEST=${DIGEST:-sha256}
SINK=${SINK:-pwrite}
OUT_DIR=${OUT_DIR:-$MEMFS/model-out}
DATA_DIR=${DATA_DIR:-$MEMFS/df-data-cli}

kubectl exec "$CLI_POD" -- sh -c "
  set -e
  rm -rf ${OUT_DIR} && mkdir -p ${OUT_DIR}
  cd ${BENCH_DIR}
  MODEL_TASK_ID=${TASK_ID} MODEL_MANIFEST_PIECE=${MANIFEST_PIECE} \
  ./bin/efa_cross_node client \
    --provider ${PROVIDER} --device ${DEVICE} --fabric-tag ${FABRIC_TAG} \
    --parent-host ${SRV_IP} --tcp-port ${TCP_PORT} --rdma-port ${RDMA_PORT} \
    --transport ${TRANSPORT} \
    --concurrency ${CONCURRENCY} --chunk-mib ${CHUNK_MIB} --max-inflight ${MAX_INFLIGHT} \
    --digest ${DIGEST} --sink ${SINK} \
    --out-dir ${OUT_DIR} --data-dir ${DATA_DIR} 2>&1 \
  | grep -E 'OK piece|MODEL_TRANSFER_DONE|MODEL_TRANSFER_COST|discovered|panic|error'
"
