#!/usr/bin/env bash
# Recreates the two hostNetwork bench pods on the GPU nodes and installs everything the harness
# needs: libfabric with its verbs provider, the ibverbs device plugins, and a large tmpfs.
#
# hostNetwork is required because the RoCE devices are not exposed to the pod network namespace,
# and IPC_LOCK is required to pin registered memory. The pods hold no state, so re-running this is
# the fastest way to recover from a lost pod.
#
# SRV_NODE and CLI_NODE are cluster-specific and must be set for anything but the cluster these
# results were taken on. They must be two different nodes on the same fabric.
set -euo pipefail

SRV_NODE=${SRV_NODE:-chi3-en11-13-s1}
CLI_NODE=${CLI_NODE:-chi3-en11-3-s1}
MEMFS_SIZE=${MEMFS_SIZE:-220G}

create_pod() {
  local name=$1 node=$2

  kubectl delete pod "$name" --ignore-not-found --wait=true >/dev/null

  kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
spec:
  hostNetwork: true
  nodeName: ${node}
  restartPolicy: Never
  containers:
    - name: bench
      image: ubuntu:24.04
      command: ["bash", "-lc", "sleep infinity"]
      securityContext:
        privileged: true
        capabilities:
          add: ["IPC_LOCK", "NET_RAW", "SYS_RESOURCE"]
      resources:
        requests:
          cpu: "4"
          memory: 8Gi
YAML

  kubectl wait --for=condition=Ready "pod/${name}" --timeout=180s >/dev/null
  echo "created ${name} on ${node}"
}

provision_pod() {
  local name=$1

  kubectl exec "$name" -- bash -lc "
    set -e
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq libfabric1 libfabric-bin libibverbs1 ibverbs-providers \
      librdmacm1 ibverbs-utils rdmacm-utils >/dev/null
    mkdir -p /bench/bin /mnt/memfs
    mountpoint -q /mnt/memfs || mount -t tmpfs -o size=${MEMFS_SIZE} tmpfs /mnt/memfs
  "

  echo "--- ${name}: fabric providers ---"
  kubectl exec "$name" -- bash -lc 'fi_info -p verbs 2>&1 | grep -E "provider|domain" | head -4'
  kubectl exec "$name" -- bash -lc 'ibv_devinfo | grep -E "hca_id|state:" | head -4'
}

create_pod rdma-df-srv "$SRV_NODE"
create_pod rdma-df-cli "$CLI_NODE"
provision_pod rdma-df-srv
provision_pod rdma-df-cli

echo
echo "setup complete. next:"
echo "  ./deploy.sh"
echo "  kubectl exec rdma-df-srv -- /bench/gen-llama-13b.sh /mnt/memfs/llama-2-13b-chat-hf"
echo "  FILES_DIR=/mnt/memfs/llama-2-13b-chat-hf DATA_DIR=/mnt/memfs/df-data ./server.sh"
