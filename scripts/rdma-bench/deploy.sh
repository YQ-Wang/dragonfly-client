#!/usr/bin/env bash
# Builds the efa_cross_node harness and pushes it into both bench pods.
set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN="$REPO/target/release/examples/efa_cross_node"

cargo build --release --features rdma \
  --manifest-path "$REPO/Cargo.toml" \
  -p dragonfly-client-storage --example efa_cross_node

for pod in rdma-df-srv rdma-df-cli; do
  kubectl exec "$pod" -- mkdir -p /bench/bin
  kubectl cp "$BIN" "$pod:/bench/bin/efa_cross_node"
  kubectl exec "$pod" -- chmod +x /bench/bin/efa_cross_node
  echo "deployed to $pod"
done

kubectl cp "$REPO/scripts/rdma-bench/gen-llama-13b.sh" rdma-df-srv:/bench/gen-llama-13b.sh
kubectl exec rdma-df-srv -- chmod +x /bench/gen-llama-13b.sh
echo "deployed dataset generator to rdma-df-srv"
