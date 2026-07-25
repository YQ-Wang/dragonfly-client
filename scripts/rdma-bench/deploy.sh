#!/usr/bin/env bash
# Builds the efa_cross_node harness and pushes it into both bench pods.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN="$REPO/target/release/examples/efa_cross_node"

cargo build --release --features rdma \
  --manifest-path "$REPO/Cargo.toml" \
  -p dragonfly-client-storage --example efa_cross_node

for pod in "$SRV_POD" "$CLI_POD"; do
  kubectl exec "$pod" -- mkdir -p "$BENCH_DIR/bin"
  kubectl cp "$BIN" "$pod:$BENCH_DIR/bin/efa_cross_node"
  kubectl exec "$pod" -- chmod +x "$BENCH_DIR/bin/efa_cross_node"
  # A binary built against a different libfabric than the pod has will fail at the first
  # transfer instead of at startup, which is a confusing way to find out.
  kubectl exec "$pod" -- sh -c "ldd $BENCH_DIR/bin/efa_cross_node | grep 'not found' && exit 1 || true"
  echo "deployed to $pod"
done

kubectl cp "$REPO/scripts/rdma-bench/gen-llama-13b.sh" "$SRV_POD:$BENCH_DIR/gen-llama-13b.sh"
kubectl exec "$SRV_POD" -- chmod +x "$BENCH_DIR/gen-llama-13b.sh"
echo "deployed dataset generator to $SRV_POD"
