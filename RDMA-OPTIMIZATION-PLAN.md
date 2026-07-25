# RDMA P2P Optimization Plan

> Work currently in flight, and the one measurement still missing, are tracked in `RDMA-WIP.md`.

## Goal

Increase complete Dragonfly piece-transfer goodput and concurrency while keeping RDMA optional,
bounded, observable, and safe to fall back to TCP. Optimizations are accepted based on complete
source-read-to-target-write measurements rather than fabric-only bandwidth.

This plan borrows the reusable registered-buffer and bounded-inflight ideas from
`ai-dynamo/modelexpress`'s host artifact path. Its GPU-direct NIXL/VMM design is not a direct fit
for Dragonfly's current host-storage `AsyncRead` contract. The comparison was refreshed against
Modelexpress commit `750239f3c0d31498c39d11694998e32d5cb2d62f` (2026-07-23).

## Constraints

- Preserve piece digest verification and storage semantics.
- Preserve TCP fallback until an RDMA transfer has completed successfully.
- Never depend on a provider's unexpected-message queue for correctness.
- Bound registered memory and posted operations independently of piece size.
- Keep RDMA disabled by default and keep mixed-version deployments functional through TCP
  fallback.
- Treat EFA, verbs/RxM, and software-provider validation as different evidence levels.

## Baseline

The prototype RFC records 64 MiB, single-EFA-domain results of 8.90 Gbps at concurrency 1 and
45.75 Gbps at concurrency 8 with 4 MiB chunks. A 1 MiB chunk reached 48.22 Gbps at concurrency 8.
Those results consumed the receiver into memory and are not a complete Dragonfly task benchmark.

RoCE hardware results, the harness, and its driver scripts are now committed: see
`RDMA-ONPREM-VALIDATION.md` and `scripts/rdma-bench/`. Read that document before trusting any RDMA
throughput number in this repository. It also records why the first round of RoCE measurements was
invalid — the harness capped its own upload limiter at 1 GiB/s, which is 8.6 Gbps, so it measured a
leaky bucket rather than the fabric. Any harness that constructs a limiter must take it from
`config.upload.bandwidth_limit` the way `dfdaemon/main.rs` does, and print it.

Before claiming an optimization, check in a benchmark that records:

- source storage read, rendezvous, registration/pool wait, fabric transfer, digest, and target
  storage write time;
- task goodput, CPU, pinned bytes, pool hit/miss, posted-operation high-water mark, CQ errors,
  `FI_EAGAIN`, and TCP fallback reason;
- 1, 4, and 8 concurrent 64 MiB pieces with 1 MiB and 4 MiB chunks;
- cold and warm registration pools; and
- one versus every available hardware rail.

## Work plan

### Phase 1: bounded transfer windows

Status: implemented and validated locally with the software libfabric provider.

- Add `storage.server.rdma.maxInflightChunks`, defaulting to 16 and validated in `[1, 4096]`.
- Add `storage.server.rdma.maxConcurrentTransfers`, defaulting to 64, so idle or overloaded
  rendezvous connections cannot create unbounded tasks or storage readers.
- Negotiate the lower client/server value in rendezvous protocol v2.
- Replace the post-all handshake with explicit contiguous receive windows.
- Keep at most the negotiated number of receive and send operations posted for one piece.
- Use a two-window sender ring, when it fits the registration budget, so storage can fill the next
  registered window while the current window is sent; reuse each half after its completions and
  fall back to sequential reuse of one window under a tighter budget.
- Retain the full receiver lease for now so a transfer failure is still visible before the
  caller starts a storage write and can fall back to TCP.
- Allocate non-overlapping 4096-tag blocks per transfer and fail closed at counter exhaustion;
  never rely on probabilistic hash collision resistance for receive matching.
- Bound the shared provider address-vector cache and reject lengths that cannot be represented by
  the host address space.
- Bound the complete server operation by `download.pieceTimeout`, propagate control-plane errors
  while receives are pending, and reject malformed or trailing rendezvous payloads.

Expected result: bounded endpoint queue use, lower upload-side registered memory for large pieces,
and overlap between source reads and fabric transfer. For the common 64 MiB/4 MiB case, the default
16-chunk window introduces no additional window boundary. Smaller windows must be benchmarked
before becoming the default.

Follow-up: introduce a transfer-aware streaming reader and restartable partial-write contract, then
extend the registered ring to the receiver so fabric transfer, digesting, and target write overlap
without losing TCP fallback.

Local validation completed for this phase:

- `cargo test -p dragonfly-client-config` (39 tests)
- `cargo test -p dragonfly-client-storage --features rdma` (70 unit tests and 11 software-provider
  integration tests)
- `cargo test -p dragonfly-client --features rdma` (50 library tests, 10 `dfget` tests, and
  binary/doc-test targets)
- RDMA-enabled Clippy with warnings denied for the changed client, configuration, and storage
  crates; three pre-existing Rust 1.97 style lints in unrelated utility/GC files were explicitly
  allowed
- repository-wide Rust formatting check

The adversarial integration cases cover an error arriving while a receive is pending, malformed
window ordering, a stalled peer, admission overload, a registration budget that permits only one
sender window, multiple chunk/window boundary combinations, and all three piece namespaces.

The RDMA integration test uses the software libfabric provider; EFA and verbs hardware validation
remain rollout requirements.

### Open confidence gates

The software-provider results establish protocol and lifecycle behavior, not production speed or
hardware-provider correctness. Do not claim the strategy is fully proven until all of these close:

- run cross-node EFA and verbs/RxM fault-injection and long-duration soak tests, including peer
  death with posted receives, CQ errors, and provider queue pressure;
- check in complete source-read-to-target-write A/B benchmarks against the original
  implementation, including CPU and fallback rate. Partially closed: cross-node verbs/RxM
  receiver-side A/B benchmarks are committed in `RDMA-ONPREM-VALIDATION.md`, which also lists what
  they do not cover — no TCP comparison through the same piece path, no sender-side attribution, and
  no CPU, pinned-byte, or fallback-rate figures;
- export the pool, queue, admission, fallback-reason, and complete-transfer timing metrics needed
  for a canary rollback decision; and
- validate the default `maxInflightChunks` and `maxConcurrentTransfers` under representative task
  sizes and concurrency rather than treating their current safety-oriented defaults as
  throughput-optimal.

### Phase 2: batch the operation hot path

- Add batch post APIs to the Rust/libfabric shim boundary.
- Replace one boxed context and oneshot allocation per chunk with a reusable generational context
  slab.
- Reap completions in window batches and evaluate selective completion only where provider
  ordering makes buffer reuse unambiguous.
- Benchmark CPU per GiB and throughput at 1 MiB chunks before and after the change.

Gate: no cancellation, endpoint-retirement, or buffer-lifetime regression under fault injection.

### Phase 3: multi-rail and NUMA placement

- Enumerate compatible provider domains instead of opening one configured domain only.
- Create one `Fabric` per selected rail and publish a stable rail-set capability epoch.
- Bind CQ progress threads and registered-buffer allocation to the rail-local NUMA node.
- Distribute independent pieces across rails first; stripe one piece only if task-level
  distribution cannot saturate the available rails.
- Retain a single-rail mode for diagnosis and rollback.

Gate: demonstrate scaling from one to two rails without increasing error/fallback rate, then repeat
for all available EFA devices. Report storage, memory-bandwidth, and CPU limits separately.

### Phase 4: RDMA-aware scheduling and retry

- Advertise fresh RDMA readiness, provider, fabric tag, rail set, and capability epoch with peer
  metadata.
- Filter incompatible parents before selection while preserving Dragonfly's existing
  idle-bandwidth weighting.
- Classify semantic, compatibility, local-fabric, remote-fabric, timeout, and size errors.
- Retry another already-collected compatible parent for transport failures before failing the
  task; do not retry `NOT_FOUND` over TCP to the same parent unless TCP can change the outcome.
- Export selection, attempt, fallback, and retry-reason metrics.

Gate: improve task p95 completion time during injected peer/RDMA failures without increasing
duplicate source traffic materially.

### Phase 5: connection and storage-path experiments

- Reuse or multiplex TCP rendezvous connections when small-piece setup time is measurable.
- Evaluate content-store mmap registration to remove the sender copy only with explicit eviction
  and registration-lifetime rules.
  - Implemented first cut: `storage.server.rdma.mmapContent` maps finished on-disk pieces and
    fills the registered send ring from the mapping (with AsyncRead fallback for cache hits /
    map failures). Full NIC registration of mapped pages remains future work.
  - Registering mapped content pages was evaluated and deferred, on either side of the transfer.
    On a node that also trains, it pins page-cache pages and consumes NIC memory-region entries
    shared with NCCL on the same device, and on NVMe-backed content it faults in blocks that are
    about to be overwritten in full while moving durability to `msync`. See "What is left" in
    `RDMA-ONPREM-VALIDATION.md`. Doing this needs a registration cache and a shared-device budget,
    not a per-piece registration.
- Consider sharing upload/download `Fabric` instances only after failure-isolation soak tests.
- Consider one-sided RMA or `FI_HMEM` only for a future authenticated remote-key model or a
  GPU-resident cache API.

## Validation matrix

Every phase should pass:

- rendezvous encoding, bounds, mixed-version fallback, and malformed-window tests;
- software-provider end-to-end transfer across multiple windows;
- early disconnect, timeout, cancellation, CQ error, endpoint retirement, and memlock exhaustion;
- regular, persistent, and persistent-cache piece namespaces;
- digest mismatch and partial target-write recovery;
- EFA cross-node tests and a long-duration mixed TCP/RDMA soak; and
- verbs/RxM hardware tests before claiming RoCE or InfiniBand support.

## Rollout gates

1. Land each phase independently with before/after benchmark artifacts.
2. Keep the default disabled through hardware soak testing.
3. Canary on one compatible fabric with a near-zero fallback rate.
4. Require at least 20% faster complete-task time or materially lower CPU at equal throughput.
5. Roll back automatically when correctness failures, CQ errors, or fallback rate exceed the
   canary threshold.
