# RFC: RDMA transport for Dragonfly P2P piece transfer

Status: **Implemented prototype, opt-in** (production canary only)

Tracking issue: [dragonflyoss/client#1926](https://github.com/dragonflyoss/client/issues/1926)

## 1. Summary

Add RDMA as an optional Dragonfly piece transport for AWS EFA and conventional
RoCE/InfiniBand networks. RDMA carries only bulk piece bytes; TCP remains the reliable control
channel and mandatory per-piece fallback.

The implementation:

- is gated by the `rdma` Cargo feature and disabled by default;
- preserves the existing `Downloader` trait and piece digest verification;
- uses libfabric `FI_EP_RDM` with two-sided tagged messaging;
- supports EFA through native `efa` RDM and conventional RDMA through `verbs;ofi_rxm`;
- exchanges provider-opaque endpoint addresses over a bounded TCP rendezvous protocol;
- pools transfer buffers within a configured memory budget; and
- falls back to TCP on discovery, compatibility, setup, or transfer failure.

The initial scope is host-to-host transfer into Dragonfly's local content store. It is not
GPUDirect, a GPU collective, or a replacement for NCCL/NIXL.

## 2. Why libfabric instead of `async-rdma`

Issue #1926 originally proposes `datenlord/async-rdma` for RoCE/InfiniBand. This RFC expands the
target to AWS EFA because the deployment includes p6 GPU nodes.

The current `async-rdma` 0.5.0 public connection modes (`RCSocket`, `RCCM`, and `RCIBV`) use
standard ibverbs Reliable Connected (RC) queue pairs and exchange QPN/LID/GID endpoint information
([`ConnectionType`](https://docs.rs/async-rdma/0.5.0/async_rdma/enum.ConnectionType.html)).

EFA uses Amazon's Scalable Reliable Datagram (SRD) protocol and does not support RC queue pairs.
EFA can be programmed through libfabric's `efa` provider or EFA-specific direct-verbs APIs
(`IBV_QPT_DRIVER`/`efadv`), but an RC implementation cannot run on EFA unchanged
([AWS EFA](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa.html),
[`efadv_create_qp_ex`](https://www.man7.org/linux/man-pages/man3/efadv_create_qp_ex.3.html)).
Supporting EFA in `async-rdma` would require a distinct backend rather than a configuration change.

Libfabric provides one application surface for both targets:

- native `efa` RDM for EFA/SRD
  ([`fi_efa(7)`](https://ofiwg.github.io/libfabric/main/man/fi_efa.7.html)); and
- `verbs;ofi_rxm` RDM for RoCE/InfiniBand, with RxM supplying RDM/tagged semantics over the verbs
  message provider ([`fi_rxm(7)`](https://ofiwg.github.io/libfabric/main/man/fi_rxm.7.html)).

If EFA is not a project requirement, `async-rdma` remains a reasonable candidate for a narrower
RoCE/InfiniBand-only implementation.

### FFI choice

The prototype owns a small C shim over the required libfabric calls and exposes opaque handles to
safe Rust. OFIWG now publishes the low-level generated
[`ofi-libfabric-sys`](https://crates.io/crates/ofi-libfabric-sys) bindings, but they do not provide
the asynchronous ownership and cancellation model needed here. The official bindings should be
reconsidered before the exposed libfabric surface grows.

## 3. Architecture

### Control and discovery

The existing TCP piece endpoint accepts a lightweight `Discover` probe. An initialized RDMA server
returns:

```text
Capability {
    rdma_port,
    provider,   // concrete name such as "efa" or "verbs;ofi_rxm"
    fabric_tag,
}
```

This avoids a `dragonfly-api` release. Older peers do not recognize the optional probe; builds with
RDMA support but a disabled server return an incompatible error. Both cases trigger TCP fallback.

`fabricTag` is an operator-supplied, fail-closed reachability label. Both peers must advertise the
same non-empty value and exact concrete provider name. For EFA, the tag should identify the VPC and
Availability Zone. EFA traffic is not routable across VPCs or Availability Zones; a cluster
placement group is recommended for performance but is not required.

Successful discovery is cached for 60 seconds. Incompatibility and failed probes suppress another
RDMA attempt to that parent for 60 seconds. Transfer failures evict the successful capability
cache.

### Per-piece transfer

The RDMA rendezvous uses a bounded, versioned protocol over TCP:

```text
Client -> parent TCP endpoint : Discover
Parent -> client              : Capability | Error

Client -> RDMA rendezvous : Request {
                              kind, task_id, piece_number, capability,
                              client_endpoint, tag, chunk_size,
                              max_inflight_chunks
                            }
Parent -> client          : Ready {
                              offset, length, digest,
                              server_endpoint, chunk_size,
                              max_inflight_chunks
                            } | Error
Client -> parent          : RecvPosted { start_chunk, chunk_count }
       ... fi_tsend/fi_trecv transfer one bounded window ...
       ... repeat RecvPosted and transfer until complete ...
Parent -> client          : Done
```

Rendezvous protocol v2 negotiates the lower `max_inflight_chunks` value. The client posts one
contiguous receive window before sending `RecvPosted`, which avoids depending on finite EFA
unexpected-message resources without attempting to consume the provider's entire posted-receive
queue. The parent validates the next expected window, stages that window into a reusable registered
two-window ring, and fills the next half from storage while the current half is sent. It reaps every
completion before reusing a half. When two windows do not fit the registration budget, the parent
sequentially reuses one window. The client retains one completed piece lease so an RDMA failure
remains eligible for TCP fallback before downstream storage mutation.

This is intentionally not Vortex TLV over RDMA. The application-level piece operations and result
contract remain unchanged, while the RDMA-specific endpoint and receive-before-send coordination
use dedicated framing. Existing Vortex TCP remains the fallback.

The receiver returns a lease-backed `AsyncRead` over exactly the landed logical range. Dropping the
reader recycles the buffer. Downstream digest verification is unchanged.

### Implementation map

- `dragonfly-client-storage/src/rdma/shim.c`: minimal libfabric C ABI.
- `dragonfly-client-storage/src/rdma/fabric.rs`: endpoint, address vector, buffer pool, operations,
  cancellation, tags, and CQ progress.
- `dragonfly-client-storage/src/rdma/rendezvous.rs`: bounded frames and capability policy.
- `dragonfly-client-storage/src/client/rdma.rs`: RDMA piece receiver.
- `dragonfly-client-storage/src/server/rdma.rs`: RDMA piece sender and rendezvous server.
- `dragonfly-client/src/resource/piece_downloader.rs`: lazy client fabric and discovery caches.
- `dragonfly-client/src/resource/piece.rs`: mandatory per-piece TCP fallback.
- `dragonfly-client/src/bin/dfdaemon/main.rs`: server startup and capability publication.

Each `Fabric` owns one domain/device and one shared RDM endpoint. A daemon that both downloads and
serves currently opens separate client and server `Fabric` instances.

## 4. Configuration

```yaml
download:
  protocol: rdma          # prefer RDMA; always fall back to TCP per piece

storage:
  server:
    rdma:
      enable: true        # serve over RDMA; downloading is selected independently above
      port: 4007          # TCP rendezvous only; payload uses libfabric
      provider: efa       # auto | efa | verbs
      allowSoftwareProvider: false
      device: rdmap79s0-rdm
      fabricTag: vpc-0123/use1-az1
      maxRegisteredBytes: 512MiB
      chunkSize: 4MiB
      maxInflightChunks: 16
      maxConcurrentTransfers: 64
      transferTimeout: 10s
```

Defaults remain safe: `download.protocol` is `tcp`, RDMA serving is disabled, software providers
are disallowed, the transfer-buffer budget is 512 MiB, the preferred chunk size is 4 MiB, and the
server admits at most 64 concurrent RDMA rendezvous transfers. The per-operation timeout is 10
seconds.

A receive-only daemon may leave `enable: false`; it still needs a non-empty `fabricTag` and usable
provider/device settings when `download.protocol` is `rdma`. Discovery advertises the parent's
actual rendezvous port, so ports need not be fleet-uniform.

`transferTimeout` bounds individual fabric operations and rendezvous waits.
`maxInflightChunks` bounds the posted send/receive operations and sender staging memory for one
piece. Peers negotiate the lower value.
`maxConcurrentTransfers` bounds accepted RDMA rendezvous tasks and storage readers; excess
connections are closed so the downloader immediately falls back to TCP.
`download.pieceTimeout` independently bounds the complete piece download.

## 5. Correctness and safety

### Buffer and operation lifetime

Every post inserts its operation context and `Arc<PinnedBuf>` into the pending map before calling
libfabric. The entry remains until the completion is reaped.

On timeout, the wrapper calls `fi_cancel` and waits for a cancellation or late completion. If no
completion arrives during the grace period, the operation and buffer remain quarantined until
endpoint teardown rather than reusing memory the provider may still own.

### Registered-memory budget

A best-fit pool amortizes allocation and memory registration. `maxRegisteredBytes` bounds active
and idle pooled transfer buffers and their registrations. On a miss, undersized idle buffers are
evicted before waiting for new budget, preventing cached registrations from starving larger
requests.

The process-shared provider address-vector cache is also bounded; new unique endpoint addresses
fail closed after the cap instead of growing process and provider state indefinitely.

Pool counters are available in-process but are not yet exported as production metrics.

### Concurrency

The shim requests and verifies `FI_THREAD_SAFE`. Posts and registrations may run concurrently.
Cancellation and CQ reaping share a narrow lock so `fi_cancel` cannot race operation-context
destruction.

Each `Fabric` has one progress thread. It drains up to 32 CQ entries per call, actively yields or
sleeps briefly while operations are pending, and uses a longer sleep while idle.

### Tags and bounds

A process-local allocator reserves a disjoint block of 4,096 tags per transfer. Chunks use
consecutive tags from that base, and allocator exhaustion fails closed instead of wrapping into an
in-flight block.

Transfers are capped at 4,096 chunks. Peers negotiate the lower configured chunk size and provider
maximum.

### Integrity and security

RDMA reliability does not replace piece digest verification. The receiver exposes only the logical
piece length, and the existing storage path verifies the same offset, length, and digest as TCP.

The RDMA data plane is not encrypted or independently peer-authenticated. It relies on
fabric/network isolation and digest verification, matching rather than strengthening the current
TCP trust model. Two-sided messaging avoids exposing remote keys. A stronger authentication model
should be designed jointly for TCP and RDMA.

Only finished pieces are served. Content that is still being written is never exposed.

## 6. Validation

Automated validation commands:

```shell
cargo test -p dragonfly-client-storage --features rdma
cargo test -p dragonfly-client --features rdma \
  receive_only_config_can_initialize_downloader_fabric
cargo test -p dragonfly-client-config
cargo clippy -p dragonfly-client --features rdma --all-targets -- -D warnings
cargo clippy -p dragonfly-client-storage --features rdma --all-targets -- -D warnings
```

Coverage includes software-provider transfer, framing, discovery, compatibility/fallback, missing
pieces, asymmetric chunk negotiation, memory-budget rejection, pooled-buffer reuse and shutdown,
cancellation reaping, receive-only client configuration, and logical-range safety.

### Recorded EFA measurements

Cross-node measurements used two `p6-b200.48xlarge` workers, one EFA domain
(`rdmap79s0-rdm`), 64 MiB pieces, 24 timed operations, checksum verification, and a warmed pool.
The harness used production client, server, storage, and rendezvous code; the receiver consumed
bytes into memory rather than measuring a complete Dragonfly task write.

| Concurrency | TCP goodput | RDMA goodput, 4 MiB chunks | RDMA / TCP |
|---:|---:|---:|---:|
| 1 | 3.98 Gbps | 8.90 Gbps | 2.24x |
| 4 | 11.38 Gbps | 30.01 Gbps | 2.64x |
| 8 | 19.19 Gbps | 45.75 Gbps | 2.38x |

A 1 MiB chunk reached 48.22 Gbps at concurrency 8. Native unchecked `fi_pingpong` reached
approximately 309 Gbps on the same domain. Replacing `InspectReader + io::copy` with
`InspectWriter + copy_buf` improved an in-memory 64 MiB write/CRC microbenchmark by 7.9%.

These are manually recorded prototype results. The temporary benchmark harness and raw output were
not committed, so the figures are not yet a reproducible repository benchmark and must not be
generalized to complete model distribution.

## 7. Limitations and rollout

The implementation is suitable for review and an opt-in prototype. Before a production canary it
still needs:

- exported transfer, fallback, latency, CQ-error, and buffer-pool metrics;
- checked-in reproducible end-to-end benchmarks, including local storage and model loading;
- peer-termination, rendezvous interruption, CQ/provider failure, and memlock exhaustion tests;
- long-duration EFA and mixed-version soak testing; and
- RoCE/InfiniBand hardware validation.

Current performance limitations:

- one domain per `Fabric`, with no striping across the eight EFA devices;
- a two-window sender ring but a whole-piece registered receive lease rather than an end-to-end
  streaming ring, content-store mmap, or GPU memory;
- one TCP rendezvous connection per piece; and
- no one-sided RMA or `FI_HMEM`.

Possible future work includes multi-rail striping, content-store mmap registration, scheduler-visible
RDMA capability, reusable rendezvous connections, and optional `FI_HMEM`/GPUDirect. One-sided RMA
should be considered only where hardware offload and a safe remote-key authorization model justify
it.

RDMA should remain disabled by default. A production canary should require repeatable complete-task
improvement (for example, at least 20% faster completion or materially lower CPU at equal
throughput), no correctness failures, and a near-zero fallback rate within a correctly configured
fabric.

## 8. Open questions

1. Is EFA part of the required scope, making libfabric the preferred common abstraction, or should
   the project target only RoCE/InfiniBand with `async-rdma`?
2. Should Discover/Capability remain on the TCP piece endpoint, or should `dragonfly-api` advertise
   RDMA capability for scheduler-aware parent selection?
3. Is an operator-supplied `fabricTag` acceptable for the initial release?
4. Should the C shim remain, or should the prototype adopt `ofi-libfabric-sys` before merge?
5. What authentication mechanism should protect both TCP and RDMA piece transfer?
6. Is single-domain performance sufficient, or is multi-rail EFA required before production use?
