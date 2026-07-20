# RFC: RDMA transport for Dragonfly P2P piece transfer (EFA + regular RDMA)

Status: **Implemented** (phases 1–2; see "Implementation status" below)

## Implementation status (2026-07-19)

Implemented in `dragonfly-client` behind the `rdma` cargo feature (`cargo build --features rdma`,
requires libfabric headers/library at build time; default builds are unchanged and dependency-free):

| Piece | Location |
|---|---|
| C shim over libfabric (tagged RDM, opaque handles) | `dragonfly-client-storage/src/rdma/shim.c` + `build.rs` |
| Safe fabric wrapper (progress thread, MR/buffer lifetime guards, pinned-memory budget, tag allocation) | `dragonfly-client-storage/src/rdma/fabric.rs` |
| Rendezvous wire protocol + fail-closed wire compatibility check | `dragonfly-client-storage/src/rdma/rendezvous.rs` |
| Capability negotiation policy (moved here from `resource/rdma.rs`, which now re-exports it) | `dragonfly-client-storage/src/rdma/negotiation.rs` |
| `RDMAClient` / `RDMAServer` | `dragonfly-client-storage/src/{client,server}/rdma.rs` |
| `RDMADownloader` (lazy fabric init, incompatible-parent cache) | `dragonfly-client/src/resource/piece_downloader.rs` |
| Per-piece TCP fallback in the three download paths | `dragonfly-client/src/resource/piece.rs` |
| Server startup wiring | `dragonfly-client/src/bin/dfdaemon/main.rs` |

**Deviation from §9:** no `dragonfly-api` proto change was needed for phase 1. The
`SyncPiecesResponse` fields would require releasing a new `dragonfly-api` crate (pinned
`=2.2.28`), so capability exchange happens on the rendezvous connection itself: the
downloader dials the parent's IP on the (fleet-uniform) configured `rdma.port`, sends its
provider/fabric-tag/endpoint in the request, and the parent answers `Ready` or an
`Incompatible` error which the downloader caches (10 min) before using TCP. The proto
fields in §9 remain the right long-term advertisement path and `CollectedParent.rdma`
is already plumbed for them.

Enable it with (all peers need the same `port` and a matching non-empty `fabricTag`):

```yaml
download:
  protocol: rdma          # prefer RDMA, always fall back to TCP per piece
storage:
  server:
    rdma:
      enable: true        # serve pieces over RDMA
      port: 4007          # TCP rendezvous port (bulk bytes go over the fabric)
      provider: efa       # efa | verbs | auto
      fabricTag: vpc-0123/use1-az1   # reachability domain; peers must match
```

Validated end-to-end (10 MiB piece, chunked tagged transfer, digest intact, fallback and
negotiation paths) against real libfabric using its `tcp`/`sockets` providers — the same
code path as `efa`/`verbs`. Remaining hardware validation: RoCE/IB testbed and real EFA
instances (§11 phases 3+), plus zero-copy/`FI_HMEM` GPUDirect work (§11 phases 4–5).
Scope: `dragonflyoss/client` (Rust dfdaemon) + a proto change in `dragonflyoss/api`
Target (phase 1): **host-to-host RDMA** that fills the local content store faster on GPU
nodes. GPUDirect (NIC→GPU memory) is explicitly a later phase and out of the initial scope.

Implementation note: the client repository currently contains the disabled-by-default RDMA
configuration schema and hardware-independent capability/fallback policy. It does **not** yet
contain a libfabric data plane, advertise RDMA through `dragonfly-api`, or start an RDMA server.
Setting `download.protocol: rdma` must not be documented as usable until those pieces land.

---

## 1. Motivation

Today peers exchange pieces over a custom framed protocol ("Vortex") carried on TCP or QUIC.
On GPU clusters the NICs are RDMA-capable (AWS EFA, or RoCE/InfiniBand on-prem) and the TCP
data path leaves most of that bandwidth on the table and burns host CPU on copies. We want an
RDMA transport for the bulk piece bytes, usable on **both**:

- **AWS EFA** nodes (p4d/p5/etc.), and
- **regular RDMA** nodes (RoCE v2 / InfiniBand).

## 2. The constraint that shapes the whole design

EFA and "classic" RDMA are not the same verbs surface:

- Classic RDMA (RoCE/IB) uses **RC (Reliable Connected) queue pairs**. The mature Rust crates
  (`ibverbs`, `async-rdma`) are RC-based and work here.
- **EFA does not support RC at all.** It uses Amazon's SRD and is reached through
  **libfabric (OFI)**, not raw `libibverbs`. An ibverbs/RC program does not run on EFA without a
  rewrite (see `fi_efa(7)`).

There is an existing comment in `piece_collector.rs` that bakes in the wrong assumption:

```rust
// If protocol is rdma, the IP is used to exchange the queue pair endpoint of IBVerbs.
pub download_ip: Option<String>,
```

A QP/IBVerbs rendezvous handles RoCE/IB but **cannot** address EFA. To support both with one
code path we standardize on **libfabric** and advertise a **provider-opaque endpoint address**
(`fi_getname()` bytes), not a GID+QPN. libfabric gives us:

- `efa` provider → EFA/SRD, and
- `verbs` provider → RoCE/InfiniBand.

> Decision D1: **libfabric is the single transport** for both fabrics. Do not introduce an
> ibverbs/RC stack in parallel; if EFA is required we pay the libfabric cost regardless, and two
> RDMA stacks doubles the correctness surface.
>
> Cost: libfabric's Rust bindings are immature, so we own a thin `bindgen` FFI layer
> (`dragonfly-client-storage/src/client/rdma/ffi.rs` or a small `libfabric-sys` crate). This is
> the single largest risk item.

This proposal targets the `efa` fabric, not `efa-direct`. The `efa` RDM endpoint supports tagged
messaging and provider-managed segmentation for pieces larger than the device MTU. `efa-direct`
has a smaller capability set and its message size is limited to roughly one MTU; it is not a
drop-in transport for Dragonfly's 4–64 MiB pieces.

## 3. Where it plugs in (no data-path rewrite)

The piece transport is already abstracted. RDMA is a **third transport behind the same trait**:

| Layer | File today | Change |
|---|---|---|
| Protocol selector | `Download.protocol` (`dfdaemon.rs`, default `"tcp"`) | accept `"rdma"` (= prefer RDMA, fall back to TCP) |
| Client trait | `Downloader` in `resource/piece_downloader.rs` | add `"rdma"` arm → `RDMADownloader` |
| Client transport | `storage/src/client/{tcp,quic}.rs` | add `client/rdma.rs` (`RDMAClient`) |
| Server transport | `storage/src/server/{tcp,quic}.rs` | add `server/rdma.rs` (`RDMAServer`) |
| Parent advertisement | `SyncPiecesResponse {ip, tcp_port, quic_port}` set in `grpc/dfdaemon_upload.rs` | add `rdma_port` + `rdma_endpoint` + `rdma_provider` + `rdma_fabric_tag` |
| Parent selection | `CollectedParent {download_ip, download_tcp_port, download_quic_port}` in `piece_collector.rs` | add `download_rdma_*` fields |

The Vortex request/response semantics (`DownloadPiece` → `PieceContent{offset,length,digest}+bytes`)
are unchanged. Only **how the bulk `length` bytes move** changes: a registered RDMA buffer instead
of `tokio::io::copy` over a socket.

Crucially, the `Downloader` trait already returns
`Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)>` = `(reader, offset, digest)`. The RDMA
client lands bytes into a buffer and returns a `Cursor` over it as the reader — **the trait does
not change**, so the caller (`resource/piece.rs`) and its digest verification are untouched.

## 4. Control plane vs data plane

- **Control / rendezvous (reliable, small):** the `DownloadPiece` request, the piece metadata
  reply, endpoint-address exchange, and errors travel over a normal TCP connection to the parent's
  `rdma_port` (the same `socket2`/keepalive setup as `server/tcp.rs`). This reuses battle-tested
  reliable framing for the parts that must not be lost, and gives a clean, synchronous place to do
  the address handshake.
- **Data plane (bulk):** only the piece content bytes move over libfabric.

> Decision D2: keep a **separate reliable rendezvous channel** rather than carrying control as
> tiny tagged RDMA messages. EFA has limited unexpected-message buffering; doing control over TCP
> removes a class of drop/retry bugs and makes fallback trivial (we already have a live TCP conn).

## 5. Transfer model

Use libfabric **`FI_EP_RDM`** endpoints with **two-sided tagged messaging** (`fi_tsend`/`fi_trecv`):

1. Client → parent (over rendezvous TCP): `DownloadPiece{task_id, number}` + `client_endpoint`
   (its `fi_getname` blob) + a unique `tag`.
2. Parent reads the piece `(offset, length, digest)` and a content reader from `storage`, registers
   the content buffer, replies (over rendezvous TCP) with
   `PieceReady{offset, length, digest, parent_endpoint, tag}` (or `Error`).
3. Client sizes and registers a `length`-byte recv buffer, `fi_trecv`s with `tag`, then acks "ready"
   on the rendezvous channel.
4. Parent `fi_tsend`s the content; both poll their completion queues.
5. Client verifies `digest` over the landed buffer (identical to the TCP path) and returns
   `(Cursor::new(buf), offset, digest)`.

> Decision D3: **two-sided send/recv is the baseline, not one-sided RMA read.** Reasons:
> (a) RMA read isn't available on all EFA hardware generations; (b) two-sided never exposes an
> `rkey`, so a peer can't read/write our memory out of band — smaller security blast radius.
> One-sided `fi_read` is a later optimization, gated on the `FI_RMA` capability bit.
>
> Decision D4: **rendezvous (receiver posts before sender sends)**, step 3 before step 4, to avoid
> EFA unexpected-message overflow.

## 6. Capability negotiation & mandatory fallback

RDMA only works **within a compatible fabric**. This is the second-biggest source of "RDMA is
broken/slow" reports after MR bugs, so it is a first-class concern:

- **EFA:** same VPC and Availability Zone. EFA device traffic is not routable and cannot cross a
  VPC or AZ. A cluster placement group is recommended for low latency/high throughput, but AWS
  explicitly does not require one. Peers also need EFA security-group rules that allow traffic to
  and from the EFA-enabled security group.
- **RoCE/IB:** a provider-compatible, operator-defined fabric reachability domain. RoCE v2 can be
  routed when the network is configured for it, so the implementation must not hard-code an L2 or
  subnet assumption.

Mechanism:

1. A parent that runs the RDMA server advertises `rdma_port`, `rdma_endpoint`, `rdma_provider`,
   and a **`rdma_fabric_tag`** (operator-supplied reachability label) in every
   `SyncPiecesResponse`.
2. A downloader attempts RDMA **only if** all hold: local RDMA enabled; parent advertised an
   `rdma_endpoint`; `provider` is compatible; **`fabric_tag` matches**. Otherwise it uses
   `download_tcp_port`.
3. Any RDMA setup/transfer error for a piece (AV insert fails, no completion before timeout, MR
   failure) **falls back to TCP for that piece** and never aborts the task.

> Decision D5: **TCP remains a mandatory floor.** `protocol = "rdma"` means *prefer* RDMA with TCP
> fallback, not *RDMA only*. The parent always runs the TCP server. This guarantees zero
> regression for mixed/cross-fabric fleets and makes rollout safe. (This is a slight semantic
> widening of the current `protocol` field — documented in §10.)

For the first implementation, `rdma_fabric_tag` is mandatory rather than auto-derived. A safe EFA
value includes the VPC id and AZ id (for example `vpc-123/use1-az1`). Placement-group identity may
be added as a scheduling/performance hint, but must not be used as a reachability requirement.

## 7. Correctness hazards (the "don't introduce issues" section)

These are the things that turn into data corruption, hangs, or OOM if done casually. Each has a
required mitigation.

1. **MR lifetime vs storage GC — use-after-free in hardware.** RDMA NICs DMA into/out of pinned
   memory independently of the CPU. If the content store evicts/deletes a task file (or unmaps a
   region) while an MR over it is live, the NIC can DMA into freed memory. **Mitigation:** the MR
   registration cache must pin the piece (refcount in `Storage`) for the duration of any in-flight
   op; deregister (`fi_close` on the MR) **before** the file is unmapped/closed; storage GC must
   consult the MR cache. Never deregister-then-DMA or free-then-deregister.

2. **Pinned-memory limits & OOM.** Registration pins physical pages and is bounded by the NIC's max
   MR count and `ulimit -l`. **Mitigation:** bound the registration cache by
   `max_registered_bytes` (config), LRU-evict idle MRs, and surface a metric. Registration is
   expensive — cache by `(task_id, piece_number)`; don't register per request.

3. **Timeout/cancel must not free a buffer the NIC still owns.** On timeout you cannot just drop
   the recv buffer. **Mitigation:** `fi_cancel` the op and wait for the cancellation completion (or
   tear the endpoint down) before releasing the buffer/MR. Tie buffer ownership to the op via an
   owned guard.

4. **Digest verification stays on.** Do **not** skip the existing per-piece digest check because
   "RDMA is reliable." An offset/length/MR bug corrupts silently. The RDMA path returns the same
   `(offset, digest)` and must deliver exactly `length` bytes so `resource/piece.rs` verification is
   byte-identical to TCP.

5. **Security — no kernel stack, no TLS.** RDMA bypasses the normal IP data path. The current Vortex
   TCP piece protocol contains a task id and piece number but does not authenticate the requesting
   peer or encrypt piece bytes, so the rendezvous channel cannot honestly be described as an
   existing authenticated channel. **Mitigations:** (a) use two-sided messaging so no `rkey` is
   exposed (D3); (b) bind the rendezvous listener only on the cluster interface and enforce EFA
   security-group/fabric isolation (ordinary Kubernetes NetworkPolicy may not govern OS-bypass
   traffic); (c) retain digest verification, which detects
   injection/corruption but is not authentication; (d) add a short-lived scheduler-issued transfer
   capability before claiming a stronger security model than today's TCP path; (e) if one-sided RMA
   is later enabled, MRs must be the **exact piece buffer, read-only, short-lived** — never a broad
   region with remote access.

6. **Threading model.** libfabric objects are not all thread-safe by default. **Mitigation:** open
   the domain with `FI_THREAD_SAFE`, or confine each endpoint to a single progress task and hand
   work to it via channels. Specify one model and stick to it; document it at the top of
   `client/rdma.rs`.

7. **Endpoint sharing.** EFA endpoints are heavyweight. **Mitigation:** share one RDM endpoint per
   device and multiplex transfers by unique `tag` + completion `context`; do not open an endpoint
   per piece. Tag = hash(task_id, piece_number, nonce) to prevent cross-talk.

8. **Platform gating.** RDMA is Linux-only and untestable on the maintainer's macOS. **Mitigation:**
   put it behind a cargo feature `rdma` and `#[cfg(all(target_os = "linux", feature = "rdma"))]`.
   Default build and default config are unchanged (RDMA off).

9. **Concurrent-write pieces.** Only finished pieces are served (already true of the protocol). Do
   not register/serve a piece whose content is still being written.

## 8. Config schema (`dragonfly-client-config/src/dfdaemon.rs`)

```rust
// StorageServer gains an `rdma` block (disabled by default).
pub struct StorageServer {
    pub ip: Option<IpAddr>,
    pub tcp_port: u16,
    pub tcp_fastopen: bool,
    pub quic_port: u16,

    /// RDMA piece-transfer server. Linux + libfabric only; off by default.
    #[serde(default)]
    pub rdma: RdmaServer,
}

#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RdmaServer {
    /// enable turns on the RDMA piece-transfer server.
    pub enable: bool, // default false

    /// port is the TCP port for the out-of-band RDMA rendezvous (endpoint exchange +
    /// piece metadata). Bulk bytes move over the fabric, not this port.
    #[serde(default = "default_storage_server_rdma_port")]
    pub port: u16,

    /// provider selects the libfabric provider: "auto" | "efa" | "verbs".
    #[serde(default = "default_rdma_provider")]
    pub provider: String,

    /// device optionally pins a fabric NIC (e.g. "efa0", "rdmap16s27").
    pub device: Option<String>,

    /// fabric_tag identifies the reachability domain (for EFA: VPC id + AZ id).
    /// Two peers attempt RDMA only when non-empty fabric tags match.
    pub fabric_tag: Option<String>,

    /// max_registered_bytes bounds the pinned-memory MR cache.
    #[serde(with = "bytesize_serde", default = "default_rdma_mr_cache_bytes")]
    pub max_registered_bytes: ByteSize,

    /// Maximum time for an in-flight operation before cancellation and TCP fallback.
    #[serde(default = "default_rdma_transfer_timeout", with = "humantime_serde")]
    pub transfer_timeout: Duration,
}
```

`Download.protocol` is unchanged in type; `"rdma"` becomes a valid value meaning "prefer RDMA,
fall back to TCP". No default change.

## 9. Proto change (`dragonflyoss/api`, `dfdaemon.v2.SyncPiecesResponse`)

```proto
message SyncPiecesResponse {
  uint32 number    = 1;
  uint64 offset    = 2;
  uint64 length    = 3;
  string ip        = 4;
  optional int32 tcp_port  = 5;
  optional int32 quic_port = 6;

  // --- new (assign real field numbers per the live proto) ---
  optional int32  rdma_port       = 7;  // out-of-band rendezvous port
  optional bytes  rdma_endpoint   = 8;  // libfabric fi_getname() blob (provider-opaque)
  optional string rdma_provider   = 9;  // "efa" | "verbs"
  optional string rdma_fabric_tag = 10; // reachability domain id
}
```

`rdma_endpoint` is bytes, not a structured GID/QPN, precisely so EFA and verbs share one shape (D1).

## 10. Trait / type sketches (illustrative — not final signatures)

`DownloaderFactory::new` (`resource/piece_downloader.rs`):

```rust
"rdma" => Arc::new(RDMADownloader::new(
    config.clone(),
    DEFAULT_DOWNLOADER_CAPACITY,
    DEFAULT_DOWNLOADER_IDLE_TIMEOUT,
)),
```

`RDMADownloader` implements the **unchanged** `Downloader` trait:

```rust
#[async_trait]
impl Downloader for RDMADownloader {
    async fn download_piece(
        &self, addr: &str, number: u32, host_id: &str, task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        // 1. rendezvous over TCP `addr`; 2. RDMA recv into registered buffer;
        // 3. verify digest; 4. Ok((Box::new(Cursor::new(buf)), offset, digest))
        // On any RDMA error: return Err so the caller falls back to the TCP downloader.
    }
    // download_persistent_piece / download_persistent_cache_piece: same pattern.
}
```

Provider abstraction so `efa`/`verbs` differences live in one place:

```rust
/// Fabric wraps a libfabric RDM endpoint shared across transfers.
#[async_trait]
pub trait Fabric: Send + Sync {
    /// local_endpoint is this peer's fi_getname() blob to advertise.
    fn local_endpoint(&self) -> &[u8];
    /// register pins `buf` and returns a guard; the guard must outlive the hardware completion.
    fn register(&self, buf: &mut [u8], access: MrAccess) -> Result<MrGuard>;
    /// post_recv posts a tagged receive into a registered buffer.
    async fn post_recv(&self, mr: &MrGuard, tag: u64) -> Result<RecvOp>;
    /// tsend sends a registered buffer to `peer`, completing on the send CQ.
    async fn tsend(&self, peer: &PeerAddr, mr: &MrGuard, tag: u64) -> Result<()>;
}
```

`RDMAServer` mirrors `TCPServer`:

```rust
impl RDMAServer {
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        upload_bandwidth_limiter: Arc<RateLimiter>,
    ) -> Result<Self>;
    pub async fn run(&mut self) -> Result<()>; // rendezvous accept loop + shared Fabric endpoint
}
```

Rendezvous wire messages (over the TCP rendezvous conn; can reuse Vortex `Header`/`Tag` framing):

```text
Client → Parent : DownloadPiece{ task_id, number } + RdmaReq{ client_endpoint: bytes, tag: u64 }
Parent → Client : PieceReady{ offset: u64, length: u64, digest: string,
                              parent_endpoint: bytes, tag: u64 }   |   Error{ code, message }
Client → Parent : Ready{}            // receiver has posted fi_trecv (enforces D4 rendezvous)
        ... bulk content moves over libfabric (fi_tsend/fi_trecv) ...
Parent → Client : (optional) Done{}  // or rely on the CQ completion
```

## 11. Phasing

1. **Plumbing + fallback, no perf:** config block, proto fields, `RDMADownloader`/`RDMAServer`
   scaffolding, capability advertisement, fabric-tag negotiation, TCP fallback. Prove no regression.
2. **Regular RDMA via libfabric `verbs`:** RDM tagged messaging + bounce-buffer MRs. Testable on
   RoCE / SoftRoCE without special hardware.
3. **EFA on AWS:** `efa` provider, VPC/AZ-aware negotiation; validate on real EFA
   instances.
4. **Zero-copy + one-sided:** mmap content region + MR cache; optional `fi_read` where `FI_RMA`.
5. **(Optional) GPUDirect:** `FI_HMEM`/CUDA buffers + GPU-memory piece destinations. Only if the
   goal becomes "land bytes in GPU memory" rather than "fill the local store faster".

## 12. Testing

- **CI / no hardware:** unit-test rendezvous framing, fabric-tag matching, fallback decisions, MR
  cache eviction with a mock `Fabric`. SoftRoCE (rxe) covers the `verbs` provider end-to-end in a
  Linux CI container.
- **Hardware:** a RoCE/IB testbed (phase 2) and real EFA instances (phase 3). EFA has no software
  emulator — phase 3 needs AWS.
- **Fault injection:** kill a parent mid-transfer (assert TCP fallback completes the task); evict a
  task during an in-flight MR (assert no UAF, op cancelled cleanly); exceed `max_registered_bytes`
  (assert eviction, no OOM).

## 13. Open questions

- **Q1 (scope):** host-to-host (fill local store) vs GPUDirect (NIC→GPU memory)? This RFC assumes
  host-to-host; GPUDirect changes buffer ownership and adds `FI_HMEM`.
- **Q2 (answered):** the current Vortex TCP piece path is not authenticated or encrypted. RDMA must
  at minimum preserve network isolation and per-piece digest verification; stronger peer
  authentication should be designed once and shared by both transports (§7.5).
- **Q3 (phase-1 decision):** require operators to set `fabric_tag`. Automatic EC2/Kubernetes
  discovery can follow after it is independently tested; a false positive can produce hangs.
- **Q4:** vendor a `libfabric-sys` crate or depend on an existing one? Decides the FFI maintenance
  burden (the top risk).

## 14. Value and go/no-go criteria

This feature has value when Dragonfly distributes large model artifacts concurrently across
EFA/RoCE GPU nodes and profiling shows the current TCP/ENA path or host CPU is the bottleneck. It
is unlikely to help small pieces, storage-limited nodes, cross-AZ/VPC traffic, or workloads that
already distribute tensors through NCCL/NIXL. The host-to-host phase still reads from storage,
hashes every piece, and writes the receiver's local store; it is not GPUDirect.

Do not enable RDMA by default based only on link-speed microbenchmarks. The go/no-go benchmark must
compare complete Dragonfly tasks with TCP and RDMA using the same piece size, concurrency, digest,
and storage configuration, and record:

- task completion time and aggregate goodput;
- sender/receiver CPU time and memory-copy pressure;
- p50/p95/p99 piece latency and TCP fallback rate;
- pinned/registered bytes and registration-cache hit rate;
- performance with one peer, fan-out, and a deliberate cross-fabric parent.

A reasonable rollout gate is a repeatable end-to-end improvement (for example at least 20% faster
task completion or materially lower CPU at equal throughput) with no correctness failures and a
near-zero fallback rate inside a correctly configured fabric. Otherwise the operational and unsafe
FFI complexity is not justified.
