# RDMA P2P validation

Measured on branch `rdma-test-0723`. The optimization work and its cost attribution were done across
two on-prem GPU nodes with `hostNetwork` pods so the containers see the physical RoCE devices; the
comparison against TCP in [RDMA against TCP](#rdma-against-tcp) was measured separately on two AWS
EFA nodes, which is the only place EFA numbers appear. All content is served from and written to a
memory filesystem, so no result below is limited by disk. The harness and its driver scripts are
committed in `scripts/rdma-bench/`; see [Reproducing](#reproducing).

## Environment

| | |
|---|---|
| Nodes | `chi3-en11-13-s1` (server), `chi3-en11-3-s1` (client) |
| Fabric | 4 × Mellanox RoCE (`rocep25s0`, `rocep41s0`, `rocep59s0`, `rocep83s0`), Ethernet link layer |
| libfabric provider | `verbs;ofi_rxm`, single rail (`rocep25s0`) |
| Fabric baseline | `ib_write_bw` 390 Gbps on one device |
| Network baseline | `iperf3` 314 Gbps on the management network |
| Filesystem | `tmpfs`, 220 GiB, mounted at `/mnt/memfs` in both pods |
| Transfer settings | 4 MiB chunks, 16 chunks in flight (64 MiB receive windows) |
| Workload | `meta-llama/Llama-2-13b-chat-hf` file layout: 10 files, 26,034,233,427 bytes, of which 3 safetensors shards carry 99.99% |

The real weights are not reachable: the only S3 credentials in the cluster (secret `aws-s3` in
`kubeflow-p13n`) are rejected with `InvalidClientTokenId`, so `gen-llama-13b.sh` reproduces the
upstream file names and exact byte sizes with random content. RDMA goodput does not depend on the
byte values, and every reported transfer was verified against SHA-256 digests computed at seed time.

## The measurement was wrong before

`examples/efa_cross_node.rs` built the server's upload limiter with a hard-coded 1 GiB/s bucket:

```rust
RateLimiter::builder().initial(1024 * 1024 * 1024).refill(1024 * 1024 * 1024)
```

1 GiB/s is 8.6 Gbps. Every earlier "RDMA is barely faster than TCP" number (8–12 Gbps) was the leaky
bucket draining, not the fabric. Production `dfdaemon` defaults to 50 GB/s
(`default_upload_bandwidth_limit`), so the cap existed only in the harness. The harness now builds
the limiter from `config.upload.bandwidth_limit` exactly like `dfdaemon/main.rs` does, and prints it
at startup so the mistake cannot recur silently.

## Where the time goes

The harness attributes every byte to the fabric, the digest, or the filesystem, selected with
`--digest {sha256,crc32,none}` and `--sink {pwrite,tokio,null}`. Each optimization below was chosen
from this attribution rather than guessed. Ranges are the spread over repeated runs.

| Configuration | Aggregate | Per stream |
|---|---|---|
| Pure transport (`--digest none --sink null`) | 147–158 Gbps | ~53 Gbps |
| CRC32 + tmpfs (what `dfdaemon` does) | 51–54 Gbps | ~20 Gbps |
| SHA-256 verified + tmpfs | 37.7–38.1 Gbps | ~14 Gbps |

The fabric accounts for 18–38 ms of a ~4 s transfer, under 1%. It is never the constraint;
receive-side CPU is.

Goodput scales almost linearly with the number of concurrent pieces, which is the direct evidence
that the per-stream path rather than the fabric or the network is the limit:

| Concurrent pieces | 1 | 2 | 3 | 10 |
|---|---|---|---|---|
| CRC32 + tmpfs | 20.5 Gbps | 32.9 Gbps | 52.6 Gbps | 51.2–53.6 Gbps |

Concurrency 3 and 10 agree because only 3 files are large enough to matter, so 3 is the effective
stream count for this workload.

## Optimizations applied

Each was measured on the full 26 GB workload at CRC32, the digest `dfdaemon` actually computes:

| Change | Goodput |
|---|---|
| Baseline after the limiter fix | 34.5 Gbps |
| Write registered windows into content storage, no `AsyncRead` bounce buffer | 41.8 Gbps |
| One `pwrite` per window instead of `tokio::fs::File`, which copies into its own buffer first | 44.5 Gbps |
| Run the digest and the write on separate blocking threads instead of in series | **51–54 Gbps** |

SHA-256-verified goodput improved over the same sequence from 20.1 to 38 Gbps, and every file digest
matched on every run.

### Receive path

`RDMAStreamReader::next_window` hands out `ReceivedWindow`s that borrow the registered receive ring
directly, so `Content::write_piece_from_rdma_stream` hashes and writes out of the memory the NIC
DMA-ed into. `AsyncRead` is still implemented and still used for small reads; the two APIs share a
cursor, so a window that `poll_read` half-drained yields only its remaining bytes (covered by
`next_window_returns_only_the_bytes_async_read_left_behind`).

The write is bounded to the piece length. `write_piece` gets this for free from
`AsyncReadExt::take`; the window path has to check it explicitly, or a parent that streams too much
would overwrite the following pieces in the task file before the trailing length check noticed
(covered by `test_write_piece_from_rdma_stream_rejects_overlong_stream`).

### TCP fallback contract

`Piece::download_piece_from_parent_over_rdma` owns the whole RDMA attempt, so `download_from_parent`
either returns a finished piece or falls through to the TCP piece server. A rendezvous or fabric
failure happens before any write. A write failure can leave a partial piece, so the piece metadata
is marked failed and restarted before the error propagates, which lets the TCP retry rewrite the
range from the start. This preserves the plan's constraint that TCP fallback stays available until
an RDMA transfer has completed successfully.

## RDMA against TCP

The earlier version of this document compared RDMA against a 314 Gbps `iperf3` number, which was
not a fair baseline: raw sockets with no piece protocol and no filesystem is not what RDMA replaces.
What RDMA replaces is the TCP piece server, so the harness now drives both.

`--transport tcp` downloads the same seeded pieces from the same parent over the Vortex piece
server, and hands the received bytes to the same digest and the same sink, batched into the same
sized blocks as an RDMA receive window. The only difference left is how bytes arrive: a tagged
fabric message written by the NIC into registered memory, or a socket read into a buffer. Run it
with `scripts/rdma-bench/compare.sh`, which alternates the two transports at each concurrency so
that a drifting machine shows up as noise in both columns rather than as a win for whichever ran
first.

Two things decide what the comparison can show, and both are properties of the workload rather than
of the transport:

- **Concurrency.** One stream is latency-bound and understates RDMA. Enough streams saturate the
  link on either transport and understate it again. The gap is widest in between.
- **What touches the bytes.** At `DIGEST=crc32 SINK=pwrite` the receiver spends most of its time in
  the digest and the write, which are identical on both transports, so the end-to-end ratio is much
  smaller than the transport ratio. `DIGEST=none SINK=null` isolates the transports; the honest
  summary quotes both.

### Measured on EFA

Run on two `p6-b200.48xlarge` Ray GPU pods, using one rail (`rdmap79s0-rdm`) of the eight EFA devices
each node carries, against AWS libfabric 1.30.0. Unlike the RoCE hosts above, these pods are not
`hostNetwork`: the EFA devices come from the device plugin, and TCP goes over the pod veth through
the VPC CNI. Content is served from and written to `/dev/shm`. The workload is 48 × 512 MiB files
(24 GiB, one piece per file) at 4 MiB chunks and 16 chunks in flight, so 64 MiB windows; each point
is the best of three runs, with the transports alternating at every concurrency.

A single TCP flow, not the path as a whole, is what the TCP column is up against. Raw multi-stream
sockets between the same two pods, with no piece protocol and no filesystem, measure:

| Streams | 1 | 4 | 16 | 32 |
|---|---|---|---|---|
| Raw TCP | 4.96 Gbps | 19.83 Gbps | 74.11 Gbps | 135.57 Gbps |

That is the per-flow ceiling on this network, about 5 Gbps, and the path scales fine once there are
more flows. The piece transports inherit it directly: the TCP piece server uses one connection per
concurrent piece, so its column scales with streams, while RDMA does not have to.

**Transports only** (`DIGEST=none SINK=null`), nothing touching the bytes:

| Streams | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|---|
| RDMA | 46.98 | 84.58 | 139.51 | 218.51 | 279.63 | 279.44 |
| TCP | 3.96 | 7.95 | 14.64 | 27.89 | 49.56 | 70.59 |
| Speedup | 11.9× | 10.6× | 9.5× | 7.8× | 5.6× | 4.0× |

**What `dfdaemon` actually does** (`DIGEST=crc32 SINK=pwrite`):

| Streams | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|---|
| RDMA | 21.48 | 36.41 | 69.65 | 116.01 | 143.27 | 125.08 |
| TCP | 3.30 | 6.64 | 12.58 | 23.41 | 45.10 | 63.53 |
| Speedup | 6.5× | 5.5× | 5.5× | 5.0× | 3.2× | 2.0× |

The honest summary is a range rather than a number. On the workload `dfdaemon` runs, RDMA is worth
about 6× at low stream counts and about 2× by 32 streams; measuring only the transport moves that to
4–12×. The gap closes from both ends: TCP keeps scaling with streams, while RDMA saturates at about
280 Gbps on one rail transport-only and about 143 Gbps once CRC32 and the write are in the path.
Beyond 16 streams the CRC32 column falls back — 125 Gbps at 32 — so receive-side CPU, not the
fabric, sets the ceiling there. This is the same conclusion the RoCE numbers reached, and it holds
at four times the aggregate goodput.

Per stream the two fabrics agree closely, which is the useful cross-check: 21.5 Gbps CRC32 per
stream on EFA against ~20.5 Gbps on RoCE, and 47 Gbps transport-only against ~53 Gbps.

### The receive pipeline does not pay off, and the registration budget does not bite

Both were open questions when the pipeline landed. Neither survives measurement.

`RECEIVE_PIPELINE_DEPTH` was expected to be the change that moved the number, by removing the
control-plane round trip the parent used to idle through between windows. A/B at depth 1 against
depth 2, transport-only, best of two runs each:

| Chunk | Streams | Depth 1 | Depth 2 |
|---|---|---|---|
| 4 MiB | 1 | 44.5–44.8 Gbps | 43.3–44.9 Gbps |
| 4 MiB | 4 | 128.6–135.2 Gbps | 128.0–131.0 Gbps |
| 1 MiB | 1 | 45.4–46.4 Gbps | 47.4–47.8 Gbps |
| 1 MiB | 4 | 169.0–177.3 Gbps | 166.8–173.1 Gbps |

The differences are inside run-to-run spread and do not point one way, including at the small chunk
size where the per-window round trip is a larger fraction of the transfer and the win was supposed
to be clearest. The reason is visible in the cost breakdown: per-stream fabric throughput sits at
50–54 Gbps in every configuration, depth and chunk size included, which is the send-side staging
copy in [What is left](#what-is-left) and not a control-plane stall. Removing a round trip the
sender was not waiting on changes nothing. Note that the RoCE measurements above recorded the same
~53 Gbps per stream *before* the pipeline existed.

The registration budget is a non-issue for the same reason. Peak demand is 4 windows per transfer
(2 posted plus a depth-2 channel), so at the 512 MiB default two concurrent transfers exhaust the
budget and `try_acquire_buffer` starts returning `None`, dropping transfers to one window at a time.
It costs nothing measurable:

| Streams | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|---|
| 512 MiB budget | 44.33 | 74.69 | 134.57 | 216.00 | 295.60 | 283.93 |
| 64 GiB budget | 42.70 | 78.45 | 134.37 | 211.39 | 296.71 | 270.82 |

At 16 and 32 streams the default cannot even give every transfer one 64 MiB window, and goodput
still matches. So the default should not be raised on this evidence, and depth should not become a
knob: there is no measured setting for it to be tuned to.

The pipeline is kept anyway, deliberately and on the record here rather than as a silent default: it
costs nothing measurable, the memory it can ask for is bounded by a budget that provably does not
hurt when exhausted, and the round trip it removes is real and will start to matter as soon as the
send-side copy stops setting the per-stream ceiling. What is *not* claimed is that it helped. If the
send side is ever fixed, this is the first thing to re-measure; if it still shows nothing, remove it.

One thing did move the number, and it was not being looked for: at 4 streams, 1 MiB chunks reach
169–177 Gbps against 129–131 Gbps for 4 MiB, roughly 30% better on the same code. Chunk size is
already configurable, so this is a defaults question rather than a code change, and it wants its own
sweep across chunk size and concurrency before anything is changed.

## What was tried and rejected

**Sharding one window across several `pwrite` threads.** The write stage runs at about one core's
copy bandwidth, so splitting each window across threads at disjoint offsets looked free. It is not:
every piece of a task writes into the same task file, and the extra writers contend on the inode
lock. Measured at CRC32 on the full workload, goodput *fell* as shards were added, so the code was
reverted rather than left behind a tuning knob whose best value is "off".

| Write shards | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| Goodput | **53.5 Gbps** | 48.9 Gbps | 46.2 Gbps | 46.1 Gbps |

## Limits of this evidence

Read the numbers above with these gaps in mind:

- **One piece per file.** The harness maps each file to a single piece, whereas `dfdaemon` splits a
  file into many pieces and downloads them concurrently. Real `dfdaemon` therefore gets parallelism
  within one large file that this harness does not, so its aggregate on 3 shards should exceed the
  3-stream numbers here. The per-stream figures still apply per piece.
- **Sender side is not instrumented.** The cost breakdown covers the receiver only; source storage
  read and registration/pool wait on the server are not attributed.
- **Fabric health is only pass/fail.** Pinned bytes, pool hit/miss, posted-operation high-water
  mark, `FI_EAGAIN` counts, CQ errors, and CPU are not recorded; only `fabric_failed`.
- **One rail, warm pool only.** Cold-versus-warm registration pools were not compared, and every
  number is a single rail: `rocep25s0` on RoCE, `rdmap79s0-rdm` of eight on EFA. Chunk size was
  swept only on EFA, only at 1 and 4 MiB, and only transport-only, where 1 MiB won by ~30% at 4
  streams; that result is not characterized further.
- **The EFA pods are not `hostNetwork`,** so the TCP column there crosses the VPC CNI rather than
  the host interface. The raw-socket baseline is reported next to it for that reason.
- **Content is synthetic**, for the credential reason above.

## What is left

1. **The receive-side copy into the filesystem is the only per-stream bottleneck**, at ~20 Gbps
   against a ~53 Gbps per-stream transport. Getting past it means registering the destination file's
   mapping so the NIC lands bytes in the page cache. This is deliberately *not* done: these nodes
   also run training, and file-mapping registration pins page-cache pages and consumes NIC
   memory-region entries shared with NCCL on the same device. On NVMe-backed content it can also be
   slower, because pinning a file mapping faults the pages in and so reads blocks that are about to
   be overwritten in full, and durability moves from `write`/`fsync` to `msync`. The common form of
   this pattern (UCX/NIXL, GPUDirect) registers GPU or anonymous buffers behind a registration
   cache, not page-cache-backed file mappings.
2. **Send side** still copies from the seeded `mmap` into the staging ring, which is what caps the
   per-stream transport at ~53 Gbps. Registering the source mapping carries the same shared-device
   cost as above.
3. **One rail of four.** The fabric accounts for under 1% of transfer time, so more rails would not
   help this workload at all until the receive path is much faster.
4. `MAX_CHUNKS` is 4096, so a piece cannot exceed `chunk_size × 4096` (16 GiB at the default 4 MiB).
   Comfortable for `dfdaemon` pieces, but worth knowing before raising piece sizes.

## Reproducing

Requires `kubectl` against the cluster and a Rust toolchain with `libfabric` development headers.

```bash
cd scripts/rdma-bench

# Recreate both hostNetwork pods and install libfabric, ibverbs providers, and a 220 GiB tmpfs.
# SRV_NODE and CLI_NODE default to the nodes used above and must be set for any other cluster.
./setup.sh

# Build the harness with the rdma feature and push it, plus the dataset generator, into both pods.
./deploy.sh

# Materialize the Llama-2-13b-chat-hf file layout, then seed it and start the piece server.
kubectl exec rdma-df-srv -- /bench/gen-llama-13b.sh /mnt/memfs/llama-2-13b-chat-hf
./server.sh

# Use the task_id and manifest_piece that server.sh printed.
export TASK_ID=<task_id> MANIFEST_PIECE=<manifest_piece>
DIGEST=sha256 ./client.sh              # verify every file against the seeded digests
DIGEST=crc32 ./client.sh               # dfdaemon-equivalent goodput
DIGEST=none SINK=null ./client.sh      # transport ceiling
SINK=tokio DIGEST=crc32 ./client.sh    # the tokio::fs write path, for the A/B above
CONCURRENCY=1 DIGEST=crc32 ./client.sh # per-stream goodput
TRANSPORT=tcp ./client.sh              # the same workload over the TCP piece server

# RDMA against TCP across a concurrency sweep.
./compare.sh                           # dfdaemon-equivalent, both transports
DIGEST=none SINK=null ./compare.sh     # transports only, nothing touching the bytes

# What the default registration budget does to the receive pipeline. 65536 (the harness default) is
# high enough not to interfere; 512 is the dfdaemon default.
MAX_REGISTERED_MIB=512 DIGEST=none SINK=null ./client.sh
```

The EFA numbers above skip `setup.sh` and run in two existing GPU pods, which already carry AWS's
libfabric and the devices. `/mnt/memfs` is a symlink to `/dev/shm` there, and the dataset is 48 ×
512 MiB files rather than the Llama layout, so that concurrency can be swept past three streams:

```bash
export PROVIDER=efa DEVICE=rdmap79s0-rdm FABRIC_TAG=efa-test-az
export SRV_POD=<gpu-worker-1> CLI_POD=<gpu-worker-2>
export CONCURRENCIES="1 2 4 8 16 32"
./deploy.sh
FILES_DIR=/mnt/memfs/manyfiles ./server.sh
```

Building on a node without the EFA installer needs libfabric and a `libefa` new enough for it:
AWS libfabric 1.30 wants `efadv_query_qp_wqs@EFA_1.4`, which Ubuntu's `libefa1` does not export, so
copy `/opt/amazon/efa` and `libefa.so*` out of a GPU pod and point `LIBFABRIC_INCLUDE_DIR` and
`LIBFABRIC_LIB_DIR` at them. `deploy.sh` runs `ldd` in the pod to catch a mismatch before it turns
into a first-transfer failure.

`scripts/rdma-bench/README.md` documents the scripts and the environment variables that point them
at an EFA cluster instead of a RoCE one.
