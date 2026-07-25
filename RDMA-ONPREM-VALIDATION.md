# RDMA P2P validation on RoCE

Measured on branch `rdma-test-0723` across two on-prem GPU nodes with `hostNetwork` pods so the
containers see the physical RoCE devices. All content is served from and written to a memory
filesystem, so no result below is limited by disk. The harness and its driver scripts are committed
in `scripts/rdma-bench/`; see [Reproducing](#reproducing).

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

> **Status.** The comparison has not been run on hardware yet. The EFA nodes this was to run on are
> unschedulable for want of GPU capacity, and neither of the two hosts otherwise available carries
> an RDMA device. The harness path itself is exercised: both transports complete and verify the same
> dataset over the libfabric software provider. Numbers go here once the nodes come back.

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
- **One chunk size, one rail, warm pool only.** Only 4 MiB chunks on `rocep25s0` were swept here,
  and cold-versus-warm registration pools were not compared.
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
```

`scripts/rdma-bench/README.md` documents the scripts and the environment variables that point them
at an EFA cluster instead of a RoCE one.
