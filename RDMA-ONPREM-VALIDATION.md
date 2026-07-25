# RDMA P2P validation on RoCE

Measured on two on-prem GPU nodes (`chi3-en11-3-s1`, `chi3-en11-13-s1`) with `hostNetwork` pods so
the containers see the physical RoCE devices. All content is served from and written to a memory
filesystem, so no result below is limited by disk.

## Environment

| | |
|---|---|
| Fabric | 4 × Mellanox RoCE (`rocep25s0`, `rocep41s0`, `rocep59s0`, `rocep83s0`), Ethernet link layer |
| libfabric provider | `verbs;ofi_rxm` |
| Fabric baseline | `ib_write_bw` 390 Gbps on one device |
| TCP baseline | `iperf3` 314 Gbps on the management network |
| Filesystem | `tmpfs`, 220 GiB, mounted at `/mnt/memfs` in both pods |
| Workload | `meta-llama/Llama-2-13b-chat-hf` file layout: 10 files, 26,034,233,427 bytes, of which 3 safetensors shards carry 99.99% |

The real weights are not reachable: the only S3 credentials in the cluster (secret `aws-s3` in
`kubeflow-p13n`) are rejected with `InvalidClientTokenId`, so the dataset reproduces the upstream
file names and exact byte sizes with random content. RDMA goodput does not depend on the byte
values, and every transfer below is verified against the SHA-256 digests computed at seed time.

## The measurement was wrong before

`examples/efa_cross_node.rs` built the server's upload limiter with a hard-coded 1 GiB/s bucket:

```rust
RateLimiter::builder().initial(1024 * 1024 * 1024).refill(1024 * 1024 * 1024)
```

1 GiB/s is 8.6 Gbps. Every earlier "RDMA is barely faster than TCP" number (8–12 Gbps) was the
leaky bucket draining, not the fabric. Production `dfdaemon` defaults to 50 GB/s
(`default_upload_bandwidth_limit`), so the cap existed only in the harness. The harness now builds
the limiter from `config.upload.bandwidth_limit` exactly like `dfdaemon/main.rs` does, and prints it
at startup so the mistake cannot recur silently.

## Where the time goes

The harness attributes every byte to one of three stages (`--digest`, `--sink`), which is how each
optimization below was chosen rather than guessed:

| Configuration | Aggregate goodput | Per-shard |
|---|---|---|
| Pure transport (`--digest none --sink null`) | 158 Gbps | ~53 Gbps |
| CRC32 + tmpfs (what `dfdaemon` does) | 52.7 Gbps | ~20 Gbps |
| SHA-256 verified + tmpfs | 38.0 Gbps | ~14 Gbps |

The fabric accounts for 23–38 ms of a 4–5 s transfer. It is never the constraint. Receive-side CPU
is, and because a model repository holds only a handful of large files, per-stream throughput
matters more than aggregate.

## Optimizations applied

Each was measured on the full 26 GB workload at CRC32 (the digest `dfdaemon` actually computes):

| Change | Goodput |
|---|---|
| Baseline after the limiter fix | 34.5 Gbps |
| Write registered windows straight into content storage, no `AsyncRead` bounce buffer | 41.8 Gbps |
| One `pwrite` per window instead of `tokio::fs::File` (which copies into its own buffer first) | 41.8 → 44.5 Gbps |
| Run the digest and the write on separate blocking threads instead of in series | **52.7 Gbps** |

SHA-256-verified goodput improved over the same sequence from 20.1 to 38.0 Gbps, and every file
digest matched on every run.

### Receive path

`RDMAStreamReader::next_window` hands out `ReceivedWindow`s that borrow the registered receive ring
directly, so `Content::write_piece_from_rdma_stream` hashes and writes out of the memory the NIC
DMA-ed into. `AsyncRead` is still implemented and still used for small reads; the two APIs share a
cursor, so a window that `poll_read` half-drained yields only its remaining bytes (covered by
`next_window_returns_only_the_bytes_async_read_left_behind`).

The write is bounded to the piece length. `write_piece` got this for free from
`AsyncReadExt::take`; the window path has to check it explicitly, or a parent that streams too much
would overwrite the following pieces in the task file before the trailing length check noticed
(covered by `test_write_piece_from_rdma_stream_rejects_overlong_stream`).

## What was tried and rejected

**Sharding one window across several `pwrite` threads.** The write stage runs at about one core's
copy bandwidth, so splitting each window across threads at disjoint offsets looked free. It is not:
every piece of a task writes into the same task file, and the extra writers contend on the inode
lock. Measured at CRC32 on the full workload, goodput *fell* as shards were added.

| Write shards | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| Goodput | **53.5 Gbps** | 48.9 Gbps | 46.2 Gbps | 46.1 Gbps |

## What is left

1. **The receive-side copy into the filesystem is the only per-stream bottleneck**, at 21 Gbps
   against a 53 Gbps per-stream transport. Getting past it means registering the destination file's
   mapping so the NIC lands bytes in the page cache. This is deliberately *not* done here: these
   nodes also run training, and file-mapping registration pins page-cache pages and consumes NIC
   memory-region entries shared with NCCL on the same device. On NVMe-backed content it can also be
   slower, because pinning a file mapping faults the pages in and so reads blocks that are about to
   be overwritten in full. The common form of this pattern (UCX/NIXL, GPUDirect) registers GPU or
   anonymous buffers behind a registration cache, not page-cache-backed file mappings.
2. **Send side** still copies from the seeded `mmap` into the staging ring, which is what caps the
   per-stream transport at ~53 Gbps. Registering the source mapping carries the same shared-device
   cost as above.
3. **One rail of four.** Every measurement uses a single device and the nodes have four active RoCE
   NICs, but the fabric accounts for under 1% of transfer time, so more rails would not help the
   model download at all.
4. `MAX_CHUNKS` is 4096, so a piece cannot exceed `chunk_size × 4096` (16 GiB at the default 4 MiB).
   Comfortable for `dfdaemon` pieces, but worth knowing before raising piece sizes.

## Rebuilding the rig

`setup.sh` recreates both `hostNetwork` pods from scratch (privileged with `IPC_LOCK` so registered
memory can be pinned, `libfabric` plus `ibverbs-providers` from the distro, and a 220 GiB tmpfs).
The pods carry no state, so re-running it is the fastest way to recover from a lost pod.

## Reproducing

```bash
cd ~/workspace/rdma-bench
./deploy.sh                                        # push the built harness into both pods
kubectl exec rdma-df-srv -- /bench/gen-llama-13b.sh /mnt/memfs/llama-2-13b-chat-hf
FILES_DIR=/mnt/memfs/llama-2-13b-chat-hf DATA_DIR=/mnt/memfs/df-data ./server.sh

export TASK_ID=<task_id from server.sh> MANIFEST_PIECE=10
DIGEST=sha256 SINK=pwrite CONCURRENCY=10 ./client.sh   # verified correctness
DIGEST=crc32  SINK=pwrite CONCURRENCY=10 ./client.sh   # dfdaemon-equivalent goodput
DIGEST=none   SINK=null   CONCURRENCY=10 ./client.sh   # transport ceiling
```
