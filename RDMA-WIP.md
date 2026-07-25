# RDMA work in progress

Working notes for the branch that hardens the RDMA piece transport and adds a like-for-like TCP
comparison. Everything here is either done and verified, or blocked on hardware. Fold what is
useful into the other `RDMA-*.md` documents and delete this file before opening the PR.

Last updated: 2026-07-25.

## Prompt to resume

Paste this to start the next session. It assumes no prior context.

```text
Continue the RDMA piece-transport work in ~/workspace/dragonfly-client. Read RDMA-WIP.md first —
it records what was changed, what was verified, and what is still missing. RDMA-IMPLEMENTATION.md,
RDMA-ONPREM-VALIDATION.md, RDMA-P2P-RFC.md, and RDMA-OPTIMIZATION-PLAN.md are the background docs.

The code changes are already done, formatted, linted, and tested; treat them as a starting point to
validate, not to redo. The one thing missing is the measurement the work was for: RDMA has never
been compared against TCP on real hardware. Everything else is secondary to getting that number.

Start by checking whether two GPU workers can actually be scheduled:

    kubectl get pods | grep gpu-workers

Last session they sat Pending for 25+ minutes on InsufficientPhysicalGPUs for ml.p5en.48xlarge, and
the head node has no RDMA device, so there is no local substitute. If capacity is still unavailable,
say so early rather than burning the session waiting — then pick up the "Open questions for the PR"
in RDMA-WIP.md instead, which need reasoning rather than hardware.

If the workers do come up, follow the runbook in RDMA-WIP.md and:

1. Run scripts/rdma-bench/compare.sh for the dfdaemon-equivalent workload (crc32 + pwrite) and again
   with DIGEST=none SINK=null to isolate the transports. Sweep concurrency; a single stream is
   latency-bound and understates RDMA, and enough streams saturate the link on either transport.
2. Check whether the receive-side pipelining added last session actually paid off. It is the one
   change expected to move the number. Look at the fabric column of MODEL_TRANSFER_COST against the
   RoCE baseline in RDMA-ONPREM-VALIDATION.md, and expect the win to be clearest at low concurrency
   or small chunk sizes, where the per-window round trip is a larger fraction of the transfer.
3. Watch for the transfers quietly degrading to pipeline depth 1. Peak registered memory per
   transfer is now up to 4 windows (2 posted plus a depth-2 channel), which is 256MiB against the
   512MiB default budget, so two concurrent transfers can hit try_acquire_buffer returning None.
   That path is safe but invisible; if it is happening, it changes the right default.
4. Write the numbers into the "RDMA against TCP" section of RDMA-ONPREM-VALIDATION.md (stubbed with
   a Status block) and the "与 TCP 的对照" section of RDMA-IMPLEMENTATION.md, which is in Chinese to
   match the rest of that file. Then delete RDMA-WIP.md and open the PR.

Constraints:
- Do not quote a speedup number anywhere until it has been measured on hardware. The existing RoCE
  figures are RDMA-only and say nothing about how RDMA compares to TCP.
- If a measurement contradicts the reasoning behind a change, report that rather than explaining it
  away. A change that does not help should be reverted, not left behind as a tuning knob.
- Keep the branch PR-ready: cargo fmt --all --check and
  cargo clippy --workspace --all-targets --features rdma -- -D warnings must stay clean.
- dragonfly-client-backend (2 tests) and dragonfly-client-util (3 doctests) fail for pre-existing
  environmental reasons in crates this branch does not touch. Do not chase them.
```

## Where this stands

The code changes are complete, reviewed, formatted, linted, and tested. The one thing left is the
measurement the whole exercise was for: **RDMA against TCP on real hardware has not been run.** The
harness supports it and has been exercised end to end over the libfabric software provider, but the
EFA nodes could not be scheduled (see [Blocked on](#blocked-on)).

Do not quote a speedup number anywhere until that run happens. The existing RoCE figures in
`RDMA-ONPREM-VALIDATION.md` are RDMA-only and say nothing about how RDMA compares to TCP.

## What changed

### Correctness and robustness

**`fi_close` is no longer assumed to be a DMA barrier**
(`dragonfly-client-storage/src/rdma/fabric.rs`). The abort path frees buffers that were posted to a
failed endpoint, which is only safe if closing the endpoint stops the device. libfabric does not
promise that; it asks the application to complete or cancel everything first, which is exactly what
the abort path could not do. `endpoint_close_drains()` now gates the release on the provider: `efa`
and `verbs` destroy the queue pair in the kernel, and `tcp`/`udp`/`sockets`/`shm` do no device DMA.
Anything else quarantines the registrations for the life of the process. `FabricInner::drop` leaks
any non-empty pending map rather than only leaking when the endpoint is still open, and logs it.

**A flaky parent no longer costs a round trip per piece**
(`dragonfly-client/src/resource/piece_downloader.rs`). Transfer failures used to evict the discovery
cache without recording anything, so a parent whose fabric was broken but whose TCP was fine got
rediscovered and retried for every single piece; RDMA was pure overhead against such a peer. There
is now one penalty box for both failure kinds, with the kind passed in explicitly rather than
sniffed from the error variant (an unreachable parent and an incompatible one produce the same
variant, so the distinction is not recoverable after the fact):

| Failure | Penalty |
| --- | --- |
| `Incompatible` — provider or `fabricTag` mismatch | flat 60s; nothing for a backoff to discover |
| `Transport` — unreachable, busy, or a transfer that died | 2s doubling to 60s |

Success clears the entry. An expired entry is kept, carrying its accumulated backoff, so a parent
that fails every time is not reset to the shortest penalty by each retry. Four unit tests cover it.

**Configuration that cannot work is rejected at load** (`dragonfly-client-config/src/dfdaemon.rs`).
`chunkSize` must be 64KiB–1GiB, `transferTimeout` 1s–10m, and `maxRegisteredBytes` must hold at
least one window (`chunkSize * maxInflightChunks`). The last one matters most: a budget below one
window rejects every transfer at admission, so RDMA would add a rendezvous round trip to every piece
and never carry any data. Three tests.

**Admission overload answers instead of hanging up**
(`dragonfly-client-storage/src/server/rdma.rs`, `rdma/rendezvous.rs`). A parent at
`maxConcurrentTransfers` used to drop the socket, which reaches the client as a connection reset and
is indistinguishable from a broken fabric — and with the new penalty box that would have parked a
merely busy parent. It now writes `ERROR_CODE_BUSY` (5). The client maps that to a `Transport`
failure and the short backoff.

**`acquire_buffer` on the server is bounded** by `transferTimeout`. The registration budget is
shared, so this call can block behind other transfers; unbounded, the task sat there holding an
admission slot until the client's own timeout fired, quietly shrinking the server's concurrency
under budget pressure. On timeout it aborts with `ERROR_CODE_BUSY`.

### Performance

**The receive path pipelines two windows** (`dragonfly-client-storage/src/client/rdma.rs`). This is
the significant one. The parent may not send a window until its `RecvPosted` frame arrives, and the
client used to post exactly one window at a time: post, wait for every completion, hand the window
to the consumer, then post the next. Between the last completion of window N and `RecvPosted` for
N+1 there was nothing posted on the fabric, so the parent idled for a control-plane round trip on
every window — and its two-window send ring was useless, because it had the next window staged from
storage with nowhere to put it.

The client now keeps `RECEIVE_PIPELINE_DEPTH` (2) windows posted and writes both `RecvPosted` frames
back to back. The parent reads them in order and can start window N+1 as soon as N is on the wire.

Two supporting changes:

- `Fabric::try_acquire_buffer` (new, non-blocking) is used for the second window. Waiting for budget
  while already holding a registration is how concurrent transfers deadlock each other; returning
  `None` drops that transfer back to one window at a time instead. `Fabric::registered_budget_bytes`
  exposes the ceiling.
- The `Done` frame guard changed from "this is the final window" to "every window has been posted".
  With a pipeline the parent can legitimately report Done while an earlier window is still being
  drained, and nothing orders that TCP frame against the remaining local completions.

Expect this to show up in the `fabric` column of the cost breakdown, which is the only stage that
measures the transport.

### Harness and scripts

- `--transport rdma|tcp` on the `efa_cross_node` example. TCP downloads the same seeded pieces from
  the same parent through `TCPClient`, and both transports feed one shared consumer loop, so the
  difference reported is the transport and not two hand-written loops that drifted apart. TCP reads
  into a buffer recycled across blocks, sized to the RDMA window, so it is charged neither for an
  allocation per window nor for different batching.
- `--provider software` opts into the libfabric software providers for checking the harness itself.
  It is opt-in precisely so a software provider is never measured by accident.
- `scripts/rdma-bench/` now has `config.sh` (shared settings, so one set of scripts drives both RoCE
  and EFA), `compare.sh` (the sweep, alternating transports at each concurrency), and `README.md`.
  `client.sh`, `server.sh`, `deploy.sh`, and `setup.sh` were parameterized to match.

### Test quality

`rdma_transfer.rs` port allocation was racy: bind to port 0, close, hand the number to a server that
binds later, and the kernel happily gives the same port to a parallel test. Roughly 1 run in 7
failed with `AddrInUse`. Ports now come from a process-wide counter in a range the OS does not use
for ephemeral allocation. The admission test also stopped assuming the accept loop had run; it now
completes a handshake to prove the permit is held. 15 consecutive runs clean.

## Verification

| Check | Result |
| --- | --- |
| `cargo fmt --all --check` | clean |
| `cargo clippy --workspace --all-targets --features rdma -- -D warnings` | clean |
| `cargo test --workspace --features rdma` | all pass except the pre-existing failures below |
| `rdma_transfer` integration suite, 15 consecutive runs | 11/11 each time |
| Both transports end to end over the software provider | pass, digests verified |

Pre-existing failures, unrelated to this branch and in crates it does not touch — do not go chasing
them:

- `dragonfly-client-backend`, 2 tests: `Invalid cross-device link`, an artifact of `/tmp` and the
  workspace being different filesystems in this container.
- `dragonfly-client-util`, 3 doctests: `cgroups` and `sysinfo::cpu` examples that do not compile.

## Blocked on

The EFA hosts are `ml.p5en.48xlarge` Ray GPU workers, and they could not be scheduled:

```
Warning  InsufficientPhysicalGPUs  Pod is pending for 8 ml.p5en.48xlarge tier 2 GPUs
                                   due to insufficient physical GPU availability.
```

Two workers sat `Pending` for over 25 minutes. The previous session's pods (`fk9b6`, `ntwn4`) had
already been reclaimed by the autoscaler. The head node has no RDMA device — no `/sys/class/infiniband`,
no `fi_info` — so there is no local substitute.

## Runbook for next session

1. **Get two GPU workers and keep them.** The autoscaler reclaims idle workers after ~30 minutes,
   which is what lost the previous session's pods.

   ```bash
   cd ~/workspace && nohup python hold_gpu_workers.py > /tmp/hold_gpu.log 2>&1 &
   kubectl get pods | grep gpu-workers          # wait for 2 Running
   ```

2. **Build.** The head node needs these or the build and the run fail in confusing ways:

   ```bash
   export PATH="$HOME/.cargo/bin:$PATH"
   export LIBCLANG_PATH=/usr/lib/llvm-18/lib    # librocksdb-sys bindgen
   export LD_LIBRARY_PATH=/opt/amazon/efa/lib   # AWS libefa, not the system one
   cd ~/workspace/dragonfly-client
   cargo build --release --features rdma -p dragonfly-client-storage --example efa_cross_node
   ```

3. **Provision and seed.** EFA skips `setup.sh` — the node image carries AWS's libfabric and a pod
   that installs its own will not drive the device, so run inside the GPU pods.

   ```bash
   cd scripts/rdma-bench
   export PROVIDER=efa DEVICE=rdmap85s0-rdm FABRIC_TAG=efa-test-az
   export SRV_POD=<worker-1> CLI_POD=<worker-2>
   ./deploy.sh
   # /mnt/memfs is a symlink to /dev/shm in these pods; ~48 x 512MiB parts is enough
   # to sweep concurrency past 3 streams. See provision-rdma-bench.sh in ~/workspace.
   FILES_DIR=/mnt/memfs/manyfiles ./server.sh    # prints TASK_ID and MANIFEST_PIECE
   ```

4. **Run the comparison.**

   ```bash
   export TASK_ID=<...> MANIFEST_PIECE=<...>
   ./compare.sh                          # dfdaemon-equivalent: crc32 + pwrite
   DIGEST=none SINK=null ./compare.sh    # transports only
   ```

5. **Also confirm the pipelining paid off**, since it is the one change expected to move the number.
   Compare the `fabric` column of `MODEL_TRANSFER_COST` against the RoCE baseline in
   `RDMA-ONPREM-VALIDATION.md` (fabric was 18–38ms of a ~4s transfer there, so on that workload the
   headroom is small; the win should be clearer at low concurrency or small chunk sizes, where the
   per-window round trip is a larger fraction).

6. **Write the numbers into** the `RDMA against TCP` section of `RDMA-ONPREM-VALIDATION.md`, which
   is stubbed with a `Status` block, and the `与 TCP 的对照` section of `RDMA-IMPLEMENTATION.md`.
   Then delete this file.

### Gotchas worth remembering

- `[profile.release]` sets `panic = "abort"`, so a panicking release binary dumps a 37MB core in the
  repo root. Two of those turned up mid-session and looked alarming; they were just config panics.
- `pkill -f <pattern>` over `kubectl exec` matches the shell running the `pkill`. Kill in a separate
  exec, and start servers with `setsid nohup ... < /dev/null`.
- `fi_info -p efa -e rdm` returns nothing useful. Use `fi_info -p efa -c FI_TAGGED`; the shim filters
  `efa-direct` itself because it is MTU-limited and not a drop-in for multi-MiB pieces.
- A binary built against a different libfabric than the pod carries fails at the first transfer, not
  at startup. `deploy.sh` now runs `ldd` to catch it early.

## Open questions for the PR

- `RECEIVE_PIPELINE_DEPTH` is a constant at 2, matching the parent's send ring. Whether it should be
  configurable depends on whether the measurement shows depth mattering; leaving it fixed avoids a
  knob nobody knows how to set.
- Peak registered memory per transfer is now up to 4 windows: 2 posted plus the depth-2 window
  channel. At the defaults that is 256MiB against a 512MiB budget, so two concurrent transfers will
  hit `try_acquire_buffer` returning `None` and quietly run at depth 1. That degradation is safe but
  invisible; if the measurement shows it happening, either the default budget should rise or the
  channel should shrink to 1.
- Config validation requires only one window of budget, not two. Requiring two would make the
  pipeline reliable but would reject configurations that work today.
