# RDMA bench harness

Drives the `efa_cross_node` example across two hosts to measure what the RDMA piece transport
achieves end to end, and to compare it against the TCP piece server it falls back to.

The harness seeds a directory of files into Dragonfly storage on the parent, one file per piece,
then downloads every piece from a second host and verifies it. That is the same code path
`dfdaemon` uses for peer-to-peer piece transfer, without the scheduler in the way.

## Layout

| Script | Purpose |
| --- | --- |
| `config.sh` | Shared settings. Everything below reads it; override from the environment. |
| `setup.sh` | Creates two `hostNetwork` pods with libfabric and a tmpfs. RoCE only. |
| `deploy.sh` | Builds the harness and copies it into both pods. |
| `gen-llama-13b.sh` | Materializes a Llama-2-13B-shaped dataset to transfer. |
| `server.sh` | Seeds the dataset and starts the parent. Prints the task id to use. |
| `client.sh` | Runs one transfer and prints goodput with a per-stage cost breakdown. |
| `compare.sh` | Runs RDMA and TCP over the same workload and prints them side by side. |

## Running it

```bash
export PROVIDER=verbs DEVICE=rocep25s0          # RoCE
export PROVIDER=efa DEVICE=rdmap85s0-rdm        # or AWS EFA

./setup.sh                                      # RoCE only, see below
./deploy.sh
kubectl exec "$SRV_POD" -- /bench/gen-llama-13b.sh /mnt/memfs/llama-2-13b-chat-hf
./server.sh                                     # prints task_id and manifest_piece

export TASK_ID=... MANIFEST_PIECE=...
./client.sh                                     # one RDMA run
./compare.sh                                    # RDMA against TCP
```

On EFA, skip `setup.sh`. The node image already carries AWS's libfabric, and a pod that installs
its own will not drive the device; run the harness inside the existing GPU pods instead and point
`SRV_POD` and `CLI_POD` at them.

## Reading the output

`client.sh` prints an aggregate line and a cost breakdown:

```
MODEL_TRANSFER_DONE transport=rdma files=48 bytes=25769803776 elapsed=... aggregate_goodput=... Gbps
MODEL_TRANSFER_COST cost[fabric=... digest=... write=...]
```

The three costs are the stages that touch every byte, summed across concurrent transfers. They
overlap, so with concurrency above 1 they add up to more than the wall clock; what matters is which
one is largest. `fabric` is time the consumer spent waiting for bytes to arrive, so it is the only
one that measures the transport. A run where `digest` or `write` dominates is measuring the CPU or
the filesystem, and says nothing about RDMA. Use `DIGEST=none SINK=null` to see the transport
ceiling, and `DIGEST=crc32 SINK=pwrite` for what `dfdaemon` actually does.

`MAX_REGISTERED_MIB` defaults to 64 GiB so that the registration budget cannot quietly cap a run.
The receive pipeline wants 4 windows per concurrent transfer (2 posted plus a depth-2 channel), so
at `dfdaemon`'s 512 MiB default two concurrent transfers exhaust it and fall back to one window at a
time. Set `MAX_REGISTERED_MIB=512` to measure that rather than infer it.

## Gotchas

- `fi_info -p efa -e rdm` returns nothing useful. Use `fi_info -p efa -c FI_TAGGED`. The shim
  filters `efa-direct` itself, because it is MTU-limited and not a drop-in for multi-MiB pieces.
- A binary built against a different libfabric than the pod carries fails at the first transfer
  rather than at startup, which is a confusing way to find out. `deploy.sh` runs `ldd` to catch it.
- `pkill -f <pattern>` over `kubectl exec` also matches the shell running the `pkill`. Kill in a
  separate `exec`, and start servers with `setsid nohup ... < /dev/null`.
- `[profile.release]` sets `panic = "abort"`, so a panicking release binary drops a ~37 MB core in
  the repo root. Those are usually config panics, not fabric failures.
- On EFA nodes the tests and the harness need `LD_LIBRARY_PATH=/opt/amazon/efa/lib`; without it a
  debug test binary fails to start with `libfabric.so.1: cannot open shared object file`.

## Comparing fairly

`compare.sh` exists because a transport number on its own is not an argument for the transport.
Both sides read the same seeded pieces from the same parent, hand the same sized blocks to the same
digest, and write through the same sink. The only difference is whether bytes arrive by tagged
fabric message into registered memory or by socket read into a buffer.

Two things to keep in mind when reading the result:

- Concurrency matters more than any single number. A single stream is latency-bound and
  understates RDMA; enough streams saturate the link on either transport and understate it again.
  The interesting region is in between, which is why the script sweeps.
- The dataset must be larger than what the parent can serve from page cache, or the comparison
  turns into a memory bandwidth test that both transports pass.
