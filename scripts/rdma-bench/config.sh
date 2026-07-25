#!/usr/bin/env bash
# Shared settings for the RDMA bench scripts. Everything is overridable from the environment, so
# the same scripts drive a RoCE cluster and an AWS EFA cluster.
#
# RoCE (the defaults):
#   PROVIDER=verbs DEVICE=rocep25s0
#
# AWS EFA:
#   PROVIDER=efa DEVICE=rdmap85s0-rdm SRV_POD=<pod> CLI_POD=<pod>
#
# The pods must be on two different hosts on the same fabric, and must run with hostNetwork so the
# RDMA devices are visible, and with IPC_LOCK so registered memory can be pinned.

PROVIDER=${PROVIDER:-verbs}
DEVICE=${DEVICE:-rocep25s0}
FABRIC_TAG=${FABRIC_TAG:-rdma-bench}

SRV_POD=${SRV_POD:-rdma-df-srv}
CLI_POD=${CLI_POD:-rdma-df-cli}

TCP_PORT=${TCP_PORT:-4001}
RDMA_PORT=${RDMA_PORT:-4007}

CHUNK_MIB=${CHUNK_MIB:-4}
MAX_INFLIGHT=${MAX_INFLIGHT:-16}

# Registered-memory budget. Well above the dfdaemon default of 512 MiB so that a run measures the
# transport rather than the budget; set it to 512 to measure what the default does to the receive
# pipeline, which needs 4 windows (2 posted plus a depth-2 channel) per concurrent transfer.
MAX_REGISTERED_MIB=${MAX_REGISTERED_MIB:-65536}

BENCH_DIR=${BENCH_DIR:-/bench}
MEMFS=${MEMFS:-/mnt/memfs}
