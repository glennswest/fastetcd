#!/usr/bin/env bash
# Lease keep-alive throughput (fastetcd#92), as etcd's `benchmark
# lease-keepalive`: CONNS clients (default 100), each with its own lease
# and keep-alive stream, TOTAL keep-alives in all (default 50000).
#
#   sc-build 'tests/keepalive_bench.sh v1.22.0'
#   sc-build 'MEMBERS=3 tests/keepalive_bench.sh v1.22.0'
#
# tests/read_latency.sh does the rest: this tree and the baseline ref,
# data dirs on this machine's disk, METRICS=1 for the WAL lines.
set -euo pipefail
cd "$(dirname "$0")/.."
export BENCH_ARGS="--mode keepalive --conns ${CONNS:-100} --total ${TOTAL:-50000}"
exec tests/read_latency.sh "$@"
