#!/usr/bin/env bash
# Read latency under write load (fastetcd#71), on this machine's disk.
#
#   tests/read_latency.sh [baseline-ref]
#
# Builds this tree (release) and, if given, a baseline ref in a git
# worktree, then runs each as a single member with its data dir under
# tmp/ (a real disk, not tmpfs) and drives it with
# `fastetcd-bench --mode read-under-load`: 40 clients looping GET + CAS
# Txn, a prefix Range every fifth loop, and a sequential probe of 200
# linearizable then 200 serializable Ranges of one key. Run it through
# sc-build: `sc-build 'tests/read_latency.sh v1.8.0'`.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/read-latency
rm -rf "$WORK"
mkdir -p "$WORK"
echo "data volume: $(df -hT "$WORK" | tail -1)"

cargo build --release --locked -p fastetcd-server -p fastetcd-ctl
BENCH=$ROOT/target/release/fastetcd-bench

run() {
    local name=$1 bin=$2 dir=$WORK/$1
    mkdir -p "$dir"
    "$bin" --data-dir "$dir/data" \
        --listen-client-urls http://127.0.0.1:23790 \
        --listen-peer-urls http://127.0.0.1:23800 \
        --listen-metrics-url "" >"$dir/log" 2>&1 &
    local pid=$!
    for _ in $(seq 100); do
        curl -sf http://127.0.0.1:23790/health >/dev/null && break
        sleep 0.2
    done
    sleep 2
    echo "== $name ($("$bin" --version))"
    "$BENCH" --endpoint http://127.0.0.1:23790 --mode read-under-load \
        --conns 40 --duration-secs 20 --probes 200
    kill "$pid"
    wait "$pid" || true
}

if [ $# -ge 1 ]; then
    git worktree add --detach "$WORK/base-src" "$1" >/dev/null
    (cd "$WORK/base-src" && CARGO_TARGET_DIR=$ROOT/target/baseline \
        cargo build --release --locked -p fastetcd-server)
    run "baseline $1" "$ROOT/target/baseline/release/fastetcd"
    git worktree remove --force "$WORK/base-src"
fi
run "this tree $(git rev-parse --short HEAD)" "$ROOT/target/release/fastetcd"
