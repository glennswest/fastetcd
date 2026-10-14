#!/usr/bin/env bash
# RAM a raft snapshot costs to build and to install (fastetcd#62).
#
#   tests/snapshot_memory.sh [baseline-ref]
#
# Builds this tree (release) and, if given, a baseline ref. For each:
#   1. one member, n1, loaded with KEYS keys of VAL_BYTES (30000 x 10 KB,
#      ~300 MB), with a small --snapshot-count so its log is purged;
#   2. n1 restarted (its snapshot kept, so nothing is built at start):
#      its peak RSS so far (VmHWM) is the base, with the memory gauges
#      that explain it; then its retained snapshot file is deleted;
#   3. n2 added as a learner (the JSON gateway's member add) and started:
#      n1's log is purged and its snapshot gone, so it builds one on
#      demand ("the retained snapshot is gone") and n2 installs it;
#   4. once n2 holds n1's revision: n1's VmHWM (what the build and send
#      cost above the base) and n2's (its whole life: the install).
# Run it through sc-build: `sc-build 'tests/snapshot_memory.sh v1.32.2'`.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/snapshot-memory
rm -rf "$WORK"
mkdir -p "$WORK"
KEYS=${KEYS:-30000}
VAL_BYTES=${VAL_BYTES:-10000}

cargo build --release --locked -p fastetcd-server -p fastetcd-ctl
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
BENCH=$TARGET/release/fastetcd-bench

BINS=("tree=$TARGET/release/fastetcd")
if [ -n "${1:-}" ]; then
    base=$1
    # sc-build's checkout carries no tags and no remote: fetch the ref.
    if ! git rev-parse --verify -q "$base^{commit}" >/dev/null; then
        git fetch -q https://github.com/glennswest/fastetcd.git "refs/tags/$base:refs/tags/$base" ||
            git fetch -q https://github.com/glennswest/fastetcd.git "$base"
    fi
    git worktree add --detach "$WORK/base-src" "$base" >/dev/null
    (cd "$WORK/base-src" && CARGO_TARGET_DIR=$TARGET/baseline \
        cargo build --release --locked -p fastetcd-server)
    git worktree remove --force "$WORK/base-src"
    BINS=("$base=$TARGET/baseline/release/fastetcd" "${BINS[@]}")
fi

c1=http://127.0.0.1:23791 c2=http://127.0.0.1:23792
p1=http://127.0.0.1:23801 p2=http://127.0.0.1:23802
CLUSTER="n1=$p1,n2=$p2"

hwm() { awk '/^VmHWM/{printf "%.0f", $2/1024}' "/proc/$1/status"; }
rss() { awk '/^VmRSS/{printf "%.0f", $2/1024}' "/proc/$1/status"; }
# The revision a member has applied, read locally (serializable).
revision() {
    curl -sf -X POST "$1/v3/kv/range" -d '{"key":"AA==","serializable":true}' |
        python3 -c 'import json,sys; print(json.load(sys.stdin)["header"]["revision"])'
}

start_n1() {
    "$1" --name n1 --data-dir "$2/n1" --listen-client-urls "$c1" \
        --listen-peer-urls "$p1" --initial-advertise-peer-urls "$p1" \
        --initial-cluster "n1=$p1" --listen-metrics-url 127.0.0.1:23811 \
        --snapshot-count 1000 --max-in-snapshot-log-to-keep 100 \
        >>"$2/log1" 2>&1 &
    N1=$!
    for _ in $(seq 150); do
        curl -sf "$c1/health" >/dev/null && return
        sleep 0.2
    done
    echo "n1 did not come up"; tail -n 20 "$2/log1"; exit 1
}

run() {
    local name=$1 bin=$2 dir=$WORK/$1
    mkdir -p "$dir"
    echo "== $name ($("$bin" --version)): $KEYS keys x $VAL_BYTES B"
    start_n1 "$bin" "$dir"
    "$BENCH" --endpoint "$c1" --mode put --conns 32 --total "$KEYS" --keys "$KEYS" \
        --val-bytes "$VAL_BYTES" | grep -E 'throughput' | sed 's/^/   load: /'
    sleep 3
    local rev
    rev=$(revision "$c1")
    echo "   n1 at revision $rev, data file $(du -m "$dir/n1/fastetcd.redb" | cut -f1) MiB, \
retained snapshots: $(ls "$dir/n1/snapshots" | grep -c '\.snap$' || true)"
    kill "$N1"; wait "$N1" 2>/dev/null || true
    start_n1 "$bin" "$dir"
    sleep 3
    local base
    base=$(hwm "$N1")
    echo "   n1 restarted: RSS $(rss "$N1") MiB, peak $base MiB; $(curl -sf http://127.0.0.1:23811/metrics |
        awk '/^(fastetcd_wal_cached_bytes|fastetcd_write_behind_bytes|fastetcd_value_cache_bytes|fastetcd_key_index_bytes) /{printf "%s %.0f MiB  ", $1, $2/1048576}')"
    rm -f "$dir"/n1/snapshots/*

    curl -sf -X POST "$c1/v3/cluster/member/add" -d "{\"peerURLs\":[\"$p2\"],\"isLearner\":true}" >/dev/null
    "$bin" --name n2 --data-dir "$dir/n2" --listen-client-urls "$c2" \
        --listen-peer-urls "$p2" --initial-advertise-peer-urls "$p2" \
        --initial-cluster "$CLUSTER" --initial-cluster-state existing --listen-metrics-url "" \
        >>"$dir/log2" 2>&1 &
    local n2=$! t0=$SECONDS got=0
    while [ $((SECONDS - t0)) -lt 300 ]; do
        got=$(revision "$c2" 2>/dev/null || echo 0)
        [ "$got" = "$rev" ] && break
        sleep 0.5
    done
    if [ "$got" != "$rev" ]; then
        echo "   n2 did not catch up (revision $got of $rev)"; tail -n 20 "$dir/log2"
    else
        echo "   n2 caught up in $((SECONDS - t0)) s; n2 snapshots: $(ls "$dir/n2/snapshots" 2>/dev/null | grep -c '\.snap$' || true), \
n1 snapshots: $(ls "$dir/n1/snapshots" 2>/dev/null | grep -c '\.snap$' || true)"
    fi
    sleep 2
    echo "   peak RSS: n1 (build + send) $(hwm "$N1") MiB (base $base), n2 (install) $(hwm "$n2") MiB"
    echo "   after: n2 retained snapshots $(ls "$dir/n2/snapshots" 2>/dev/null | grep -c '\.snap$' || true); \
n1 built on demand: $(grep -c 'retained snapshot is gone' "$dir/log1" || true); \
snapshot not kept (n2): $(grep -c 'could not be kept' "$dir/log2" || true); \
n2 installed: $(grep -ciE 'install(ed|ing)? snapshot' "$dir/log2" || true)"
    kill "$N1" "$n2"; wait "$N1" "$n2" 2>/dev/null || true
}

for b in "${BINS[@]}"; do
    run "${b%%=*}" "${b#*=}"
done
