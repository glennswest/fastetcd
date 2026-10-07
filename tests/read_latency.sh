#!/usr/bin/env bash
# Read latency under write load (fastetcd#71), on this machine's disk.
#
#   tests/read_latency.sh [baseline-ref]
#   MEMBERS=3 tests/read_latency.sh [baseline-ref]
#
# Builds this tree (release) and, if given, a baseline ref in a git
# worktree, then runs each with its data dirs under
# tmp/ (a real disk, not tmpfs), as one member or MEMBERS local ones,
# and drives it with
# `fastetcd-bench --mode read-under-load`: 40 clients looping GET + CAS
# Txn, a prefix Range every fifth loop, and a sequential probe of 200
# linearizable then 200 serializable Ranges of one key. Run it through
# sc-build: `sc-build 'tests/read_latency.sh v1.8.0'`. METRICS=1 also
# serves /metrics and prints each member's WAL, checkpoint, leader and
# proposal lines after the run (#85), and the members' election and
# slow-fsync log lines, to match the bench's slowest writes against (#83).
# DATA_ROOT=/dev/shm puts the data dirs on tmpfs instead: no fsync can
# stall there, so a stall that remains is fastetcd's, not the disk's.
# BENCH_ARGS replaces the bench's arguments (tests/keepalive_bench.sh).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/read-latency
rm -rf "$WORK"
mkdir -p "$WORK"
DATA=$WORK
if [ -n "${DATA_ROOT:-}" ]; then
    DATA=$(mktemp -d -p "$DATA_ROOT" fastetcd-read-latency.XXXXXX)
    trap 'rm -rf "$DATA"' EXIT
fi
echo "data volume: $(df -hT "$DATA" | tail -1)"

cargo build --release --locked -p fastetcd-server -p fastetcd-ctl
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
BENCH=$TARGET/release/fastetcd-bench

MEMBERS=${MEMBERS:-1}
BENCH_FAILED=0

# Start MEMBERS members of `bin`, data dirs under $1 and logs in $3;
# print their pids.
start_members() {
    local dir=$1 bin=$2 logs=$3 cluster="" i
    for i in $(seq "$MEMBERS"); do
        cluster+="${cluster:+,}n$i=http://127.0.0.1:$((23800 + i))"
    done
    for i in $(seq "$MEMBERS"); do
        local args=(--data-dir "$dir/data$i"
            --listen-client-urls "http://127.0.0.1:$((23790 + i))"
            --listen-peer-urls "http://127.0.0.1:$((23800 + i))"
            --listen-metrics-url "$([ "${METRICS:-0}" = 1 ] && echo "127.0.0.1:$((23810 + i))" || true)")
        if [ "$MEMBERS" -gt 1 ]; then
            args+=(--name "n$i" --initial-cluster "$cluster"
                --initial-advertise-peer-urls "http://127.0.0.1:$((23800 + i))"
                --initial-cluster-token read-latency)
        fi
        "$bin" "${args[@]}" >"$logs/log$i" 2>&1 &
        echo $!
    done
}

run() {
    local name=$1 bin=$2 dir=$WORK/$1 i
    mkdir -p "$dir" "$DATA/$1"
    local pids
    pids=$(start_members "$DATA/$1" "$bin" "$dir")
    for i in $(seq "$MEMBERS"); do
        for _ in $(seq 100); do
            curl -sf "http://127.0.0.1:$((23790 + i))/health" >/dev/null && break
            sleep 0.2
        done
    done
    sleep 4
    echo "== $name ($("$bin" --version)), $MEMBERS member(s)"
    # One member's client port, or (several members) two of them: the
    # leader may be either, so both a leader and a follower get measured.
    for i in $(seq "$((MEMBERS > 1 ? 2 : 1))"); do
        echo "-- client port of member $i"
        # shellcheck disable=SC2086
        if ! "$BENCH" --endpoint "http://127.0.0.1:$((23790 + i))" \
            ${BENCH_ARGS:---mode read-under-load --conns 40 --duration-secs 20 --probes 200} \
            2>"$dir/bench.err"; then
            echo "BENCH FAILED: $(grep -m1 -o 'message: "[^"]*"' "$dir/bench.err" || tail -n 2 "$dir/bench.err")"
            echo "-- members' elections, warnings and errors:"
            grep -hiE "vote|elect|leader|WARN|ERROR|panic" "$dir"/log* | tail -n 60 || true
            BENCH_FAILED=1
        fi
    done
    if [ "${METRICS:-0}" = 1 ]; then
        for i in $(seq "$MEMBERS"); do
            echo "-- metrics of member $i"
            curl -sf "http://127.0.0.1:$((23810 + i))/metrics" |
                grep -E '^(fastetcd_wal|fastetcd_checkpoint|fastetcd_proposal|etcd_server_(is_leader|leader_changes|proposals_pending))' || true
        done
        echo "-- members' elections and slow fsyncs:"
        grep -hE "slow raft WAL|vote|elect|leader" "$dir"/log* | grep -E "WARN|INFO" | tail -n 40 || true
    fi
    local pid
    for pid in $pids; do
        if ! kill "$pid" 2>/dev/null; then
            echo "member pid $pid had already exited; its log ends:"
            tail -n 20 "$dir"/log* || true
        fi
    done
    sleep 1
}

if [ $# -ge 1 ]; then
    # sc-build's checkout carries no tags and no remote: fetch the ref
    # if it is missing.
    base=$1
    if ! git rev-parse -q --verify "$base^{commit}" >/dev/null; then
        remote=$(git remote get-url origin 2>/dev/null ||
            echo https://github.com/glennswest/fastetcd.git)
        git fetch -q --depth=1 "$remote" "$base"
        base=FETCH_HEAD
    fi
    git worktree add --detach "$WORK/base-src" "$base" >/dev/null
    (cd "$WORK/base-src" && CARGO_TARGET_DIR=$TARGET/baseline \
        cargo build --release --locked -p fastetcd-server)
    run "baseline $1" "$TARGET/baseline/release/fastetcd"
    git worktree remove --force "$WORK/base-src"
fi
run "this tree $(git rev-parse --short HEAD)" "$TARGET/release/fastetcd"
exit "$BENCH_FAILED"
