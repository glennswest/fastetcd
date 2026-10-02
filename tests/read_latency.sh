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
# serves /metrics and prints each member's WAL and checkpoint lines
# after the run (#85).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/read-latency
rm -rf "$WORK"
mkdir -p "$WORK"
echo "data volume: $(df -hT "$WORK" | tail -1)"

cargo build --release --locked -p fastetcd-server -p fastetcd-ctl
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
BENCH=$TARGET/release/fastetcd-bench

MEMBERS=${MEMBERS:-1}

# Start MEMBERS members of `bin` under $1's directory; print their pids.
start_members() {
    local dir=$1 bin=$2 cluster="" i
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
        "$bin" "${args[@]}" >"$dir/log$i" 2>&1 &
        echo $!
    done
}

run() {
    local name=$1 bin=$2 dir=$WORK/$1 i
    mkdir -p "$dir"
    local pids
    pids=$(start_members "$dir" "$bin")
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
        "$BENCH" --endpoint "http://127.0.0.1:$((23790 + i))" --mode read-under-load \
            --conns 40 --duration-secs 20 --probes 200
    done
    if [ "${METRICS:-0}" = 1 ]; then
        for i in $(seq "$MEMBERS"); do
            echo "-- metrics of member $i"
            curl -sf "http://127.0.0.1:$((23810 + i))/metrics" |
                grep -E '^(fastetcd_wal|fastetcd_checkpoint)' || true
        done
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
