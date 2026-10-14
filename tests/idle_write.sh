#!/usr/bin/env bash
# A lone write after idle vs writes back to back, one member (fastetcd#102).
#
#   tests/idle_write.sh [baseline-ref]
#
# Builds this tree (release) and, if given, a baseline ref, then for each
# build and each data volume (tmp/ on the real disk, and /dev/shm) starts
# one member and runs `fastetcd-bench --mode idle-put` twice on one client:
# PROBES lone puts each after IDLE_MS of quiet (2000), then PROBES puts
# back to back (--idle-ms 0). Around each run it reads the member's WAL
# metrics, so a put's time splits into the WAL fdatasync (fsyncs, mean
# fsync) and the rest. Run it through sc-build:
# `sc-build 'tests/idle_write.sh v1.12.0'`.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/idle-write
rm -rf "$WORK"
mkdir -p "$WORK"
SHM=$(mktemp -d -p /dev/shm fastetcd-idle.XXXXXX)
trap 'rm -rf "$SHM"' EXIT
PROBES=${PROBES:-30}
IDLE_MS=${IDLE_MS:-2000}

cargo build --release --locked -p fastetcd-server -p fastetcd-ctl
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
BENCH=$TARGET/release/fastetcd-bench
CLIENT=http://127.0.0.1:23791
METRICS=127.0.0.1:23811

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

# fsyncs and fsync seconds so far.
wal() {
    curl -sf "http://$METRICS/metrics" |
        awk '/^fastetcd_wal_fsyncs(_total)? /{n=$2} /^fastetcd_wal_fsync_seconds(_total)? /{s=$2} END{print n+0, s+0}'
}

phase() {
    local label=$1 idle=$2
    local before after out
    before=$(wal)
    out=$("$BENCH" --endpoint "$CLIENT" --mode idle-put --probes "$PROBES" --idle-ms "$idle" --keys 100)
    after=$(wal)
    echo "$out" | grep -E "^  $label"
    read -r n0 s0 <<<"$before"
    read -r n1 s1 <<<"$after"
    python3 -c "
n = $n1 - $n0; s = $s1 - $s0
print(f'      WAL over the run: {n:.0f} fsyncs, mean fsync {s / n * 1000 if n else 0:.2f} ms')"
}

run() {
    local name=$1 bin=$2 vol=$3 dir=$4
    mkdir -p "$dir"
    "$bin" --data-dir "$dir/data" --listen-client-urls "$CLIENT" \
        --listen-peer-urls http://127.0.0.1:23801 --listen-metrics-url "$METRICS" \
        >"$WORK/$name-$vol.log" 2>&1 &
    local pid=$!
    for _ in $(seq 100); do
        curl -sf "$CLIENT/health" >/dev/null && break
        sleep 0.2
    done
    sleep 2
    echo "== $name ($("$bin" --version)), data on $vol ($(df -T "$dir" | awk 'NR==2{print $2}'))"
    phase "lone put after idle" "$IDLE_MS"
    phase "put back to back" 0
    kill "$pid"
    wait "$pid" 2>/dev/null || true
}

for b in "${BINS[@]}"; do
    run "${b%%=*}" "${b#*=}" disk "$WORK/${b%%=*}"
    run "${b%%=*}" "${b#*=}" tmpfs "$SHM/${b%%=*}"
done
