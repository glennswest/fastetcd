#!/usr/bin/env bash
# Large values on a bounded volume (fastetcd#182).
#
#   tests/large_values_space.sh [report]
#
# One member, --quota-backend-bytes VOLUME (default 1 GiB, stormcos's
# fastetcd-data), ~900 KiB values (stormcentral#1169's fill). Prints, for
# the data file, the WAL and the snapshots, both their apparent and their
# allocated sizes after each step, and the store's used ratio.
#   1. fill to TARGET_MIB (320) written: no NOSPACE, the WAL bounded;
#   2. fill on until the alarm refuses a write;
#   3. delete every key: the alarm clears by itself (no disarm) and a
#      write works again.
# `report` only prints (for a baseline that fails the checks).
# Run it through sc-build: `sc-build tests/large_values_space.sh`.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
MODE=${1:-check}
WORK=$ROOT/tmp/large-values
rm -rf "$WORK"
mkdir -p "$WORK"
VOLUME=${VOLUME:-1073741824}
TARGET_MIB=${TARGET_MIB:-320}
VAL=${VAL:-921600}

cargo build --release --locked -p fastetcd-server -p fastetcd-ctl
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
BENCH=$TARGET/release/fastetcd-bench
EP=http://127.0.0.1:23791
METRICS=http://127.0.0.1:23811/metrics
DATA=$WORK/data

"$TARGET/release/fastetcd" --name n1 --data-dir "$DATA" \
    --listen-client-urls $EP --advertise-client-urls $EP \
    --listen-peer-urls http://127.0.0.1:23801 \
    --listen-metrics-urls http://127.0.0.1:23811 \
    --quota-backend-bytes "$VOLUME" --space-check-interval-secs 2 \
    >"$WORK/log" 2>&1 &
PID=$!
trap 'kill -9 $PID 2>/dev/null || true; grep -E "fastetcd::space|fastetcd::snapshot" "$WORK/log" | cut -c1-260 | tail -40' EXIT
for _ in $(seq 100); do curl -sf "$EP/health" >/dev/null && break; sleep 0.2; done

fail=0
check() { # name ok?
    if [ "$2" = 1 ]; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi
}
metric() { curl -s "$METRICS" | awk -v m="$1" '$1==m{print $2; exit}'; }
mib() { echo $(($1 / 1048576)); }
sizes() { # apparent allocated, for a path, in MiB
    local a b
    a=$(du -s --apparent-size -B1 "$1" 2>/dev/null | cut -f1)
    b=$(du -s -B1 "$1" 2>/dev/null | cut -f1)
    echo "$(mib "${a:-0}")/$(mib "${b:-0}")"
}
report() {
    sleep 3 # a space sample, and the checkpoint that may purge
    printf '%-14s written=%4d MiB file=%s wal=%s snap=%s (apparent/allocated MiB) used_ratio=%s\n' \
        "$1" "$(mib "$written")" "$(sizes "$DATA/fastetcd.redb")" "$(sizes "$DATA/wal")" \
        "$(sizes "$DATA/snapshots")" "$(metric fastetcd_store_space_used_ratio)"
}
written=0
# Puts until `limit` MiB are written or a put is refused: `$BENCH` counts
# refusals as failed ops and prints them. Keys go on from the last round.
round=0
refused=0
fill() {
    local limit=$1 out n
    while [ $((written / 1048576)) -lt "$limit" ]; do
        n=40
        round=$((round + 1))
        out=$("$BENCH" --endpoint $EP --mode put --conns 2 --total $n --val-bytes "$VAL" \
            --keys 100000 --key-prefix "fill/$round/" 2>&1) || true
        if grep -qiE 'gave up|exhausted|exceeded|no operation' <<<"$out"; then
            refused=1
            echo "$out" | grep -iE 'errors|exhausted|exceeded' | cut -c1-200 | head -3
            return
        fi
        written=$((written + n * VAL))
        report "fill"
    done
}

fill "$TARGET_MIB"
report "at-target"
wal_alloc=$(du -s -B1 "$DATA/wal" | cut -f1)
check "no NOSPACE at ${TARGET_MIB} MiB written (refused=$refused)" "$([ $refused = 0 ] && echo 1 || echo 0)"
check "WAL within an eighth of the volume + a snapshot's lag: $(mib "$wal_alloc") MiB" \
    "$([ "$wal_alloc" -le $((VOLUME / 4)) ] && echo 1 || echo 0)"

fill 4000
report "alarm"
check "the alarm refuses a write once the volume is full" "$([ $refused = 1 ] && echo 1 || echo 0)"
curl -s "$EP/health"; echo

# Everything goes: etcd's gateway, prefix delete of "fill/".
curl -sf -X POST "$EP/v3/kv/deleterange" \
    -d "{\"key\":\"$(printf 'fill/' | base64)\",\"range_end\":\"$(printf 'fill0' | base64)\"}"; echo
cleared=0
for _ in $(seq 120); do
    if curl -sf -X POST "$EP/v3/kv/put" -d "{\"key\":\"$(printf after | base64)\",\"value\":\"$(printf v | base64)\"}" >/dev/null; then
        cleared=1
        break
    fi
    sleep 5
done
report "after-delete"
check "after deleting the keys the alarm cleared by itself and a write works" "$cleared"

[ "$MODE" = report ] && exit 0
exit $fail
