#!/usr/bin/env bash
# /readyz latency under write load and compaction (fastetcd#145).
#
#   tests/readyz_latency.sh [baseline-ref] [rounds]
#
# Builds this tree (release) and, if given, a baseline ref in a git
# worktree. Each round runs every build in turn as one member, data under
# tmp/ (a real disk): `fastetcd-bench --mode put` with 1000 clients over
# 1000 keys, a compaction every 2 s (through the JSON gateway, at the
# revision then minus 1000), and /readyz polled every 20 ms the whole time,
# each answer's time measured by curl. Prints p50 / p99 / max of /readyz
# and how many answers took over 50 ms (stormd's p99 budget for it).
# Run it through sc-build: `sc-build 'tests/readyz_latency.sh v1.32.0 2'`.
# CONNS sets the bench's clients (1000).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/readyz-latency
rm -rf "$WORK"
mkdir -p "$WORK"
echo "data volume: $(df -hT "$WORK" | tail -1)"

cargo build --release --locked -p fastetcd-server -p fastetcd-ctl
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
BENCH=$TARGET/release/fastetcd-bench
CLIENT=http://127.0.0.1:23791

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
ROUNDS=${2:-1}

revision() {
    curl -sf -X POST "$CLIENT/v3/kv/range" -d '{"key":"AA=="}' |
        python3 -c 'import json,sys; print(json.load(sys.stdin)["header"]["revision"])'
}

compactor() {
    local rev
    while sleep 2; do
        rev=$(revision 2>/dev/null) || continue
        [ "$rev" -gt 1000 ] || continue
        curl -sf -X POST "$CLIENT/v3/kv/compaction" -d "{\"revision\":\"$((rev - 1000))\"}" >/dev/null || true
    done
}

# ROUTES (default `readyz`): the routes probed in turn, e.g.
# `readyz/non_learner readyz/serializable_read readyz/linearizable_read`
# to tell which part of an answer is slow.
ROUTES=${ROUTES:-readyz}
prober() {
    local r
    while :; do
        for r in $ROUTES; do
            curl -s -o /dev/null -w "$r %{http_code} %{time_total}\n" "$CLIENT/$r" || echo "$r 000 0"
            sleep 0.02
        done
    done
}

run() {
    local name=$1 bin=$2 dir=$WORK/$1-$3
    mkdir -p "$dir"
    "$bin" --data-dir "$dir/data" --listen-client-urls "$CLIENT" \
        --listen-peer-urls http://127.0.0.1:23801 --listen-metrics-url "" \
        >"$dir/log" 2>&1 &
    local pid=$!
    for _ in $(seq 100); do
        curl -sf "$CLIENT/health" >/dev/null && break
        sleep 0.2
    done
    sleep 2
    prober >"$dir/idle.txt" &
    local p=$!
    sleep 5
    kill "$p"
    prober >"$dir/readyz.txt" &
    local probe=$!
    compactor &
    local comp=$!
    "$BENCH" --endpoint "$CLIENT" --mode put --conns "${CONNS:-1000}" --total 300000 --keys 1000 \
        >"$dir/bench.txt" 2>"$dir/bench.err" || echo "BENCH FAILED: $(tail -n 2 "$dir/bench.err")"
    kill "$probe" "$comp" 2>/dev/null || true
    wait "$probe" "$comp" 2>/dev/null || true
    kill "$pid"
    wait "$pid" 2>/dev/null || true
    echo "== round $3: $name ($("$bin" --version))"
    grep -E 'throughput|latency' "$dir/bench.txt" | head -n 2 | sed 's/^/   put: /'
    echo "   compactions: $(grep -c 'compaction complete' "$dir/log" || true)"
    for f in idle readyz; do
        python3 - "$dir/$f.txt" "$f" <<'EOF'
import sys
rows = [l.split() for l in open(sys.argv[1]) if len(l.split()) == 3]
for route in dict.fromkeys(r for r, _, _ in rows):
    mine = [(c, s) for r, c, s in rows if r == route]
    bad = sum(1 for c, _ in mine if c != "200")
    t = sorted(float(s) * 1000 for _, s in mine)
    pct = lambda q: t[min(len(t) - 1, int(q * len(t)))]
    over = sum(1 for x in t if x > 50)
    print(f"   {sys.argv[2]:6} /{route:26} n {len(t):5}  p50 {pct(.5):6.1f}  p99 {pct(.99):6.1f}  "
          f"max {t[-1]:6.1f} ms  over 50 ms {over}  not 200 {bad}")
    for c, s in mine:
        if c != "200":
            print(f"          not 200: code {c} after {float(s) * 1000:.1f} ms")
EOF
    done
}

for r in $(seq "$ROUNDS"); do
    for b in "${BINS[@]}"; do
        run "${b%%=*}" "${b#*=}" "$r"
    done
done
