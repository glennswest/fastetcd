#!/usr/bin/env bash
# Run fastetcd's test-container suites (#36) in an sc-build job.
#
#   sc-build tests/test_container.sh
#
# Builds what test/build.sh stages (the static test and the commit's
# static fastetcd), starts a local member standing in for the node's
# fastetcd, and runs `/test short`, `/test medium` and a scaled-down
# `/test long` (2 waves, 10 s holds) against it, as stormcentral would on a
# test machine. Prints every result line; exits 1 if a suite did not exit 0.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/test-container
rm -rf "$WORK"
mkdir -p "$WORK"

test/build.sh
STAGE=$ROOT/test/.stage
ls -l "$STAGE"
file "$STAGE"/* 2>/dev/null || true

port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
NODE_PORT=$(port)
PEER_PORT=$(port)
"$STAGE/fastetcd" --name node --data-dir "$WORK/node" \
    --listen-client-urls "http://127.0.0.1:$NODE_PORT" \
    --listen-peer-urls "http://127.0.0.1:$PEER_PORT" --listen-metrics-url "" \
    >"$WORK/node.log" 2>&1 &
NODE=$!
trap 'kill $NODE 2>/dev/null || true' EXIT
for _ in $(seq 100); do
    curl -sf "http://127.0.0.1:$NODE_PORT/health" >/dev/null && break
    sleep 0.2
done

FAILED=0
suite() {
    local s=$1 timeout=$2
    shift 2
    echo "#### /test $s"
    local rc=0
    env STORM_SUITE="$s" STORM_RUN_ID="sc-$s-$$" STORM_NODE=127.0.0.1 \
        STORM_TIMEOUT="$timeout" STORM_COMMIT="$(git rev-parse --short=12 HEAD)" \
        FASTETCD_TEST_NODE_PORT="$NODE_PORT" FASTETCD_TEST_BIN="$STAGE/fastetcd" \
        FASTETCD_TEST_DIR="$WORK" "$@" \
        "$STAGE/fastetcd-test" "$s" >"$WORK/$s.jsonl" 2>"$WORK/$s.err" || rc=$?
    cat "$WORK/$s.jsonl"
    echo "exit $rc"
    if [ "$rc" != 0 ]; then
        tail -n 20 "$WORK/$s.err"
        FAILED=1
    fi
}
suite short 120
suite medium 1800
suite long 2400 FASTETCD_TEST_LONG_SCALE=0.005 FASTETCD_TEST_LONG_HOLD_SECS=10 FASTETCD_TEST_LONG_WAVES=2

# short left nothing on the "node".
left=$(curl -s -X POST "http://127.0.0.1:$NODE_PORT/v3/kv/range" \
    -d '{"key":"L3N0b3JtLXRlc3Qv","range_end":"L3N0b3JtLXRlc3Qw","count_only":true}')
echo "keys left under /storm-test/ on the node: $left"
case "$left" in *'"count"'*) echo "short left keys behind"; FAILED=1 ;; esac
exit "$FAILED"
