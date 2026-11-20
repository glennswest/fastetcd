#!/usr/bin/env bash
# Run etcd's robustness suite against this tree's fastetcd (#195).
#
#   sc-build 'tests/robustness/run.sh [go test -run pattern]'
#
# Fetches etcd v3.6.11 and the Go its .go-version names (sha256
# checked), applies fastetcd-v3.6.11.patch (report/wal.go reads
# `fastetcd persisted-requests` instead of an etcd WAL), builds this
# tree's fastetcd, puts it with the `etcd` shim in a bin dir and runs
# `go test ./robustness -run <pattern>` (default
# TestRobustnessExploratory) with `--bin-dir` there. Exits as go test
# does; the summary of each scenario's result is printed last.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
WORK=$ROOT/tmp/robustness
ETCD_REF=v3.6.11
RUN=${1:-TestRobustnessExploratory}
rm -rf "$WORK"
mkdir -p "$WORK/bin"

git clone -q --depth 1 --branch "$ETCD_REF" https://github.com/etcd-io/etcd.git "$WORK/etcd"
git -C "$WORK/etcd" apply "$ROOT/tests/robustness/fastetcd-$ETCD_REF.patch"

GO_VERSION=${GO_VERSION:-$(cat "$WORK/etcd/.go-version")}
GO_TGZ=go$GO_VERSION.linux-amd64.tar.gz
curl -sSfL -o "$WORK/$GO_TGZ" "https://dl.google.com/go/$GO_TGZ"
echo "$(curl -sSfL "https://dl.google.com/go/$GO_TGZ.sha256")  $WORK/$GO_TGZ" | sha256sum -c - >/dev/null
tar -xzf "$WORK/$GO_TGZ" -C "$WORK"
export GOTOOLCHAIN=local GOPATH=$WORK/gopath GOCACHE=$WORK/gocache

cargo build -q --release --locked -p fastetcd-server
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
cp "$TARGET/release/fastetcd" "$WORK/bin/fastetcd"
cp "$ROOT/tests/robustness/etcd" "$WORK/bin/etcd"
"$WORK/bin/etcd" --version

cd "$WORK/etcd/tests"
rc=0
"$WORK/go/bin/go" test ./robustness -run "$RUN" -v -count=1 -timeout 45m \
    -args --bin-dir="$WORK/bin" >"$WORK/go-test.log" 2>&1 || rc=$?
# The log is long (every member's output); the end of each failing
# scenario and the result of each are what to read.
grep -nE -B2 -A12 '^\s+[a-z_]+\.go:[0-9]+: ' "$WORK/go-test.log" | grep -vE '^\S*-\s*$' | tail -400 || true
echo "==== member logs (last 15 lines each)"
for log in "$WORK"/logs/*.log; do
    [ -e "$log" ] || continue
    echo "-- ${log##*/}"
    tail -15 "$log" | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-400
done
echo "==== results"
grep -E '^\s*--- (PASS|FAIL|SKIP)' "$WORK/go-test.log" || true
echo "robustness: go test exited $rc"
exit $rc
