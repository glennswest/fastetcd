#!/usr/bin/env bash
# Run etcd's robustness suite against this tree's fastetcd (#195).
#
#   sc-build 'tests/robustness/run.sh [go test -run pattern]'
#   e.g. 'TestRobustnessExploratory/.*/ClusterOfSize1'; GO_TIMEOUT
#   (default 45m) bounds the go test run.
#
# Fetches etcd v3.6.11 and the Go its .go-version names (sha256
# checked), applies fastetcd-v3.6.11.patch (report/wal.go reads
# `fastetcd persisted-requests` instead of an etcd WAL), builds this
# tree's fastetcd, puts it with the `etcd` shim in a bin dir and runs
# `go test ./robustness -run <pattern>` (default
# TestRobustnessExploratory) with `--bin-dir` there. Exits as go test
# does; the summary of each scenario's result is printed last.
#
# A failed scenario's report (each client's operations and watch events,
# the suite's history.html) is saved under RESULTS_DIR, on the same
# filesystem as the members' data dirs (the suite moves those there by
# rename). The build VM does not outlive the job, so the reports, without
# the data dirs, are printed at the end as a base64 tar.gz between
# `==== artifacts` and `==== end artifacts`.
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
export RESULTS_DIR=${TMPDIR:-/tmp}/robustness-results
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

cargo build -q --release --locked -p fastetcd-server
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
cp "$TARGET/release/fastetcd" "$WORK/bin/fastetcd"
cp "$ROOT/tests/robustness/etcd" "$WORK/bin/etcd"
"$WORK/bin/etcd" --version

cd "$WORK/etcd/tests"
rc=0
"$WORK/go/bin/go" test ./robustness -run "$RUN" -v -count=1 -timeout "${GO_TIMEOUT:-45m}" \
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
if [ "$rc" != 0 ]; then
    # With go test's own log (a timeout's goroutine dump is in it).
    gzip -c "$WORK/go-test.log" >"$RESULTS_DIR/go-test.log.gz"
fi
if [ -n "$(ls -A "$RESULTS_DIR")" ]; then
    tar -C "$RESULTS_DIR" -czf "$WORK/reports.tgz" --exclude='server-*' .
    echo "==== artifacts ($(stat -c %s "$WORK/reports.tgz") bytes, base64 tar.gz of RESULTS_DIR without data dirs)"
    base64 -w 0 "$WORK/reports.tgz"
    echo
    echo "==== end artifacts"
fi
# A scenario the harness skips (e.g. a member without what it needs) is
# not a pass: at least one must have run.
if ! grep -qE '^\s+--- (PASS|FAIL): [^ ]+/' "$WORK/go-test.log"; then
    echo "robustness: no scenario ran (all skipped)"
    exit 1
fi
exit $rc
