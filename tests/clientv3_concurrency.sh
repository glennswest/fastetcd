#!/usr/bin/env bash
# etcd's Go client: Range sort and revision filters, and clientv3's
# concurrency Mutex and Election, on fastetcd (fastetcd#154).
#
#   sc-build 'tests/clientv3_concurrency.sh [baseline-ref]'
#
# Downloads Go (sha256 checked), builds tests/clientv3_concurrency
# (clientv3 v3.5.17) and this tree's fastetcd, and runs it against one
# member. With a baseline ref, the same against that version (reported,
# not required to pass: before #154 the Mutex was not exclusive).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/clientv3-concurrency
rm -rf "$WORK"
mkdir -p "$WORK"
GO_VERSION=${GO_VERSION:-1.26.8}
GO_TGZ=go$GO_VERSION.linux-amd64.tar.gz
curl -sSfL -o "$WORK/$GO_TGZ" "https://dl.google.com/go/$GO_TGZ"
echo "$(curl -sSfL "https://dl.google.com/go/$GO_TGZ.sha256")  $WORK/$GO_TGZ" | sha256sum -c - >/dev/null
tar -xzf "$WORK/$GO_TGZ" -C "$WORK"
export GOTOOLCHAIN=local GOPATH=$WORK/gopath GOCACHE=$WORK/gocache GOFLAGS=-mod=mod
(cd tests/clientv3_concurrency && "$WORK/go/bin/go" mod tidy && "$WORK/go/bin/go" build -o "$WORK/client" .)

cargo build -q --release --locked -p fastetcd-server
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# run <name> <fastetcd binary> [locks]: the client's exit status.
run() {
    local name=$1 bin=$2 mode=${3:-}
    local dir=$WORK/$1$mode
    mkdir -p "$dir"
    local cp pp ep
    cp=$(port) pp=$(port)
    ep=http://127.0.0.1:$cp
    "$bin" --data-dir "$dir/data" --listen-client-urls "$ep" --advertise-client-urls "$ep" \
        --listen-peer-urls "http://127.0.0.1:$pp" --listen-metrics-url "" >>"$dir/log" 2>&1 &
    local pid=$!
    for _ in $(seq 100); do curl -sf "$ep/health" >/dev/null && break; sleep 0.2; done
    echo "== $name ($("$bin" --version))${mode:+, $mode only}"
    local rc=0
    timeout 120 "$WORK/client" "$ep" $mode || rc=$?
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    return $rc
}

if [ $# -ge 1 ]; then
    base=$1
    if ! git rev-parse -q --verify "$base^{commit}" >/dev/null; then
        git fetch -q --depth=1 https://github.com/glennswest/fastetcd.git "$base"
        base=FETCH_HEAD
    fi
    git worktree add --detach "$WORK/base-src" "$base" >/dev/null
    (cd "$WORK/base-src" && CARGO_TARGET_DIR=$TARGET/baseline cargo build -q --release --locked -p fastetcd-server)
    run "baseline $1" "$TARGET/baseline/release/fastetcd" || echo "   (baseline failed, as expected before #154)"
    run "baseline $1" "$TARGET/baseline/release/fastetcd" locks || echo "   (baseline failed, as expected before #154)"
    git worktree remove --force "$WORK/base-src"
fi
run "this tree" "$TARGET/release/fastetcd"
echo "clientv3_concurrency: ok"
