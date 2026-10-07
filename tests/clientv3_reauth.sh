#!/usr/bin/env bash
# etcd's Go client re-authenticates through a fastetcd member restart
# (fastetcd#105).
#
#   sc-build 'tests/clientv3_reauth.sh [baseline-ref]'
#
# Downloads Go (sha256 checked), builds tests/clientv3_reauth (clientv3
# v3.5.17) and this tree's fastetcd, enables auth, and keeps one clientv3
# client across a restart of the member, which loses every token: the
# client's next Get and Put must succeed after clientv3 refreshes its
# token by itself. With a baseline ref, the same against that version too
# (reported, not required to pass: before #105 it could not).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/clientv3-reauth
rm -rf "$WORK"
mkdir -p "$WORK"
GO_VERSION=${GO_VERSION:-1.26.8}
GO_TGZ=go$GO_VERSION.linux-amd64.tar.gz
curl -sSfL -o "$WORK/$GO_TGZ" "https://dl.google.com/go/$GO_TGZ"
echo "$(curl -sSfL "https://dl.google.com/go/$GO_TGZ.sha256")  $WORK/$GO_TGZ" | sha256sum -c - >/dev/null
tar -xzf "$WORK/$GO_TGZ" -C "$WORK"
export GOTOOLCHAIN=local GOPATH=$WORK/gopath GOCACHE=$WORK/gocache GOFLAGS=-mod=mod
(cd tests/clientv3_reauth && "$WORK/go/bin/go" mod tidy && "$WORK/go/bin/go" build -o "$WORK/reauth" .)

cargo build -q --release --locked -p fastetcd-server
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# run <name> <fastetcd binary>: 0 when the client gets through the restart.
run() {
    local name=$1 bin=$2 dir=$WORK/$1
    mkdir -p "$dir"
    local cp pp
    cp=$(port) pp=$(port)
    local ep=http://127.0.0.1:$cp
    start() {
        "$bin" --data-dir "$dir/data" --listen-client-urls "$ep" --advertise-client-urls "$ep" \
            --listen-peer-urls "http://127.0.0.1:$pp" --listen-metrics-url "" >>"$dir/log" 2>&1 &
        echo $! >"$dir/pid"
        for _ in $(seq 100); do curl -sf "$ep/health" >/dev/null && return 0; sleep 0.2; done
        echo "not up"; return 1
    }
    echo "== $name ($("$bin" --version))"
    start
    "$WORK/reauth" setup "$ep"
    mkfifo "$dir/go"
    "$WORK/reauth" run "$ep" <"$dir/go" >"$dir/client" 2>&1 &
    local client=$!
    exec 3>"$dir/go"
    for _ in $(seq 100); do grep -q "^ok put before" "$dir/client" 2>/dev/null && break; sleep 0.1; done
    cat "$dir/client"
    kill "$(cat "$dir/pid")"; wait "$(cat "$dir/pid")" 2>/dev/null || true
    echo "   member restarted (every token is gone)"
    start
    echo go >&3
    exec 3>&-
    local rc=0
    wait "$client" || rc=$?
    tail -n +2 "$dir/client"
    kill "$(cat "$dir/pid")" 2>/dev/null || true
    return $rc
}

FAILED=0
if [ $# -ge 1 ]; then
    base=$1
    if ! git rev-parse -q --verify "$base^{commit}" >/dev/null; then
        git fetch -q --depth=1 https://github.com/glennswest/fastetcd.git "$base"
        base=FETCH_HEAD
    fi
    git worktree add --detach "$WORK/base-src" "$base" >/dev/null
    (cd "$WORK/base-src" && CARGO_TARGET_DIR=$TARGET/baseline cargo build -q --release --locked -p fastetcd-server)
    run "baseline $1" "$TARGET/baseline/release/fastetcd" || echo "   (baseline failed, as expected before #105)"
    git worktree remove --force "$WORK/base-src"
fi
run "this tree $(git rev-parse --short HEAD)" "$TARGET/release/fastetcd" || FAILED=1
[ "$FAILED" = 0 ] && echo "clientv3_reauth: passed" || echo "clientv3_reauth: FAILED"
exit "$FAILED"
