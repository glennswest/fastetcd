#!/usr/bin/env bash
# etcd's Go client joins fastetcd's watch fragments (fastetcd#58).
#
#   sc-build tests/clientv3_fragment.sh
#
# Downloads Go (sha256 checked), builds tests/clientv3_fragment (clientv3
# v3.5.17) and this tree's fastetcd, and runs the client against one
# member: see tests/clientv3_fragment/main.go.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/clientv3-fragment
rm -rf "$WORK"
mkdir -p "$WORK"
GO_VERSION=${GO_VERSION:-1.26.8}
GO_TGZ=go$GO_VERSION.linux-amd64.tar.gz
curl -sSfL -o "$WORK/$GO_TGZ" "https://dl.google.com/go/$GO_TGZ"
echo "$(curl -sSfL "https://dl.google.com/go/$GO_TGZ.sha256")  $WORK/$GO_TGZ" | sha256sum -c - >/dev/null
tar -xzf "$WORK/$GO_TGZ" -C "$WORK"
export GOTOOLCHAIN=local GOPATH=$WORK/gopath GOCACHE=$WORK/gocache GOFLAGS=-mod=mod
(cd tests/clientv3_fragment && "$WORK/go/bin/go" mod tidy && "$WORK/go/bin/go" build -o "$WORK/fragment" .)

cargo build -q --release --locked -p fastetcd-server
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
cp=$(port) pp=$(port)
ep=http://127.0.0.1:$cp
"$TARGET/release/fastetcd" --data-dir "$WORK/data" --listen-client-urls "$ep" --advertise-client-urls "$ep" \
    --listen-peer-urls "http://127.0.0.1:$pp" --listen-metrics-url "" >"$WORK/log" 2>&1 &
pid=$!
trap 'kill $pid 2>/dev/null || true' EXIT
for _ in $(seq 100); do curl -sf "$ep/health" >/dev/null && break; sleep 0.2; done
"$TARGET/release/fastetcd" --version
"$WORK/fragment" "$ep"
