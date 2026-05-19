#!/usr/bin/env bash
# Optional wire-compat smoke test using upstream etcdctl.
#
# Usage:
#   ./tests/etcdctl_smoke.sh              # auto-detect etcdctl
#   ETCDCTL=/path/to/etcdctl ./tests/etcdctl_smoke.sh
#
# Exits non-zero on any failure. Designed to be cheap (~10s) and
# diagnose obvious wire-protocol breakages a unit test would miss.
# For deeper validation, point etcd-io/etcd/tests/robustness at the
# same fastetcd process (see docs/02-testing.md).

set -euo pipefail

ETCDCTL="${ETCDCTL:-etcdctl}"
PORT="${PORT:-23790}"
PEER_PORT="${PEER_PORT:-23800}"

if ! command -v "${ETCDCTL}" >/dev/null 2>&1; then
    echo "etcdctl not found on PATH. Set ETCDCTL=/path/to/etcdctl or install it." >&2
    echo "Tip: 'go install go.etcd.io/etcd/etcdctl/v3@latest' from a recent etcd checkout." >&2
    exit 2
fi

ENDPOINT="127.0.0.1:${PORT}"
DATA_DIR="$(mktemp -d)"
trap 'rm -rf "${DATA_DIR}"; if [[ -n "${FASTETCD_PID:-}" ]]; then kill "${FASTETCD_PID}" 2>/dev/null || true; fi' EXIT

echo ">> Building fastetcd..."
cargo build --release -p fastetcd-server --bin fastetcd >/dev/null

echo ">> Starting fastetcd on ${ENDPOINT}..."
target/release/fastetcd \
    --name smoke-node \
    --data-dir "${DATA_DIR}" \
    --listen-client-url "${ENDPOINT}" \
    --listen-peer-url "127.0.0.1:${PEER_PORT}" \
    >"${DATA_DIR}/fastetcd.log" 2>&1 &
FASTETCD_PID=$!

# Wait up to 5s for the client port to accept.
for _ in $(seq 1 50); do
    if "${ETCDCTL}" --endpoints="${ENDPOINT}" endpoint health >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done

run() {
    echo "   $* "
    "${ETCDCTL}" --endpoints="${ENDPOINT}" "$@"
}

echo ">> put / get / del"
run put foo bar
run get foo
run del foo
run get foo

echo ">> range with prefix"
run put app/a 1
run put app/b 2
run put app/c 3
run get app/ --prefix --keys-only

echo ">> txn"
run put counter 0
run txn <<'TXN'
value("counter") = "0"

put counter "1"


TXN
run get counter

echo ">> lease grant/keepalive/revoke"
LEASE_ID="$(run lease grant 30 | awk '{print $2}')"
run put ephemeral hi --lease="${LEASE_ID}"
run lease timetolive "${LEASE_ID}" --keys
run lease revoke "${LEASE_ID}"
run get ephemeral

echo ">> member list"
run member list

echo ">> endpoint status"
run endpoint status --write-out=table

echo "OK: etcdctl smoke passed."
