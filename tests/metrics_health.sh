#!/usr/bin/env bash
# Health routes on the metrics listener (fastetcd#133, stormcos#192).
#
#   tests/metrics_health.sh
#
# Starts this tree's fastetcd as a StormCOS node runs it: the client port
# mutual TLS (--client-cert-auth), the metrics listener plain HTTP. A
# probe holding no client certificate cannot reach /livez on the client
# port (the handshake fails); on the metrics port it gets /health,
# /livez, /readyz and the per-check routes with the client port's
# answers, and /metrics still works. Run it through sc-build:
# `sc-build tests/metrics_health.sh`.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/metrics-health
rm -rf "$WORK"
mkdir -p "$WORK"

cargo build --locked -p fastetcd-server
BIN=${CARGO_TARGET_DIR:-$ROOT/target}/debug/fastetcd

P=$WORK/pki
mkdir -p "$P"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=ca" \
    -keyout "$P/ca.key" -out "$P/ca.pem" 2>/dev/null
leaf() { # leaf <name> <cn> <extensions>
    openssl req -newkey rsa:2048 -nodes -subj "/CN=$2" \
        -keyout "$P/$1.key" -out "$P/$1.csr" 2>/dev/null
    printf '%s\n' "$3" >"$P/$1.ext"
    openssl x509 -req -in "$P/$1.csr" -CA "$P/ca.pem" -CAkey "$P/ca.key" -CAcreateserial \
        -days 2 -extfile "$P/$1.ext" -out "$P/$1.pem" 2>/dev/null
}
leaf server fastetcd "subjectAltName=IP:127.0.0.1,DNS:localhost
extendedKeyUsage=serverAuth"
leaf probe root "extendedKeyUsage=clientAuth"

port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
CP=$(port); PP=$(port); MP=$(port)
CLIENT=https://127.0.0.1:$CP
METRICS=http://127.0.0.1:$MP
"$BIN" --data-dir "$WORK/data" --listen-client-urls "$CLIENT" --advertise-client-urls "$CLIENT" \
    --listen-peer-urls "http://127.0.0.1:$PP" --initial-advertise-peer-urls "http://127.0.0.1:$PP" \
    --initial-cluster "default=http://127.0.0.1:$PP" \
    --cert-file "$P/server.pem" --key-file "$P/server.key" \
    --trusted-ca-file "$P/ca.pem" --client-cert-auth \
    --listen-metrics-url "127.0.0.1:$MP" >"$WORK/fastetcd.log" 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT

WITH_CERT=(curl -s --cacert "$P/ca.pem" --cert "$P/probe.pem" --key "$P/probe.key")
for _ in $(seq 150); do
    [ "$("${WITH_CERT[@]}" "$CLIENT/readyz" || true)" = "ok" ] && break
    sleep 0.2
done

FAILED=0
ok() { echo "ok    $1"; }
fail() { echo "FAIL  $1"; FAILED=1; }
# check <what> <expected code> <expected body or ''> <curl args...>
check() {
    local what=$1 code=$2 body=$3; shift 3
    local out got
    out=$(curl -s -w '\n%{http_code}' "$@" || true)
    got=${out##*$'\n'}
    out=${out%$'\n'*}
    if [ "$got" != "$code" ]; then
        fail "$what: HTTP $got, want $code: $out"
    elif [ -n "$body" ] && [ "$out" != "$body" ]; then
        fail "$what: body '$out', want '$body'"
    else
        ok "$what ($got)"
    fi
}

echo "== client port $CLIENT (mutual TLS), metrics $METRICS"
if curl -s -k "$CLIENT/livez" >/dev/null 2>&1; then
    fail "a probe with no client certificate got through the client port"
else
    ok "client port refuses a probe with no client certificate"
fi
check "client port /livez with a certificate" 200 ok \
    --cacert "$P/ca.pem" --cert "$P/probe.pem" --key "$P/probe.key" "$CLIENT/livez"

check "metrics /health" 200 '{"health":"true","reason":""}' "$METRICS/health"
check "metrics /health?serializable=true" 200 '' "$METRICS/health?serializable=true"
check "metrics /livez" 200 ok "$METRICS/livez"
check "metrics /readyz" 200 ok "$METRICS/readyz"
check "metrics /readyz/linearizable_read" 200 ok "$METRICS/readyz/linearizable_read"
check "metrics /livez/serializable_read" 200 ok "$METRICS/livez/serializable_read"
check "metrics /readyz/nonsense" 404 '' "$METRICS/readyz/nonsense"
check "metrics /nonsense" 404 'not found' "$METRICS/nonsense"
for path in /health '/readyz?verbose' '/livez?verbose'; do
    a=$("${WITH_CERT[@]}" "$CLIENT$path")
    b=$(curl -s "$METRICS$path")
    if [ "$a" = "$b" ]; then ok "$path: same answer on both ports"; else fail "$path: client '$a' vs metrics '$b'"; fi
done
if curl -s "$METRICS/metrics" | grep -q '^etcd_server_has_leader 1$'; then
    ok "metrics /metrics still served"
else
    fail "metrics /metrics: no etcd_server_has_leader 1"
fi
if curl -s "$METRICS/metrics" | grep -qE '^etcd_server_health_success_total [1-9]'; then
    ok "health checks on the metrics port are counted"
else
    fail "etcd_server_health_success_total did not count"
fi

if [ "$FAILED" != 0 ]; then
    echo "--- fastetcd.log"; tail -30 "$WORK/fastetcd.log"
    exit 1
fi
echo "all passed"
