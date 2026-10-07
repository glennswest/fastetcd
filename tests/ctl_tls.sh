#!/usr/bin/env bash
# fastetcd-ctl over a TLS client port (fastetcd#59).
#
#   sc-build tests/ctl_tls.sh
#
# Makes a CA, a server certificate for 127.0.0.1 and client certificates
# (CN root, CN alice) with openssl, then starts this tree's fastetcd:
#
#   1. --cert-file/--key-file only: every fastetcd-ctl command with
#      --cacert, a scheme-less endpoint, the ETCDCTL_CACERT env var.
#   2. --client-cert-auth: refused without a client certificate, every
#      command with one; auth on (via the v3 gateway), the certificate's
#      CN is the user (root may run `auth members`, alice may not), and
#      --user's token goes on every command.
#
# and the refusals: http:// against a TLS port, https:// without
# --cacert, TLS options with http://, --cert without --key, the wrong CA.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/ctl-tls
rm -rf "$WORK"
mkdir -p "$WORK"

cargo build --locked -p fastetcd-server -p fastetcd-ctl
T=${CARGO_TARGET_DIR:-$ROOT/target}/debug
BIN=$T/fastetcd
CTL=$T/fastetcd-ctl

# ---- certificates ------------------------------------------------------------
P=$WORK/pki
mkdir -p "$P"
ca() { # ca <name>
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=$1" \
        -keyout "$P/$1.key" -out "$P/$1.pem" 2>/dev/null
}
leaf() { # leaf <name> <cn> <ca> <extensions>
    openssl req -newkey rsa:2048 -nodes -subj "/CN=$2" \
        -keyout "$P/$1.key" -out "$P/$1.csr" 2>/dev/null
    printf '%s\n' "$4" >"$P/$1.ext"
    openssl x509 -req -in "$P/$1.csr" -CA "$P/$3.pem" -CAkey "$P/$3.key" -CAcreateserial \
        -days 2 -extfile "$P/$1.ext" -out "$P/$1.pem" 2>/dev/null
}
ca ca
ca other-ca
leaf server fastetcd ca "subjectAltName=IP:127.0.0.1,DNS:localhost
extendedKeyUsage=serverAuth"
leaf root root ca "extendedKeyUsage=clientAuth"
leaf alice alice ca "extendedKeyUsage=clientAuth"

port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
FAILED=0
ok() { echo "ok    $1"; }
fail() { echo "FAIL  $1"; FAILED=1; }
# expect_ok <what> <command...>: it succeeds; prints its output.
expect_ok() {
    local what=$1; shift
    local out
    if out=$("$@" 2>&1); then ok "$what"; else fail "$what: $out"; fi
    printf '%s\n' "$out" | sed 's/^/        /' | head -12
}
# expect_err <what> <pattern> <command...>: it fails, saying <pattern>.
expect_err() {
    local what=$1 pat=$2; shift 2
    local out
    if out=$("$@" 2>&1); then
        fail "$what: succeeded: $out"
    elif printf '%s' "$out" | grep -qiE "$pat"; then
        ok "$what"
    else
        fail "$what: no /$pat/ in: $out"
    fi
    printf '%s\n' "$out" | grep -iE "$pat" | head -2 | sed 's/^/        /'
}

PID=
start() { # start <name> <extra flags...>
    local name=$1; shift
    CP=$(port)
    EP=https://127.0.0.1:$CP
    "$BIN" --name "$name" --data-dir "$WORK/$name" --listen-client-urls "$EP" \
        --advertise-client-urls "$EP" --listen-peer-urls "http://127.0.0.1:$(port)" \
        --listen-metrics-url "" --cert-file "$P/server.pem" --key-file "$P/server.key" "$@" \
        >"$WORK/$name.log" 2>&1 &
    PID=$!
    for _ in $(seq 150); do
        "$CTL" --endpoint "$EP" --cacert "$P/ca.pem" --cert "$P/root.pem" --key "$P/root.key" \
            status >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    echo "not up: $EP"; tail -20 "$WORK/$name.log"; exit 1
}
trap '[ -n "$PID" ] && kill $PID 2>/dev/null || true' EXIT

# ---- 1. server TLS only ------------------------------------------------------
start tls
echo "== member with --cert-file at $EP"
C=("$CTL" --endpoint "$EP" --cacert "$P/ca.pem")
expect_ok "status over TLS (--cacert)" "${C[@]}" status
expect_ok "put" "${C[@]}" put /k v1
expect_ok "get" "${C[@]}" get /k
[ "$("${C[@]}" get /k | sed -n 2p)" = v1 ] && ok "get returns the value" || fail "get returns the value"
expect_ok "del --prefix" "${C[@]}" del / --prefix
REV=$("${C[@]}" put /k v2 | sed -n 's/^OK rev=//p')
expect_ok "compact" "${C[@]}" compact "$REV"
expect_ok "alarm" "${C[@]}" alarm
expect_ok "defrag" "${C[@]}" defrag
expect_ok "snapshot-save" "${C[@]}" snapshot-save "$WORK/snap.backup"
expect_ok "auth members" "${C[@]}" auth members
expect_ok "a scheme-less endpoint is https:// with TLS options" \
    "$CTL" --endpoint "127.0.0.1:$CP" --cacert "$P/ca.pem" status
expect_ok "ETCDCTL_CACERT" env ETCDCTL_CACERT="$P/ca.pem" "$CTL" --endpoint "$EP" status
expect_err "https:// without --cacert is refused, naming the flag" "no --cacert" \
    "$CTL" --endpoint "$EP" status
expect_err "TLS options with an http:// endpoint are refused" "not https://" \
    "$CTL" --endpoint "http://127.0.0.1:$CP" --cacert "$P/ca.pem" status
expect_err "a plaintext dial of the TLS port fails" "connect|transport|h2|protocol|broken pipe|reset" \
    "$CTL" --endpoint "http://127.0.0.1:$CP" status
expect_err "a certificate from another CA is not trusted" "connect|certificate|invalid|unknown" \
    "$CTL" --endpoint "$EP" --cacert "$P/other-ca.pem" status
expect_err "--cert without --key is refused" "--cert needs --key" \
    "${C[@]}" --cert "$P/root.pem" status
kill "$PID"; wait "$PID" 2>/dev/null || true

# ---- 2. --client-cert-auth ---------------------------------------------------
start mtls --trusted-ca-file "$P/ca.pem" --client-cert-auth
echo "== member with --client-cert-auth at $EP"
R=("$CTL" --endpoint "$EP" --cacert "$P/ca.pem" --cert "$P/root.pem" --key "$P/root.key")
A=("$CTL" --endpoint "$EP" --cacert "$P/ca.pem" --cert "$P/alice.pem" --key "$P/alice.key")
expect_err "no client certificate: the handshake is refused" "connect|certificate|transport|handshake|reset|broken pipe" \
    "$CTL" --endpoint "$EP" --cacert "$P/ca.pem" status
expect_ok "status with a client certificate" "${R[@]}" status
expect_ok "put" "${R[@]}" put /app/x 1
expect_ok "get" "${R[@]}" get /app/x
expect_ok "snapshot-save" "${R[@]}" snapshot-save "$WORK/snap2.backup"
expect_ok "ETCDCTL_CERT / ETCDCTL_KEY" env ETCDCTL_CACERT="$P/ca.pem" ETCDCTL_CERT="$P/root.pem" \
    ETCDCTL_KEY="$P/root.key" "$CTL" --endpoint "$EP" status

# Auth on, through the v3 gateway (curl, as the root certificate).
gw() { curl -sSf --cacert "$P/ca.pem" --cert "$P/root.pem" --key "$P/root.key" -d "$2" "$EP$1"; }
gw /v3/auth/role/add '{"name":"root"}' >/dev/null
gw /v3/auth/user/add '{"name":"root","password":"rootpw"}' >/dev/null   # not a secret: test fixture
gw /v3/auth/user/grant '{"user":"root","role":"root"}' >/dev/null
gw /v3/auth/user/add '{"name":"alice","password":"alicepw"}' >/dev/null # not a secret: test fixture
gw /v3/auth/enable '{}' >/dev/null
echo "== auth enabled: users root, alice (no roles)"
expect_ok "CN root runs auth members (root only)" "${R[@]}" auth members
expect_ok "CN root runs defrag (root only)" "${R[@]}" defrag
expect_err "CN alice may not run auth members" "permission denied" "${A[@]}" auth members
expect_err "CN alice may not put" "permission denied" "${A[@]}" put /app/x 2
expect_ok "--user root's token wins over CN alice, on put" "${A[@]}" --user root:rootpw put /app/x 3
expect_ok "--user root's token wins over CN alice, on status" "${A[@]}" --user root:rootpw alarm
expect_err "a wrong --user password is refused" "authentication failed|invalid" \
    "${R[@]}" --user root:nope status
[ "$("${R[@]}" get /app/x | sed -n 2p)" = 3 ] && ok "the put as root landed" || fail "the put as root landed"

echo
[ "$FAILED" = 0 ] && echo "ctl_tls: all checks passed" || echo "ctl_tls: FAILED"
exit "$FAILED"
