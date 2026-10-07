#!/usr/bin/env bash
# fastetcd-migrate end to end, from a real etcd (#60).
#
#   sc-build 'tests/migrate_e2e.sh [etcd-version]'     default v3.5.17
#
# Downloads the etcd release (sha256 from its SHA256SUMS), writes keys,
# leases (one short, to watch it expire after the move), roles and users
# into it with etcdctl, enables auth, saves a snapshot, migrates it with
# fastetcd-migrate, starts fastetcd on the result, and checks with etcd's
# own etcdctl: keys, leases and their keys, users logging in with their
# etcd passwords, RBAC enforced, the short lease expiring.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
ETCD_VERSION=${1:-v3.5.17}
WORK=$ROOT/tmp/migrate-e2e
rm -rf "$WORK"
mkdir -p "$WORK"

ETCD_TGZ=etcd-$ETCD_VERSION-linux-amd64.tar.gz
ETCD_URL=https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION
curl -sSfL -o "$WORK/$ETCD_TGZ" "$ETCD_URL/$ETCD_TGZ"
curl -sSfL -o "$WORK/SHA256SUMS" "$ETCD_URL/SHA256SUMS"
(cd "$WORK" && grep " $ETCD_TGZ\$" SHA256SUMS | sha256sum -c -)
tar -xzf "$WORK/$ETCD_TGZ" -C "$WORK"
ETCD=$WORK/etcd-$ETCD_VERSION-linux-amd64/etcd
CTL=$WORK/etcd-$ETCD_VERSION-linux-amd64/etcdctl

cargo build --release --locked -p fastetcd-server -p fastetcd-migrate
T=${CARGO_TARGET_DIR:-$ROOT/target}/release

port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
FAILED=0
check() { # check <what> <expected> <actual>
    if [ "$2" = "$3" ]; then echo "ok    $1"; else echo "FAIL  $1: expected [$2], got [$3]"; FAILED=1; fi
}
wait_up() {
    for _ in $(seq 100); do curl -sf "$1/health" >/dev/null && return 0; sleep 0.2; done
    echo "not up: $1"; return 1
}

# ---- etcd: data, leases, auth ---------------------------------------------
EP=http://127.0.0.1:$(port)
"$ETCD" --data-dir "$WORK/etcd" --listen-client-urls "$EP" --advertise-client-urls "$EP" \
    --listen-peer-urls "http://127.0.0.1:$(port)" >"$WORK/etcd.log" 2>&1 &
EPID=$!
wait_up "$EP"
e() { "$CTL" --endpoints "$EP" "$@"; }
e put /plain p >/dev/null
e put /app/x appv >/dev/null
LONG=$(e lease grant 600 | awk '{print $2}')
SHORT=$(e lease grant 12 | awk '{print $2}')
e put /session/long l --lease="$LONG" >/dev/null
e put /session/short s --lease="$SHORT" >/dev/null
e role add root >/dev/null
e role add app >/dev/null
e role grant-permission app readwrite /app/ --prefix >/dev/null
e user add root:rootpw >/dev/null      # not a secret: test fixture
e user grant-role root root >/dev/null
e user add alice:alicepw >/dev/null    # not a secret: test fixture
e user grant-role alice app >/dev/null
e auth enable >/dev/null
e --user root:rootpw snapshot save "$WORK/snap.db" >/dev/null
kill "$EPID"; wait "$EPID" 2>/dev/null || true
echo "== etcd $ETCD_VERSION: 4 keys, leases $LONG (600 s) and $SHORT (12 s), users root and alice, auth on; snapshot $(stat -c %s "$WORK/snap.db") bytes"

# ---- migrate, start fastetcd -------------------------------------------------
"$T/fastetcd-migrate" --from "$WORK/snap.db" --to "$WORK/fastetcd"
FEP=http://127.0.0.1:$(port)
"$T/fastetcd" --data-dir "$WORK/fastetcd" --listen-client-urls "$FEP" \
    --listen-peer-urls "http://127.0.0.1:$(port)" --listen-metrics-url "" >"$WORK/fastetcd.log" 2>&1 &
FPID=$!
trap 'kill $FPID 2>/dev/null || true' EXIT
wait_up "$FEP"
STARTED=$(date +%s)
f() { "$CTL" --endpoints "$FEP" "$@"; }

# ---- checks, with etcdctl ------------------------------------------------------
check "root logs in with its etcd password" "p" "$(f --user root:rootpw get /plain --print-value-only 2>&1)"
check "alice reads /app/ with her etcd password" "appv" "$(f --user alice:alicepw get /app/x --print-value-only 2>&1)"
check "a wrong password is refused" "refused" "$(f --user alice:nope get /app/x >/dev/null 2>&1 && echo accepted || echo refused)"
DENIED=$(f --user alice:alicepw get /plain 2>&1 || true)
echo "   alice get /plain: $DENIED"
check "alice is denied /plain (RBAC came across)" "denied" \
    "$(echo "$DENIED" | grep -qiE 'permission ?denied' && echo denied || echo allowed)"
check "no token is refused (auth stayed on)" "refused" "$(f get /plain >/dev/null 2>&1 && echo accepted || echo refused)"
check "both leases came across" "2" "$(f --user root:rootpw lease list | sed -n 's/^found \([0-9]*\) leases$/\1/p')"
check "the long lease holds its key" "/session/long" \
    "$(f --user root:rootpw lease timetolive "$LONG" --keys | grep -o '/session/long')"
check "the long lease kept its TTL" "600" \
    "$(f --user root:rootpw lease timetolive "$LONG" | sed -n 's/.*granted with TTL(\([0-9]*\)s).*/\1/p')"
check "the short lease's key is there at first" "s" "$(f --user root:rootpw get /session/short --print-value-only)"
# etcd restores a lease with its full TTL (no checkpointing): 12 s from
# fastetcd's start, plus the expiry tick.
for _ in $(seq 40); do
    [ -z "$(f --user root:rootpw get /session/short --print-value-only)" ] && break
    sleep 1
done
check "the short lease expired, and its key with it, after the move" "" \
    "$(f --user root:rootpw get /session/short --print-value-only)"
echo "   (expired $(( $(date +%s) - STARTED )) s after fastetcd started)"
check "the long lease's key is still there" "l" "$(f --user root:rootpw get /session/long --print-value-only)"
exit "$FAILED"
