#!/usr/bin/env bash
# `etcdctl member add` of a voter right after a cluster comes up
# (fastetcd#234).
#
#   sc-build tests/member_add_voter.sh            # this tree
#   sc-build tests/member_add_voter.sh <ref>      # another build (a negative control)
#
# With etcd's own etcdctl (v3.5.17, sha256 checked), ROUNDS times (5):
# starts three fresh members n1..n3, waits until all three are healthy,
# then at once adds a voter through n1 and another through n2 (neither is
# started: a voter set of 5 with 2 down keeps its quorum). Each add must
# succeed, and the member list must show both as voters. Before #234 an
# add could fail `Unavailable` with "the cluster is already undergoing a
# configuration change".
# Exit status: the number of failed checks.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
REF=${1:-}
ROUNDS=${ROUNDS:-5}
WORK=$ROOT/tmp/member-add-voter
rm -rf "$WORK"
mkdir -p "$WORK"
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}

if [ -n "$REF" ]; then
    rev=$REF
    if ! git rev-parse -q --verify "$REF^{commit}" >/dev/null; then
        git fetch -q --depth=1 https://github.com/glennswest/fastetcd.git "$REF"
        rev=FETCH_HEAD
    fi
    git worktree add --detach "$WORK/src" "$rev" >/dev/null 2>&1
    (cd "$WORK/src" && CARGO_TARGET_DIR=$TARGET/mav-ref cargo build -q --release --locked -p fastetcd-server)
    git worktree remove --force "$WORK/src"
    BIN=$TARGET/mav-ref/release/fastetcd
else
    cargo build -q --release --locked -p fastetcd-server
    BIN=$TARGET/release/fastetcd
fi
echo "== $("$BIN" --version)${REF:+ (ref $REF)}"

ETCD_VERSION=v3.5.17
TGZ=etcd-$ETCD_VERSION-linux-amd64.tar.gz
URL=https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION
curl -sSfL -o "$WORK/$TGZ" "$URL/$TGZ"
curl -sSfL -o "$WORK/SHA256SUMS" "$URL/SHA256SUMS"
(cd "$WORK" && grep " $TGZ\$" SHA256SUMS | sha256sum -c - >/dev/null)
tar -xzf "$WORK/$TGZ" -C "$WORK"
CTL=$WORK/etcd-$ETCD_VERSION-linux-amd64/etcdctl

port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
declare -a CP PP PID
cleanup() { for i in 1 2 3; do [ -n "${PID[$i]:-}" ] && kill "${PID[$i]}" 2>/dev/null || true; done; }
trap cleanup EXIT
on() { local i=$1; shift; "$CTL" --endpoints "http://127.0.0.1:${CP[$i]}" --command-timeout=10s "$@"; }
wait_healthy() { for _ in $(seq 150); do on "$1" endpoint health >/dev/null 2>&1 && return 0; sleep 0.2; done; return 1; }

FAILED=0
check() { # check <what> <ok?>
    if [ "$2" = 1 ]; then echo "ok    $1"; else echo "FAIL  $1"; FAILED=$((FAILED + 1)); fi
}

for r in $(seq "$ROUNDS"); do
    D=$WORK/round$r
    mkdir -p "$D"
    CLUSTER=""
    for i in 1 2 3; do CP[$i]=$(port); PP[$i]=$(port); CLUSTER+="${CLUSTER:+,}n$i=http://127.0.0.1:${PP[$i]}"; done
    for i in 1 2 3; do
        "$BIN" --name "n$i" --data-dir "$D/data$i" \
            --listen-client-urls "http://127.0.0.1:${CP[$i]}" --advertise-client-urls "http://127.0.0.1:${CP[$i]}" \
            --listen-peer-urls "http://127.0.0.1:${PP[$i]}" --initial-advertise-peer-urls "http://127.0.0.1:${PP[$i]}" \
            --initial-cluster "$CLUSTER" --initial-cluster-token "voter$r" --listen-metrics-url "" >>"$D/log$i" 2>&1 &
        PID[$i]=$!
    done
    up=1
    for i in 1 2 3; do wait_healthy "$i" || up=0; done
    check "round $r: three members healthy" "$up"
    for via in 1 2; do
        url="http://127.0.0.1:$(port)"
        if out=$(on "$via" member add "v$r$via" --peer-urls="$url" 2>&1); then ok=1; else ok=0; fi
        check "round $r: member add (voter) through n$via" "$ok"
        [ "$ok" = 1 ] || echo "      $out"
        listed=0
        for _ in $(seq 25); do
            if on 1 member list -w json 2>/dev/null | python3 -c '
import json, sys
url = sys.argv[1]
ms = json.load(sys.stdin)["members"]
sys.exit(0 if any(url in m.get("peerURLs", []) and not m.get("isLearner") for m in ms) else 1)' "$url"; then
                listed=1; break
            fi
            sleep 0.2
        done
        check "round $r: the member added through n$via is listed as a voter" "$listed"
    done
    cleanup
    for i in 1 2 3; do wait "${PID[$i]}" 2>/dev/null || true; done
    if [ "$FAILED" -gt 0 ] && [ ! -e "$WORK/shown" ]; then
        touch "$WORK/shown"
        for i in 1 2 3; do echo "---- round $r log$i (membership lines)"; grep -iE "membership|learner|undergoing|change_membership|leader" "$D/log$i" | tail -25 || true; done
    fi
done
echo "== $FAILED failed"
exit "$FAILED"
