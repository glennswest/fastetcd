#!/usr/bin/env bash
# A member added at runtime is replicated to by every leader, before and
# after the others restart with unchanged flags (fastetcd#211, finding
# 77b6d12bfcd4).
#
#   sc-build tests/member_routes.sh            # this tree
#   sc-build tests/member_routes.sh <ref>      # another build (a negative control)
#
# With etcd's own etcdctl (v3.5.17, sha256 checked) it:
#   1. starts three members n1..n3 (`--initial-cluster` names only them);
#   2. `member add --learner n4` through n1, starts n4 and promotes it (`existing`, its id as
#      `--node-id`); waits until n4 serves a write;
#   3. moves leadership to an original member other than the one that
#      served the MemberAdd, writes through it, and checks n4 serves it;
#      every original member's MemberList has n4;
#   4. restarts n1..n3 with the same flags, moves leadership to an
#      original member, writes, checks n4 serves it and every member
#      lists n4.
# Before #211 raft dialed through a map seeded from `--initial-cluster`
# and changed only by the Cluster RPC a member served itself, so step 3
# (and 4) had no route to n4.
# Exit status: the number of failed checks.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
REF=${1:-}
WORK=$ROOT/tmp/member-routes
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
    (cd "$WORK/src" && CARGO_TARGET_DIR=$TARGET/mr-ref cargo build -q --release --locked -p fastetcd-server)
    git worktree remove --force "$WORK/src"
    BIN=$TARGET/mr-ref/release/fastetcd
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
CLUSTER=""
for i in 1 2 3 4; do
    CP[$i]=$(port); PP[$i]=$(port)
done
for i in 1 2 3; do CLUSTER+="${CLUSTER:+,}n$i=http://127.0.0.1:${PP[$i]}"; done
EXTRA=() # n4's own flags

start() { # start <i> [extra flags]
    local i=$1; shift
    "$BIN" --name "n$i" --data-dir "$WORK/data$i" \
        --listen-client-urls "http://127.0.0.1:${CP[$i]}" --advertise-client-urls "http://127.0.0.1:${CP[$i]}" \
        --listen-peer-urls "http://127.0.0.1:${PP[$i]}" --initial-advertise-peer-urls "http://127.0.0.1:${PP[$i]}" \
        --initial-cluster-token routes --listen-metrics-url "" "$@" >>"$WORK/log$i" 2>&1 &
    PID[$i]=$!
}
stop() { kill "${PID[$1]}" 2>/dev/null || true; wait "${PID[$1]}" 2>/dev/null || true; }
cleanup() { for i in 1 2 3 4; do [ -n "${PID[$i]:-}" ] && kill "${PID[$i]}" 2>/dev/null || true; done; }
trap cleanup EXIT
on() { local i=$1; shift; "$CTL" --endpoints "http://127.0.0.1:${CP[$i]}" --command-timeout=5s "$@"; }

FAILED=0
check() { # check <what> <ok?>
    if [ "$2" = 1 ]; then echo "ok    $1"; else echo "FAIL  $1"; FAILED=$((FAILED + 1)); fi
}
status() { # status <i>: "<member id hex> <leader id hex>"
    on "$1" endpoint status -w json 2>/dev/null |
        python3 -c 'import json,sys; s=json.load(sys.stdin)[0]["Status"]; print("%x %x" % (s["header"]["member_id"], s["leader"]))'
}
leader_of() { # leader_of <i>: the leader's id (hex) as member i sees it
    status "$1" | awk '{print $2}'
}
id_of() { status "$1" | awk '{print $1}'; }
serves() { # serves <i> <key> <value>: within 15 s, from its own state
    for _ in $(seq 75); do
        [ "$(on "$1" get "$2" --consistency=s --print-value-only 2>/dev/null)" = "$3" ] && return 0
        sleep 0.2
    done
    return 1
}
lists_n4() { # lists_n4 <i>: member i's MemberList has n4's peer URL
    on "$1" member list 2>/dev/null | grep -q "http://127.0.0.1:${PP[4]}"
}
move_to() { # move_to <i>: leadership onto member i (an original)
    local want leader j
    want=$(id_of "$1")
    for _ in $(seq 50); do
        leader=$(leader_of "$1" || true)
        [ "$leader" = "$want" ] && return 0
        for j in 1 2 3 4; do
            if [ "$(id_of "$j" 2>/dev/null || true)" = "$leader" ]; then
                on "$j" move-leader "$want" >/dev/null 2>&1 || true
            fi
        done
        sleep 0.3
    done
    return 1
}
wait_healthy() { for _ in $(seq 150); do on "$1" endpoint health >/dev/null 2>&1 && return 0; sleep 0.2; done; return 1; }

# ---- 1. three members --------------------------------------------------------
for i in 1 2 3; do start "$i" --initial-cluster "$CLUSTER"; done
for i in 1 2 3; do wait_healthy "$i"; done
on 1 put /k v0 >/dev/null

# ---- 2. member add n4 through n1, start it ---------------------------------
# As a learner, then promoted once it runs: the order 03-deploy gives
# for a log that has batched (#77).
added=$(on 1 member add n4 --learner --peer-urls="http://127.0.0.1:${PP[4]}")
ID4=$(echo "$added" | awk '/^Member/ {print $2; exit}')
echo "== n4 added as $ID4"
EXTRA=(--initial-cluster "$CLUSTER,n4=http://127.0.0.1:${PP[4]}" --initial-cluster-state existing --node-id "$(printf '%d' "0x$ID4")")
start 4 "${EXTRA[@]}"
on 1 put /k v1 >/dev/null
check "n4 serves a write once added" "$(serves 4 /k v1 && echo 1 || echo 0)"
promoted=0
for _ in $(seq 50); do on 1 member promote "$ID4" >/dev/null 2>&1 && { promoted=1; break; }; sleep 0.2; done
check "n4 promoted to a voter" "$promoted"

# ---- 3. another original member leads ---------------------------------------
# Not n1, which served the MemberAdd (its own map had n4's route before #211).
move_to 2 || true
check "n2 leads ($(leader_of 2 || echo none))" "$([ "$(leader_of 2 || true)" = "$(id_of 2)" ] && echo 1 || echo 0)"
on 2 put /k v2 >/dev/null || true
check "n4 serves a write made under n2's leadership" "$(serves 4 /k v2 && echo 1 || echo 0)"
for i in 1 2 3; do
    check "n$i's MemberList has n4" "$(lists_n4 "$i" && echo 1 || echo 0)"
done

# ---- 4. restart the originals with unchanged flags --------------------------
for i in 1 2 3; do
    stop "$i"
    start "$i" --initial-cluster "$CLUSTER"
    wait_healthy "$i" || true
done
move_to 3 || true
check "n3 leads after the restarts ($(leader_of 3 || echo none))" "$([ "$(leader_of 3 || true)" = "$(id_of 3)" ] && echo 1 || echo 0)"
on 3 put /k v3 >/dev/null || true
check "n4 serves a write after the originals restarted" "$(serves 4 /k v3 && echo 1 || echo 0)"
for i in 1 2 3; do
    check "n$i's MemberList has n4 after its restart" "$(lists_n4 "$i" && echo 1 || echo 0)"
done

if [ "$FAILED" -gt 0 ]; then
    for i in 1 2 3 4; do echo "--- log$i (tail)"; tail -15 "$WORK/log$i"; done
fi
echo "== $FAILED failed"
exit "$FAILED"
