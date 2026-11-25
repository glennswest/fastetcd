#!/usr/bin/env bash
# Replication to a member that is down backs off (fastetcd#226).
#
#   sc-build tests/replication_backoff.sh            # this tree
#   sc-build tests/replication_backoff.sh <ref>      # another build (a negative control)
#
# With etcd's own etcdctl (v3.5.17, sha256 checked) it:
#   1. starts three members n1..n3 and writes once;
#   2. adds a learner at a peer URL nothing listens on, never started (the
#      case #226 was found in, #208), and counts the replication errors
#      the leader logs about it over 5 s: at most 25 (about 2 a second,
#      the backoff's 500 ms, plus its first steps);
#   3. stops a follower and counts the same way about it;
#   4. starts it again, writes through the leader, and checks the member
#      serves that write within 3 s.
# Before #226 the leader retried a member it could not dial about every
# millisecond, logging thousands of errors in 5 s. (A member stopped
# behind a connection already made was retried about 3 times a second
# before too: tonic's own reconnects pace it.)
# Exit status: the number of failed checks.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
REF=${1:-}
WORK=$ROOT/tmp/replication-backoff
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
    (cd "$WORK/src" && CARGO_TARGET_DIR=$TARGET/rb-ref cargo build -q --release --locked -p fastetcd-server)
    git worktree remove --force "$WORK/src"
    BIN=$TARGET/rb-ref/release/fastetcd
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

# ---- 2. a learner nothing listens for -------------------------------------------
DOWN=""
errors() { # errors() [target]: replication errors logged by the members up
    local n=0 i
    for i in 1 2 3; do
        [ "$i" = "$DOWN" ] && continue
        n=$((n + $(grep -c "error replication to target=${1:-}" "$WORK/log$i" || true)))
    done
    echo "$n"
}
added=$(on 1 member add ghost --learner --peer-urls="http://127.0.0.1:$(port)")
GHOST=$(echo "$added" | awk '/^Member/ {print $2; exit}')
GHOST_DEC=$(printf '%d' "0x$GHOST")
echo "== learner $GHOST ($GHOST_DEC) added, never started"
sleep 1
before=$(errors "$GHOST_DEC")
sleep 5
after=$(errors "$GHOST_DEC")
n=$((after - before))
echo "== $n replication errors about it in 5 s"
check "replication to a member nothing listens for backs off ($n errors in 5 s, at most 25)" "$([ "$n" -le 25 ] && echo 1 || echo 0)"
on 1 member remove "$GHOST" >/dev/null || true

# ---- 3. stop a follower, count the errors about it ------------------------------
LEADER=$(leader_of 1)
for i in 1 2 3; do [ "$(id_of "$i")" != "$LEADER" ] && { DOWN=$i; break; }; done
DOWN_DEC=$(printf '%d' "0x$(id_of "$DOWN")")
echo "== leader $LEADER; stopping n$DOWN"
stop "$DOWN"
sleep 1
before=$(errors "$DOWN_DEC")
sleep 5
after=$(errors "$DOWN_DEC")
n=$((after - before))
echo "== $n replication errors about it in 5 s"
check "replication to a stopped member backs off ($n errors in 5 s, at most 25)" "$([ "$n" -le 25 ] && echo 1 || echo 0)"

# ---- 4. start it again: it catches up promptly -----------------------------------
start "$DOWN" --initial-cluster "$CLUSTER"
for _ in $(seq 100); do on "$DOWN" endpoint status >/dev/null 2>&1 && break; sleep 0.1; done
up=$(date +%s%N)
for i in 1 2 3; do [ "$(id_of "$i" 2>/dev/null || true)" = "$LEADER" ] && W=$i; done
on "${W:-1}" put /k v1 >/dev/null
caught=0
for _ in $(seq 30); do
    if [ "$(on "$DOWN" get /k --consistency=s --print-value-only 2>/dev/null)" = v1 ]; then caught=1; break; fi
    sleep 0.1
done
ms=$(( ($(date +%s%N) - up) / 1000000 ))
echo "== n$DOWN served the write ${ms} ms after it answered"
check "the restarted member serves a new write within 3 s (${ms} ms)" "$caught"

echo "== $FAILED failed"
exit "$FAILED"
