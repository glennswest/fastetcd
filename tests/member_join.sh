#!/usr/bin/env bash
# A member added with `member add` and started with
# `--initial-cluster-state existing`, without `--node-id`, runs as the id
# `member add` gave it (fastetcd#232).
#
#   sc-build tests/member_join.sh            # this tree
#   sc-build tests/member_join.sh <ref>      # another build (a negative control)
#
# With etcd's own etcdctl (v3.5.17, sha256 checked) it:
#   1. starts three members n1..n3;
#   2. `member add --learner n4` through n1, starts n4 as etcd's
#      procedure does (`--initial-cluster` listing n4, `existing`, no
#      `--node-id`); n4 must report the id `member add` printed, serve a
#      write, and be promoted;
#   3. restarts n4 with the same flags: the same id, and it serves;
#   4. `member update` moves n4 to a new peer URL; n4 restarted there
#      keeps its id and serves.
# Before #232 n4 ran as the hash of its --name, an id the membership did
# not have: it served nothing.
# Exit status: the number of failed checks.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
REF=${1:-}
WORK=$ROOT/tmp/member-join
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
    (cd "$WORK/src" && CARGO_TARGET_DIR=$TARGET/mj-ref cargo build -q --release --locked -p fastetcd-server)
    git worktree remove --force "$WORK/src"
    BIN=$TARGET/mj-ref/release/fastetcd
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

# ---- 2. member add n4, start it as etcd's procedure does ----------------------
added=$(on 1 member add n4 --learner --peer-urls="http://127.0.0.1:${PP[4]}")
ID4=$(echo "$added" | awk '/^Member/ {print $2; exit}')
echo "== n4 added as $ID4"
N4=(--initial-cluster "$CLUSTER,n4=http://127.0.0.1:${PP[4]}" --initial-cluster-state existing)
start 4 "${N4[@]}"
wait_healthy 4 || true
check "n4 runs as the id member add gave it ($(id_of 4 || echo none) = $ID4)" "$([ "$(id_of 4 || true)" = "$ID4" ] && echo 1 || echo 0)"
on 1 put /k v1 >/dev/null
check "n4 serves a write once added" "$(serves 4 /k v1 && echo 1 || echo 0)"
promoted=0
for _ in $(seq 50); do on 1 member promote "$ID4" >/dev/null 2>&1 && { promoted=1; break; }; sleep 0.2; done
check "n4 promoted to a voter" "$promoted"
grep -E "member add. gave its peer URL" "$WORK/log4" | tail -1 || true

# ---- 3. restart n4 with the same flags ------------------------------------------
stop 4
start 4 "${N4[@]}"
wait_healthy 4 || true
check "n4 keeps its id after a restart ($(id_of 4 || echo none))" "$([ "$(id_of 4 || true)" = "$ID4" ] && echo 1 || echo 0)"
on 1 put /k v2 >/dev/null
check "n4 serves a write after a restart" "$(serves 4 /k v2 && echo 1 || echo 0)"

# ---- 4. member update n4 to a new peer URL, restart it there --------------------
NEWPP=$(port)
on 1 member update "$ID4" --peer-urls="http://127.0.0.1:$NEWPP" >/dev/null
stop 4
PP[4]=$NEWPP
N4=(--initial-cluster "$CLUSTER,n4=http://127.0.0.1:${PP[4]}" --initial-cluster-state existing)
start 4 "${N4[@]}"
wait_healthy 4 || true
check "n4 keeps its id at its new peer URL ($(id_of 4 || echo none))" "$([ "$(id_of 4 || true)" = "$ID4" ] && echo 1 || echo 0)"
on 1 put /k v3 >/dev/null
check "n4 serves a write at its new peer URL" "$(serves 4 /k v3 && echo 1 || echo 0)"
grep -E "its raft membership has at its peer URL" "$WORK/log4" | tail -1 || true

echo "== $FAILED failed"
exit "$FAILED"
