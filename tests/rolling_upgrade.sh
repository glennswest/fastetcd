#!/usr/bin/env bash
# A real rolling upgrade (or downgrade) of a 3-member cluster, one member
# process at a time (fastetcd#65).
#
#   sc-build 'tests/rolling_upgrade.sh <from-ref> [<to-ref>]'
#   sc-build 'tests/rolling_upgrade.sh tree v1.24.1'    # a downgrade
#
# <to-ref> defaults to this tree; `tree` names this tree either side. Each ref is built in a git worktree
# (fetched from GitHub if the job has no such ref). With etcd's own
# etcdctl (v3.5.17, sha256 checked) it:
#   1. starts three members on <from>: auth on (root; alice reading and
#      writing /app/ only), a 600 s lease with a key, data;
#   2. runs four writers, each putting a new key every ~50 ms through
#      all three endpoints, and logs every acknowledged one;
#   3. replaces the members one at a time: SIGTERM, then <to> on the same
#      data dir and flags, and waits until it applies new writes;
#   4. checks: every acknowledged key is there (linearizable reads); the
#      longest stretch with no acknowledged write; root and alice log in
#      and RBAC holds; the lease and its key; each replaced member wrote
#      its pre-version backup (<data-dir>/backups); batched log entries
#      (fastetcd_proposals_batched_total) stay 0 on every member while one
#      runs a fastetcd older than 1.10, and appear once none does; every
#      member applies the same last index.
# Exit status: the number of failed checks.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
FROM=${1:?usage: rolling_upgrade.sh <from-ref> [<to-ref>]}
TO=${2:-}
WORK=$ROOT/tmp/rolling-upgrade
rm -rf "$WORK"
mkdir -p "$WORK"
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
MAX_OUTAGE_S=${MAX_OUTAGE_S:-15}

# ---- binaries ---------------------------------------------------------------
build_ref() { # build_ref <ref> <out-name>: prints the binary
    local ref=$1 name=$2 rev=$1
    if ! git rev-parse -q --verify "$ref^{commit}" >/dev/null; then
        git fetch -q --depth=1 https://github.com/glennswest/fastetcd.git "$ref"
        rev=FETCH_HEAD
    fi
    git worktree add --detach "$WORK/src-$name" "$rev" >/dev/null 2>&1
    (cd "$WORK/src-$name" && CARGO_TARGET_DIR=$TARGET/ru-$name cargo build -q --release --locked -p fastetcd-server) >&2
    git worktree remove --force "$WORK/src-$name"
    echo "$TARGET/ru-$name/release/fastetcd"
}
this_tree() {
    cargo build -q --release --locked -p fastetcd-server >&2
    echo "$TARGET/release/fastetcd"
}
TO=${TO:-tree}
if [ "$FROM" = tree ]; then OLD=$(this_tree); FROM="this tree $(git rev-parse --short HEAD)"; else OLD=$(build_ref "$FROM" from); fi
if [ "$TO" = tree ]; then NEW=$(this_tree); TO="this tree $(git rev-parse --short HEAD)"; else NEW=$(build_ref "$TO" to); fi
OLD_V=$("$OLD" --version | awk '{print $2}')
NEW_V=$("$NEW" --version | awk '{print $2}')
echo "== rolling $FROM ($OLD_V) -> $TO ($NEW_V)"

ETCD_VERSION=v3.5.17
TGZ=etcd-$ETCD_VERSION-linux-amd64.tar.gz
URL=https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION
curl -sSfL -o "$WORK/$TGZ" "$URL/$TGZ"
curl -sSfL -o "$WORK/SHA256SUMS" "$URL/SHA256SUMS"
(cd "$WORK" && grep " $TGZ\$" SHA256SUMS | sha256sum -c - >/dev/null)
tar -xzf "$WORK/$TGZ" -C "$WORK"
CTL=$WORK/etcd-$ETCD_VERSION-linux-amd64/etcdctl

# ---- cluster ----------------------------------------------------------------
port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
declare -a CP PP MP PID BIN
CLUSTER=""
for i in 1 2 3; do
    CP[$i]=$(port); PP[$i]=$(port); MP[$i]=$(port)
    CLUSTER+="${CLUSTER:+,}n$i=http://127.0.0.1:${PP[$i]}"
done
EPS="http://127.0.0.1:${CP[1]},http://127.0.0.1:${CP[2]},http://127.0.0.1:${CP[3]}"

start() { # start <i> <binary>
    local i=$1 bin=$2
    BIN[$i]=$bin
    "$bin" --name "n$i" --data-dir "$WORK/data$i" \
        --listen-client-urls "http://127.0.0.1:${CP[$i]}" --advertise-client-urls "http://127.0.0.1:${CP[$i]}" \
        --listen-peer-urls "http://127.0.0.1:${PP[$i]}" --initial-advertise-peer-urls "http://127.0.0.1:${PP[$i]}" \
        --initial-cluster "$CLUSTER" --initial-cluster-token rolling \
        --listen-metrics-url "127.0.0.1:${MP[$i]}" >>"$WORK/log$i" 2>&1 &
    PID[$i]=$!
    echo "--- $(date -u +%H:%M:%S.%N | cut -c1-12) member $i: $("$bin" --version) (pid ${PID[$i]})" >>"$WORK/log$i"
}
stop() { # stop <i>
    kill "${PID[$1]}" 2>/dev/null || true
    wait "${PID[$1]}" 2>/dev/null || true
}
cleanup() {
    for i in 1 2 3; do [ -n "${PID[$i]:-}" ] && kill "${PID[$i]}" 2>/dev/null || true; done
    [ -n "${WRITERS:-}" ] && kill $WRITERS 2>/dev/null || true
}
trap cleanup EXIT
metric() { # metric <i> <name>: the value, 0 if absent
    curl -sf "http://127.0.0.1:${MP[$1]}/metrics" 2>/dev/null | awk -v n="$2" '$1==n {print $2; f=1} END {if (!f) print 0}'
}
ctl() { "$CTL" --endpoints "$EPS" --command-timeout=5s "$@"; }
root() { ctl --user root:rootpw "$@"; } # not a secret: test fixture
on() { local i=$1; shift; "$CTL" --endpoints "http://127.0.0.1:${CP[$i]}" --command-timeout=5s --user root:rootpw "$@"; } # not a secret: test fixture

FAILED=0
check() { # check <what> <ok?>
    if [ "$2" = 1 ]; then echo "ok    $1"; else echo "FAIL  $1"; FAILED=$((FAILED + 1)); fi
}

for i in 1 2 3; do start "$i" "$OLD"; done
for _ in $(seq 150); do ctl endpoint health >/dev/null 2>&1 && break; sleep 0.2; done
ctl put /plain p >/dev/null

# ---- auth, lease, data ------------------------------------------------------
# Through one member: before 1.26 an auth change through a second member
# right after one through the first could be refused as divergence while
# the third had not applied the first yet (found by this test, #65).
first() { "$CTL" --endpoints "http://127.0.0.1:${CP[1]}" --command-timeout=5s "$@"; }
first role add root >/dev/null
first user add root:rootpw >/dev/null # not a secret: test fixture
first user grant-role root root >/dev/null
first role add app >/dev/null
first role grant-permission app readwrite /app/ --prefix >/dev/null
first user add alice:alicepw >/dev/null # not a secret: test fixture
first user grant-role alice app >/dev/null
first auth enable >/dev/null
# Also through one member: before 1.23.1 a token reaching a member that
# has not applied it yet got fastetcd's own text, and etcdctl does not
# authenticate again on that (#105).
first --user root:rootpw put /app/x appv >/dev/null # not a secret: test fixture
first --user root:rootpw put /secret/s sv >/dev/null # not a secret: test fixture
LEASE=$(first --user root:rootpw lease grant 600 | awk '{print $2}') # not a secret: test fixture
first --user root:rootpw put /lease/k lv --lease="$LEASE" >/dev/null # not a secret: test fixture
echo "== $OLD_V on all three: auth on, lease $LEASE, writers starting"

# ---- writers ----------------------------------------------------------------
WRITERS=""
for w in 1 2 3 4; do
    (
        n=0
        while [ ! -e "$WORK/stop" ]; do
            n=$((n + 1))
            if root put "/roll/w$w/$(printf %07d "$n")" v >/dev/null 2>&1; then
                echo "$(date +%s.%N) $n" >>"$WORK/acked$w"
            else
                n=$((n - 1))
            fi
            sleep 0.05
        done
    ) &
    WRITERS+=" $!"
done
sleep 3

# ---- replace one member at a time ---------------------------------------------
batched_while_mixed=0
for i in 1 2 3; do
    t=$(date +%s.%N)
    stop "$i"
    start "$i" "$NEW"
    # Applying again: a write made now shows up in its own (serializable) state.
    up=0
    for _ in $(seq 300); do
        if root put "/roll/probe$i" "$t" >/dev/null 2>&1 &&
            [ "$(on "$i" get "/roll/probe$i" --consistency=s --print-value-only 2>/dev/null)" = "$t" ]; then
            up=1; break
        fi
        sleep 0.2
    done
    check "member $i on $NEW_V applies writes again ($(awk -v a="$t" -v b="$(date +%s.%N)" 'BEGIN {printf "%.1f", b - a}') s after its stop)" "$up"
    backups=$(ls "$WORK/data$i/backups" 2>/dev/null | wc -l)
    if [ "$OLD_V" != "$NEW_V" ]; then
        check "member $i wrote its pre-version backup (data$i/backups: $backups file(s))" "$([ "$backups" -ge 1 ] && echo 1 || echo 0)"
    fi
    sleep 3
    # While a member older than 1.10 is in the cluster, nothing is batched.
    if [ "$i" -lt 3 ]; then
        old_major_minor=$(echo "$OLD_V" | awk -F. '{print $1*1000+$2}')
        if [ "$old_major_minor" -lt 1010 ]; then
            for j in 1 2 3; do
                b=$(metric "$j" fastetcd_proposals_batched_total)
                [ "${b%.*}" -gt 0 ] && batched_while_mixed=1
            done
        fi
    fi
    echo "   after member $i: batched entries per member: $(for j in 1 2 3; do printf '%s ' "$(metric "$j" fastetcd_proposals_batched_total)"; done)"
    # A keep-alive across the mixed cluster (through Raft while a member
    # is older than 1.23, in the leader's RAM once none is, #92).
    ka=$(on "$i" lease keep-alive --once "$LEASE" 2>&1 | tail -1)
    check "a lease keep-alive answers after member $i ($ka)" "$(echo "$ka" | grep -q 'TTL(600)' && echo 1 || echo 0)"
done
old_mm=$(echo "$OLD_V" | awk -F. '{print $1*1000+$2}')
new_mm=$(echo "$NEW_V" | awk -F. '{print $1*1000+$2}')
if [ "$old_mm" -lt 1010 ] && [ "$new_mm" -ge 1010 ]; then
    check "no batched entry while a member was older than 1.10" "$([ "$batched_while_mixed" = 0 ] && echo 1 || echo 0)"
fi
sleep 8
touch "$WORK/stop"
wait $WRITERS 2>/dev/null || true
WRITERS=""

# ---- checks -------------------------------------------------------------------
if [ "$new_mm" -ge 1010 ]; then
    total=0
    for j in 1 2 3; do b=$(metric "$j" fastetcd_proposals_batched_total); total=$((total + ${b%.*})); done
    check "writes are batched once every member runs >= 1.10 ($total batched proposals)" "$([ "$total" -gt 0 ] && echo 1 || echo 0)"
fi
lost=0
acked=0
longest=0
for w in 1 2 3 4; do
    have=$(root get "/roll/w$w/" --prefix --keys-only | grep -c "^/roll/w$w/" || true)
    last=$(tail -1 "$WORK/acked$w" | awk '{print $2}')
    acked=$((acked + last))
    [ "$have" -lt "$last" ] && lost=$((lost + last - have))
    gap=$(awk 'NR>1 {g=$1-p; if (g>m) m=g} {p=$1} END {printf "%.1f", m}' "$WORK/acked$w")
    longest=$(awk -v a="$gap" -v b="$longest" 'BEGIN {print (a > b) ? a : b}')
done
check "every acknowledged write is there ($acked acknowledged, $lost missing)" "$([ "$lost" = 0 ] && echo 1 || echo 0)"
check "the longest stretch without an acknowledged write: ${longest} s (bound ${MAX_OUTAGE_S} s)" "$(awk -v a="$longest" -v b="$MAX_OUTAGE_S" 'BEGIN {print (a <= b) ? 1 : 0}')"
check "root logs in and reads" "$([ "$(root get /plain --print-value-only)" = p ] && echo 1 || echo 0)"
check "alice reads /app/x" "$([ "$(ctl --user alice:alicepw get /app/x --print-value-only 2>/dev/null)" = appv ] && echo 1 || echo 0)" # not a secret: test fixture
check "alice is denied /secret/s" "$(ctl --user alice:alicepw get /secret/s >/dev/null 2>&1 && echo 0 || echo 1)" # not a secret: test fixture
check "a wrong password is refused" "$(ctl --user alice:nope get /app/x >/dev/null 2>&1 && echo 0 || echo 1)" # not a secret: test fixture
ttl=$(root lease timetolive "$LEASE" --keys)
echo "   $ttl"
check "the lease is alive and holds its key" "$(echo "$ttl" | grep -q 'remaining(' && echo "$ttl" | grep -q '/lease/k' && echo 1 || echo 0)"
check "the lease's key is there" "$([ "$(root get /lease/k --print-value-only)" = lv ] && echo 1 || echo 0)"
sleep 2
applied=""
for j in 1 2 3; do applied+="$(metric "$j" etcd_server_proposals_applied_total) "; done
same=$(echo "$applied" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l)
check "every member applied the same last index ($applied)" "$([ "$same" = 1 ] && echo 1 || echo 0)"
for j in 1 2 3; do
    if ! kill -0 "${PID[$j]}" 2>/dev/null; then
        check "member $j is running" 0
        tail -20 "$WORK/log$j"
    fi
done
echo "-- each member's version change, as it logged it:"
for j in 1 2 3; do
    grep -hiE "^--- |version|migrat|wal|backup" "$WORK/log$j" | grep -viE "slow raft WAL|keep-alive" | head -12 | sed "s/^/   [$j] /" | cut -c1-220
done
if [ "$FAILED" != 0 ]; then
    echo "-- members' warnings and errors:"
    grep -hE "WARN|ERROR|panic" "$WORK"/log* | tail -40 || true
fi
echo "rolling_upgrade $OLD_V -> $NEW_V: $FAILED failed"
exit "$FAILED"
