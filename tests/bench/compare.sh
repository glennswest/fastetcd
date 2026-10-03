#!/usr/bin/env bash
# Upstream etcd vs fastetcd, side by side (fastetcd#90).
#
#   tests/bench/compare.sh <etcd-version>            e.g. v3.7.2
#
# Downloads the official etcd linux-amd64 release (checked against the
# release's SHA256SUMS), a Go toolchain to build etcd's own `benchmark`
# tool from the same tag, builds fastetcd at each tag in FASTETCD_TAGS,
# then runs on this machine's disk, one contender at a time:
#
#   bench      etcd's benchmark: put (1 client, 1000 clients), range
#              (linearizable, serializable), txn-put, watch,
#              lease-keepalive; plus RSS, CPU and bytes written during
#              the 1000-client put (resources.csv)
#   durability fastetcd-bench writes sequential keys, every member is
#              SIGKILLed mid-write, restarted; acknowledged keys missing
#              and the time to the first linearizable read
#   k8s        rustkube's test/e2e/get-latency.sh (a real kube-apiserver)
#              on each contender
#
# Environment:
#   MEMBERS        "1" (default), "3", or "1 3"
#   PARTS          default "bench durability"; add "k8s" for the apiserver rig
#   FASTETCD_TAGS  default "v1.11.0 v1.12.0"
#   DISK           label for the CSVs (default: dev-ssd)
#   OUT            results directory (default tmp/compare/out)
#   PORT_BASE      first port used (default 24000); two runs at once on
#                  one host need different bases
#   PUT_TOTAL / RANGE_TOTAL / KEEPALIVE_TOTAL / WATCH_PUTS   workload sizes
#   GO_VERSION     default 1.26.8 (what etcd v3.7.2's go.mod asks for)
#
# Through sc-build: sc-build 'MEMBERS=1 tests/bench/compare.sh v3.7.2'.
# Everything lives under tmp/compare; nothing needs root.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
ETCD_VERSION=${1:?usage: tests/bench/compare.sh <etcd-version, e.g. v3.7.2>}
MEMBERS=${MEMBERS:-1}
PARTS=${PARTS:-bench durability}
FASTETCD_TAGS=${FASTETCD_TAGS:-v1.11.0 v1.12.0}
DISK=${DISK:-dev-ssd}
WORK=$ROOT/tmp/compare
OUT=${OUT:-$WORK/out}
PORT_BASE=${PORT_BASE:-24000}
PUT_TOTAL=${PUT_TOTAL:-20000}
RANGE_TOTAL=${RANGE_TOTAL:-100000}
KEEPALIVE_TOTAL=${KEEPALIVE_TOTAL:-50000}
WATCH_PUTS=${WATCH_PUTS:-2000}
GO_VERSION=${GO_VERSION:-1.26.8}
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}

rm -rf "$WORK/run"
mkdir -p "$WORK/bin" "$WORK/run" "$OUT/raw"
echo "== host: $(nproc) cpus, $(awk '/MemTotal/ {printf "%.0f GiB", $2/1048576}' /proc/meminfo), $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
echo "== data volume: $(df -hT "$WORK" | tail -1)"

# ---- etcd release ---------------------------------------------------------
ETCD_TGZ=etcd-$ETCD_VERSION-linux-amd64.tar.gz
ETCD_URL=https://github.com/etcd-io/etcd/releases/download/$ETCD_VERSION
if [ ! -x "$WORK/etcd-$ETCD_VERSION/etcd" ]; then
    curl -sSfL -o "$WORK/$ETCD_TGZ" "$ETCD_URL/$ETCD_TGZ"
    curl -sSfL -o "$WORK/SHA256SUMS" "$ETCD_URL/SHA256SUMS"
    (cd "$WORK" && grep " $ETCD_TGZ\$" SHA256SUMS | sha256sum -c -)
    tar -xzf "$WORK/$ETCD_TGZ" -C "$WORK"
    mv "$WORK/etcd-$ETCD_VERSION-linux-amd64" "$WORK/etcd-$ETCD_VERSION"
fi
ETCD=$WORK/etcd-$ETCD_VERSION/etcd
ETCDCTL=$WORK/etcd-$ETCD_VERSION/etcdctl
echo "== etcd: $("$ETCD" --version | head -1), sha256 $(grep " $ETCD_TGZ\$" "$WORK/SHA256SUMS" | cut -d' ' -f1)"

# ---- etcd's benchmark tool, from the same tag -----------------------------
BENCHMARK=$WORK/bin/benchmark-$ETCD_VERSION
if [ ! -x "$BENCHMARK" ]; then
    GO_TGZ=go$GO_VERSION.linux-amd64.tar.gz
    if [ ! -x "$WORK/go/bin/go" ]; then
        curl -sSfL -o "$WORK/$GO_TGZ" "https://dl.google.com/go/$GO_TGZ"
        echo "$(curl -sSfL "https://dl.google.com/go/$GO_TGZ.sha256")  $WORK/$GO_TGZ" | sha256sum -c -
        tar -xzf "$WORK/$GO_TGZ" -C "$WORK"
    fi
    rm -rf "$WORK/etcd-src"
    git clone -q --depth 1 --branch "$ETCD_VERSION" https://github.com/etcd-io/etcd.git "$WORK/etcd-src"
    (cd "$WORK/etcd-src" &&
        GOTOOLCHAIN=local GOPATH=$WORK/gopath GOCACHE=$WORK/gocache \
            "$WORK/go/bin/go" build -o "$BENCHMARK" ./tools/benchmark)
fi

# ---- fastetcd at each tag, and this tree's fastetcd-bench -------------------
for tag in $FASTETCD_TAGS; do
    [ -x "$WORK/bin/fastetcd-$tag" ] && continue
    rev=$tag
    if ! git rev-parse -q --verify "$tag^{commit}" >/dev/null; then
        # sc-build's checkout has no tags and no remote.
        git fetch -q --depth=1 https://github.com/glennswest/fastetcd.git "refs/tags/$tag:refs/tags/$tag"
    fi
    rm -rf "$WORK/src-$tag"
    git worktree prune
    git worktree add -q --detach "$WORK/src-$tag" "$rev"
    (cd "$WORK/src-$tag" && CARGO_TARGET_DIR=$TARGET/compare \
        cargo build -q --release --locked -p fastetcd-server)
    cp "$TARGET/compare/release/fastetcd" "$WORK/bin/fastetcd-$tag"
    git worktree remove --force "$WORK/src-$tag"
done
cargo build -q --release --locked -p fastetcd-ctl
FEBENCH=$TARGET/release/fastetcd-bench

CONTENDERS="etcd-$ETCD_VERSION"
for tag in $FASTETCD_TAGS; do CONTENDERS+=" fastetcd-$tag"; done

# ---- clusters -----------------------------------------------------------------
client_port() { echo $((PORT_BASE + $1)); }
peer_port() { echo $((PORT_BASE + 100 + $1)); }
metrics_port() { echo $((PORT_BASE + 200 + $1)); }
endpoints() { # comma list of every member's client URL
    local n=$1 i s=""
    for i in $(seq "$n"); do s+="${s:+,}http://127.0.0.1:$(client_port "$i")"; done
    echo "$s"
}

PIDS=()
# start <contender> <members> <dir>: start (or restart, keeping data) a cluster.
start() {
    local who=$1 n=$2 dir=$3 i cluster=""
    for i in $(seq "$n"); do cluster+="${cluster:+,}n$i=http://127.0.0.1:$(peer_port "$i")"; done
    PIDS=()
    for i in $(seq "$n"); do
        local c="http://127.0.0.1:$(client_port "$i")" p="http://127.0.0.1:$(peer_port "$i")"
        case $who in
        etcd-*)
            "$ETCD" --name "n$i" --data-dir "$dir/data$i" \
                --listen-client-urls "$c" --advertise-client-urls "$c" \
                --listen-peer-urls "$p" --initial-advertise-peer-urls "$p" \
                --initial-cluster "$cluster" --initial-cluster-token bench \
                --listen-metrics-urls "http://127.0.0.1:$(metrics_port "$i")" \
                --log-level warn >>"$dir/log$i" 2>&1 &
            ;;
        fastetcd-*)
            local args=(--data-dir "$dir/data$i" --listen-client-urls "$c"
                --listen-peer-urls "$p" --listen-metrics-url "127.0.0.1:$(metrics_port "$i")")
            if [ "$n" -gt 1 ]; then
                args+=(--name "n$i" --initial-cluster "$cluster"
                    --initial-advertise-peer-urls "$p" --initial-cluster-token bench)
            fi
            "$WORK/bin/$who" "${args[@]}" >>"$dir/log$i" 2>&1 &
            ;;
        esac
        PIDS+=($!)
    done
}

# Until a linearizable read succeeds on every member (fastetcd's /health
# does not wait for a leader, #69).
wait_ready() {
    local n=$1 i
    for i in $(seq "$n"); do
        for _ in $(seq 600); do
            "$ETCDCTL" --endpoints "http://127.0.0.1:$(client_port "$i")" --command-timeout 2s \
                get /ready >/dev/null 2>&1 && break
            sleep 0.1
        done
    done
}

stop() { # kill and wait; $1 = signal (default TERM)
    local pid
    for pid in "${PIDS[@]}"; do kill "-${1:-TERM}" "$pid" 2>/dev/null || true; done
    for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
    PIDS=()
}

rss_kb() { local s=0 pid; for pid in "${PIDS[@]}"; do s=$((s + $(awk '/VmRSS/ {print $2}' "/proc/$pid/status" 2>/dev/null || echo 0))); done; echo $s; }
cpu_ticks() { local s=0 pid; for pid in "${PIDS[@]}"; do s=$((s + $(awk '{print $14 + $15}' "/proc/$pid/stat" 2>/dev/null || echo 0))); done; echo $s; }
io_field() { local s=0 pid; for pid in "${PIDS[@]}"; do s=$((s + $(awk -v f="$1:" '$1 == f {print $2}' "/proc/$pid/io" 2>/dev/null || echo 0))); done; echo $s; }

# ---- results -----------------------------------------------------------------
BENCH_CSV=$OUT/bench.csv
RES_CSV=$OUT/resources.csv
DUR_CSV=$OUT/durability.csv
K8S_CSV=$OUT/k8s.csv
[ -f "$BENCH_CSV" ] || echo "disk,members,contender,workload,clients,total,rps,p50_ms,p90_ms,p99_ms,p999_ms,max_ms" >"$BENCH_CSV"
[ -f "$RES_CSV" ] || echo "disk,members,contender,rss_idle_mib,rss_load_max_mib,cpu_s_per_1000_puts,disk_write_kib_per_1000_puts,write_syscall_kib_per_1000_puts" >"$RES_CSV"
[ -f "$DUR_CSV" ] || echo "disk,members,contender,acked,lost,recovery_ms" >"$DUR_CSV"
[ -f "$K8S_CSV" ] || echo "disk,members,contender,phase,who,what,p50_ms,p99_ms,max_ms" >"$K8S_CSV"

# parse <raw-file>: rps,p50,p90,p99,p99.9,max from the last summary.
parse() {
    python3 - "$1" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
last = text.rsplit("Summary:", 1)[-1]
def num(pat):
    m = re.search(pat, last)
    return float(m.group(1)) if m else float("nan")
rps = num(r"Requests/sec:\s+([0-9.]+)")
pct = lambda p: num(r"\b" + re.escape(p) + r"% in ([0-9.]+) secs") * 1000
slow = num(r"Slowest:\s+([0-9.]+) secs") * 1000
print(f"{rps:.1f},{pct('50'):.3f},{pct('90'):.3f},{pct('99'):.3f},{pct('99.9'):.3f},{slow:.3f}")
PY
}

# bench_one <contender> <members> <name> <clients> <total> <benchmark args...>
bench_one() {
    local who=$1 n=$2 name=$3 clients=$4 total=$5
    shift 5
    local raw="$OUT/raw/bench-$DISK-$n-$who-$name.txt"
    echo "-- $who x$n: $name"
    if "$BENCHMARK" --endpoints "$(endpoints "$n")" --precise "$@" >"$raw" 2>&1; then
        local row
        row=$(parse "$raw")
        echo "   $row"
        echo "$DISK,$n,$who,$name,$clients,$total,$row" >>"$BENCH_CSV"
    else
        echo "   FAILED (see $raw): $(tail -n 3 "$raw" | tr '\n' ' ')"
        echo "$DISK,$n,$who,$name,$clients,$total,FAILED,,,,," >>"$BENCH_CSV"
    fi
}

run_bench() {
    local who=$1 n=$2 dir=$WORK/run/$who-$n-bench
    mkdir -p "$dir"
    start "$who" "$n" "$dir"
    wait_ready "$n"
    sleep 2
    local rss_idle
    rss_idle=$(rss_kb)
    bench_one "$who" "$n" put-1c 1 2000 --conns=1 --clients=1 \
        put --key-size=8 --sequential-keys --total=2000 --val-size=256

    # The 1000-client put, with resources measured around it.
    local t0 c0 w0 x0 max=0 sampler
    t0=$(date +%s.%N) c0=$(cpu_ticks) w0=$(io_field write_bytes) x0=$(io_field wchar)
    (while :; do r=$(rss_kb); [ "$r" -gt "$max" ] && max=$r && echo "$max" >"$dir/rss_max"; sleep 0.5; done) &
    sampler=$!
    bench_one "$who" "$n" put-1000c 1000 "$PUT_TOTAL" --conns=100 --clients=1000 \
        put --key-size=8 --sequential-keys --total="$PUT_TOTAL" --val-size=256
    kill "$sampler" 2>/dev/null || true
    wait "$sampler" 2>/dev/null || true
    local tick
    tick=$(getconf CLK_TCK)
    awk -v d="$DISK" -v n="$n" -v who="$who" -v idle="$rss_idle" -v max="$(cat "$dir/rss_max" 2>/dev/null || echo 0)" \
        -v c="$(( $(cpu_ticks) - c0 ))" -v tick="$tick" -v w="$(( $(io_field write_bytes) - w0 ))" \
        -v x="$(( $(io_field wchar) - x0 ))" -v total="$PUT_TOTAL" \
        'BEGIN { k = 1000 / total;
                 printf "%s,%s,%s,%.1f,%.1f,%.3f,%.1f,%.1f\n", d, n, who, idle/1024, max/1024,
                        c/tick*k, w/1024*k, x/1024*k }' >>"$RES_CSV"
    tail -n 1 "$RES_CSV"

    "$ETCDCTL" --endpoints "$(endpoints "$n")" put foo bar >/dev/null
    bench_one "$who" "$n" range-lin-1000c 1000 "$RANGE_TOTAL" --conns=100 --clients=1000 \
        range foo --consistency=l --total="$RANGE_TOTAL"
    bench_one "$who" "$n" range-ser-1000c 1000 "$RANGE_TOTAL" --conns=100 --clients=1000 \
        range foo --consistency=s --total="$RANGE_TOTAL"
    bench_one "$who" "$n" txn-put-1000c 1000 "$PUT_TOTAL" --conns=100 --clients=1000 \
        txn-put --txn-ops=1 --key-size=8 --val-size=256 --key-space-size=100000 --total="$PUT_TOTAL"
    bench_one "$who" "$n" watch-1000w 10 "$WATCH_PUTS" --conns=10 --clients=10 \
        watch --streams=10 --watch-per-stream=100 --watched-key-total=100 --put-total="$WATCH_PUTS"
    bench_one "$who" "$n" lease-keepalive-1000c 1000 "$KEEPALIVE_TOTAL" --conns=100 --clients=1000 \
        lease-keepalive --total="$KEEPALIVE_TOTAL"
    stop
    du -sh "$dir" | sed "s|^|   data on disk after the run: |"
}

run_durability() {
    local who=$1 n=$2 dir=$WORK/run/$who-$n-dur
    mkdir -p "$dir"
    start "$who" "$n" "$dir"
    wait_ready "$n"
    echo "-- $who x$n: durability (SIGKILL every member mid-write)"
    "$FEBENCH" --endpoint "http://127.0.0.1:$(client_port 1)" --mode durability-write \
        --conns 16 --duration-secs 60 >"$dir/acked" 2>&1 &
    local writer=$!
    sleep 8
    stop KILL
    wait "$writer" || true
    start "$who" "$n" "$dir"
    local check
    check=$("$FEBENCH" --endpoint "http://127.0.0.1:$(client_port 1)" --mode durability-check \
        --acked-file "$dir/acked" 2>&1) || true
    echo "$check" | sed 's/^/   /'
    local acked lost rec
    acked=$(awk '$1 == "acked" {print $2}' <<<"$check")
    lost=$(awk '$1 == "lost" {print $2}' <<<"$check")
    rec=$(awk '$1 == "recovery_ms" {print $2}' <<<"$check")
    echo "$DISK,$n,$who,${acked:-FAILED},${lost:-},${rec:-}" >>"$DUR_CSV"
    cp "$dir/acked" "$OUT/raw/durability-$DISK-$n-$who-acked.txt"
    stop
}

# ---- Kubernetes-shaped: rustkube's apiserver rig -------------------------------
RUSTKUBE=$WORK/rustkube
k8s_prepare() {
    if [ ! -d "$RUSTKUBE" ]; then
        git clone -q --depth 1 https://github.com/glennswest/rustkube.git "$RUSTKUBE"
    fi
    echo "== rustkube $(git -C "$RUSTKUBE" rev-parse --short HEAD)"
    (cd "$RUSTKUBE" && CARGO_TARGET_DIR=$TARGET/rustkube \
        cargo build -q --release -p kube-apiserver -p kube-controller-manager -p kube-scheduler)
    # The rig starts its datastore with fastetcd's flags; etcd's differ.
    cat >"$WORK/bin/etcd-as-fastetcd" <<EOF
#!/usr/bin/env bash
args=()
while [ \$# -gt 0 ]; do
    case \$1 in
    --data-dir) args+=(--data-dir "\$2"); shift 2 ;;
    --listen-client-urls) args+=(--listen-client-urls "\$2" --advertise-client-urls "\$2"); shift 2 ;;
    --listen-peer-urls) args+=(--listen-peer-urls "\$2" --initial-advertise-peer-urls "\$2" --initial-cluster "default=\$2"); shift 2 ;;
    --listen-metrics-url) args+=(--listen-metrics-urls "http://\$2"); shift 2 ;;
    *) shift ;;
    esac
done
exec "$ETCD" --log-level warn "\${args[@]}"
EOF
    chmod +x "$WORK/bin/etcd-as-fastetcd"
}

run_k8s() {
    local who=$1 bin
    case $who in
    etcd-*) bin=$WORK/bin/etcd-as-fastetcd ;;
    *) bin=$WORK/bin/$who ;;
    esac
    local raw="$OUT/raw/k8s-$DISK-1-$who.txt"
    echo "-- $who: rustkube get-latency.sh"
    (cd "$RUSTKUBE" && RK_BIN=$TARGET/rustkube/release RK_FASTETCD=$bin RK_RELEASE=1 \
        bash test/e2e/get-latency.sh) >"$raw" 2>&1 || true
    grep -E "^\s+(idle|load) |background:|over .* s: apiserver cpu" "$raw" | sed 's/^/   /' || true
    python3 - "$raw" "$DISK" "$who" >>"$K8S_CSV" <<'PY'
import re, sys
raw, disk, who = sys.argv[1:4]
pat = re.compile(r"^\s+(idle|load)\s+(\w+)\s+(.+?)\s+p50\s+([0-9.]+) ms\s+p99\s+([0-9.]+) ms\s+max\s+([0-9.]+) ms")
for line in open(raw):
    m = pat.match(line)
    if m:
        print(f"{disk},1,{who},{m[1]},{m[2]},{m[3].strip()},{m[4]},{m[5]},{m[6]}")
    m = re.search(r"background: (\d+) clients, ([0-9.]+) renew loops/s", line)
    if m:
        print(f"{disk},1,{who},load,{m[1]} clients,renew loops/s,{m[2]},,")
PY
}

# ---- run -----------------------------------------------------------------------
for n in $MEMBERS; do
    for who in $CONTENDERS; do
        case " $PARTS " in *" bench "*) run_bench "$who" "$n" ;; esac
        case " $PARTS " in *" durability "*) run_durability "$who" "$n" ;; esac
    done
done
case " $PARTS " in
*" k8s "*)
    k8s_prepare
    for who in $CONTENDERS; do run_k8s "$who"; done
    ;;
esac

echo "== results in $OUT"
for f in "$BENCH_CSV" "$RES_CSV" "$DUR_CSV" "$K8S_CSV"; do
    if [ "$(wc -l <"$f")" -gt 1 ]; then
        echo "--- $(basename "$f")"
        cat "$f"
    fi
done
