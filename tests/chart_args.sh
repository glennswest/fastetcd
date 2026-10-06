#!/usr/bin/env bash
# The Helm chart's pods start (fastetcd#53).
#
#   tests/chart_args.sh
#
# Renders deploy/charts/fastetcd with helm, takes the container's
# `sh -c` script from the StatefulSet and runs it as the pod would (one
# replica, the binary and data dir swapped for this tree's build and a
# scratch dir, free ports), then checks the member answers /health and a
# put/get. Done for the default values, `space.autoDefrag=false`, and
# extraArgs carrying the other boolean flags in etcd's `--flag=value`
# form. The chart once rendered `--auto-defrag=true`, which the binary
# refused at argument parsing: every pod exited. Run it through
# sc-build: `sc-build tests/chart_args.sh`.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=$ROOT/tmp/chart-args
rm -rf "$WORK"
mkdir -p "$WORK"

command -v helm >/dev/null || { echo "chart_args: helm not found" >&2; exit 1; }
cargo build --locked -p fastetcd-server -p fastetcd-ctl
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
BIN=$TARGET/debug/fastetcd
CTL=$TARGET/debug/fastetcd-ctl

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'
}

# Render with `--set`s ($2...), run the pod's script, check it serves.
case_run() {
    local name=$1 dir=$WORK/$1
    shift
    mkdir -p "$dir/data"
    local cp pp mp
    cp=$(free_port) pp=$(free_port) mp=$(free_port)
    helm template fe deploy/charts/fastetcd --show-only templates/statefulset.yaml \
        --set replicaCount=1 --set ports.client="$cp" --set ports.peer="$pp" \
        --set ports.metrics="$mp" "$@" >"$dir/statefulset.yaml"
    # The script is the block after `- -c` / `- |`, up to `ports:`.
    awk '/^ *- \|$/ {on=1; next} /^ *ports:$/ {on=0} on' "$dir/statefulset.yaml" |
        sed -e 's/^ \{14\}//' \
            -e "s#/usr/local/bin/fastetcd#$BIN#" \
            -e "s#/var/lib/fastetcd#$dir/data#" >"$dir/run.sh"
    echo "== $name: $(grep -o -- '--[a-z-]*=\?[^ ]*' "$dir/run.sh" | grep -E 'defrag|backup|gateway|cert-auth|pprof' | tr '\n' ' ')"
    POD_NAME=fe-0 POD_NAMESPACE=test FASTETCD_INITIAL_CLUSTER_STATE=new \
        sh "$dir/run.sh" >"$dir/log" 2>&1 &
    local pid=$! ok=0
    for _ in $(seq 150); do
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi
        if curl -sf "http://127.0.0.1:$cp/health" >/dev/null &&
            "$CTL" --endpoint "http://127.0.0.1:$cp" put /chart "$name" >/dev/null 2>&1 &&
            [ "$("$CTL" --endpoint "http://127.0.0.1:$cp" get /chart 2>/dev/null | tail -n 1)" = "$name" ]; then
            ok=1
            break
        fi
        sleep 0.2
    done
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    if [ "$ok" = 1 ]; then
        echo "   pod started and serves"
    else
        echo "   FAILED: the pod did not start; its output ends:"
        tail -n 15 "$dir/log"
        FAILED=1
    fi
}

FAILED=0
case_run defaults
case_run no-autodefrag --set space.autoDefrag=false
case_run bool-flags \
    --set 'extraArgs[0]=--upgrade-backup=false' \
    --set 'extraArgs[1]=--enable-grpc-gateway=false' \
    --set 'extraArgs[2]=--force-new-cluster=false' \
    --set 'extraArgs[3]=--enable-pprof=true'
exit "$FAILED"
