#!/usr/bin/env bash
# The fastetcd golden, built here, from this checkout, and nowhere else (#257,
# stormcentral#1836: "do repos, repos are not expensive"; stormcos's stage
# mode is retired).
#
#   deploy/build-image.sh fastetcd OUT
#
# stormcentral's component build runs it (kind `tree`: `deploy/build-golden.sh
# <name> OUT`, which execs this) in a checkout fetched at the exact commit, on
# a fresh build VM. OUT is copied into the sealed read-only golden
# `golden-fastetcd-<build>`, sized from the content. The golden is laid at
# /pallets/fastetcd and run by the kubelet as the static pod `etcd`
# (stormcos#559).
#
# What the golden holds — only what is fastetcd's (the master's option A on
# rustkube#403, the same shape here):
#
#   usr/bin/fastetcd, usr/bin/fastetcd-ctl, usr/bin/fastetcd-migrate
#                             static musl, the release profile (`dist` if
#                             Cargo.toml ever defines one), the whole
#                             workspace built as the stage built it. The
#                             paths the static pod and irondirectory use.
#   stormd -> .stormd/bin/stormd   the release's one stormd, mounted at
#                             /.stormd by the kubelet (rustkube-node#465):
#                             stormcentral#1628's phase 3, so no stormd bytes
#                             are in the image and no stormd input is needed
#                             (a tree build mounts none, stormcentral#1846).
#   bin/<applet>, usr/bin/<applet>  links to ../stormd (stormd's applet
#                             list), the tools a `kubectl exec` gets.
#   etc/stormd/config.toml    the stage's config for fastetcd, unchanged: the
#                             one [[process]], its readyz probe, --data-dir as
#                             env_default FASTETCD_DATA_DIR with the
#                             membership and raft-timing defaults beside it
#                             (#282, #635, #648), and the stage's log limits.
#   bin sbin usr/sbin data dev proc sys run tmp var/log/stormd .stormd
#                             the stormdbase mount points, as the stage made.
#
# What the stage also put in this golden and this does NOT: the static pod
# `etc/kubernetes/static/etcd.json` and `etcd.env`, made by stormcos's
# gen-static-pod.py from the config above and stormcos's
# deploy/static-pods/etcd.yaml. That is stormcos's (its template, probes and
# volumes) and moves to stormcos's content golden (stormcos#1659), generated
# from this golden's config as today. Until stormcos carries it, the stage
# keeps building the golden nodes run: the registry row is flipped from
# special to tree only when both halves exist.
#
# GOLDEN_ARCH: x86_64 only, as the stage (aarch64 special goldens are
# stormcos#1489). Anything else exits 2.
set -euo pipefail
usage="usage: deploy/build-image.sh fastetcd OUT"
name=${1:?$usage}
out=${2:?$usage}
[ "$name" = fastetcd ] || { echo "build-image: this repo builds the fastetcd golden, not '$name'" >&2; exit 2; }
arch=${GOLDEN_ARCH:-x86_64}
case "$arch" in amd64) arch=x86_64 ;; esac
[ "$arch" = x86_64 ] || { echo "build-image: GOLDEN_ARCH=$arch: x86_64 only (as the stage; stormcos#1489)" >&2; exit 2; }
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source "$HOME/.cargo/env" 2>/dev/null || true
TARGET=x86_64-unknown-linux-musl
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')}"
commit=$(git rev-parse HEAD)
version=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
# The profile a golden ships (stormblock#209): `dist` when the workspace
# defines one, else `release` — the stage's rule and stormcentral's.
profile=release; grep -q '^\[profile\.dist\]' Cargo.toml && profile=dist
BINS="fastetcd fastetcd-ctl fastetcd-migrate"

echo "==> cargo build --profile $profile --locked --target $TARGET --workspace ($commit)"
SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)" \
    cargo build --profile "$profile" --locked --target "$TARGET" --quiet --workspace
for b in $BINS; do
    bin="$CARGO_TARGET_DIR/$TARGET/$profile/$b"
    [ -x "$bin" ] || { echo "build-image: cargo produced no $bin" >&2; exit 1; }
    # Static: the golden has no loader and no libc.
    if command -v file >/dev/null 2>&1; then
        file "$bin" | grep -Eq 'static-pie linked|statically linked' || { echo "build-image: $b is not static: $(file "$bin")" >&2; exit 1; }
    fi
    # It runs, and is the program it is named for.
    v=$("$bin" --version 2>&1) || { echo "build-image: $b --version failed: $(printf '%s\n' "$v" | tail -2)" >&2; exit 1; }
    case "$v" in *"$version"*) ;; *) echo "build-image: $b --version says '$v', not $version" >&2; exit 1 ;; esac
done

# The stormdbase layout (stormcos's stormdbase_stage, less the stormd copy).
mkdir -p "$out"/{bin,usr/bin,sbin,usr/sbin,etc/stormd,data,dev,proc,sys,run,tmp,var/log/stormd,.stormd}
chmod 0755 "$out" "$out"/{bin,usr,usr/bin,sbin,usr/sbin,etc,etc/stormd,data,dev,proc,sys,run,var,var/log,var/log/stormd,.stormd}
chmod 1777 "$out/tmp"
ln -s .stormd/bin/stormd "$out/stormd"
# The applets the stage linked: what its stormd listed (`--list-commands`,
# stormd's shell::STANDALONE_COMMANDS), the same list as rustkube's
# build-image.sh. With no stormd in the image there is none to ask at build
# time; a name stormd does not know is refused by it at run time (stormd#11).
APPLETS="ls dir cat head tail cp mv rm mkdir touch chmod chown \
find ln stat pwd wc du readlink file sha256sum tee md5sum \
ifconfig ip ping curl wget netstat ss nslookup dig hostname route \
mount df free uname date id kill printenv export unset \
sleep echo env whoami which type lsof true false clear \
sort uniq cut tr sed rev base64 xxd grep test ["
set -f   # `[` is a name here, not a pattern
for a in $APPLETS; do
    ln -s ../stormd "$out/bin/$a"
    ln -s ../../stormd "$out/usr/bin/$a"
done
set +f
for b in $BINS; do
    install -m 0755 "$CARGO_TARGET_DIR/$TARGET/$profile/$b" "$out/usr/bin/$b"
done

# The config, as the stage wrote it (stormcos deploy/build-goldens.sh
# §fastetcd at f7f7ccb), with its two edits already made: --data-dir moved
# to env_default (env_default_flag; every stormd a release ships honours it,
# stormd#37), and the membership and raft-timing defaults added to the same
# table. stormd is not this container's PID 1 on a node since stormcos#559:
# the static pod's command and args are generated from this, and a stormd run
# of the image starts it.
cfg="$out/etc/stormd/config.toml"
cat > "$cfg" << 'CONF'
# fastetcd under stormd.
[general]
name = "fastetcd"
log_dir = "/var/log/stormd"

[api_health]
# This container's API health for PID 1's node summary (#458, stormd#52):
# stormd writes its probes' states here, and the unit binds health.d in.
state_file = "/run/stormpump/health.d/fastetcd.json"

[api]
# The node's address, not loopback: a component nothing off the node can reach
# cannot be asked what is wrong with it, and that is the moment someone wants
# to ask. Each component has its own port because they share the host's
# network.
bind = "0.0.0.0:9081"

[[process]]
name = "fastetcd"
command = "/usr/bin/fastetcd"
args = [
  # Every address, not 127.0.0.1: the default binds loopback, and an apiserver
  # on another node then cannot reach it. A single-node cluster still works.
  "--listen-client-urls", "https://0.0.0.0:2379",
  "--listen-peer-urls", "https://0.0.0.0:2380",
  # What a peer is told to dial. A wildcard is what to *listen* on and says
  # nothing about how to be reached; advertising one would hand every member a
  # address that means "itself".
  "--advertise-client-urls", "https://${NODE_IP}:2379",
  "--initial-advertise-peer-urls", "https://${NODE_IP}:2380",
  # Mutual TLS on the client port (#146): the whole datastore, every Secret
  # included, was plaintext and anonymous on the node's address. A serving
  # pair from the node CA, and only clients holding a pair from the same CA
  # (the apiserver, the console, stormcluster) are answered. Tier-0 is mounted
  # read-only at /etc/stormcert. The pairs are minted by the apiserver golden;
  # until they exist fastetcd exits and stormd restarts it.
  "--cert-file", "/etc/stormcert/fastetcd.crt",
  "--key-file", "/etc/stormcert/fastetcd.key",
  "--trusted-ca-file", "/etc/stormcert/ca.crt",
  "--client-cert-auth",
  # Mutual TLS between members on the peer port too (#350, fastetcd#23): raft
  # between masters was plaintext and unauthenticated. One pair, serverAuth
  # and clientAuth, from `stormcert-agent peer` (stormcert#70): issued by the
  # node CA before a join, and on a joined node the adopted cluster
  # certificate with ca.crt set to the cluster's anchors, so members on
  # different nodes trust each other. fastetcd refuses a plaintext peer URL
  # while this is on, so stormcluster's [etcd] peer_scheme is "https" in the
  # same release.
  "--peer-cert-file", "/etc/stormcert/etcd-peer.crt",
  "--peer-key-file", "/etc/stormcert/etcd-peer.key",
  "--peer-trusted-ca-file", "/etc/stormcert/ca.crt",
  "--peer-client-cert-auth",
  # Prometheus /metrics on every address, not just loopback: WAL fsync,
  # checkpoint and group-commit times are read off the node by stormcentral's
  # probes, which may not ssh in (#242). The listener serves /metrics and
  # nothing else (no client API), read-only. 0.0.0.0 rather than ${NODE_IP}
  # so the node's own scraper (deploy/metrics/ironprom-node.yml) keeps
  # reading 127.0.0.1:2381. fastetcd's flag is singular and takes host:port.
  "--listen-metrics-url", "0.0.0.0:2381",
]
# The data dir from env, so a node that joins or is promoted starts on a fresh
# one that stormcluster writes into env.d/fastetcd (#282, stormcluster#21).
# And its membership (#635): FASTETCD_NAME, FASTETCD_INITIAL_CLUSTER and
# FASTETCD_INITIAL_CLUSTER_STATE, whose defaults are fastetcd's own, so an SNO
# starts as a cluster of one. Its raft timing too (#648): stormcluster sets
# 1000/5000 for a master on an HDD/SMR disk; not everywhere, since a lone
# member waits one election timeout for its own vote at every start.
env_default = { FASTETCD_DATA_DIR = "/data/fastetcd", FASTETCD_NAME = "default", FASTETCD_INITIAL_CLUSTER = "", FASTETCD_INITIAL_CLUSTER_STATE = "new", FASTETCD_HEARTBEAT_INTERVAL = "250", FASTETCD_ELECTION_TIMEOUT = "1000" }
on_failure = "restart"
on_exit = "restart"

# No liveness here (owner 2026-10-10, stormcos#523): "liveness should be done
# by kubernetes; liveness from stormd is obsolete". The kubelet's startup,
# liveness and readiness probes are in stormcos's static pod template.

# API health (#458, stormd#49): whether the store answers. /readyz runs etcd's checks (leader, alarms, a
# read; fastetcd#69) over the client port's mutual TLS, presenting the
# apiserver's etcd client pair. Slow and stalled are logged and kept, never
# acted on: restarting the datastore takes the cluster's store with it.
[[process.api]]
name = "readyz"
url = "https://127.0.0.1:2379/readyz"
p99_ms = 50
timeout_secs = 5
client_cert_file = "/etc/stormcert/apiserver-etcd-client.crt"
client_key_file = "/etc/stormcert/apiserver-etcd-client.key"

[stormlog.file]
max_size_bytes = 8388608
max_files = 3
max_runs = 5
CONF
chmod 0644 "$cfg"
# The config parses, and its one process is the binary in the image.
python3 - "$cfg" << 'PY'
import sys, tomllib
cfg = tomllib.load(open(sys.argv[1], "rb"))
procs = cfg.get("process", [])
if [p.get("command") for p in procs] != ["/usr/bin/fastetcd"]:
    sys.exit(f"build-image: the config's processes are {[p.get('command') for p in procs]}, not ['/usr/bin/fastetcd']")
if procs[0].get("env_default", {}).get("FASTETCD_DATA_DIR") != "/data/fastetcd":
    sys.exit("build-image: FASTETCD_DATA_DIR is not /data/fastetcd")
PY

# Read back what was copied: a short copy is never sealed (stormcentral#716).
for b in $BINS; do
    [ "$(sha256sum < "$out/usr/bin/$b")" = "$(sha256sum < "$CARGO_TARGET_DIR/$TARGET/$profile/$b")" ] || { echo "build-image: usr/bin/$b does not read back as built" >&2; exit 1; }
done
# The placer that allows an empty FASTETCD_INITIAL_CLUSTER (stormcos#635,
# a7ac474) came with this stormpump: the stage's line, kept for when a tree
# build reads it (stormcentral#1846 drops it today).
echo "@@SHIPS_WITH fastetcd stormpump@>=golden-stormpump-b954ec5b4ade"
echo "==> fastetcd golden tree in $out: fastetcd $version at $commit (x86_64)"
