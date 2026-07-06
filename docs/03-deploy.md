# 03 — Deployment

This document covers running fastetcd in production. For development
runs see `README.md`.

## Container

The repo ships a multi-stage `Dockerfile` (binary built in the
image) and a `Dockerfile.ci` (binary pre-built outside, faster
image build in CI). Both produce a distroless image with the
`fastetcd` binary at `/usr/local/bin/fastetcd`.

```
docker build -t fastetcd:dev .
docker run --rm -p 2379:2379 -p 2380:2380 \
    -v fastetcd-data:/var/lib/fastetcd \
    fastetcd:dev
```

GitHub Actions is disabled for this repo (repo-level setting,
confirmed off since 2026-05-24 — not a workflow or billing issue,
just switched off). All testing, building, and packaging happens
by hand on a Linux box with the musl target and packaging tools
installed — `dev.g8.lo` — via `deploy/packaging/run-tests.sh` and
`deploy/packaging/build-release.sh`.

```
podman build -t ghcr.io/glennswest/fastetcd:vX.Y.Z -f Dockerfile.ci .
podman push ghcr.io/glennswest/fastetcd:vX.Y.Z
```

## Linux packages (rpm / deb)

Releases publish `fastetcd-vX.Y.Z-1.x86_64.rpm` and
`fastetcd_vX.Y.Z-1_amd64.deb` to [GitHub
Releases](https://github.com/glennswest/fastetcd/releases), plus a
plain `fastetcd-vX.Y.Z-x86_64-linux-musl.tar.gz` for distros that
use neither package manager. All three bundle `fastetcd` /
`fastetcd-ctl` / `fastetcd-migrate`, built statically against
`x86_64-unknown-linux-musl` — no glibc-version dependency on the
install target.

```
# Fedora / RHEL
sudo dnf install ./fastetcd-vX.Y.Z-1.x86_64.rpm

# Debian / Ubuntu
sudo dpkg -i ./fastetcd_vX.Y.Z-1_amd64.deb
```

Either install creates a `fastetcd` system user, owns
`/var/lib/fastetcd`, ships the systemd unit below to the platform's
unit dir, and runs `systemctl enable --now fastetcd` automatically
— no separate unit-file step needed. Multi-node and TLS flags go in
`/etc/fastetcd/fastetcd.conf` (see
`/usr/share/doc/fastetcd/fastetcd.conf.example`), loaded via the
unit's `EnvironmentFile=`.

Verified end-to-end on `dev.g8.lo` (Fedora 43) for v0.7.0: both the
rpm and the deb create the system user, enable, and start the
service, and a `put`/`get` through `fastetcd-ctl` round-trips. The
one caveat is testing the **deb on Fedora specifically**: Fedora's
`dpkg` build calls `setexeccon()` before running maintainer
scripts, and Fedora's SELinux policy has no context for dpkg's
scripts (only rpm's), so `postinst`/`prerm` fail under `Enforcing`
with "cannot set security execution context for maintainer script:
Invalid argument". This is a Fedora+foreign-`dpkg` limitation, not
a defect in the package — it installs and runs cleanly under
`setenforce 0`, and is a non-issue on real Debian/Ubuntu, where
`dpkg` is native.

Building the packages: `deploy/packaging/build-release.sh vX.Y.Z`
on a host with rustup (`x86_64-unknown-linux-musl` target),
`cargo-deb`, `cargo-generate-rpm`, `protoc`, and `musl-gcc`
installed. It builds the workspace, runs `cargo deb` / `cargo
generate-rpm`, bundles the tarball, and writes everything plus
`SHA256SUMS.txt` to `dist/`. Publish with `gh release create vX.Y.Z
dist/* --generate-notes`.

## systemd unit (manual install)

The unit fastetcd ships is `deploy/systemd/fastetcd.service` — it
autostarts (`WantedBy=multi-user.target`) and restarts
unconditionally on any exit (`Restart=always`, unbounded
restart-rate limit). To install it by hand instead of via the rpm/
deb above:

```
sudo useradd --system --home-dir /var/lib/fastetcd \
    --no-create-home --shell /sbin/nologin fastetcd
sudo mkdir -p /var/lib/fastetcd /etc/fastetcd
sudo chown fastetcd:fastetcd /var/lib/fastetcd
sudo cp deploy/systemd/fastetcd.service /etc/systemd/system/
sudo cp deploy/systemd/fastetcd.conf.example /etc/fastetcd/fastetcd.conf
# edit /etc/fastetcd/fastetcd.conf with your --name / --initial-cluster / etc.
sudo systemctl daemon-reload
sudo systemctl enable --now fastetcd
```

## TLS

Standard etcd-shaped flags:

```
--cert-file=/etc/fastetcd/cert.pem
--key-file=/etc/fastetcd/key.pem
--trusted-ca-file=/etc/fastetcd/ca.pem    # optional
--client-cert-auth                         # require client certs
```

When `--cert-file` + `--key-file` are set, both the client and peer
ports listen over TLS using the same identity. Operators that need
separate identities for client vs. peer should run two fastetcd
processes — multi-identity is not currently supported.

## Auth

```
# 1. Add a root user (required to enable auth).
etcdctl user add root --no-password=false
etcdctl role add root
etcdctl user grant-role root root

# 2. Add per-user roles + permissions.
etcdctl role add app-readers
etcdctl role grant-permission app-readers read /app/ /app0
etcdctl user add reader --no-password=false
etcdctl user grant-role reader app-readers

# 3. Enable.
etcdctl auth enable
```

Clients then run with `--user=reader:password` (etcdctl) or send a
`token` metadata field after calling `Authenticate`.

## Storage engine

fastetcd ships two engines:

- `redb` (default): cross-platform, ACID single-file B-tree. Good
  for development, smaller deployments, and any environment where
  single-file ops simplicity matters.
- `iouring` (Linux-only, opt-in at build time with
  `--features iouring`): tokio-uring-backed append-only WAL +
  in-memory index. Architectural fit for high write throughput
  with predictable p99 once the O_DIRECT + group-commit tuning
  lands (tracked).

Switch at runtime by recompiling with the right features and
pointing `--data-dir` at a fresh directory; cross-engine migration
is not currently supported in place.

## Multi-node

```
# On node-a:
fastetcd --name=node-a --data-dir=/var/lib/fastetcd-a \
    --listen-client-url=127.0.0.1:23791 \
    --listen-peer-url=127.0.0.1:23801 \
    --initial-cluster=node-a=http://127.0.0.1:23801,node-b=http://127.0.0.1:23802,node-c=http://127.0.0.1:23803

# Same flags on node-b/c with their own data dirs and listen URLs.
```

Each node calls `raft.initialize` with the same member set;
openraft elects a leader among them. After bootstrap, `MemberAdd`
adds new nodes; `MemberRemove` removes them.

## Backups

```
etcdctl --endpoints=$ENDPOINT snapshot save snapshot.db
```

`Maintenance.Snapshot` streams the entire engine state to the
client in 64 KiB chunks. The snapshot file can be re-imported on a
fresh fastetcd via `fastetcd-migrate --from=snapshot.db
--to=/var/lib/fastetcd-new`.

## Migration from etcd

```
# Take a snapshot from a running etcd cluster (or use a pre-existing
# snapshot file).
etcdctl --endpoints=$ETCD_ENDPOINT snapshot save etcd-snap.db

# Replay into a fresh fastetcd data dir.
fastetcd-migrate --from etcd-snap.db --to /var/lib/fastetcd \
    --preserve-revisions
```

Without `--preserve-revisions`, only the latest live value per key
is imported. Use the flag if you have clients that rely on
historical reads or watch reconnections from past revisions.
