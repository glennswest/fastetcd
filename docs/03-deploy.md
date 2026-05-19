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

CI publishes tagged images to `ghcr.io/glennswest/fastetcd:vX.Y.Z`
and `:latest`.

## systemd unit

```ini
[Unit]
Description=fastetcd
After=network.target

[Service]
Type=simple
User=fastetcd
Group=fastetcd
WorkingDirectory=/var/lib/fastetcd
ExecStart=/usr/local/bin/fastetcd \
    --name=node-a \
    --data-dir=/var/lib/fastetcd \
    --listen-client-url=0.0.0.0:2379 \
    --listen-peer-url=0.0.0.0:2380 \
    --initial-cluster=node-a=http://node-a.example.com:2380,node-b=http://node-b.example.com:2380,node-c=http://node-c.example.com:2380
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
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
