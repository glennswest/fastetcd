# fastetcd

A Rust implementation of the **etcd v3 wire protocol**, focused on
low resource overhead and predictable latency. Wire-compatible with
unmodified etcd v3 clients (third-party `etcd-client` Rust crate
exercises the full surface in CI). Multi-node Raft via `openraft`.
Two storage engines: cross-platform `redb` (default) and Linux-only
`iouring` (via `tokio-uring`).

## Status

`v0.4.0` — production-shaped: TLS, Auth (token + per-key
permissions), Prometheus `/metrics`, gRPC health, GitHub Actions CI,
distroless container, Helm chart. See `CHANGELOG.md` for the full
release history.

## Quick start

```
cargo run --release -p fastetcd-server --bin fastetcd
# in another terminal:
etcdctl --endpoints=127.0.0.1:2379 put hello world
etcdctl --endpoints=127.0.0.1:2379 get hello
```

Or via the shipped client:

```
fastetcd-ctl put hello world
fastetcd-ctl get hello
fastetcd-ctl snapshot-save /tmp/snapshot.db
```

## Goals

1. **Wire compatibility** with etcd v3 gRPC: KV, Watch, Lease,
   Maintenance, Cluster, Auth, grpc.health.v1.
2. **Multi-node from day one** — Raft is foundational, not optional.
3. **Data import** from existing etcd via
   `fastetcd-migrate --from snap.db --to data-dir --preserve-revisions`.
4. **Lower overhead** — smaller RSS floor than upstream etcd at
   idle, p99 write latency at or below upstream under sustained load,
   no GC-induced jitter on the apply path.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ tonic gRPC: KV / Watch / Lease / Maintenance / Cluster / Auth /      │
│             grpc.health.v1  + Prometheus /metrics side port          │
└────────────────────────────┬─────────────────────────────────────────┘
                             │   AuthInterceptor (token + per-key authz)
                             │   writes proposed to Raft
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│ openraft 0.9 — consensus, log replication, membership, snapshots     │
│ KvLogStore over KvStore  ·  FastetcdStateMachine wraps MvccStore     │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼   (apply)
┌──────────────────────────────────────────────────────────────────────┐
│ MVCC: revisions · generations · leases · events · compact · Txn     │
└────────────────────────────┬─────────────────────────────────────────┘
                             │   KvStore trait (runtime-selectable)
                             ▼
        ┌────────────────────┴───────────────────┐
        │                                        │
    redb engine                          wal / iouring engine
   (default, cross-platform,             (wal-engine: tokio fs;
    ACID single-file B-tree)              iouring: tokio-uring,
                                          Linux-only feature)
```

## Testing

Three concentric rings — see `docs/02-testing.md` for the full
strategy.

- **Ring 1** — `cargo test --workspace` (workspace unit + integration).
- **Ring 2** — `cargo test -p fastetcd-server --test etcd_client_compat`:
  third-party Rust `etcd-client` crate drives the full surface.
  Zero shared code with fastetcd; if this works, real etcd
  consumers work too.
- **Ring 3** — `./tests/etcdctl_smoke.sh` (requires upstream
  `etcdctl`); etcd-io/etcd's robustness suite (out-of-tree
  follow-up); Jepsen; Kubernetes e2e.

Current count: 100+ tests pass workspace-wide.

## Deployment

See `docs/03-deploy.md` for the full guide. Quick paths:

- **Container**: `docker run -p 2379:2379 -p 2380:2380
  ghcr.io/glennswest/fastetcd:latest`
- **Kubernetes**: `helm install fastetcd ./deploy/charts/fastetcd`
- **systemd**: see the unit-file template in `docs/03-deploy.md`.

## License

Apache 2.0.
