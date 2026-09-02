# fastetcd

A Rust implementation of the **etcd v3 wire protocol**, focused on
low resource overhead and predictable latency. Wire-compatible with
unmodified etcd v3 clients (third-party `etcd-client` Rust crate
exercises the full surface in the integration tests). Multi-node
Raft via `openraft`.
Two storage engines: cross-platform `redb` (default) and Linux-only
`iouring` (via `tokio-uring`).

## Status

`v0.7.0` — production-shaped: TLS, Auth (token + per-key
permissions), Prometheus `/metrics`, gRPC health, distroless
container, Helm chart, rpm/deb/tarball packaging. Multi-node client
write forwarding to the leader. Built and tested by hand on
`dev.g8.lo` (see `docs/03-deploy.md`) — GitHub Actions is disabled
for this repo. See `CHANGELOG.md` for the full release history.

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

## fastetcd vs etcd

fastetcd targets the same wire protocol and consensus semantics as
upstream etcd, but the implementation differs in ways that affect
resource use, latency predictability, and operational shape. The
goal is a drop-in replacement, not a fork of behavior.

### At a glance

| Dimension | etcd (upstream) | fastetcd |
|---|---|---|
| Language / runtime | Go, garbage-collected | Rust, no GC |
| Wire protocol | etcd v3 gRPC | etcd v3 gRPC (wire-compatible) |
| v2 HTTP API | Present (deprecated) | Not implemented (out of scope) |
| Consensus | Raft (etcd-io/raft) | Raft (`openraft`) |
| Storage engine | BoltDB (bbolt), mmap B+tree | Pluggable `KvStore`: `redb` (default) or `iouring` |
| Storage selection | Fixed | Runtime-selectable engine |
| MVCC model | Revisions, generations, leases | Same model, reimplemented |
| Watch / Lease / Txn | Full | Full, wire-compatible |
| Auth | Token / per-key RBAC | Token / per-key RBAC |
| TLS | Yes | Yes |
| Metrics | Prometheus `/metrics` | Prometheus `/metrics` |
| Health | grpc.health.v1 | grpc.health.v1 |
| Data import | n/a | `fastetcd-migrate` reads etcd BoltDB snapshots |

### Storage

etcd stores all data in a single mmap'd BoltDB (bbolt) file: an
ACID B+tree with copy-on-write pages. It is robust and battle-tested
but page-oriented and tied to mmap semantics.

fastetcd abstracts storage behind a `KvStore` trait with two
first-class engines selectable at runtime:

- **`redb`** (default) — a pure-Rust embedded ACID B-tree in a
  single file. Cross-platform, no unsafe mmap dependencies, easy to
  reason about.
- **`iouring`** (Linux-only, cargo feature) — a custom WAL plus
  `tokio-uring` with `O_DIRECT`, bypassing the page cache for the
  apply path. Aimed at predictable tail latency under sustained
  write load.

Because the engine is a trait, future backends (e.g. SPDK) can drop
in without touching the MVCC or Raft layers.

### Latency and resource profile

The main motivation for a Rust reimplementation is the absence of a
garbage collector on the hot path. etcd's apply and compaction paths
can experience GC-induced jitter under load; fastetcd has
deterministic, allocation-controlled apply with no stop-the-world
pauses. Targets:

- Smaller resident-memory (RSS) floor at idle.
- p99 write latency at or below upstream under sustained load.
- No GC-induced tail-latency spikes on the apply path.

### Compatibility boundary

fastetcd implements the etcd **v3 gRPC** surface — KV, Watch, Lease,
Txn, Maintenance, Cluster, Auth, and grpc.health.v1 — and is
exercised in CI by the third-party `etcd-client` Rust crate, which
shares zero code with fastetcd. Anything that speaks etcd v3
(including `etcdctl` and Kubernetes' `kube-apiserver`) is the
intended client. The deprecated **v2 HTTP API is not implemented**
and is out of scope — it is gone from upstream's roadmap too.

### Migration

Existing etcd data moves over with `fastetcd-migrate`, which reads an
etcd BoltDB snapshot and writes it into a fastetcd data directory,
optionally preserving original revisions:

```
fastetcd-migrate --from snap.db --to data-dir --preserve-revisions
```

### When the difference matters

- **Choose fastetcd** when you want lower idle footprint, predictable
  tail latency without GC pauses, or a pluggable storage backend —
  while keeping unmodified etcd v3 clients working.
- **Stay on upstream etcd** when you depend on the deprecated v2 HTTP
  API, or need the exact operational tooling and ecosystem maturity
  of the reference implementation.

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

Current count: 151 tests pass workspace-wide (on Linux; a few are
Linux-only, so a macOS run reports fewer).

## Disk space

fastetcd is normally deployed onto a fixed-size volume, so it manages
its own footprint rather than growing until the volume says no. It
samples the data file, the retained snapshots and the filesystem's real
free space; reclaims at a high-water mark (compact → snapshot → purge →
defragment); and raises etcd's `NOSPACE` alarm at a higher mark, where
writes are refused but reads, deletes, compaction and defragment keep
working — so the store can always be dug out.

```bash
fastetcd-ctl status          # dbSize vs dbSizeInUse vs capacity
fastetcd-ctl defrag          # return freed pages to the filesystem
fastetcd-ctl alarm           # list raised alarms
fastetcd defrag --data-dir … # offline escape hatch; works on a full volume
```

Size the volume against the cluster, not against the data — a raft
snapshot is a full copy of the database, so the volume runs roughly 10x
the live Kubernetes objects:

```bash
fastetcd sizing --nodes 100     # prints the arithmetic, not just a number
```

At default pod density: 512 MiB for 1-10 nodes, 1 GiB at 100, 2 GiB at
500, 4 GiB at 1000. `--expected-nodes N` makes the server run the same
check against its real volume at startup.

See `docs/04-disk-space.md` for the flags, metrics, sizing model and
recovery procedures.

## Deployment

See `docs/03-deploy.md` for the full guide. Quick paths:

- **Container**: `docker run -p 2379:2379 -p 2380:2380
  ghcr.io/glennswest/fastetcd:latest`
- **Kubernetes**: `helm install fastetcd ./deploy/charts/fastetcd`
- **Fedora/RHEL**: `dnf install` the `.rpm` from [GitHub
  Releases](https://github.com/glennswest/fastetcd/releases)
- **Debian/Ubuntu**: `dpkg -i` the `.deb` from [GitHub
  Releases](https://github.com/glennswest/fastetcd/releases)
- **systemd**: rpm/deb installs enable it automatically; for a
  manual install see `deploy/systemd/fastetcd.service` and
  `docs/03-deploy.md`.

## License

Apache 2.0.
