# fastetcd

A Rust implementation of the **etcd v3 wire protocol**, focused on
low resource overhead and predictable latency. A slide deck of its purpose and
functionality: [docs/presentation.md](docs/presentation.md). Wire-compatible with
unmodified etcd v3 clients (third-party `etcd-client` Rust crate
exercises the full surface in the integration tests). Multi-node
Raft via `openraft`. Data is stored in `redb`, a single-file ACID
B-tree.

## Status

`v1.13.0` (see `CHANGELOG.md`). What works today, all through Raft,
single- and multi-member, with linearizable reads by default:

- The etcd v3 gRPC API: KV (Range, Put, DeleteRange, Txn, Compact),
  Watch, Lease, Cluster, Maintenance, Auth, plus `grpc.health.v1`.
- etcd's v3 JSON gateway (`POST /v3/...`) and `GET /health`, `/livez`,
  `/readyz` on the client port; Prometheus `/metrics` on its own port.
- Auth: users, roles, per-key permissions, replicated through Raft;
  tokens; a TLS client certificate's CN as the user; admin RPCs
  root-only.
- TLS on the client and peer ports, independently.
- A bounded on-disk footprint (reclaim, NOSPACE alarm), periodic
  backups with self-restore of a lone member from a corrupt file,
  offline `backup` / `restore` / `fsck` / `defrag`, volume sizing.
- Import of an etcd BoltDB snapshot (`fastetcd-migrate`).

Not implemented (each has an issue): nested Txn (#56), MoveLeader and
Downgrade (#57), watch response fragmentation (#58), raising an alarm by
hand (deliberate), the etcd v2 API (out of scope). Differences from
etcd are listed under [Compatibility boundary](#compatibility-boundary).

## Quick start

```
cargo run --release -p fastetcd-server --bin fastetcd
# (on the StormCOS build setup: never build on the session VM; push and
#  run `sc-build`, which builds and tests on dev.g8.lo)
# in another terminal:
etcdctl --endpoints=127.0.0.1:2379 put hello world
etcdctl --endpoints=127.0.0.1:2379 get hello
```

Or via the shipped client:

```
fastetcd-ctl put hello world
fastetcd-ctl get hello
fastetcd-ctl status
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
│ client port: tonic gRPC KV / Watch / Lease / Maintenance / Cluster / │
│   Auth / grpc.health.v1, HTTP /health, v3 JSON gateway /v3/...       │
│ metrics port: Prometheus /metrics                                     │
└────────────────────────────┬─────────────────────────────────────────┘
                             │   AuthInterceptor (token + per-key authz)
                             │   writes → Proposer (group commit, #75)
                             │   linearizable reads → read index
                             │     (ConfirmLeader quorum, not RaftCore)
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│ openraft 0.9 — consensus, log replication, membership, snapshots     │
│ WalLogStore: raft log in wal/ (one sequential fsync per group, #85)  │
│ FastetcdStateMachine wraps MvccStore                                  │
└────────────────────────────┬─────────────────────────────────────────┘
                             ▼   (apply; redb checkpointed in background)
┌──────────────────────────────────────────────────────────────────────┐
│ MVCC: revisions · generations · leases · events · compact · Txn     │
│ RAM: every key's index (like etcd's treeIndex) + latest-value LRU   │
└────────────────────────────┬─────────────────────────────────────────┘
                             │   KvStore trait
                             ▼
                redb engine (ACID single-file B-tree)
```

`fastetcd-storage` also has `wal` and (Linux, feature `iouring`)
`iouring` engines behind the same trait. The server does not use them:
it always opens redb (#55).

## fastetcd vs etcd

Measured side by side with etcd v3.7.2 on a fast and a slow disk:
[docs/benchmarks/etcd-vs-fastetcd.md](docs/benchmarks/etcd-vs-fastetcd.md).
Under Kubernetes-shaped load fastetcd v1.12's reads stay fast where
etcd's wait on the disk; etcd still has more raw write throughput (#94).

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
| Storage engine | BoltDB (bbolt), mmap B+tree | `redb`, behind a `KvStore` trait |
| MVCC model | Revisions, generations, leases | Same model, reimplemented |
| Watch / Lease / Txn | Full | Wire-compatible; no nested Txn (#56), no watch fragmentation (#58) |
| Auth | Token / per-key RBAC | Token / per-key RBAC |
| TLS | Yes | Yes |
| Metrics | Prometheus `/metrics` | Prometheus `/metrics` |
| Health | grpc.health.v1, `/health` | grpc.health.v1, `/health`, `/livez`, `/readyz` |
| JSON gateway | `/v3/...` (grpc-gateway) | `/v3/...`, same routes and JSON ([docs](docs/03-deploy.md#v3-json-gateway)) |
| Data import | n/a | `fastetcd-migrate` reads etcd BoltDB snapshots |

### Storage

etcd stores all data in a single mmap'd BoltDB (bbolt) file: an
ACID B+tree with copy-on-write pages.

fastetcd stores its data (MVCC, auth, leases) in one `redb` file,
`<data-dir>/fastetcd.redb`: a pure-Rust embedded ACID B-tree, behind a
`KvStore` trait. Like etcd, the raft log is a separate sequential WAL
(`<data-dir>/wal/`): a write waits for one append-and-fsync there, and
the B-tree is made durable in the background (#85,
[docs](docs/03-deploy.md#storage-engine)). Raft snapshots are files
beside them (`<data-dir>/snapshots/`). The trait leaves room for other engines;
`fastetcd-storage` has a WAL engine and an experimental io_uring one,
not used by the server (#55).

etcd keeps every key's index in memory and leaves values to the kernel's
page cache over its mmap. fastetcd keeps every key's index in memory
too, plus the latest value of recently used keys in a cache bounded in
bytes (`--value-cache-bytes`), so a hot GET reads nothing from disk
([docs](docs/03-deploy.md#memory)).

### Latency and resource profile

The main motivation for a Rust reimplementation is the absence of a
garbage collector on the hot path. etcd's apply and compaction paths
can experience GC-induced jitter under load; fastetcd has
deterministic, allocation-controlled apply with no stop-the-world
pauses. Targets (goals, not measured claims; `fastetcd-bench` measures
a running server):

- Smaller resident-memory (RSS) floor at idle.
- p99 write latency at or below upstream under sustained load.
- No GC-induced tail-latency spikes on the apply path.

### Compatibility boundary

fastetcd implements the etcd **v3 gRPC** surface — KV, Watch, Lease,
Txn, Maintenance, Cluster, Auth, and grpc.health.v1 — and is
exercised in its test suite by the third-party `etcd-client` Rust crate,
which shares zero code with fastetcd. Anything that speaks etcd v3
(including `etcdctl` and Kubernetes' `kube-apiserver`) is the
intended client. The deprecated **v2 HTTP API is not implemented**
and is out of scope — it is gone from upstream's roadmap too.

Differences from etcd:

- `--auto-compaction-retention` counts revisions; etcd's default mode
  counts hours (#21).
- `MoveLeader` and `Downgrade` answer `Unimplemented` (#57); a nested
  Txn is refused (#56); raising an alarm by hand is refused.
- `--log-level` and `--max-request-bytes` are accepted and ignored;
  logging is set with `RUST_LOG` (#54).
- A `Put` naming a lease that does not exist is refused with etcd's
  `etcdserver: requested lease not found`, but the check runs on the
  leader just before the write is proposed, not when it is applied as
  in etcd. So a lease that expires in that instant still gets the key
  attached. Moving the check to apply waits on the lease tables
  travelling in raft snapshots (#41).

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

`cargo test --workspace`: 339 tests in 44 test binaries at v1.13.0,
including 3-member clusters over the real gRPC transport and the
third-party `etcd-client` crate (`crates/server/tests/etcd_client_compat.rs`).
GitHub Actions is off for this repo: on the StormCOS setup every push is
built and tested with `sc-build` on `dev.g8.lo`. See
`docs/02-testing.md`.

## Backups and a corrupt data file

Give fastetcd a backup directory on a **separate volume** and it takes
checksummed, point-in-time backups of the whole store (every 15 minutes
and every 10000 revisions by default, keeping 4). If a power cut on a
device that loses writes leaves the data file unopenable, a single-node
fastetcd restores itself from the newest good backup instead of
crash-looping. It keeps the corrupt file and raises the `CORRUPT` alarm
with how much was lost. A member of a multi-node cluster refuses and
says how to replace it, because it must not forget its raft vote.

```bash
fastetcd --backup-dir /backup/fastetcd …   # off unless set
etcdctl alarm list                          # CORRUPT after a restore
etcdctl alarm disarm                        # once you have looked
```

See [docs/05-backup-and-recovery.md](docs/05-backup-and-recovery.md).

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

## Auth on a cluster

Users, roles, permissions, `auth enable` and login tokens are replicated
through Raft, so a change made on any member holds on every member. A
cluster upgraded from 1.4.x whose members kept different auth state
refuses auth changes until you pick one to keep. See [docs/03-deploy.md
§ Auth on a multi-member cluster](docs/03-deploy.md#auth-on-a-multi-member-cluster).

```
fastetcd-ctl auth members                        # what each member holds
fastetcd-ctl --user root:pw auth adopt <member>  # replicate one to all
```

## Ports and configuration

| Port | Default bind | Serves |
|---|---|---|
| 2379 | `127.0.0.1` (`--listen-client-urls`) | etcd gRPC, `grpc.health.v1`, `/health` `/livez` `/readyz`, `/v3/...` gateway |
| 2380 | `127.0.0.1` (`--listen-peer-urls`) | raft between members |
| 2381 | `127.0.0.1` (`--listen-metrics-url`) | Prometheus `/metrics` |

Every flag, its default, its `FASTETCD_*` variable and the `ETCD_*`
variable read as a fallback: [docs/01-configuration.md](docs/01-configuration.md).
Metrics: [docs/03-deploy.md § Metrics](docs/03-deploy.md#metrics).

## Deployment

See `docs/03-deploy.md` for the full guide. Quick paths:

- **StormCOS**: fastetcd ships as a golden that stormcos builds from
  `main`; see [docs/03-deploy.md § How fastetcd reaches a StormCOS
  node](docs/03-deploy.md#how-fastetcd-reaches-a-stormcos-node).
- **Container**: no image is published (GHCR is not used); `docker build
  -t fastetcd:dev .` and run or push that.
- **Kubernetes**: `helm install fastetcd ./deploy/charts/fastetcd`
  with your own image. The chart currently passes `--auto-defrag=true`,
  which the binary rejects, so its pods do not start until #53 is fixed
  (workaround: remove that line from the template).
- **Fedora/RHEL**, **Debian/Ubuntu**: `dnf install` the `.rpm` or
  `dpkg -i` the `.deb` that `deploy/packaging/build-release.sh` builds
  (on GitHub Releases up to v1.2.0)
- **systemd**: rpm/deb installs enable it automatically; for a
  manual install see `deploy/systemd/fastetcd.service` and
  `docs/03-deploy.md`.

## License

Apache 2.0.
