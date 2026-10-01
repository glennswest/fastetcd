# 00 — Design

The architectural call for fastetcd, kept current with the code (last
checked against v1.8.0). Where the design goes beyond what the code
does, the text says **Design, not implemented** and names the issue.

## Position

fastetcd is a **Rust implementation of the etcd v3 wire protocol**. The bar
is wire-compatibility with unmodified etcd v3 clients. We are not building
"an etcd-like KV store" — we are building etcd, but in Rust, with a smaller
resident set and lower tail latency.

## Why this is worth building

The etcd v3 wire contract is stable, well-specified, and broadly deployed,
so there is a clearly defined compatibility target. The internal
architecture (BoltDB MVCC + Raft + gRPC) maps cleanly onto modern Rust
crates (`redb`, `openraft`, `tonic`). A Rust reimplementation can deliver
a smaller RSS floor and tighter tail latency, with no GC-induced jitter
on the apply path, without requiring any client to change.

## Non-negotiables

1. **Wire compatibility** with etcd v3 gRPC (`etcd-io/etcd/api/etcdserverpb`).
2. **Multi-node Raft from day one.** Single-node is a cluster-of-one,
   not a separate code path.
3. **MVCC semantics match etcd**: monotonic global revisions, key history
   queryable at past revisions, watchers see every revision between
   their start point and current head (modulo compaction).
4. **Linearizable reads by default** via read-index; serializable reads
   opt-in per request (`Range.serializable = true`).
5. **All mutations go through Raft.** Lease grants, lease revocations on
   expiry, compaction events, auth changes — all are Raft proposals.
   No back-channel writes to the state machine.

## Component choices

### Storage: trait-first; the server runs on redb

The storage layer is abstracted behind a `KvStore` trait. The MVCC state
machine, the raft log store (`KvLogStore`) and the raft state machine
depend on the trait, not on a concrete engine.

**`redb`** is the engine the server runs on, always
(`recovery::open_or_recover` opens `RedbEngine`): an ACID single-file
B-tree, native Rust, cross-platform. One file, `<data-dir>/fastetcd.redb`,
holds the MVCC tables, the raft log and vote, auth and leases.

**Design, not implemented (#55):** a second, Linux-only engine for
predictable tail latency. `fastetcd-storage` has a `wal` engine (an
append-only WAL plus an in-memory index, tokio fs) and an `iouring`
variant of it (cargo feature `iouring`, via `tokio-uring`, not glommio),
but O_DIRECT and group commit are not implemented, and the server has
no way to select either: no `--storage-engine` flag, no server feature.
Only the storage benchmarks use them.

**Alternatives considered, not adopted:** `fjall` (LSM; adds compaction
tuning), `sled` (pre-1.0), `rocksdb` (C++ dependency, compaction
stalls). SPDK stays a long-term option, not committed work.

### Consensus: `openraft`

Async-native, Rust-idiomatic, supports membership changes including joint
consensus, snapshot install, log compaction. Mature enough to underpin a
production system. We integrate by:

- `KvLogStore`: `RaftLogStorage` over the `KvStore` trait, so the log
  lives in the same redb file. Append, vote, truncate and purge are
  durable (fsync'd commit) before ack. The committed index is written
  without an fsync: openraft only needs it to be no higher than the
  truth (#71).
- Implementing `RaftStateMachine` as the MVCC store applying committed
  entries. Each apply commits its writes and its `last_applied_log_id`
  in one batch **without an fsync** (redb `Durability::None`): it is
  visible at once and reaches disk with the next durable commit,
  normally the next log append. A crash loses at most the last few
  applies, together with the applied position that records them, and
  the state machine replays them from the durable log into the same
  state (#71). Snapshot installs, migration bulk loads and startup
  recovery stay durable, since the log cannot replay them. Before this
  a write cost three fsyncs through redb's single writer (append,
  committed index, apply).
- Implementing `RaftNetwork` over our internal gRPC peer transport
  (`fastetcd.raft.RaftPeer` on the peer port), which also carries
  follower → leader forwarding of writes, linearizable reads and
  membership changes, and the auth-state check (`AuthSync`).

**Alternatives considered:** `raft-rs` (synchronous, callback-driven,
harder to wire to a tokio gRPC server), rolling our own (out of scope).

### gRPC: `tonic` + `prost`

Vendor the etcd `.proto` files from `etcd-io/etcd/api/` (v3.6.11,
`crates/proto/vendor.sh`). Generate stubs in the `proto` crate via a
`build.rs`, plus serde JSON impls (`pbjson-build`) for the v3 JSON
gateway. We serve the exact upstream protos; fastetcd's own services
(`RaftPeer`, `FastetcdAdmin`) are separate protos.

### Async runtime: `tokio`

Multi-threaded scheduler. **Design, not implemented:** pinning the Raft
apply loop to a dedicated thread; it runs on the shared runtime today.

### Logging: `tracing`

`tracing` to stderr, filtered by `RUST_LOG` (default `info`).
**Design, not implemented:** OTLP export; `--log-level` is ignored
(#54).

## Layout

```
crates/
  proto/      # etcd v3 + fastetcd protos: tonic stubs, pbjson serde
  storage/    # KvStore trait; redb (used), wal / iouring (not wired);
              # MVCC store: revisions, leases, auth tables, events
  raft/       # openraft adapters: log store, state machine, snapshots
              # as files, gRPC peer transport, forwarding, lease precheck
  server/     # binary `fastetcd`: gRPC services, auth, watch, gateway,
              # metrics, space monitor, backups, recovery, CLI
  migrate/    # binary `fastetcd-migrate`: etcd BoltDB snapshot → data dir
  ctl/        # binaries `fastetcd-ctl` (small client), `fastetcd-bench`
```

## Wire-compat surface

| Service       | Implemented | Not implemented |
|---------------|-------------|-----------------|
| KV            | Range, Put, DeleteRange, Txn, Compact | nested Txn (#56) |
| Watch         | Watch (bidi): ranges, prev_kv, filters, start_revision history, progress notify, compaction cancel | response fragmentation (#58) |
| Lease         | LeaseGrant, LeaseRevoke, LeaseKeepAlive, LeaseTimeToLive, LeaseLeases | authorization of lease RPCs (#47) |
| Cluster       | MemberAdd, MemberRemove, MemberUpdate, MemberList, MemberPromote | |
| Maintenance   | Alarm (GET, DEACTIVATE), Status, Defragment, Hash, HashKV, Snapshot | MoveLeader, Downgrade (#57); Alarm ACTIVATE (deliberate) |
| Auth          | all RPCs; replicated through Raft; token or client-cert CN | token expiry (#46) |

The same methods are served as etcd's v3 JSON gateway (`POST /v3/...`).

## Read path (linearizable)

1. Client sends `Range` with default `serializable = false`.
2. Server, if leader, issues a Raft read-index (no log append, just a
   heartbeat round-trip to confirm leadership) and waits for apply to
   catch up to that index. A leader that is the only voter needs no
   heartbeat: its read index is the larger of the committed index and
   the last durable log index, both from the log store, and it waits on
   the state machine's own applied index. This never enters openraft's
   RaftCore, which handles one client write at a time and awaits its
   log fsync, so a read there queued behind every write ahead of it
   (150 ms to 7 s under load, #71). The local path is taken only while
   openraft's metrics show this node as leader with a single-voter,
   non-joint membership at least as new as any membership entry the log
   store has been handed; anything else uses openraft's read-index
   (`crates/raft/src/read_index.rs`). On a multi-member cluster the
   leader's read-index still queues behind writes in RaftCore (fewer
   now: one fsync per write instead of three).
3. Server, if follower, forwards the whole Range to the leader over the
   peer transport (`ForwardRead`), which runs the barrier and reads its
   own state machine.
4. State machine snapshot read at the established revision.
5. The response header's `revision` is the revision that snapshot was
   read at (etcd: the read txn's revision), taken together with the
   engine snapshot under the write-state lock, never read again
   afterwards. A forwarded read carries the leader's read revision back
   to the follower. This is the LIST → WATCH contract: a client that
   lists at header revision R and watches from R + 1 sees every write
   exactly once (#50). For a historical `revision`, the contents are at
   that revision and the header is still the store's revision at the
   read, as in etcd.

## Write path

1. Client `Put` arrives at any node.
2. Non-leader forwards to leader (etcd does the same internally).
3. Leader proposes through openraft; awaits commit.
4. The state machine applies it to the MVCC store: assigns the next
   global revision, commits one redb write transaction (the mutation and
   the raft `last_applied` together), returns the response. A put
   naming a lease is checked on the leader before proposing (#19).

## Watch delivery guarantee

A watch never silently skips a revision in its range. Each watcher has a
cursor (the first revision it has not yet been sent); live events below
it are skipped, which also keeps create-time history replay from racing
live delivery. When a stream cannot see every committed batch — its
client reads too slowly and the in-memory event broadcast (1024
batches) overflows, or a follower installs a raft snapshot, which
applies revisions without emitting events — the watcher is caught up
from MVCC history to the current revision, as etcd does for unsynced
watchers. If the history it needs has been compacted, the watch is
cancelled with `compact_revision` set (etcd's `ErrCompacted`), and the
client re-lists and re-watches from there. Clients can therefore rely on
"no cancel" meaning "my cache is current", with no periodic re-list.

Resyncs and lag cancellations are logged at `warn` on the
`fastetcd::watch` target.

## Migration from etcd

`fastetcd-migrate` reads an etcd v3 snapshot (BoltDB file) and writes a
fresh fastetcd data directory, **without going through Raft** (a
bulk-load before the cluster starts). It reads the `key` bucket only:

- default: the latest live value of each key, as puts (every key at
  revision 1);
- `--preserve-revisions`: every record with its original revisions, so
  `Range(rev)` and `Watch(start_rev)` behave as on the source.

Not imported (#60): the `lease` bucket (keys keep their lease ids, but
no lease backs them, so they never expire) and auth. No raft snapshot is
generated: the first server start initializes raft on the migrated store.

## Open questions

- A second engine for tail latency (#55): wire engine selection, with
  on-disk format detection, or drop it.
- Watch fragmentation (#58) and nested Txn (#56).
