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
machine and the raft state machine depend on the trait, not on a
concrete engine.

**`redb`** is the engine the server runs on, always
(`recovery::open_or_recover` opens `RedbEngine`): an ACID single-file
B-tree, native Rust, cross-platform. One file, `<data-dir>/fastetcd.redb`,
holds the MVCC tables, auth and leases.

**The raft log is a WAL** (`<data-dir>/wal/`, #85): records appended to
preallocated segments (`fastetcd_storage::raft_wal`), so the fsync on a
client's path is a sequential write, not a B-tree commit. Before 1.12
the log was a pair of redb tables, and every append was a durable redb
commit that also wrote the pages of every apply since the last one.

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

- `WalLogStore` (`crates/raft/src/wal_log_store.rs`): `RaftLogStorage`
  over the WAL. `append` returns once the entries are readable; a
  writer thread syncs whatever was appended while the previous sync ran
  in one `fdatasync` and then reports each append flushed, as
  openraft's `LogFlushed` allows. openraft 0.9's RaftCore awaits that
  report before its next append, though, so in practice each fsync
  carries one entry; writes are grouped into entries by the proposer
  (write path, below). The vote is synced before
  `save_vote` returns; the committed index rides the next sync (openraft
  only needs it to be no higher than the truth, #71). Truncate is
  synced. A purge drops entries from memory at once but deletes WAL
  segments only once a checkpoint covers them (below). The old
  `KvLogStore` (the log in redb tables) remains for tests and as the
  source of the in-place upgrade.
- Implementing `RaftStateMachine` as the MVCC store applying committed
  entries. Each apply commits its writes and its `last_applied_log_id`
  as one batch **without an fsync** into the **write-behind** layer in
  front of redb (`crates/storage/src/write_behind.rs`): an immutable
  in-RAM layer, visible at once, since a snapshot is the layer list plus
  a redb snapshot taken together and reads merge them (newest wins).
  The **checkpointer**, every `--wal-checkpoint-interval-ms` (100) or
  `--wal-checkpoint-entries` applied entries, but no sooner than four
  times the last checkpoint's duration after it (so on a slow disk it
  leaves the WAL's fsyncs most of the disk, #95), writes the layers into
  redb as one non-durable commit (dropping them from the list in the
  same step for readers) and then commits durably. Every redb write is
  serialized behind one flush lock, so an apply never waits on redb's
  writer or its fsync; past `--write-behind-bytes` (64 MiB) held, an
  apply writes the layers out first (back-pressure). A crash loses at most the applies since
  the last checkpoint, together with the applied position that records
  them, and openraft replays them from the WAL into the same state
  (#71, #85). Snapshot installs, migration bulk loads and startup
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
| Lease         | LeaseGrant, LeaseRevoke, LeaseKeepAlive, LeaseTimeToLive, LeaseLeases (authorized over the lease's keys, #47) | |
| Cluster       | MemberAdd, MemberRemove, MemberUpdate, MemberList, MemberPromote | |
| Maintenance   | Alarm (GET, DEACTIVATE), Status, Defragment, Hash, HashKV, Snapshot | MoveLeader, Downgrade (#57); Alarm ACTIVATE (deliberate) |
| Auth          | all RPCs; replicated through Raft; token or client-cert CN | token expiry (#46) |

The same methods are served as etcd's v3 JSON gateway (`POST /v3/...`).

## Read path (linearizable)

1. Client sends `Range` with default `serializable = false`.
2. Server, if leader, establishes a read index and waits for its state
   machine to apply that far, without entering openraft's RaftCore,
   which handles one client write at a time and awaits its fsync, so a
   read queued there waited behind every write ahead of it (150 ms to
   7 s under load, #71; `crates/raft/src/read_index.rs`):
   - **Leadership.** A sole voter needs no confirmation. With other
     voters the leader asks each, over the `ConfirmLeader` peer RPC,
     for the term of the vote it last saved; openraft saves a vote
     before granting it, so a quorum (in every config of a joint
     membership) answering a term no newer than the leader's, asked
     after the read arrived, proves no newer leader existed then (#75).
     Reads that arrive while a round is out share the next one.
   - **Read index** = the first index of the leader's current term (its
     blank entry). The leader answers a write only after applying it,
     so every write acknowledged this term is applied, and writes
     acknowledged in earlier terms are below the blank entry (leader
     completeness). Waiting for the commit index instead (etcd) would
     also wait for writes nobody has been told about, about one fsync
     per read here, since an apply waits for redb's single writer (#75).
   - Taken only while metrics show this node as leader with its saved
     vote in the current term and no membership entry the metrics do not
     show yet; anything else, an unreachable or older member, or a round
     without quorum, uses openraft's read-index (`ensure_linearizable`).
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
3. Leader proposes through openraft; awaits commit. Proposals queue in
   a `Proposer` (`crates/raft/src/proposer.rs`) and go as one
   `FastetcdLogEntry::Batch` — one RaftCore message, one log append, one
   fsync (up to 4096 proposals or 512 KiB, #94), each applied at its own
   revision with its own answer (#75). RaftCore appends one entry at a
   time and waits for its fsync, so group commit happens here (#95):
   one batch is in RaftCore at a time, and the next is formed the
   moment it is answered, from everything that arrived meanwhile (a
   write waits about two fsyncs under load, one when alone). Replication
   reads the log in AppendEntries of at most ~1 MiB of entries
   (`wal_log_store::REPLICATION_BYTES`), so a batch always fits in one. A second
   batch in flight would make RaftCore wait on its fsync before it
   could answer the first; #75 had three, and a write waited about four
   fsyncs. Batches are
   proposed only once every member has answered `ConfirmLeader` (an
   older member cannot decode one), re-checked on membership changes.
   Applying a batch records `(index, done)` with each proposal's commit,
   so a crash in the middle replays exactly the proposals that did not
   reach disk.
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
bulk-load before the cluster starts). Keys, from the `key` bucket:

- default: the latest live value of each key, as puts (every key at
  revision 1);
- `--preserve-revisions`: every record with its original revisions, so
  `Range(rev)` and `Watch(start_rev)` behave as on the source.

Leases and auth come too (#60). Every lease in the `lease` bucket is
granted with its id and the TTL etcd itself would give it on a restart
(its remaining TTL if etcd recorded one, else its full TTL; at least
2 s), so its keys expire as they would have. A key naming a lease the
snapshot no longer holds is imported without one (counted and warned).
Roles and their permissions, users with their roles, and `auth enable`
are imported through the server's own auth operations; users keep their
etcd passwords, because fastetcd verifies etcd's bcrypt hashes
(passwords set later are argon2). No raft snapshot is generated: the
first server start initializes raft on the migrated store.

## Open questions

- A second engine for tail latency (#55): wire engine selection, with
  on-disk format detection, or drop it.
- Watch fragmentation (#58) and nested Txn (#56).
