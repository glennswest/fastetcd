# 00 — Design

Initial architectural call for fastetcd. Living document; revise as we learn.

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

### Storage: trait-first, two first-class engines

The storage layer is abstracted behind a `KvStore` trait. The MVCC state
machine and Raft state machine adapter depend on the trait, not on any
concrete engine. Two engines are first-class and selectable at runtime
via `--storage-engine=redb|iouring` (default: `redb`):

**`redb` engine.** ACID single-file B-tree, native Rust, no native
dependencies, cross-platform. The default — works wherever fastetcd
builds. Single-writer with MVCC snapshots for readers maps cleanly onto
our Raft-serialized write path. Operationally simple: one file is the
whole database.

**`iouring` engine.** `glommio` (thread-per-core io_uring) +
`O_DIRECT` + a custom group-committed WAL + in-memory MVCC index.
Linux-only, compiled behind cargo feature `iouring` (enabled by default
on Linux CI). Bypasses the kernel page cache and avoids filesystem
metadata on the critical path, targeting sub-ms p99 on small writes.

**Alternatives considered, not adopted:** `fjall` (LSM; bench-worthy
later but adds compaction tuning surface neither engine above has),
`sled` (pre-1.0 forever, less predictable), `rocksdb` (C++ dep, brings
back GC-like compaction stalls in a different dialect). **SPDK**
(userspace NVMe driver) stays a long-term option but is not committed
work — only revisit if iouring has a kernel-side tail-latency floor we
cannot push past.

The trait abstraction means we can benchmark engines side-by-side
(task #10) without rewriting the state machine.

### Consensus: `openraft`

Async-native, Rust-idiomatic, supports membership changes including joint
consensus, snapshot install, log compaction. Mature enough to underpin a
production system. We integrate by:

- Implementing `RaftLogStorage` over an engine-agnostic log abstraction
  with two impls (redb-backed; iouring + custom WAL). Append is durable
  (fsync or `O_DIRECT` write) before ack.
- Implementing `RaftStateMachine` as the MVCC store applying committed
  entries.
- Implementing `RaftNetwork` over our internal gRPC peer transport.

**Alternatives considered:** `raft-rs` (synchronous, callback-driven,
harder to wire to a tokio gRPC server), rolling our own (out of scope).

### gRPC: `tonic` + `prost`

Vendor the etcd `.proto` files from `etcd-io/etcd/api/`. Generate stubs
in the `proto` crate via a `build.rs`. No custom protocol; we serve the
exact upstream protos.

### Async runtime: `tokio`

Multi-threaded scheduler. Pin the Raft apply loop to a dedicated thread
to avoid scheduling jitter on the critical write path.

### Logging: `tracing`

Structured spans for every RPC and every Raft state transition. Optional
OTLP export.

## Layout

```
crates/
  proto/      # generated etcd v3 stubs (tonic + prost)
  storage/    # KvStore trait + redb impl + (linux) iouring impl;
              # MVCC state machine; lease registry; watch fan-out
  raft/       # openraft adapters: log storage, state machine wrapper, network
  server/     # binary: ties storage + raft + gRPC together; CLI flags
  migrate/    # binary: reads etcd BoltDB snapshot → fastetcd state machine
  ctl/        # binary: minimal etcdctl-compatible smoke client
```

## Wire-compat surface (initial scope)

| Service       | Methods (minimum)                                              |
|---------------|----------------------------------------------------------------|
| KV            | Range, Put, DeleteRange, Txn, Compact                          |
| Watch         | Watch (bidi stream), with fragmentation and progress notify    |
| Lease         | LeaseGrant, LeaseRevoke, LeaseKeepAlive (bidi), LeaseTimeToLive, LeaseLeases |
| Cluster       | MemberAdd, MemberRemove, MemberUpdate, MemberList, MemberPromote |
| Maintenance   | Alarm, Status, Defragment, Hash, HashKV, Snapshot, MoveLeader  |
| Auth          | (Phase 2) AuthEnable/Disable, User*, Role*, Authenticate       |

Auth is deferred until KV/Watch/Lease/Cluster/Maintenance are solid; the
common deployment pattern uses TLS client-cert authentication outside
the etcd Auth subsystem.

## Read path (linearizable)

1. Client sends `Range` with default `serializable = false`.
2. Server, if leader, issues a Raft read-index (no log append, just a
   heartbeat round-trip to confirm leadership) and waits for apply to
   catch up to that index.
3. Server, if follower, forwards to leader (or returns `NotLeader` with
   leader hint depending on config — match etcd's behavior).
4. State machine snapshot read at the established revision.

## Write path

1. Client `Put` arrives at any node.
2. Non-leader forwards to leader (etcd does the same internally).
3. Leader proposes through openraft; awaits commit.
4. Apply thread executes against MVCC store, assigns new global
   revision, durably writes redb txn, returns response.

## Migration from etcd

`crates/migrate` reads an etcd v3 snapshot (BoltDB file). The relevant
buckets are `key` (MVCC), `lease`, `auth`, `meta`. We:

1. Walk the `key` bucket in revision order, applying each as a state
   machine entry **without going through Raft** (this is a bulk-load,
   pre-cluster-start path).
2. Restore leases, current revision, compaction state.
3. Generate a Raft snapshot at the restored revision, so the cluster
   starts with that snapshot as its initial state on every node.

## Open questions

- TLS termination: in-process via `rustls` (preferred) or sidecar?
  Default to in-process to preserve etcd's `--cert-file/--key-file` flag
  shape.
- Auth: defer or include in v0.1? Lean defer.
- `Maintenance.Defragment` semantics differ per engine — redb has
  compact-in-place (verify it is online); the iouring engine will
  expose a WAL-segment rotation or compaction trigger with its own
  semantics. Document both.
- Watch event ordering across compaction boundaries — etcd has specific
  guarantees for `ErrCompacted`; match exactly across both engines.
- Default cargo features for the `storage` crate: `iouring` enabled on
  Linux, disabled elsewhere. Verify CI matrix covers both engines on
  Linux.

## What v0.1 ships

- 3-node cluster (1-node also supported).
- Range/Put/DeleteRange/Txn/Compact.
- Watch (single-key + range + prev_kv + progress notify).
- Lease (Grant/Revoke/KeepAlive/TTL).
- MemberList + Status (real values).
- Snapshot RPC (full state dump).
- Migration tool for etcd snapshots.
- Smoke-tested against `etcdctl` and at least one third-party etcd v3
  client.

What's deferred to v0.2+: Auth, MoveLeader, Defragment, Downgrade,
Alarm-on-disk-quota.
