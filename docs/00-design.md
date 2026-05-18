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

### Storage: `redb`

ACID, single-file, native Rust, B-tree (matches BoltDB's shape, simplifying
MVCC translation). Single-writer with MVCC snapshots for readers, which
is exactly what we need: writes serialize through the Raft apply thread,
readers serve from snapshots.

**Alternatives considered:** `fjall` (LSM; better for write-heavy but adds
compaction tuning surface), `sled` (mature but pre-1.0 forever, less
predictable), custom WAL+B-tree (out of scope to build from scratch).

Re-evaluate redb if benchmarks show write amplification or compaction
pauses that hurt p99 under sustained load.

### Consensus: `openraft`

Async-native, Rust-idiomatic, supports membership changes including joint
consensus, snapshot install, log compaction. Mature enough to underpin a
production system. We integrate by:

- Implementing `RaftLogStorage` over a redb table (append-only with
  truncation; fsync per batch before ack).
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
  storage/    # MVCC state machine, lease registry, watch fan-out
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
- Defragment semantics on redb: redb has compact-in-place — verify it is
  online before claiming `Maintenance.Defragment` support.
- Watch event ordering across compaction boundaries — etcd has specific
  guarantees for `ErrCompacted`; match exactly.

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
