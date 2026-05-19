# fastetcd

A Rust implementation of the **etcd v3 wire protocol**, focused on low
resource overhead and predictable latency. Wire-compatible with unmodified
etcd v3 clients, with a migration path from existing etcd data.

## Goals

1. **Wire compatibility** with etcd v3 gRPC: KV, Watch, Lease, Maintenance,
   Cluster, Auth.
2. **Multi-node from day one.** Raft consensus is foundational, not
   optional. Single-node is the degenerate cluster-of-one case.
3. **Data import** from existing etcd installations: read an etcd BoltDB
   snapshot and replay into fastetcd preserving revisions.
4. **Lower overhead** — a smaller RSS floor than upstream etcd at idle,
   p99 write latency at or below upstream under sustained load, and no
   GC-induced jitter on the apply path.

## Non-goals

- v2 HTTP API (deprecated upstream; not in scope).
- Extensions that would diverge the wire contract.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ tonic gRPC: KV / Watch / Lease / Maintenance / Cluster / Auth        │
└────────────────────────────┬─────────────────────────────────────────┘
                             │   (writes funneled through leader)
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│ openraft  ── consensus, log replication, membership, snapshots       │
└────────────────────────────┬─────────────────────────────────────────┘
                             │   (apply)
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│ MVCC state machine: key→revision index, revision→value store,        │
│                     leases, compaction, watches                      │
└────────────────────────────┬─────────────────────────────────────────┘
                             │   (KvStore trait — engine-agnostic)
                             ▼
        ┌────────────────────┴───────────────────┐
        │                                        │
   redb engine                            iouring engine
   (default, cross-platform,              (Linux, glommio + O_DIRECT
    ACID single-file B-tree)               + group-committed WAL)
```

Peer transport is internal gRPC. Read-index linearizable reads by default;
serializable reads available per request (matches etcd `--consistency=l/s`).

## Status

Pre-alpha. Project scaffolding in progress. See
[`docs/00-design.md`](docs/00-design.md) for the design call and
[`CLAUDE.md`](CLAUDE.md) for the live work plan.

## License

Apache 2.0.
