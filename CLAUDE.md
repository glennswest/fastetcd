# CLAUDE.md — fastetcd

Project-specific context. Cross-project rules live in `../CLAUDE.md`.

## Project summary

Rust implementation of the etcd v3 wire protocol. Wire-compatible gRPC.
Multi-node Raft from day one. Importer for existing etcd BoltDB data.
Focused on low resource overhead and predictable latency.

## Version

**`0.0.0`** — pre-alpha, no public surface yet.

Version locations (keep in sync):
- `Cargo.toml` workspace `[workspace.package] version`
- This file (the line above)
- Tags `vX.Y.Z`

## Architecture pillars

- **Storage**: `redb` (ACID B-tree, single Rust crate, single file).
- **Consensus**: `openraft` — core, not optional. Single-node is a
  cluster-of-one.
- **gRPC**: `tonic` + `prost`, generated from vendored etcd `.proto` files.
- **Async runtime**: `tokio` (multi-threaded).
- **Logging/tracing**: `tracing` + `tracing-subscriber`.

## Repo layout

```
crates/
  proto/      # tonic-generated etcd v3 stubs
  storage/    # MVCC state machine over redb
  raft/       # openraft glue: log storage, state machine adapter, transport
  server/     # binary: gRPC frontend + raft node + lifecycle
  migrate/    # binary: read etcd BoltDB → write into fastetcd
  ctl/        # binary: minimal etcdctl-compat smoke client
docs/
  00-design.md
benches/
```

## Work plan

Tracked live in the Claude task system. Snapshot of the order:

1. Project scaffolding (done)
2. Cargo workspace + crate skeletons
3. Vendor etcd protos, wire up tonic codegen
4. MVCC state machine over redb
5. openraft integration (log storage, state machine adapter)
6. Raft peer transport (gRPC) + discovery
7. KV gRPC service
8. Watch service
9. Lease service
10. Maintenance + Cluster services
11. Migration tool from etcd BoltDB
12. Benchmarks vs upstream etcd

## Constraints & rules

- **Wire compatibility is the bar.** If unmodified etcd v3 clients don't
  work, the feature isn't done.
- **No v2 HTTP API.** Deprecated upstream; not in scope.
- **Linearizable reads by default** (read-index), serializable opt-in.
  Match etcd's semantics, not "close enough."
- **All writes go through Raft.** No direct state-machine writes from gRPC
  handlers, even for "internal" things like lease expiry — those go
  through Raft too so they survive failover.
- **Compaction-correctness is non-negotiable.** Revision history truncation
  must coordinate with watchers and raft log compaction.
