# CLAUDE.md — fastetcd

Project-specific context. Cross-project rules live in `../CLAUDE.md`.

## Project summary

Rust implementation of the etcd v3 wire protocol. Wire-compatible gRPC.
Multi-node Raft from day one. Importer for existing etcd BoltDB data.
Focused on low resource overhead and predictable latency.

## Version

**`0.3.0`** — adds the full Auth gRPC surface (Phase 1: User/Role
management + Authenticate with argon2), real `Maintenance.Defragment`
backed by engine-level compaction, and a real `IouringEngine` on
Linux backed by `tokio-uring`. The only remaining v0.1.0-era gap
is the openraft 0.10 upgrade for real `MoveLeader`, deferred until
0.10 stabilizes.

Version locations (keep in sync):
- `Cargo.toml` workspace `[workspace.package] version`
- This file (the line above)
- Tags `vX.Y.Z`

## Architecture pillars

- **Storage**: trait-first abstraction (`KvStore`) with two first-class
  engines selectable at runtime: `redb` (default, cross-platform) and
  `iouring` (Linux, `glommio` + `O_DIRECT` + custom WAL; behind cargo
  feature `iouring`).
- **Consensus**: `openraft` — core, not optional. Single-node is a
  cluster-of-one.
- **gRPC**: `tonic` + `prost`, generated from vendored etcd `.proto` files.
- **Async runtime**: `tokio` for the gRPC frontend and Raft node;
  `glommio` runtime is internal to the iouring engine.
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
3. Vendor etcd protos, wire up tonic codegen (done)
4. Design `KvStore` trait + implement redb engine
5. MVCC state machine over `KvStore`
6. openraft integration (log storage trait + redb impl, state machine adapter)
7. Raft peer transport (gRPC) + discovery
8. KV gRPC service
9. Watch service
10. Lease service
11. Maintenance + Cluster services
12. iouring engine implementing `KvStore` (Linux-only, cargo feature)
13. Migration tool from etcd BoltDB
14. Benchmarks: redb engine vs iouring engine vs upstream etcd

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
