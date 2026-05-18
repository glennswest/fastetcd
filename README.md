# fastetcd

A Rust, wire-compatible replacement for **etcd v3** designed for realtime and
low-overhead environments. Drop-in for `kube-apiserver` and `etcdctl`, with a
migration path from existing etcd clusters.

## Motivation

Sibling project [`lean-sno`](../lean-sno) measures the Go-runtime tax on
Single Node OpenShift and identifies etcd as one of the top per-process
contributors — **300–800 MB RSS**, GC-induced jitter, and fsync stalls that
push pod-start tail latency on a control-plane-on-the-same-box deployment.
A Rust etcd that preserves the wire contract removes that contributor without
forcing the rest of the OCP control plane to change.

## Goals

1. **API compatibility** with etcd v3 gRPC: KV, Watch, Lease, Maintenance,
   Cluster, Auth. Bar is: unmodified `kube-apiserver` and `etcdctl` work.
2. **Multi-node from day one.** Raft consensus is foundational, not optional.
   Single-node is the degenerate cluster-of-one case.
3. **Data import** from existing etcd clusters: read a BoltDB snapshot and
   replay into fastetcd preserving revisions.
4. **Significantly lower overhead** — target RSS floor an order of magnitude
   below etcd at idle, p99 write latency at or below etcd under
   apiserver-shaped load.

## Non-goals

- v2 HTTP API (deprecated upstream; we are not bringing it back).
- Fork-of-etcd extensions or non-standard features that would diverge the
  wire contract.

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
                             ▼
                       redb (single file, B-tree, ACID)
```

Peer transport is internal gRPC. Read-index linearizable reads by default;
serializable reads available per request (matches etcd `--consistency=l/s`).

## Status

Pre-alpha. Project scaffolding in progress. See
[`docs/00-design.md`](docs/00-design.md) for the design call and
[`CLAUDE.md`](CLAUDE.md) for the live work plan.

## License

Apache 2.0.
