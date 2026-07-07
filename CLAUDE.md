# CLAUDE.md — fastetcd

Project-specific context. Cross-project rules live in `../CLAUDE.md`.

## Project summary

Rust implementation of the etcd v3 wire protocol. Wire-compatible gRPC.
Multi-node Raft from day one. Importer for existing etcd BoltDB data.
Focused on low resource overhead and predictable latency.

## Version

**`0.8.1`** — Security fix (#6): `--client-cert-auth` was never
actually enforced. The flag had no `env` binding and no entry in the
`ETCD_*`→`FASTETCD_*` compat shim, so `ETCD_CLIENT_CERT_AUTH=true`
(the documented drop-in config) was silently ignored — the flag
stayed `false`, TLS was built with no client-CA verifier, and any
client could complete the handshake and read/write anonymously on
both the gRPC port and the `/health` route. Wired the `env` binding +
compat pair (same for `peer-client-cert-auth`) and set
`client_auth_optional(false)` explicitly. Verified end to end:
certless TLS clients are now rejected at the handshake; CA-signed
clients still succeed.

Previous: **`0.8.0`** — Found and fixed the real reason releases had stalled:
GitHub Actions is disabled at the repo-settings level (confirmed off
since 2026-05-24, not a workflow bug or billing issue) — every push
and tag since then, including `v0.6.0`/`v0.7.0`, silently ran
nothing. Decision: leave Actions off; `dev.g8.lo` is now the sole
build+test path, scripted via `deploy/packaging/run-tests.sh` and
`deploy/packaging/build-release.sh` (the now-dead
`.github/workflows/ci.yml` was deleted). Published the missing
`v0.7.0` GitHub Release from `dev.g8.lo`, and verified its rpm/deb
end to end there (systemd unit, `fastetcd-ctl put`/`get` through
Raft) — noting a Fedora + SELinux `Enforcing` quirk with foreign
`dpkg` maintainer scripts that isn't a package defect.

Previous: **`0.7.0`** — Fixed a multi-node blocker (#4): client writes sent to
a non-leader now actually forward to the leader over the existing
peer channel (new `RaftPeer.ForwardWrite` RPC / `WriteForwarder` in
`crates/raft/src/network.rs`), instead of relaying openraft's
`ForwardToLeader` error straight to the client with an empty
address. `raft.initialize()` also now carries real peer-URL
`BasicNode` addresses. Added etcd-compat `GET /health` (+ `/livez`,
`/readyz`) on the client gRPC port itself via tonic 0.12's
`Routes`↔`axum::Router` interop (#5), so LB/k8s health probes work
without a second port.

Earlier: **`0.6.0`** — `ETCD_*` env var drop-in compat (falls back to etcd's
env names when `FASTETCD_*` is unset), a systemd unit
(`deploy/systemd/fastetcd.service`, autostart + unconditional
auto-restart) and rpm/deb packaging (`crates/server/Cargo.toml`
`[package.metadata.deb]` / `[package.metadata.generate-rpm]`) that
bundle all three binaries as static `x86_64-unknown-linux-musl`
builds — avoids baking in a glibc-version requirement from the
packaging host. Verified end to end on dev.g8.lo (Fedora 43):
`dnf install` / `dpkg -i` both create the `fastetcd` system user,
enable, and start the service automatically.

Older: **`0.5.0`** — Kubernetes-ready: grpc.health.v1.Health for
service-mesh probes, a Helm chart at `deploy/charts/fastetcd`,
real `fastetcd-ctl` client (put/get/del/snapshot-save), README
rewrite. Only remaining gap from the v0.1.0 era is the openraft
0.10 upgrade for real `MoveLeader`.

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
