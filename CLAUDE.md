# CLAUDE.md — fastetcd

Project-specific context. Cross-project rules live in `../CLAUDE.md`.

## Project summary

Rust implementation of the etcd v3 wire protocol. Wire-compatible gRPC.
Multi-node Raft from day one. Importer for existing etcd BoltDB data.
Focused on low resource overhead and predictable latency.

## Version

**`1.0.0`** — First stable release. The wire-compat bar is met end to
end: unmodified etcd v3 clients (etcdctl, client-go, kubeadm) drive
KV/Watch/Lease/Cluster/Maintenance/Auth through Raft, single- and
multi-node, with linearizable reads by default. Shipped as the #10 fix
below closes the last known correctness gap surfaced by the rustkube
soak tests.

Headline fix — linearizable reads (#10). A single-client
read-modify-write loop (GET `mod_revision` → txn
`Compare::mod_revision(Equal)` + put) saw ~25% spurious CAS failures on
a cluster with no concurrent writer. Root cause: fastetcd never
implemented linearizable reads — `serve_range` ignored `serializable`
(`let _ = req.serializable;`) and always read local state, so a
follower whose state machine lags, or a leader that has silently lost
leadership, answered a default read with stale data; the client's next
GET then returned a `mod_revision` older than committed, and the
following CAS compared against it and failed. Fix: a default
(non-serializable) Range now runs a read barrier first — on the leader
`Raft::ensure_linearizable` (ReadIndex + wait-for-apply), on a follower
forward the whole Range to the leader over a new `RaftPeer.ForwardRead`
RPC. Cluster-of-one satisfies the barrier trivially, so single-node
reads stay a direct local read; `serializable=true` still opts out.
The single-machine test harness applies too fast to show the
staleness, so the regression test asserts the barrier itself: a node
that can't confirm leadership must *fail* a linearizable read rather
than serve stale local state (verified it fails without the fix).

Previous: **`0.8.3`** — Three durability/compat fixes found by the rustkube
cluster tests.

`#9` — a single node couldn't survive a reboot. `FastetcdStateMachine`
rebuilt `last_applied_log_id`/`last_membership` as `None` on *every*
open (`main.rs` called `new(mvcc)`, which had no restore path), so
openraft saw an empty state machine next to a populated MVCC store and
replayed the log from index 0. That crash-looped once a snapshot had
purged the early entries ("expected index [0, N), got [None, None)")
and, when the log happened to be intact, silently double-applied every
mutation — the second failure mode wasn't in the report. Both fields
now persist into `mvcc_meta`, staged before an entry is dispatched and
folded into that entry's own `WriteBatch`, so the mutation and the log
id describing it commit in one fsync. Note this was never log
corruption: the raft log was fine, the state machine just didn't know
where it was.

Data dirs already in the bad state have no applied position, so
startup adopts the log's `last_purged_log_id` as a floor (openraft
only purges what it has applied *and* snapshotted). **This re-applies
a bounded tail and can advance the revision past what clients
previously observed** — logged loudly at `warn`. Strictly better than
the old workaround (wiping the data dir), but not lossless.

`#8` — a rejoined member stayed at an old MVCC revision. Not a
snapshot-transfer problem: `rebuild_mvcc` replaces every MVCC table by
writing straight through the engine, but `MvccStore` caches
`current_rev`/`compact_rev`/`next_lease_id` from open. The snapshot
landed on disk while the handle served the stale counters — reads
clamp to `current_rev` (hiding every key above it, the reported "504
of 1004") and new writes allocate from `current_rev + 1`, colliding
with the snapshot's revisions. Added `MvccStore::reload_write_state`.
v0.8.2 fixed the *leader* side of this path (torn snapshot build);
this is the follower side, which is why #8 stayed open.

`#7` — `etcdctl member add/remove` against a follower returned
openraft's raw `ForwardToLeader` error. Membership changes go through
openraft's own APIs rather than the replicated log, so they can't ride
on the `ForwardWrite` RPC added for #4; added a sibling
`RaftPeer.ForwardMembership` over the same peer channel plus
`ServerState::propose_add_learner`/`propose_set_voters`.

Each regression test was confirmed to fail without its fix, not merely
pass with it.

Previous: **`0.8.2`** — Atomic snapshot build: capture the MVCC handle
and `last_applied` together under the state-machine lock, so a
concurrent apply can't produce a snapshot whose data predates its own
`last_applied_log_id`.

Previous: **`0.8.1`** — Security fix (#6): `--client-cert-auth` was never
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
