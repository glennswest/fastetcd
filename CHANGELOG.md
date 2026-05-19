# Changelog

## [Unreleased]

### 2026-05-19
- **feat(storage + server):** `Maintenance.Defragment` is real.
  New `KvStore::defragment()` trait method (default no-op for
  engines that don't compact). redb engine wraps its
  `Database` in a `tokio::sync::RwLock` and runs `compact()`
  under the write lock; commit/snapshot take the read lock so
  defrag serializes against writes without starving reads. WAL
  engine compacts by replaying its in-memory index into a fresh
  WAL file and atomically renaming. `Maintenance.Defragment`
  delegates to the active engine's implementation.
  Test: write+overwrite 50 keys, defragment, verify the latest
  value is still readable.
- **feat(server):** Auth gRPC service — Phase 1. Implements every
  Auth RPC (`AuthEnable` / `Disable` / `Status`, `Authenticate`,
  full User / Role CRUD, grant / revoke). Passwords hashed with
  `argon2` (default cost). Authenticate validates the stored hash
  and returns a 32-byte hex-encoded random token; tokens live in
  an in-memory registry on the local node. AuthEnable refuses
  unless a `root` user has been added (matches etcd's
  precondition). RoleDelete cascade-drops the role from every
  user that referenced it. New `mvcc/auth.rs` defines persisted
  `StoredUser` / `StoredRole` / `StoredPermission` types; auth
  data lives in dedicated tables outside the MVCC revisioned
  space (matching etcd's split). 6 new gRPC tests pass.
- **Phase 2 (not in this commit):** per-request token enforcement
  via a tower::Layer wrapper. The Phase 1 `AuthInterceptor`
  placeholder is wired but accepts every request — tonic 0.12's
  sync interceptor signature can't await the token registry
  cleanly; switching to a tower::Layer is the right shape.

## [v0.2.0] — 2026-05-19

Closes every known gap from v0.1.0 except the iouring kernel-bypass
backend (which remains genuinely multi-week follow-up work). 84
tests pass workspace-wide (+9 since v0.1.0). The `etcd-client`
third-party Rust client successfully drives a fastetcd server
through put / get / range / delete / txn / lease / watch /
member-list / status without modification — the strongest
wire-compat signal we can ship in CI.

### Added

- **Lease auto-expiry ticker** (`crates/server/src/lease_expiry.rs`).
  Background task runs on the leader only, once per second walks
  the persisted lease set, and proposes `LeaseRevoke` entries for
  any whose `deadline_unix_secs < now`. Cascade-delete fires through
  the same path explicit revokes use, so watchers see the delete
  events normally. Closes the v0.1.0 "no auto-expiry" gap.
- **Real cluster membership handlers** (`crates/server/src/cluster.rs`).
  `MemberAdd` derives a stable node id from the peer URL, registers
  the URL with the live `PeerEndpoints` map, calls openraft's
  `add_learner`, and (for non-learner) `change_membership` to
  promote. `MemberRemove` rebuilds the voter set minus the target
  and drops peer/directory entries (refuses self-removal).
  `MemberUpdate` rewrites the peer URL. `MemberPromote` lifts a
  learner to voter. `MemberList` returns the live directory with
  name + URLs + learner state. Replaces the v0.1.0 `Unimplemented`
  stubs.
- **Revision-preserving migration**
  (`MvccStore::bulk_load_records`, `MigrationMode::PreserveRevisions`).
  `fastetcd-migrate --preserve-revisions` writes `mvcc_kv` +
  `mvcc_idx` (+ `lease_keys`) directly so source records retain
  their original `create_revision`, `mod_revision`, `version`, and
  `lease`. After migration with the flag, `Range(rev)` and
  `Watch(start_rev)` behave identically to the source server's
  history.
- **Third-party Rust client compatibility tests**
  (`crates/server/tests/etcd_client_compat.rs`). Uses the
  `etcd-client` crate (etcdv3/etcd-client) — which shares no code
  with fastetcd — to drive fastetcd through put / get / range with
  prefix / delete-range with prev_kv / Txn compare-and-set / lease
  grant+attach+revoke with cascade-delete / Watch / MemberList /
  Status. 8 tests, ~0.5s, runs in normal CI without needing the
  Go toolchain.
- **`tests/etcdctl_smoke.sh`** — optional out-of-tree wire-compat
  smoke using the canonical `etcdctl` client. Boots a release-mode
  fastetcd, runs a representative command set, tears down. Skips
  with a clear message when etcdctl isn't installed.
- **`docs/02-testing.md`** — three-ring testing strategy: workspace
  tests, third-party client, upstream etcd suites (with concrete
  pointers at the robustness suite + Jepsen + K8s e2e for higher
  rings).

### Changed

- `Maintenance.MoveLeader` now validates the target is a current
  voter (`FailedPrecondition` if not — matches etcd) and returns a
  typed `Unimplemented` with a precise explanation of the
  openraft-0.9 limitation. Real leader transfer arrives with an
  openraft 0.10 upgrade.

### Remaining known gaps

- **iouring kernel-bypass file I/O.** `WalEngine` ships the
  architecture; swapping the file-I/O layer for glommio + `O_DIRECT`
  remains the right next step for sub-ms p99 on Linux.
- **openraft 0.10 upgrade** — needed for `MoveLeader`'s real
  implementation and potentially other primitives.
- **Auth gRPC service**.
- **`Defragment` / `Downgrade`**.

## [v0.1.0] — 2026-05-19

### 2026-05-19
- **test(server):** Third-party Rust client compatibility tests.
  New `tests/etcd_client_compat.rs` uses the widely-used
  `etcd-client` crate (etcdv3/etcd-client) — which shares no code
  with fastetcd — to validate the wire protocol end-to-end: put /
  get / range with prefix / delete-range with prev_kv / Txn
  compare-and-set / Lease grant+attach+revoke with cascade-delete /
  Watch (drop create-ack + progress notifies) / MemberList /
  Status. 8 tests, runs in ~0.5s. This is the strongest in-repo
  wire-compat signal.
- **test(harness):** `tests/etcdctl_smoke.sh` — optional shell
  harness that boots a release-build fastetcd and runs `etcdctl`
  through a representative command set (put/get/del/range/txn/
  lease/member/endpoint-status). Skipped when etcdctl isn't on
  PATH.
- **docs:** New `docs/02-testing.md` lays out the three-ring
  testing strategy (workspace tests → third-party Rust client →
  upstream etcd robustness / etcdctl / Jepsen / Kubernetes e2e)
  and points future correctness work at etcd's robustness suite
  as the highest-ROI investment.
- **feat(storage + migrate):** Revision-preserving migration. New
  `MvccStore::bulk_load_records(BulkKey[], next_rev)` writes
  `mvcc_kv` + `mvcc_idx` (and `lease_keys`) directly so source
  records retain their original `create_revision`, `mod_revision`,
  `version`, `lease`, and tombstone state. `fastetcd-migrate`
  gains `--preserve-revisions` (and a corresponding
  `MigrationMode::PreserveRevisions` library API). After migration
  with the new flag, `Range(rev)` and `Watch(start_rev)` behave
  identically to the source server's history. Multi-generation
  per-key is collapsed to "everything up to the most recent
  tombstone + its generation"; multi-generation history support
  is a follow-up.
- **feat(server):** `Maintenance.MoveLeader` now validates the
  target is a current voter and returns a typed
  `FailedPrecondition` for invalid targets; for valid ones it
  surfaces openraft 0.9's lack of an explicit `transfer_leader`
  primitive via `Unimplemented` with a precise message. Real
  transfer arrives with the openraft 0.10 upgrade.
- **feat(server):** Real cluster membership handlers. `MemberAdd`
  derives a stable node id from the peer URL (FNV-1a hash, masked
  to 63 bits), registers the URL with the live `PeerEndpoints` map
  used by `GrpcNetworkFactory` (so the leader can dial the new node
  immediately), and calls openraft's `add_learner`; voting members
  additionally call `change_membership` to promote. `MemberRemove`
  removes from voters via `change_membership(remaining, false)` and
  drops the peer/directory entries (refuses to remove self).
  `MemberUpdate` rewrites the peer URL entry; `MemberPromote` lifts
  a learner to voter. `MemberList` returns the live directory which
  also tracks `name`, `client_urls`, and `is_learner` per member.
  The directory is seeded from `--initial-cluster` on bootstrap.
  Replaces the previous `Unimplemented` stubs. New test verifies
  MemberAdd-as-learner appears in MemberList.
- **feat(server):** Lease auto-expiry ticker. Background task runs
  on the leader only (followers are no-ops) and once per second
  walks the persisted lease set; any lease with
  `deadline_unix_secs < now` is auto-revoked via the same
  `FastetcdLogEntry::LeaseRevoke` path explicit revokes use, so
  attached keys cascade-delete through Raft. Leadership transitions
  hand the work to the new leader on its next tick. Closes a known
  gap from v0.1.0. End-to-end test (`lease_expiry_grpc.rs`) grants
  a 1-second lease, attaches two keys, waits 2.5s, and verifies the
  lease is gone and both keys are deleted.

## [v0.1.0] — 2026-05-19

Initial usable release. fastetcd boots a single-node or multi-node
cluster, serves the full etcd v3 KV / Watch / Lease / Cluster /
Maintenance gRPC surface, and persists state through openraft.

### Added

- **Project scaffold and Cargo workspace** with six crates: `proto`,
  `storage`, `raft`, `server`, `migrate`, `ctl`. Apache 2.0 license.
- **Vendored etcd v3.6.11 `.proto` files** (`rpc.proto`, `kv.proto`,
  `auth.proto`) with a reproducible `vendor.sh` pipeline that strips
  gogoproto / grpc-gateway / versionpb annotations. `tonic-build`
  codegen emits stubs under `fastetcd_proto::{etcdserverpb, mvccpb,
  authpb}`.
- **Engine-agnostic `KvStore` trait** with concrete `WriteBatch`
  value, `Snapshot` trait, `WriteOptions`, and a conformance test
  suite reusable by any backend.
- **`redb` storage engine** (default; cross-platform). Single-file
  ACID B-tree. Passes the conformance suite (6 tests).
- **`WalEngine` storage engine** (opt-in via `wal-engine` feature).
  Append-only WAL + in-memory `BTreeMap` index, replay-on-open.
  Passes the conformance suite + persistence-across-reopen.
- **MVCC state machine** over `KvStore`: revisions
  (`Revision { main, sub }` packed 16-byte BE), per-key
  generation index, revision-keyed value table, atomic
  multi-mutation `apply`. `Range` supports current/historical
  reads, `limit` (with `more`), `keys_only`, `count_only`.
  `Compact(rev)` walks `KeyIndex` and prunes generations whose
  tombstone is `<= rev`; preserves the floor record per key for
  exact-rev historical reads. `Txn` with `Compare`
  (Version/CreateRev/ModRev/Value/Lease × Equal/NotEqual/Greater/
  Less, single-key or range) and atomic success/failure branches
  sharing one revision. `range_events()` backs Watch historical
  replay.
- **Lease layer** on `MvccStore`: lease records persisted in the
  `lease` table, `lease_keys` reverse index updated on every
  Put/DeleteRange with non-zero lease, `apply_lease_grant`,
  `apply_lease_revoke` cascade-deletes attached keys, `lease_ttl`
  computes remaining seconds, `lease_list`.
- **openraft integration**: `FastetcdLogEntry` (Apply / Txn /
  Compact / LeaseGrant / LeaseRevoke / LeaseKeepAlive / Noop),
  `FastetcdLogResponse`, `FastetcdStateMachine` wrapping
  `MvccStore`, `KvLogStore` (persistent `RaftLogStorage` over
  `KvStore`), `FastetcdSnapshotBuilder`.
- **Multi-node Raft peer transport**: internal proto
  `fastetcd.raft.RaftPeer` (AppendEntries / Vote / InstallSnapshot)
  carrying bincode-serialized openraft messages.
  `GrpcNetworkFactory` lazily dials peers over tonic;
  `RaftPeerService` handles inbound RPCs.
- **KV gRPC service**: Range, Put, DeleteRange, Txn, Compact.
  Routes mutations through Raft; reads served from `MvccStore`
  (serializable; on single-node this is linearizable since there
  are no replicas). `OutOfRange` on compacted-rev reads.
- **Watch gRPC service** (Phase 1 + 2): live event fan-out via a
  `broadcast::channel` on `MvccStore` plus historical replay from
  `start_revision`. Range filters, NOPUT/NODELETE filters,
  `prev_kv`, progress-notify timer, compacted-rev cancellation.
- **Lease gRPC service**: Grant, Revoke, KeepAlive (bidi stream),
  TimeToLive (with optional attached-keys list), Leases.
- **Cluster gRPC service**: `MemberList` returns self;
  Add/Remove/Update/Promote return `Unimplemented` (membership
  changes via openraft are tracked as follow-up work).
- **Maintenance gRPC service**: Status (real raft + MVCC values),
  Hash / HashKV (SHA-256 folded to u32), Snapshot (streaming),
  Alarm, Defragment (no-op), MoveLeader / Downgrade unimplemented.
- **`fastetcd-migrate` tool**: reads an etcd v3 BoltDB snapshot
  (via `bbolt-rs`), keeps the latest record per user-key, bulk-
  applies into a fresh `MvccStore`. Tombstones detected via the
  trailing `b't'` on bolt keys.
- **`fastetcd` server binary** with etcd-shaped flags:
  `--name`, `--node-id`, `--cluster-id`, `--data-dir`,
  `--listen-client-url`, `--listen-peer-url`, `--initial-cluster`,
  `--initial-cluster-state`. Multi-port serving: client services
  on one socket, RaftPeer service on the peer socket. Single
  `redb` file in `--data-dir` backs both MVCC state and Raft log.
- **Criterion benchmark harness** for the MVCC layer: put-single,
  range-single, batched put, delete-range, comparable across
  engines (`redb` vs `wal`).
- **75 tests pass workspace-wide.** All gRPC services have
  end-to-end tests through real tonic clients; a 3-node multinode
  test validates Raft replication over the gRPC peer transport.

### Known gaps

These are documented and tracked as follow-up work:

- **iouring kernel-bypass file I/O.** The `iouring` cargo feature
  exists and depends on `wal-engine`, but the glommio + `O_DIRECT`
  file-I/O layer that delivers the actual sub-ms p99 is not yet
  implemented. `WalEngine` provides the architectural shape that
  swap will live under.
- **Lease auto-expiry.** Deadlines are persisted and
  `LeaseTimeToLive` returns correct remaining seconds, but no
  leader-side ticker auto-revokes expired leases. Clients must
  explicitly revoke for cascade-delete to fire.
- **Cluster membership changes** (`MemberAdd` / `Remove` / `Update`
  / `Promote`). The peer transport is in; the gRPC handlers
  currently return `Unimplemented`. Wiring through openraft's
  `change_membership` API is straightforward follow-up.
- **Auth gRPC service** is deferred. Common deployments use TLS
  client-cert authentication outside the etcd Auth subsystem.
- **`MoveLeader` / `Defragment` / `Downgrade`** return
  `Unimplemented`.
- **Migration tool revision history**: currently imports only the
  latest record per user-key. Revision-preserving import is a
  follow-up (it requires direct mvcc_kv / mvcc_idx writes
  bypassing the public `apply` path).

### Storage architecture (recap)

```
┌──────────────────────────────────────────────────────────────────┐
│ tonic gRPC: KV / Watch / Lease / Cluster / Maintenance           │
└──────────────────────────────┬───────────────────────────────────┘
                               │ (writes proposed through Raft)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ openraft — consensus, log replication, membership, snapshots     │
│ KvLogStore over KvStore   ·   FastetcdStateMachine over MvccStore │
└──────────────────────────────┬───────────────────────────────────┘
                               │ (apply)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ MVCC state machine: revisions, generations, leases, events       │
└──────────────────────────────┬───────────────────────────────────┘
                               │ (KvStore trait, runtime-selectable)
                               ▼
                ┌──────────────┴──────────────┐
                │                             │
            redb engine                  wal-engine
        (default, B-tree)         (append-only WAL +
                                   in-memory BTreeMap;
                                   iouring backend below
                                   this is future work)
```

## [v0.0.0] — pre-release

See git history for individual commits prior to v0.1.0.
