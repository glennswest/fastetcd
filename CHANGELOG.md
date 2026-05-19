# Changelog

## [Unreleased]

### 2026-05-18
- **chore:** Initial project scaffolding — README, CLAUDE.md, CHANGELOG,
  .gitignore, design doc. No code yet.
- **docs:** Design decision recorded: redb storage, openraft consensus,
  tonic gRPC, multi-node from day one. See `docs/00-design.md`.
- **docs:** Reframed project positioning to remove specific usage-context
  framing; fastetcd is positioned as a Rust etcd v3 implementation focused
  on low resource overhead and predictable latency.
- **chore:** Set up Cargo workspace with six crates — `proto`, `storage`,
  `raft`, `server`, `migrate`, `ctl`. Skeleton only; libraries are empty
  and binaries print a "skeleton" notice.

### 2026-05-19
- **feat(server):** Implement the Watch gRPC service — Phase 1.
  Bidirectional stream multiplexing many watchers per connection.
  `WatchCreate` (with key/range, filters, prev_kv flag,
  progress_notify), `WatchCancel`, and `WatchProgressRequest`
  supported. Live event fan-out backed by a new
  `tokio::sync::broadcast` channel on `MvccStore`: every committed
  apply/txn emits an `EventBatch` carrying `Put` / `Delete`
  `MvccEvent`s with optional `prev_kv`. Per-stream tasks: one
  forwards filtered events; one ticks `ProgressNotify` every 10s
  for watchers that enabled it. Watchers created at
  `start_revision < compact_rev` receive a `canceled` response with
  `compact_revision` set, matching etcd. 5 new gRPC tests pass.
- **Known gap:** historical replay (watch starting at a past
  `start_revision <= current`) is not yet implemented; deferred to
  the next watch commit. Workaround: clients can `Range` at the
  desired revision then watch from `start_revision = 0`.
- **feat(server):** Implement the Cluster + Maintenance gRPC services.
  Cluster: `MemberList` returns self (one entry, with `peer_urls` /
  `client_urls` from the CLI flags); `MemberAdd` / `MemberRemove` /
  `MemberUpdate` / `MemberPromote` return `Status::unimplemented`
  until peer transport (task #13) is in. Maintenance: `Status`
  populates real values from openraft metrics (leader, term, applied
  index) and the MVCC engine (db_size, revision); `Hash` / `HashKV`
  compute SHA-256 over the `mvcc_kv` table (folded to a u32 for the
  wire shape); `Snapshot` streams the bincode-serialized state
  machine snapshot in 64 KiB chunks; `Defragment` is a no-op;
  `MoveLeader` / `Downgrade` return `Status::unimplemented`.
  `Alarm` returns an empty list. 5 new gRPC-level tests pass.
- **feat(server):** Implement the KV gRPC service. `KvService`
  implements all five `Kv` RPCs (Range, Put, DeleteRange, Txn,
  Compact). Mutating ops propose `FastetcdLogEntry`s through
  `Raft::client_write`; reads serve directly from `MvccStore`
  (serializable on single-node, which is equivalent to linearizable
  with no peers). Header `cluster_id`, `member_id`, `revision`, and
  `raft_term` are populated from `ServerState` + Raft metrics.
  Compact maps `MvccError::Compacted` to gRPC `OutOfRange` matching
  etcd. The `fastetcd` binary now boots a single-node Raft cluster
  and serves the KV service on `--listen-client-url`. End-to-end
  tests through real tonic clients pass (6 cases: put+range,
  prev_kv, delete-range, historical read, txn success, compact).
  Watch / Lease / Maintenance / Cluster services land in the next
  commits.
- **feat(raft):** openraft integration (single-node). `TypeConfig`,
  `FastetcdLogEntry` (Apply / Txn / Compact / Noop), and
  `FastetcdLogResponse`. `FastetcdStateMachine` wraps `MvccStore` and
  dispatches every committed entry to the matching MVCC operation;
  snapshot building serializes the full MVCC state +
  `last_applied_log_id` + membership into `Cursor<Vec<u8>>`.
  `MemLogStore` (in-memory `BTreeMap<u64, Entry>`) implements
  `RaftLogStorage` + `RaftLogReader` — production KvStore-backed log
  storage lands in task #14. Integration test: a one-node cluster
  proposes an `Apply` entry via `client_write`, sees revision 1
  returned, then reads the value back from MVCC. Added serde derives
  to MVCC `Mutation`, `MutationResult`, `RangeResult`, `Compare`,
  `CompareOp`, `CompareTarget`, `RangeOp`, `TxnOp`, `TxnOpResult`,
  `TxnResult` so log entries serialize round-trip.
- **feat(storage):** MVCC `Txn(compare, success, failure)`. Compare
  types: `Version`, `CreateRevision`, `ModRevision`, `Value`, `Lease`
  with `Equal | NotEqual | Greater | Less`. Single-key and range
  compares (range compares require all keys in the range to satisfy);
  absent keys compare against the implicit zero record (matches
  etcd). Ops in chosen branch run in order: reads see the pre-mutation
  snapshot, all writes share one new `main` revision (with distinct
  sub-revisions). Internal refactor: extracted `apply_inner` /
  `range_inner` so `apply()`, `range()`, and `txn()` share one write
  lock. 7 new Txn tests (46 total pass).
- **feat(storage):** MVCC `Compact(rev)`. Walks every `KeyIndex` and
  prunes generations whose tombstone is `<= rev`; in surviving
  generations drops all puts strictly older than the latest put
  `<= rev` (preserving the floor so `Range` at `rev` still works).
  Dropped `KvRecord`s removed atomically. Persists `compact_rev` so
  the floor survives reopen. Range check is now strict: reads at
  `target_rev < compact_rev` return `MvccError::Compacted`; reads at
  `target_rev == compact_rev` succeed (off-by-one fix from previous
  commit). 8 new tests (39 total pass).
- **feat(storage):** MVCC state machine over `KvStore`. Per-key
  generation index (`KeyIndex` / `Generation`), revision-keyed values
  (`KvRecord`), and atomic multi-mutation `apply` that assigns one
  `main` revision per call with distinct `sub` per mutation. Range
  reads support current state, historical revision (`target_rev`),
  `limit` (with `more` flag), `keys_only`, `count_only`. `prev_kv` on
  Put and DeleteRange returns the prior records. Future-revision and
  compacted-revision errors match etcd's messages. 19 mvcc unit
  tests pass on the redb backend (including reopen-persistence).
  Compact + Txn deferred to follow-up commit (task #17).
- **feat(storage):** Define the engine-agnostic `KvStore` trait and
  implement the `redb` engine. Concrete `WriteBatch` value type (no
  trait-object downcast). Conformance test suite under
  `crate::kvstore::conformance` runs against any engine; six tests pass
  on the redb backend (put/get/delete, range scan, delete-range,
  snapshot isolation, count, engine name). `iouring` engine module
  ships as a feature-gated skeleton — calls return
  `StorageError::Misuse` until task #15 lands.
- **feat(proto):** Vendor etcd v3.6.11 .proto files (`rpc.proto`, `kv.proto`,
  `auth.proto`) and wire up `tonic-build` codegen. Reproducible
  re-vendoring via `crates/proto/vendor.sh` and `strip_annotations.py`,
  which removes annotations we don't need (gogoproto, grpc-gateway,
  versionpb metadata). Generated stubs re-exported from
  `fastetcd_proto::{etcdserverpb, mvccpb, authpb}`.
- **docs:** Storage layer reframed as a trait-first abstraction with two
  first-class engines selectable at runtime: `redb` (default,
  cross-platform) and `iouring` (Linux, `glommio` + `O_DIRECT` + custom
  WAL; behind cargo feature `iouring`). Both are supported, not
  prototype-and-replace. SPDK remains a long-term option, not committed
  work. Design doc, README, CLAUDE.md updated.
