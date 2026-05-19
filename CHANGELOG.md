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
