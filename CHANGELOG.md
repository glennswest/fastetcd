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
