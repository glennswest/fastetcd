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
