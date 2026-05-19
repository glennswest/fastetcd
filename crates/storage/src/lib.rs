//! Storage layer for fastetcd.
//!
//! Two responsibilities:
//!
//! 1. **`KvStore` trait** — engine-agnostic key-value store interface
//!    (see [`kvstore`]). Higher layers — the MVCC state machine, the
//!    Raft log adapter — depend on this trait only.
//!
//! 2. **Concrete engines** implementing the trait:
//!    - [`redb_engine::RedbEngine`] — default; cross-platform; ACID
//!      single-file B-tree.
//!    - [`iouring_engine::IoUringEngine`] — Linux only, behind cargo
//!      feature `iouring`; `glommio` + `O_DIRECT` + custom WAL.
//!
//! The MVCC state machine layer and the lease/watch fan-out layer live
//! in this crate too, but land in later milestones.

pub mod kvstore;

#[cfg(feature = "redb-engine")]
pub mod redb_engine;

#[cfg(all(feature = "iouring", target_os = "linux"))]
pub mod iouring_engine;

pub use kvstore::{KvStore, Snapshot, StorageError, StorageResult, WriteBatch, WriteOptions};
