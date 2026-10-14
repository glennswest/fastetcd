//! Storage layer for fastetcd.
//!
//! Two responsibilities:
//!
//! 1. **`KvStore` trait** — engine-agnostic key-value store interface
//!    (see [`kvstore`]). Higher layers — the MVCC state machine, the
//!    Raft log adapter — depend on this trait only.
//!
//! 2. **The engine**: [`redb_engine::RedbEngine`], a single-file ACID
//!    B-tree, behind [`write_behind::WriteBehind`]. The `wal` and
//!    `iouring` engines were removed in 1.32.2 (fastetcd#55): the server
//!    relies on redb's presync, corruption handling and defragment, and
//!    the raft log is its own sequential WAL ([`raft_wal`]).
//!
//! The MVCC state machine (revisions, leases, auth, events) lives here
//! too ([`mvcc`]).

pub mod fs_space;
pub mod kvstore;
pub mod mvcc;
/// Non-durable commits held in RAM, written to the engine in batches (fastetcd#85).
pub mod write_behind;
/// The raft log's sequential write-ahead log (fastetcd#85).
#[cfg(unix)]
pub mod raft_wal;

#[cfg(feature = "redb-engine")]
pub mod redb_engine;

pub use fs_space::FsSpace;
pub use kvstore::{
    KvStore, Snapshot, StorageError, StorageResult, StoreUsage, WriteBatch, WriteOptions,
};
pub use mvcc::{Mutation, MutationResult, MvccError, MvccResult, MvccStore, RangeResult};
