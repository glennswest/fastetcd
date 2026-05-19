//! MVCC state machine layer over [`KvStore`](crate::KvStore).
//!
//! This module models etcd's multi-version concurrency control:
//!
//! - Revisions: monotonic per-mutation counters (`Revision { main, sub }`).
//! - Per-key generation index: tracks the lifecycle of every key as a
//!   sequence of `[create -> puts -> tombstone]` generations.
//! - Revision-keyed values: the actual stored bytes live at
//!   `(user_key, revision)` so historical reads are a point lookup.
//!
//! The public entry point is [`MvccStore`], which exposes:
//!
//! - `current_revision()` / `compact_revision()` getters.
//! - `apply(mutations)` for atomic multi-op writes (the Raft apply
//!   loop calls into this).
//! - `range(...)` for current-state and historical reads.
//!
//! Compaction and `Txn` semantics are implemented in the next module
//! commit (tracked as task #17).

pub mod auth;
pub mod event;
pub mod lease;
pub mod record;
pub mod revision;
pub mod store;

pub use event::{EventBatch, EventKind, MvccEvent};
pub use lease::{LeaseId, LeaseRecord};
pub use record::{Generation, KeyIndex, KvRecord};
pub use revision::Revision;
pub use store::{
    BulkKey, Compare, CompareOp, CompareTarget, LeaseGrantResult, LeaseRevokeResult,
    LeaseTtlResult, Mutation, MutationResult, MvccError, MvccResult, MvccStore, RangeOp,
    RangeResult, TxnOp, TxnOpResult, TxnResult,
};
