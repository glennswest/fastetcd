// openraft::StorageError is large, and these signatures are fixed by
// the RaftLogStorage / RaftStateMachine traits — we cannot box it.
#![allow(clippy::result_large_err)]

//! openraft glue for fastetcd.
//!
//! Modules:
//! - [`types`] — `TypeConfig`, `FastetcdLogEntry`, `FastetcdLogResponse`.
//! - [`state_machine`] — `FastetcdStateMachine` wrapping `MvccStore`.
//! - [`snapshot_store`] — retained on-disk snapshots with roll-off.
//! - [`snapshot_data`] — `SnapshotFile`, the file-backed snapshot body.
//! - [`log_store`] — `RaftLogStorage` impl over an in-memory map for
//!   now; a KvStore-backed impl lands in task #14.

pub mod kv_log_store;
pub mod log_store;
pub mod network;
pub mod snapshot_data;
pub mod snapshot_store;
pub mod state_machine;
pub mod types;

pub use network::{
    empty_peers, GrpcNetwork, GrpcNetworkFactory, PeerEndpoints, RaftPeerService, WriteForwarder,
};

pub use snapshot_data::SnapshotFile;
pub use snapshot_store::SnapshotStore;
pub use state_machine::{FastetcdSnapshotBuilder, FastetcdStateMachine};
pub use types::{FastetcdLogEntry, FastetcdLogResponse, ForwardedRead, NodeId, TypeConfig};
