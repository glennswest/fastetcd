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
//! - [`log_store`] — `RaftLogStorage` impl over an in-memory map (tests).
//! - [`kv_log_store`] — the persistent `RaftLogStorage` the server runs.
//! - [`read_index`] — the read barrier in front of linearizable reads.

pub mod kv_log_store;
pub mod log_store;
pub mod network;
pub mod precheck;
pub mod read_index;
pub mod snapshot_data;
pub mod snapshot_store;
pub mod state_machine;
pub mod types;

pub use network::{
    auth_status, dial_peer, empty_peers, AuthSyncError, GrpcNetwork, GrpcNetworkFactory, PeerEndpoints, PeerTls,
    RaftPeerService, WriteForwarder,
};

pub use read_index::{read_barrier, LocalReadIndex, ReadBarrierError};
pub use snapshot_data::SnapshotFile;
pub use snapshot_store::SnapshotStore;
pub use state_machine::{FastetcdSnapshotBuilder, FastetcdStateMachine};
pub use types::{FastetcdLogEntry, FastetcdLogResponse, ForwardedRead, NodeId, TypeConfig};
