// openraft::StorageError is large, and these signatures are fixed by
// the RaftLogStorage / RaftStateMachine traits — we cannot box it.
#![allow(clippy::result_large_err)]

//! openraft glue for fastetcd.
//!
//! Modules:
//! - [`types`] — `TypeConfig`, `FastetcdLogEntry`, `FastetcdLogResponse`.
//! - [`state_machine`] — `FastetcdStateMachine` wrapping `MvccStore`.
//! - [`log_store`] — `RaftLogStorage` impl over an in-memory map for
//!   now; a KvStore-backed impl lands in task #14.

pub mod kv_log_store;
pub mod log_store;
pub mod network;
pub mod state_machine;
pub mod types;

pub use network::{
    empty_peers, GrpcNetwork, GrpcNetworkFactory, PeerEndpoints, RaftPeerService, WriteForwarder,
};

pub use state_machine::{FastetcdSnapshotBuilder, FastetcdStateMachine};
pub use types::{FastetcdLogEntry, FastetcdLogResponse, ForwardedRead, NodeId, TypeConfig};
