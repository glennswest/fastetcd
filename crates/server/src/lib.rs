// Nearly every fn here returns tonic::Status, which is large by
// design; boxing it would churn every gRPC handler signature for no
// real gain.
#![allow(clippy::result_large_err)]

//! fastetcd server library.
//!
//! The binary in `bin/fastetcd` is a thin shell around this crate.
//! Submodules:
//!
//! - [`state`] — shared `ServerState` (Raft handle, state machine,
//!   IDs) passed into every gRPC service.
//! - [`conv`] — converters between etcd wire types and internal
//!   MVCC types.
//! - [`kv`] — `KvService` implementing the etcd v3 KV gRPC service.

pub mod admin;
pub mod auth;
pub mod authz;
pub mod cluster;
pub mod compaction;
pub mod conv;
pub mod kv;
pub mod lease;
pub mod lease_expiry;
pub mod maintenance;
pub mod metrics;
pub mod space;
pub mod state;
pub mod watch;

pub use state::ServerState;
