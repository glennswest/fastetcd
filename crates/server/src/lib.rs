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

pub mod cluster;
pub mod conv;
pub mod kv;
pub mod lease;
pub mod maintenance;
pub mod state;
pub mod watch;

pub use state::ServerState;
