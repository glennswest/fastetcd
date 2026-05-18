//! openraft glue for fastetcd.
//!
//! Provides:
//! - `LogStorage` — implements openraft's RaftLogStorage over a redb table.
//!   Append is durable (fsync) before ack. Truncation, install-snapshot, log
//!   compaction tied to MVCC compaction.
//! - `StateMachine` — wraps `fastetcd_storage` so apply(entry) goes through
//!   MVCC. Snapshot/restore call into the storage layer.
//! - `Network` — gRPC peer transport (AppendEntries / Vote / InstallSnapshot).
//!
//! Implementation lands in tasks #12–#14.
