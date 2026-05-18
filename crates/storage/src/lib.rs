//! MVCC state machine for fastetcd.
//!
//! Applies committed Raft entries against a redb-backed store. Exposes:
//! - apply(entry) — single mutation path, called from the Raft apply loop
//! - range(key_range, revision) — read at a specific MVCC revision
//! - snapshot() / restore(snapshot) — Raft snapshot install hooks
//! - watch_subscribe(key_range, start_rev) — fan-out for the Watch service
//!
//! The actual implementation lands in the MVCC milestone (task #4).
