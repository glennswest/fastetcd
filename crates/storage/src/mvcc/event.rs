//! Events emitted by [`MvccStore`](super::MvccStore) on every successful
//! mutation commit. Consumed by the Watch service to fan out to
//! interested watchers.

use serde::{Deserialize, Serialize};

use super::record::KvRecord;

/// What kind of event this is, mapping 1:1 onto `mvccpb::Event.EventType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    Put,
    Delete,
}

/// One key-value mutation event. Carries the current record (post-
/// mutation; tombstones have `deleted = true`) and optionally the
/// prior record. The Watch service decides whether to surface
/// `prev_kv` based on the watcher's request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MvccEvent {
    pub kind: EventKind,
    pub kv: KvRecord,
    pub prev_kv: Option<KvRecord>,
}

/// A revision-tagged batch of events produced by one commit. All
/// events in a batch share the same `main` revision; subscribers may
/// rely on this when sequencing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBatch {
    pub revision: i64,
    pub events: Vec<MvccEvent>,
}
