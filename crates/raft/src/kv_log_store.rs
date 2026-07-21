//! Persistent `RaftLogStorage` over the engine-agnostic `KvStore`.
//!
//! Tables (created on first use):
//!   - `raft_log`   — `index_be(8) -> bincode(Entry<TypeConfig>)`
//!   - `raft_meta`  — keys:
//!       * `b"vote"`               -> `bincode(Vote<NodeId>)`
//!       * `b"committed"`          -> `bincode(Option<LogId<NodeId>>)`
//!       * `b"last_purged_log_id"` -> `bincode(LogId<NodeId>)`
//!
//! Append fsyncs before invoking the `LogFlushed` callback, satisfying
//! openraft's "log durable before ack" requirement (the underlying
//! engine commits with `WriteOptions::sync = true` by default).

use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::AnyError;
use openraft::Entry;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;

use fastetcd_storage::{KvStore, WriteBatch, WriteOptions};

use crate::types::{NodeId, TypeConfig};

const TABLE_LOG: &str = "raft_log";
const TABLE_META: &str = "raft_meta";

const META_VOTE: &[u8] = b"vote";
const META_COMMITTED: &[u8] = b"committed";
const META_LAST_PURGED: &[u8] = b"last_purged_log_id";

/// Persistent Raft log storage. Cheaply clonable; the inner state is
/// an `Arc<dyn KvStore>`.
#[derive(Clone)]
pub struct KvLogStore {
    engine: Arc<dyn KvStore>,
}

impl KvLogStore {
    pub fn new(engine: Arc<dyn KvStore>) -> Self {
        Self { engine }
    }
}

fn idx_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

fn io_err<E: std::fmt::Display>(verb: ErrorVerb, e: E) -> StorageError<NodeId> {
    StorageIOError::new(
        ErrorSubject::Log(openraft::LogId {
            leader_id: Default::default(),
            index: 0,
        }),
        verb,
        AnyError::error(format!("{e}")),
    )
    .into()
}

impl RaftLogReader<TypeConfig> for KvLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let start_bound = match range.start_bound() {
            Bound::Included(i) => Bound::Included(idx_key(*i).to_vec()),
            Bound::Excluded(i) => Bound::Excluded(idx_key(*i).to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_bound = match range.end_bound() {
            Bound::Included(i) => Bound::Included(idx_key(*i).to_vec()),
            Bound::Excluded(i) => Bound::Excluded(idx_key(*i).to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let snap = self
            .engine
            .snapshot()
            .await
            .map_err(|e| io_err(ErrorVerb::Read, e))?;
        let rows = snap
            .range(TABLE_LOG, start_bound, end_bound, 0)
            .await
            .map_err(|e| io_err(ErrorVerb::Read, e))?;
        let mut out = Vec::with_capacity(rows.len());
        for (_, bytes) in rows {
            let entry: Entry<TypeConfig> =
                bincode::deserialize(&bytes).map_err(|e| io_err(ErrorVerb::Read, e))?;
            out.push(entry);
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for KvLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let snap = self
            .engine
            .snapshot()
            .await
            .map_err(|e| io_err(ErrorVerb::Read, e))?;
        // last_log_id: the highest-index entry in raft_log. Read only the
        // last row — a full range scan here loads the entire (possibly
        // huge, un-purged) log into RAM on every startup, which hangs the
        // node before it can bind its peer port (fastetcd#13).
        let last = snap
            .last(TABLE_LOG)
            .await
            .map_err(|e| io_err(ErrorVerb::Read, e))?
            .map(|(_, bytes)| -> Result<LogId<NodeId>, StorageError<NodeId>> {
                let e: Entry<TypeConfig> =
                    bincode::deserialize(&bytes).map_err(|err| io_err(ErrorVerb::Read, err))?;
                Ok(e.log_id)
            })
            .transpose()?;

        // last_purged_log_id from meta.
        let last_purged_bytes = snap
            .get(TABLE_META, META_LAST_PURGED)
            .await
            .map_err(|e| io_err(ErrorVerb::Read, e))?;
        let last_purged: Option<LogId<NodeId>> = match last_purged_bytes {
            Some(b) => Some(bincode::deserialize(&b).map_err(|e| io_err(ErrorVerb::Read, e))?),
            None => None,
        };

        let last_log_id = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = bincode::serialize(vote).map_err(|e| io_err(ErrorVerb::Write, e))?;
        let mut batch = WriteBatch::new();
        batch.put(TABLE_META, META_VOTE, &bytes);
        self.engine
            .commit(batch, WriteOptions::default())
            .await
            .map_err(|e| io_err(ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let snap = self
            .engine
            .snapshot()
            .await
            .map_err(|e| io_err(ErrorVerb::Read, e))?;
        let bytes = snap
            .get(TABLE_META, META_VOTE)
            .await
            .map_err(|e| io_err(ErrorVerb::Read, e))?;
        match bytes {
            Some(b) => {
                let vote: Vote<NodeId> =
                    bincode::deserialize(&b).map_err(|e| io_err(ErrorVerb::Read, e))?;
                Ok(Some(vote))
            }
            None => Ok(None),
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes =
            bincode::serialize(&committed).map_err(|e| io_err(ErrorVerb::Write, e))?;
        let mut batch = WriteBatch::new();
        batch.put(TABLE_META, META_COMMITTED, &bytes);
        self.engine
            .commit(batch, WriteOptions::default())
            .await
            .map_err(|e| io_err(ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let snap = self
            .engine
            .snapshot()
            .await
            .map_err(|e| io_err(ErrorVerb::Read, e))?;
        let bytes = snap
            .get(TABLE_META, META_COMMITTED)
            .await
            .map_err(|e| io_err(ErrorVerb::Read, e))?;
        match bytes {
            Some(b) => {
                let committed: Option<LogId<NodeId>> =
                    bincode::deserialize(&b).map_err(|e| io_err(ErrorVerb::Read, e))?;
                Ok(committed)
            }
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut batch = WriteBatch::new();
        for entry in entries {
            let bytes = bincode::serialize(&entry).map_err(|e| io_err(ErrorVerb::Write, e))?;
            batch.put(TABLE_LOG, &idx_key(entry.log_id.index), &bytes);
        }
        // sync=true ensures fsync before commit returns.
        self.engine
            .commit(batch, WriteOptions::default())
            .await
            .map_err(|e| io_err(ErrorVerb::Write, e))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Delete entries with index >= log_id.index via a range delete,
        // so we never load the (possibly large) tail into RAM (#13).
        // `[0xFF; 9]` is greater than any 8-byte index key, so the range
        // covers [index, end).
        let start = idx_key(log_id.index).to_vec();
        let mut batch = WriteBatch::new();
        batch.delete_range(TABLE_LOG, &start, &[0xFFu8; 9]);
        self.engine
            .commit(batch, WriteOptions::default())
            .await
            .map_err(|e| io_err(ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Delete entries with index <= log_id.index via a range delete
        // (bounded memory — the whole purged prefix was previously loaded
        // into RAM, which is a large part of the #13 blowup) and record
        // the new last_purged_log_id. `end` is one byte past the target
        // key, so the exclusive range [empty, end) includes index.
        let mut end = idx_key(log_id.index).to_vec();
        end.push(0);
        let mut batch = WriteBatch::new();
        batch.delete_range(TABLE_LOG, &[], &end);
        let bytes = bincode::serialize(&log_id).map_err(|e| io_err(ErrorVerb::Write, e))?;
        batch.put(TABLE_META, META_LAST_PURGED, &bytes);
        self.engine
            .commit(batch, WriteOptions::default())
            .await
            .map_err(|e| io_err(ErrorVerb::Write, e))?;
        Ok(())
    }
}
