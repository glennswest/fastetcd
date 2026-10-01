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
//! engine commits with `WriteOptions::sync = true` by default). So do
//! votes, truncation and purges. `save_committed` does not: the
//! committed index is advisory (openraft only needs it to be no higher
//! than the truth, and an older value is that), and its fsync used to
//! run inline in RaftCore once per write (fastetcd#71).

use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Index of the last committed entry openraft has told us about
    /// (0 = none), for `etcd_server_proposals_committed_total` (#29).
    committed_index: Arc<AtomicU64>,
    progress: LogProgress,
}

/// What the log store has seen, read by the local read barrier
/// (`crate::read_index`, fastetcd#71) without a round trip through
/// RaftCore. Every value is `index + 1`, 0 meaning none, because the
/// first log entry (a cluster's initial membership) is at index 0.
#[derive(Clone, Default, Debug)]
pub struct LogProgress {
    /// Highest committed index openraft has saved.
    pub(crate) committed: Arc<AtomicU64>,
    /// Highest index durably in the log: at startup the last entry on
    /// disk, then each append once its fsync returned.
    pub(crate) last_durable: Arc<AtomicU64>,
    /// Highest index of a membership entry handed to `append` since
    /// this process started. Raised before the entry is written, so
    /// openraft's metrics showing a membership at least this new means
    /// they show every membership in the log.
    pub(crate) membership: Arc<AtomicU64>,
    /// Term of the vote this store last saved (durably). openraft saves
    /// a vote before it grants it or acts on it, so no vote of a higher
    /// term exists on this member while this says otherwise. A member
    /// answers a leader's `ConfirmLeader` from it (fastetcd#75).
    pub(crate) vote_term: Arc<AtomicU64>,
    /// `(term, index + 1)` of the first entry appended in the newest term
    /// seen, (0, 0) if unknown. A new leader's first entry (its blank)
    /// is that index: its read index is never below it, so a read sees
    /// what earlier leaders committed (fastetcd#75).
    pub(crate) term_start: Arc<std::sync::Mutex<(u64, u64)>>,
}

impl LogProgress {
    /// The term of the vote this member last saved.
    pub fn saved_vote_term(&self) -> u64 {
        self.vote_term.load(Ordering::Acquire)
    }

    /// `index + 1` of the first entry of `term` in the log, if known.
    pub(crate) fn term_start(&self, term: u64) -> Option<u64> {
        let (t, i) = *self.term_start.lock().unwrap();
        (t == term && i > 0).then_some(i)
    }

    fn note_vote(&self, vote: &Vote<NodeId>) {
        self.vote_term.fetch_max(vote.leader_id.term, Ordering::AcqRel);
    }
}

impl KvLogStore {
    pub fn new(engine: Arc<dyn KvStore>) -> Self {
        Self {
            engine,
            committed_index: Arc::new(AtomicU64::new(0)),
            progress: LogProgress::default(),
        }
    }

    /// The store's progress, for [`crate::read_index::LocalReadIndex`].
    /// Shared by every clone; take it before the store goes to openraft.
    pub fn progress(&self) -> LogProgress {
        self.progress.clone()
    }

    /// The committed index, kept current as openraft saves it. Every
    /// clone of this store shares it, so take the handle before the
    /// store is handed to openraft.
    pub fn committed_index(&self) -> Arc<AtomicU64> {
        self.committed_index.clone()
    }

    fn note_committed(&self, committed: &Option<LogId<NodeId>>) {
        if let Some(c) = committed {
            self.committed_index.fetch_max(c.index, Ordering::Relaxed);
            self.progress.committed.fetch_max(c.index + 1, Ordering::Release);
        }
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
        if let Some(l) = &last_log_id {
            self.progress.last_durable.fetch_max(l.index + 1, Ordering::Release);
        }
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
        // Before returning: openraft grants or acts on the vote only
        // after this returns.
        self.progress.note_vote(vote);
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
                self.progress.note_vote(&vote);
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
        // No fsync: the next durable commit (an append, a vote) carries
        // it, and a crash that loses it leaves an older committed index,
        // which openraft accepts (fastetcd#71).
        self.note_committed(&committed);
        self.engine
            .commit(batch, WriteOptions { sync: false })
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
                self.note_committed(&committed);
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
        let mut last = None;
        for entry in entries {
            if matches!(entry.payload, openraft::EntryPayload::Membership(_)) {
                self.progress
                    .membership
                    .fetch_max(entry.log_id.index + 1, Ordering::Release);
            }
            {
                let mut ts = self.progress.term_start.lock().unwrap();
                if entry.log_id.leader_id.term > ts.0 {
                    *ts = (entry.log_id.leader_id.term, entry.log_id.index + 1);
                }
            }
            let bytes = bincode::serialize(&entry).map_err(|e| io_err(ErrorVerb::Write, e))?;
            batch.put(TABLE_LOG, &idx_key(entry.log_id.index), &bytes);
            last = Some(entry.log_id.index);
        }
        // sync=true ensures fsync before commit returns.
        self.engine
            .commit(batch, WriteOptions::default())
            .await
            .map_err(|e| io_err(ErrorVerb::Write, e))?;
        if let Some(last) = last {
            self.progress.last_durable.fetch_max(last + 1, Ordering::Release);
        }
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
        // Only a follower truncates; the local read barrier is for a
        // leader, but keep the value true.
        self.progress
            .last_durable
            .fetch_min(log_id.index, Ordering::Release);
        {
            let mut ts = self.progress.term_start.lock().unwrap();
            if ts.1 > log_id.index {
                *ts = (0, 0);
            }
        }
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
