//! `RaftLogStorage` + `RaftLogReader` implementations.
//!
//! For now this is an in-memory store sufficient for single-node and
//! local-multi-node integration testing. A persistent KvStore-backed
//! impl lands in task #14 and will replace this for production use.

use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::Entry;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::Vote;
use tokio::sync::Mutex;

use crate::types::{NodeId, TypeConfig};

/// In-memory log storage. Clone-safe; the inner state is `Arc<Mutex<...>>`.
#[derive(Clone, Default)]
pub struct MemLogStore {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    /// Persistent log entries keyed by index.
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// Highest log id ever purged. Returned by `get_log_state` as
    /// `last_purged_log_id`.
    last_purged_log_id: Option<LogId<NodeId>>,
    /// Most recent persisted vote.
    vote: Option<Vote<NodeId>>,
    /// Most recent persisted committed log id (optional in openraft;
    /// we persist it to make restart behavior more predictable in
    /// tests).
    committed: Option<LogId<NodeId>>,
}

impl MemLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftLogReader<TypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let g = self.inner.lock().await;
        Ok(g.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let g = self.inner.lock().await;
        let last_log_id = g.log.values().last().map(|e| e.log_id).or(g.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: g.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut g = self.inner.lock().await;
        g.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let g = self.inner.lock().await;
        Ok(g.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut g = self.inner.lock().await;
        g.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let g = self.inner.lock().await;
        Ok(g.committed)
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
        {
            let mut g = self.inner.lock().await;
            for entry in entries {
                g.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory store: nothing to fsync. Notify openraft immediately.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut g = self.inner.lock().await;
        let keys: Vec<u64> = g.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            g.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut g = self.inner.lock().await;
        let keys: Vec<u64> = g
            .log
            .range(..=log_id.index)
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            g.log.remove(&k);
        }
        g.last_purged_log_id = Some(log_id);
        Ok(())
    }
}
