//! Regression test for fastetcd#13 — `get_log_state` must read only the
//! last log entry, not materialize the whole log. Startup calls it, and
//! a large un-purged log (tens of thousands of entries) otherwise loads
//! gigabytes into RAM before the node can bind its peer port and elect.

use std::sync::Arc;

use openraft::storage::RaftLogStorage;
use openraft::{Entry, EntryPayload, LeaderId, LogId};
use tempfile::tempdir;

use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::TypeConfig;
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::{KvStore, WriteBatch, WriteOptions};

#[tokio::test]
async fn get_log_state_reads_only_the_tail_of_a_large_log() {
    let dir = tempdir().unwrap();
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(dir.path().join("l.redb")).unwrap());

    // Write 50k log entries directly to the raft_log table, as they exist
    // on disk (key = index big-endian, value = bincode(Entry)). Batched so
    // the setup itself stays reasonable.
    let n: u64 = 50_000;
    let term = 7u64;
    let mut batch = WriteBatch::new();
    for index in 1..=n {
        let entry: Entry<TypeConfig> = Entry {
            log_id: LogId {
                leader_id: LeaderId::new(term, 1),
                index,
            },
            payload: EntryPayload::Blank,
        };
        batch.put("raft_log", &index.to_be_bytes(), &bincode::serialize(&entry).unwrap());
        if index % 5000 == 0 {
            engine.commit(std::mem::take(&mut batch), WriteOptions::default())
                .await
                .unwrap();
        }
    }
    engine.commit(batch, WriteOptions::default()).await.unwrap();

    let mut log = KvLogStore::new(engine);
    let state = log.get_log_state().await.unwrap();
    let last = state.last_log_id.expect("last_log_id present");
    assert_eq!(last.index, n, "must report the true highest index");
    assert_eq!(last.leader_id.term, term);
    assert!(state.last_purged_log_id.is_none());
}
