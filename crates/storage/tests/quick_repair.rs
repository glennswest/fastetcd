//! A start after a kill does not walk the data file (fastetcd#176).
//!
//! Every durable commit the redb engine makes is a redb quick-repair
//! commit, so a file left open by a kill or a power cut reopens from the
//! allocator state its last durable commit saved, instead of redb's full
//! repair (every page read: minutes on an SMR disk). Each case runs a
//! child process (this test binary again) that writes through
//! `RedbEngine` and exits without closing the file, as a kill leaves it;
//! the parent reopens it.
//!
//! - after durable commits, then non-durable ones: unclean, no walk, the
//!   durable commits there (the non-durable tail is the raft log's to
//!   replay);
//! - after a defragment (redb's compaction commits without the allocator
//!   state): no walk;
//! - control: a file whose last durable commit is a plain redb commit
//!   (what fastetcd wrote before #176) is walked, and the open after that
//!   walk leaves a quick-repair commit, so a kill right after it is not
//!   walked again.

#![cfg(feature = "redb-engine")]

use std::ops::Bound;
use std::path::{Path, PathBuf};

use fastetcd_storage::kvstore::{KvStore, WriteBatch, WriteOptions};
use fastetcd_storage::redb_engine::RedbEngine;
use redb::{Database, TableDefinition};

const TABLE: &str = "mvcc_kv";

fn batch(from: u32, n: u32) -> WriteBatch {
    let mut b = WriteBatch::new();
    for i in from..from + n {
        b.put(TABLE, &i.to_be_bytes(), &[7u8; 512]);
    }
    b
}

/// The child: write `path` as `case` says, then exit without closing it.
#[test]
#[ignore]
fn quick_repair_child() {
    let Ok(spec) = std::env::var("QUICK_REPAIR_CHILD") else { return };
    let (case, path) = spec.split_once(',').unwrap();
    let path = PathBuf::from(path);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        match case {
            "durable-then-not" => {
                let engine = RedbEngine::open(&path).unwrap();
                for round in 0..10 {
                    engine.commit(batch(round * 100, 100), WriteOptions { sync: false }).await.unwrap();
                }
                engine.sync().await.unwrap();
                engine.commit(batch(5000, 100), WriteOptions { sync: true }).await.unwrap();
                for round in 0..5 {
                    engine.commit(batch(10_000 + round * 100, 100), WriteOptions { sync: false }).await.unwrap();
                }
                std::mem::forget(engine);
            }
            "defragment" => {
                let engine = RedbEngine::open(&path).unwrap();
                engine.commit(batch(0, 2000), WriteOptions { sync: true }).await.unwrap();
                let mut b = WriteBatch::new();
                for i in 0..1500u32 {
                    b.delete(TABLE, &i.to_be_bytes());
                }
                engine.commit(b, WriteOptions { sync: true }).await.unwrap();
                engine.defragment().await.unwrap();
                std::mem::forget(engine);
            }
            "plain-redb" => {
                // A durable commit with no allocator state, as before #176.
                let db = Database::create(&path).unwrap();
                let txn = db.begin_write().unwrap();
                {
                    let def: TableDefinition<&[u8], &[u8]> = TableDefinition::new(TABLE);
                    let mut t = txn.open_table(def).unwrap();
                    for i in 0..2000u32 {
                        t.insert(i.to_be_bytes().as_slice(), [7u8; 512].as_slice()).unwrap();
                    }
                }
                txn.commit().unwrap();
                std::mem::forget(db);
            }
            "open-only" => {
                let engine = RedbEngine::open(&path).unwrap();
                assert!(engine.repaired_at_open(), "the control's file was not walked");
                std::mem::forget(engine);
            }
            other => panic!("unknown case {other}"),
        }
    });
    std::process::exit(0);
}

fn run_child(case: &str, path: &Path) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "quick_repair_child", "--ignored", "--nocapture"])
        .env("QUICK_REPAIR_CHILD", format!("{case},{}", path.display()))
        .status()
        .unwrap();
    assert!(status.success(), "child {case} failed: {status}");
}

async fn count(engine: &RedbEngine) -> usize {
    engine
        .snapshot()
        .await
        .unwrap()
        .range(TABLE, Bound::Unbounded, Bound::Unbounded, 0)
        .await
        .unwrap()
        .len()
}

#[tokio::test]
async fn a_kill_after_a_durable_commit_is_not_walked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    run_child("durable-then-not", &path);
    let engine = RedbEngine::open(&path).unwrap();
    assert!(engine.unclean_at_open(), "the child's exit was not seen as an unclean stop");
    assert!(!engine.repaired_at_open(), "a kill after a quick-repair commit walked the file");
    // Everything up to the last durable commit; of the non-durable tail
    // after it, whatever redb kept (the raft log replays the rest).
    let n = count(&engine).await;
    assert!((1100..=1600).contains(&n), "{n} records after the kill");
}

#[tokio::test]
async fn a_kill_after_a_defragment_is_not_walked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    run_child("defragment", &path);
    let engine = RedbEngine::open(&path).unwrap();
    assert!(engine.unclean_at_open());
    assert!(!engine.repaired_at_open(), "a kill after a defragment walked the file");
    assert_eq!(count(&engine).await, 500);
}

#[tokio::test]
async fn a_walked_file_is_left_quick_to_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    // Control: a plain durable commit then a kill is walked (the child
    // asserts it), and that open, killed at once, is not walked again.
    run_child("plain-redb", &path);
    run_child("open-only", &path);
    let engine = RedbEngine::open(&path).unwrap();
    assert!(engine.unclean_at_open());
    assert!(!engine.repaired_at_open(), "the open after a walk left no quick-repair commit");
    assert_eq!(count(&engine).await, 2000);
}

#[tokio::test]
async fn a_clean_close_is_not_unclean() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    {
        let engine = RedbEngine::open(&path).unwrap();
        engine.commit(batch(0, 10), WriteOptions { sync: false }).await.unwrap();
    }
    let engine = RedbEngine::open(&path).unwrap();
    assert!(!engine.unclean_at_open());
    assert!(!engine.repaired_at_open());
    assert_eq!(count(&engine).await, 10);
}
