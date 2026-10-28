//! The background rewrite of the data file (fastetcd#177): a fresh,
//! compact copy written while reads and writes go on, then switched to.

#![cfg(feature = "redb-engine")]

use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use fastetcd_storage::kvstore::{KvStore, Snapshot, WriteBatch, WriteOptions};
use fastetcd_storage::redb_engine::{rewrite_path, RedbEngine};

const KV: &str = "mvcc_kv";
const META: &str = "mvcc_meta";

fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// `n` records of 1 KiB in `KV`, a few in `META`, then all but every
/// tenth `KV` record deleted: a file mostly of free pages.
async fn fragmented(engine: &RedbEngine, n: u32) {
    for chunk in (0..n).collect::<Vec<_>>().chunks(1000) {
        let mut b = WriteBatch::new();
        for &i in chunk {
            b.put(KV, &key(i), &[i as u8; 1024]);
        }
        engine.commit(b, WriteOptions { sync: true }).await.unwrap();
    }
    let mut b = WriteBatch::new();
    b.put(META, b"current_rev", b"42");
    b.put("empty_later", b"x", b"y");
    engine.commit(b, WriteOptions { sync: true }).await.unwrap();
    let mut b = WriteBatch::new();
    for i in (0..n).filter(|i| i % 10 != 0) {
        b.delete(KV, &key(i));
    }
    b.delete("empty_later", b"x");
    engine.commit(b, WriteOptions { sync: true }).await.unwrap();
}

async fn all(snap: &Arc<dyn Snapshot>, table: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    snap.range(table, Bound::Unbounded, Bound::Unbounded, 0).await.unwrap()
}

async fn contents(engine: &RedbEngine) -> Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)> {
    let snap = engine.snapshot().await.unwrap();
    let mut names = snap.table_names().await.unwrap();
    names.sort();
    let mut out = Vec::new();
    for n in names {
        let rows = all(&snap, &n).await;
        out.push((n, rows));
    }
    out
}

#[tokio::test]
async fn a_rewrite_keeps_every_table_and_shrinks_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    let engine = RedbEngine::open(&path).unwrap();
    fragmented(&engine, 20_000).await;
    let before = contents(&engine).await;
    let report = engine.rewrite(0).await.unwrap();
    assert_eq!(contents(&engine).await, before, "the rewritten file holds the same tables");
    assert!(before.iter().any(|(n, rows)| n == "empty_later" && rows.is_empty()), "an empty table is kept");
    assert!(
        report.after_bytes * 2 < report.before_bytes,
        "file {} -> {} bytes: not compacted",
        report.before_bytes,
        report.after_bytes
    );
    assert_eq!(report.after_bytes, std::fs::metadata(&path).unwrap().len());
    assert!(!rewrite_path(&path).exists());
    // Writes go to the new file, and survive a reopen.
    let mut b = WriteBatch::new();
    b.put(KV, b"after", b"switch");
    engine.commit(b, WriteOptions { sync: false }).await.unwrap();
    engine.sync().await.unwrap();
    drop(engine);
    let engine = RedbEngine::open(&path).unwrap();
    assert!(!engine.unclean_at_open(), "the switched file was not closed cleanly");
    let snap = engine.snapshot().await.unwrap();
    assert_eq!(snap.get(KV, b"after").await.unwrap().as_deref(), Some(&b"switch"[..]));
    assert_eq!(all(&snap, KV).await.len(), 2001);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_during_the_copy_land_in_the_new_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    let engine = RedbEngine::open(&path).unwrap();
    fragmented(&engine, 20_000).await;
    // A snapshot from before the rewrite reads on after the switch.
    let old_snap = engine.snapshot().await.unwrap();

    let writer = {
        let engine = engine.clone();
        tokio::spawn(async move {
            let mut n = 0u32;
            // Puts, deletes of copied keys, non-durable and durable.
            loop {
                let mut b = WriteBatch::new();
                b.put(KV, format!("w{n:06}").as_bytes(), &n.to_be_bytes());
                b.delete(KV, &key(n * 10));
                engine.commit(b, WriteOptions { sync: n % 7 == 0 }).await.unwrap();
                n += 1;
                if n == 1500 {
                    return n;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };
    // A slow copy (pace 20), so the writer runs through it.
    let report = engine.rewrite(20).await.unwrap();
    let written = writer.await.unwrap();
    assert!(report.caught_up_ops > 0, "no write landed during the copy: the test did not test it");

    let check = |snap: Arc<dyn Snapshot>| async move {
        let rows = all(&snap, KV).await;
        let ws = rows.iter().filter(|(k, _)| k.starts_with(b"w")).count() as u32;
        let originals = rows.iter().filter(|(k, _)| k.len() == 4).count() as u32;
        (ws, originals)
    };
    // Every write is there, and every delete took.
    assert_eq!(check(engine.snapshot().await.unwrap()).await, (written, 2000 - written.min(2000)));
    assert_eq!(all(&old_snap, KV).await.len(), 2000, "a snapshot from before the switch still reads the old file");
    drop(old_snap);
    drop(engine);
    let engine = RedbEngine::open(&path).unwrap();
    assert_eq!(check(engine.snapshot().await.unwrap()).await, (written, 2000 - written.min(2000)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_install_during_the_copy_abandons_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    let engine = RedbEngine::open(&path).unwrap();
    fragmented(&engine, 20_000).await;
    let rewrite = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.rewrite(200).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (tx, rx) = std::sync::mpsc::channel();
    tx.send(Ok(fastetcd_storage::kvstore::BatchOp::Put {
        table: KV.into(),
        key: b"installed".to_vec(),
        value: b"1".to_vec(),
    }))
    .unwrap();
    drop(tx);
    engine.commit_stream(rx, WriteOptions { sync: true }).await.unwrap();
    let err = rewrite.await.unwrap().expect_err("the rewrite went on through an install");
    assert!(err.to_string().contains("snapshot was installed"), "{err}");
    assert!(!rewrite_path(&path).exists(), "the abandoned copy was left behind");
    let snap = engine.snapshot().await.unwrap();
    assert_eq!(snap.get(KV, b"installed").await.unwrap().as_deref(), Some(&b"1"[..]));
    // And a later rewrite works.
    engine.rewrite(0).await.unwrap();
    assert_eq!(all(&engine.snapshot().await.unwrap(), KV).await.len(), 2001);
}

#[tokio::test]
async fn an_unfinished_rewrite_is_removed_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    drop(RedbEngine::open(&path).unwrap());
    std::fs::write(rewrite_path(&path), b"half a copy").unwrap();
    let _engine = RedbEngine::open(&path).unwrap();
    assert!(!rewrite_path(&path).exists());
}
