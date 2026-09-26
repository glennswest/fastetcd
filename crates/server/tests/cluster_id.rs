//! fastetcd#17: the cluster id is chosen once per store, persisted, and
//! never changed by a restart or an upgrade.

use std::path::Path;
use std::sync::Arc;

use fastetcd_server::cluster_id::{id_from_token, resolve, Source};
use fastetcd_storage::redb_engine::RedbEngine;
use fastetcd_storage::{KvStore, WriteBatch, WriteOptions};

/// Open the store as startup does, reporting whether the file was new.
fn open(path: &Path) -> (Arc<dyn KvStore>, bool) {
    let is_new = !path.exists();
    let engine: Arc<dyn KvStore> = Arc::new(RedbEngine::open(path).unwrap());
    (engine, is_new)
}

#[tokio::test]
async fn a_new_store_derives_from_the_token_and_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");

    {
        let (engine, is_new) = open(&path);
        assert!(is_new);
        let got = resolve(&engine, None, is_new, Some("prod-east")).await.unwrap();
        assert_eq!(got, (id_from_token("prod-east"), Source::Token));
    }
    // Restart without the token (or with a different one): the id the
    // store was created with stays.
    {
        let (engine, is_new) = open(&path);
        assert!(!is_new);
        let got = resolve(&engine, None, is_new, Some("something-else")).await.unwrap();
        assert_eq!(got, (id_from_token("prod-east"), Source::Persisted));
        let got = resolve(&engine, None, is_new, None).await.unwrap();
        assert_eq!(got, (id_from_token("prod-east"), Source::Persisted));
    }
}

#[tokio::test]
async fn an_explicit_flag_wins_and_replaces_the_persisted_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    {
        let (engine, is_new) = open(&path);
        resolve(&engine, None, is_new, Some("tok")).await.unwrap();
        let got = resolve(&engine, Some(4242), false, Some("tok")).await.unwrap();
        assert_eq!(got, (4242, Source::Flag));
    }
    let (engine, is_new) = open(&path);
    let got = resolve(&engine, None, is_new, Some("tok")).await.unwrap();
    assert_eq!(got, (4242, Source::Persisted), "the flag's id is the one persisted");
}

#[tokio::test]
async fn a_store_from_before_persisted_ids_keeps_reporting_1() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fastetcd.redb");
    // A data file written by an older fastetcd: data, no persisted id.
    {
        let (engine, _) = open(&path);
        let mut batch = WriteBatch::new();
        batch.put("mvcc_meta", b"some-key", b"some-value");
        engine.commit(batch, WriteOptions::default()).await.unwrap();
    }
    {
        let (engine, is_new) = open(&path);
        assert!(!is_new);
        // Even with a token now: deriving a new id would change it under
        // clients mid-upgrade.
        let got = resolve(&engine, None, is_new, Some("prod-east")).await.unwrap();
        assert_eq!(got, (1, Source::Legacy));
    }
    // ...and that is persisted too. (Each open is scoped: redb allows
    // one handle per file.)
    let (engine, is_new) = open(&path);
    let got = resolve(&engine, None, is_new, Some("prod-east")).await.unwrap();
    assert_eq!(got, (1, Source::Persisted));
}

#[tokio::test]
async fn no_flag_and_no_token_falls_back_to_1() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, is_new) = open(&dir.path().join("fastetcd.redb"));
    let got = resolve(&engine, None, is_new, None).await.unwrap();
    assert_eq!(got, (1, Source::Default));
}

#[tokio::test]
async fn zero_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, is_new) = open(&dir.path().join("fastetcd.redb"));
    assert!(resolve(&engine, Some(0), is_new, None).await.is_err());
}
