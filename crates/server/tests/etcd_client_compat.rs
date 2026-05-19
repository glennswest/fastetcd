//! Wire-compatibility test using the third-party `etcd-client`
//! Rust crate. This crate is the most widely-used Rust client for
//! etcd v3 in the open-source ecosystem and shares no code with
//! fastetcd. If its API works against fastetcd, real etcd consumers
//! will too.

mod common;
use common::start_test_server_full;

use std::time::Duration;

use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, PutOptions, Txn, TxnOp,
};

async fn client_for(endpoint: &str) -> Client {
    Client::connect([endpoint], None)
        .await
        .expect("etcd_client::Client::connect")
}

#[tokio::test]
async fn put_get_delete_via_etcd_client() {
    let h = start_test_server_full().await;
    let mut c = client_for(&h.endpoint).await;

    c.put("foo", "bar", None).await.unwrap();
    let resp = c.get("foo", None).await.unwrap();
    let kvs = resp.kvs();
    assert_eq!(kvs.len(), 1);
    assert_eq!(kvs[0].key(), b"foo");
    assert_eq!(kvs[0].value(), b"bar");

    let del = c.delete("foo", None).await.unwrap();
    assert_eq!(del.deleted(), 1);

    let resp = c.get("foo", None).await.unwrap();
    assert!(resp.kvs().is_empty());
}

#[tokio::test]
async fn range_with_prefix_via_etcd_client() {
    let h = start_test_server_full().await;
    let mut c = client_for(&h.endpoint).await;
    for k in ["app/a", "app/b", "app/c", "other"] {
        c.put(k, "v", None).await.unwrap();
    }
    let resp = c
        .get("app/", Some(GetOptions::new().with_prefix()))
        .await
        .unwrap();
    let mut keys: Vec<&[u8]> = resp.kvs().iter().map(|kv| kv.key()).collect();
    keys.sort();
    assert_eq!(keys, vec![b"app/a".as_ref(), b"app/b", b"app/c"]);
}

#[tokio::test]
async fn delete_range_with_prev_kv_via_etcd_client() {
    let h = start_test_server_full().await;
    let mut c = client_for(&h.endpoint).await;
    for k in ["k1", "k2", "k3"] {
        c.put(k, format!("val-{k}"), None).await.unwrap();
    }
    let resp = c
        .delete(
            "k1",
            Some(DeleteOptions::new().with_range("k3").with_prev_key()),
        )
        .await
        .unwrap();
    assert_eq!(resp.deleted(), 2);
    let prev: Vec<&[u8]> = resp.prev_kvs().iter().map(|kv| kv.value()).collect();
    let mut prev_owned: Vec<Vec<u8>> = prev.iter().map(|v| v.to_vec()).collect();
    prev_owned.sort();
    assert_eq!(prev_owned, vec![b"val-k1".to_vec(), b"val-k2".to_vec()]);
}

#[tokio::test]
async fn txn_compare_and_set_via_etcd_client() {
    let h = start_test_server_full().await;
    let mut c = client_for(&h.endpoint).await;
    c.put("counter", "0", None).await.unwrap();

    let txn = Txn::new()
        .when(vec![Compare::value("counter", CompareOp::Equal, "0")])
        .and_then(vec![TxnOp::put("counter", "1", None)])
        .or_else(vec![TxnOp::put("counter", "failed", None)]);
    let resp = c.txn(txn).await.unwrap();
    assert!(resp.succeeded());

    let val = c.get("counter", None).await.unwrap();
    assert_eq!(val.kvs()[0].value(), b"1");
}

#[tokio::test]
async fn lease_grant_attach_revoke_via_etcd_client() {
    let h = start_test_server_full().await;
    let mut c = client_for(&h.endpoint).await;

    let grant = c.lease_grant(30, None).await.unwrap();
    let lease_id = grant.id();
    assert!(lease_id > 0);

    c.put(
        "ephemeral",
        "v",
        Some(PutOptions::new().with_lease(lease_id)),
    )
    .await
    .unwrap();
    let r = c.get("ephemeral", None).await.unwrap();
    assert_eq!(r.kvs().len(), 1);
    assert_eq!(r.kvs()[0].lease(), lease_id);

    c.lease_revoke(lease_id).await.unwrap();
    // Brief settle for the cascade to land.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let r2 = c.get("ephemeral", None).await.unwrap();
    assert!(r2.kvs().is_empty(), "revoke must cascade-delete attached key");
}

#[tokio::test]
async fn watch_via_etcd_client_receives_put_event() {
    let h = start_test_server_full().await;
    let mut c_watch = client_for(&h.endpoint).await;
    let mut c_put = client_for(&h.endpoint).await;

    let mut stream = c_watch.watch("hot-key", None).await.unwrap();

    // First message off the stream is the create ack from the server
    // (no events). Skip it.
    let _ack = tokio::time::timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("create ack")
        .unwrap()
        .expect("watch should not end");

    c_put.put("hot-key", "first-event", None).await.unwrap();

    // Pull the event message; keep going if a progress notify slips in.
    let watch_id;
    let evt;
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(2), stream.message())
            .await
            .expect("watch should deliver event")
            .unwrap()
            .expect("watch should not end");
        if !msg.events().is_empty() {
            watch_id = msg.watch_id();
            evt = msg.events().to_vec();
            break;
        }
    }
    assert_eq!(evt.len(), 1);
    let kv = evt[0].kv().unwrap();
    assert_eq!(kv.key(), b"hot-key");
    assert_eq!(kv.value(), b"first-event");
    stream.cancel(watch_id).await.unwrap();
}

#[tokio::test]
async fn member_list_via_etcd_client() {
    let h = start_test_server_full().await;
    let mut c = client_for(&h.endpoint).await;
    let resp = c.member_list().await.unwrap();
    let members = resp.members();
    assert_eq!(members.len(), 1, "single-node cluster lists self only");
    assert_eq!(members[0].name(), "test-node");
}

#[tokio::test]
async fn status_via_etcd_client() {
    let h = start_test_server_full().await;
    let mut c = client_for(&h.endpoint).await;
    let resp = c.status().await.unwrap();
    assert_eq!(resp.version(), "3.6.0");
    assert!(resp.db_size() > 0);
}
