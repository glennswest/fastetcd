//! fastetcd#223: a watch opened on a new store gets the first write's
//! event, at revision 2, with no "revision gap" and no resync. The store
//! moves from 0 to 1 (`StartRevision`, #160) with no events; that move is
//! now published as an empty batch, so the first write's batch follows it.
//!
//! In a file of its own: `resync_count` is process-wide.

mod common;
use common::start_test_server_full;

use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::watch_client::WatchClient;
use fastetcd_server::watch::resync_count;

#[tokio::test]
async fn the_first_write_on_a_new_store_needs_no_resync() {
    let h = start_test_server_full().await;
    assert_eq!(h.state.sm.mvcc().current_revision().await, 0, "not a new store");
    let mut watch = WatchClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(4);
    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(pb::WatchCreateRequest {
            key: b"k".to_vec(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut stream = watch.watch(ReceiverStream::new(rx)).await.unwrap().into_inner();
    assert!(stream.next().await.unwrap().unwrap().created);
    let resyncs = resync_count();

    let put = kv
        .put(pb::PutRequest { key: b"k".to_vec(), value: b"v".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(put.header.unwrap().revision, 2, "the first write of a new store, as etcd's");
    let resp = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("no event for the first write")
        .unwrap()
        .unwrap();
    let revs: Vec<i64> = resp.events.iter().map(|e| e.kv.as_ref().unwrap().mod_revision).collect();
    assert_eq!(revs, vec![2]);
    assert_eq!(resync_count(), resyncs, "the stream resynced: it took the start revision for a gap");
}
