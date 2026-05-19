//! Watch service gRPC tests.

mod common;
use common::start_test_server_full;

use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::watch_client::WatchClient;
use fastetcd_proto::mvccpb;

fn create_req(key: &[u8], range_end: &[u8]) -> pb::WatchRequest {
    pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
            pb::WatchCreateRequest {
                key: key.to_vec(),
                range_end: range_end.to_vec(),
                start_revision: 0,
                progress_notify: false,
                filters: vec![],
                prev_kv: false,
                watch_id: 0,
                fragment: false,
            },
        )),
    }
}

#[tokio::test]
async fn watch_receives_put_event() {
    let h = start_test_server_full().await;
    let mut watch_client = WatchClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv_client = KvClient::connect(h.endpoint.clone()).await.unwrap();

    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    tx.send(create_req(b"foo", b"")).await.unwrap();
    let mut stream = watch_client
        .watch(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    // First response is the create ack.
    let ack = stream.next().await.unwrap().unwrap();
    assert!(ack.created);
    assert!(!ack.canceled);
    assert_eq!(ack.events.len(), 0);
    let watch_id = ack.watch_id;
    assert!(watch_id > 0);

    // Trigger a Put.
    kv_client
        .put(pb::PutRequest {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Receive the event.
    let event_resp = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("did not receive event within 2s")
        .unwrap()
        .unwrap();
    assert_eq!(event_resp.watch_id, watch_id);
    assert_eq!(event_resp.events.len(), 1);
    let e = &event_resp.events[0];
    assert_eq!(e.r#type, mvccpb::event::EventType::Put as i32);
    let kv = e.kv.as_ref().unwrap();
    assert_eq!(kv.key, b"foo");
    assert_eq!(kv.value, b"bar");
}

#[tokio::test]
async fn watch_range_only_matches_in_range() {
    let h = start_test_server_full().await;
    let mut watch_client = WatchClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv_client = KvClient::connect(h.endpoint.clone()).await.unwrap();

    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    tx.send(create_req(b"a", b"c")).await.unwrap();
    let mut stream = watch_client
        .watch(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let _ack = stream.next().await.unwrap().unwrap();

    kv_client
        .put(pb::PutRequest {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    kv_client
        .put(pb::PutRequest {
            key: b"z".to_vec(),
            value: b"99".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    kv_client
        .put(pb::PutRequest {
            key: b"b".to_vec(),
            value: b"2".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut seen: Vec<Vec<u8>> = Vec::new();
    while seen.len() < 2 {
        let resp = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("did not receive event within 2s")
            .unwrap()
            .unwrap();
        for e in resp.events {
            seen.push(e.kv.as_ref().unwrap().key.clone());
        }
    }
    seen.sort();
    assert_eq!(seen, vec![b"a".to_vec(), b"b".to_vec()]);
}

#[tokio::test]
async fn watch_with_prev_kv_returns_prior_value() {
    let h = start_test_server_full().await;
    let mut watch_client = WatchClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv_client = KvClient::connect(h.endpoint.clone()).await.unwrap();

    kv_client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v0".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
            pb::WatchCreateRequest {
                key: b"k".to_vec(),
                prev_kv: true,
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let mut stream = watch_client
        .watch(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let _ack = stream.next().await.unwrap().unwrap();

    kv_client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let e = &resp.events[0];
    let prev = e.prev_kv.as_ref().expect("prev_kv missing");
    assert_eq!(prev.value, b"v0");
}

#[tokio::test]
async fn watch_cancel_stops_events() {
    let h = start_test_server_full().await;
    let mut watch_client = WatchClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv_client = KvClient::connect(h.endpoint.clone()).await.unwrap();

    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    tx.send(create_req(b"x", b"")).await.unwrap();
    let mut stream = watch_client
        .watch(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let ack = stream.next().await.unwrap().unwrap();
    let watch_id = ack.watch_id;

    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CancelRequest(
            pb::WatchCancelRequest { watch_id },
        )),
    })
    .await
    .unwrap();
    let cancel_resp = stream.next().await.unwrap().unwrap();
    assert!(cancel_resp.canceled);
    assert_eq!(cancel_resp.watch_id, watch_id);

    // Subsequent puts should NOT produce events.
    kv_client
        .put(pb::PutRequest {
            key: b"x".to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let no_event =
        tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
    assert!(no_event.is_err(), "should have timed out (no more events)");
}

#[tokio::test]
async fn watch_at_compacted_revision_returns_canceled() {
    let h = start_test_server_full().await;
    let mut watch_client = WatchClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv_client = KvClient::connect(h.endpoint.clone()).await.unwrap();

    kv_client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v0".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    kv_client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    kv_client
        .compact(pb::CompactionRequest {
            revision: 2,
            physical: false,
        })
        .await
        .unwrap();

    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
            pb::WatchCreateRequest {
                key: b"k".to_vec(),
                start_revision: 1,
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let mut stream = watch_client
        .watch(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let resp = stream.next().await.unwrap().unwrap();
    assert!(resp.created);
    assert!(resp.canceled);
    assert_eq!(resp.compact_revision, 2);
}
