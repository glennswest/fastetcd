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
async fn watch_with_start_revision_backfills_history() {
    let h = start_test_server_full().await;
    let mut wc = WatchClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();

    // Build history first.
    kv.put(pb::PutRequest {
        key: b"k".to_vec(),
        value: b"v1".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    kv.put(pb::PutRequest {
        key: b"k".to_vec(),
        value: b"v2".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    kv.put(pb::PutRequest {
        key: b"k".to_vec(),
        value: b"v3".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();

    // Now subscribe from rev 2 — should receive the v2 and v3 events
    // (rev=2, rev=3) as historical backfill.
    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
            pb::WatchCreateRequest {
                key: b"k".to_vec(),
                start_revision: 2,
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let mut stream = wc
        .watch(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let ack = stream.next().await.unwrap().unwrap();
    assert!(ack.created);

    let backfill = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let vals: Vec<Vec<u8>> = backfill
        .events
        .iter()
        .map(|e| e.kv.as_ref().unwrap().value.clone())
        .collect();
    assert_eq!(vals, vec![b"v2".to_vec(), b"v3".to_vec()]);
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

/// #58: a watcher created with `fragment` gets a large response as
/// etcd's `sendFragments` splits it (whole events, each part under the
/// 1.5 MiB default, all but the last `fragment: true`), live and in
/// history replay; one created without it gets the response whole.
#[tokio::test]
async fn a_fragmenting_watcher_gets_large_responses_in_parts() {
    let h = start_test_server_full().await;
    let mut watch_client = WatchClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv_client = KvClient::connect(h.endpoint.clone()).await.unwrap();

    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    let mut stream = watch_client
        .watch(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let mut ids = Vec::new();
    for fragment in [true, false] {
        let mut req = create_req(b"big/", b"big0");
        if let Some(pb::watch_request::RequestUnion::CreateRequest(c)) = &mut req.request_union {
            c.fragment = fragment;
        }
        tx.send(req).await.unwrap();
        let ack = stream.next().await.unwrap().unwrap();
        assert!(ack.created);
        ids.push(ack.watch_id);
    }
    let (fragmenting, whole) = (ids[0], ids[1]);

    // One revision, 8 events of 400 KiB: 3.2 MiB in one response.
    let value = vec![b'x'; 400 * 1024];
    let puts = (0..8)
        .map(|i| pb::RequestOp {
            request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
                key: format!("big/{i}").into_bytes(),
                value: value.clone(),
                ..Default::default()
            })),
        })
        .collect();
    let rev = kv_client
        .txn(pb::TxnRequest { success: puts, ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .header
        .unwrap()
        .revision;

    // Collect until the fragmenting watcher's last part and the whole one.
    let mut parts = Vec::new();
    let mut whole_resp = None;
    while whole_resp.is_none() || parts.last().map_or(true, |p: &pb::WatchResponse| p.fragment) {
        let r = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("watch responses within 10 s")
            .unwrap()
            .unwrap();
        if r.watch_id == fragmenting {
            parts.push(r);
        } else {
            assert_eq!(r.watch_id, whole);
            whole_resp = Some(r);
        }
    }
    let whole_resp = whole_resp.unwrap();
    assert!(!whole_resp.fragment);
    assert_eq!(whole_resp.events.len(), 8);

    let check = |parts: &[pb::WatchResponse]| {
        let sizes: Vec<_> = parts.iter().map(|p| p.events.len()).collect();
        assert_eq!(sizes, vec![3, 3, 2], "events per part");
        let flags: Vec<_> = parts.iter().map(|p| p.fragment).collect();
        assert_eq!(flags, vec![true, true, false]);
        let keys: Vec<_> = parts
            .iter()
            .flat_map(|p| p.events.iter().map(|e| e.kv.as_ref().unwrap().key.clone()))
            .collect();
        let want: Vec<_> = (0..8).map(|i| format!("big/{i}").into_bytes()).collect();
        assert_eq!(keys, want);
        for p in parts {
            assert_eq!(p.header.as_ref().unwrap().revision, rev);
        }
    };
    check(&parts);

    // History replay of the same revision splits the same way.
    let mut req = create_req(b"big/", b"big0");
    if let Some(pb::watch_request::RequestUnion::CreateRequest(c)) = &mut req.request_union {
        c.fragment = true;
        c.start_revision = rev;
    }
    tx.send(req).await.unwrap();
    let ack = stream.next().await.unwrap().unwrap();
    assert!(ack.created);
    let mut replayed = Vec::new();
    while replayed.last().map_or(true, |p: &pb::WatchResponse| p.fragment) {
        let r = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("replay within 10 s")
            .unwrap()
            .unwrap();
        assert_eq!(r.watch_id, ack.watch_id);
        replayed.push(r);
    }
    check(&replayed);
}
