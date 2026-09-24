//! A watch never silently skips a revision (#16).
//!
//! A watch stream whose client stops reading backs up the forwarder
//! until the MVCC event broadcast (1024 batches) overflows and reports
//! `Lagged`. Before #16 the forwarder logged that and carried on, and
//! the skipped events were simply gone. Now the watcher is caught up
//! from history, or — if that history has been compacted — cancelled
//! with `compact_revision` set so the client re-lists.
//!
//! Both scenarios live in one test so the process-wide resync / cancel
//! counters they assert on are not disturbed by a parallel test.

mod common;
use common::start_test_server_full;

use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::watch_client::WatchClient;
use fastetcd_server::watch::{lag_cancel_count, resync_count};

/// Enough large puts to overflow the h2 flow-control windows, the
/// outbound channel and then the 1024-batch broadcast behind them.
const PUTS: usize = 3000;
const VALUE_BYTES: usize = 8 * 1024;
const WRITERS: usize = 4;

async fn open_prefix_watch(
    endpoint: &str,
) -> (
    mpsc::Sender<pb::WatchRequest>,
    tonic::Streaming<pb::WatchResponse>,
) {
    let mut wc = WatchClient::connect(endpoint.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel::<pb::WatchRequest>(8);
    tx.send(pb::WatchRequest {
        request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
            pb::WatchCreateRequest {
                key: b"lag/".to_vec(),
                range_end: b"lag0".to_vec(),
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let mut stream = wc.watch(ReceiverStream::new(rx)).await.unwrap().into_inner();
    let ack = stream.next().await.unwrap().unwrap();
    assert!(ack.created && !ack.canceled);
    (tx, stream)
}

/// Write `PUTS` distinct keys under `lag/` while nobody reads the watch.
async fn flood(endpoint: &str) -> i64 {
    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let endpoint = endpoint.to_string();
        tasks.push(tokio::spawn(async move {
            let mut kv = KvClient::connect(endpoint).await.unwrap();
            let mut last = 0;
            for i in (w..PUTS).step_by(WRITERS) {
                let r = kv
                    .put(pb::PutRequest {
                        key: format!("lag/{i:05}").into_bytes(),
                        value: vec![b'x'; VALUE_BYTES],
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_inner();
                last = r.header.unwrap().revision;
            }
            last
        }));
    }
    let mut max = 0;
    for t in tasks {
        max = max.max(t.await.unwrap());
    }
    max
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_watch_is_resynced_or_cancelled_never_gapped() {
    // --- 1. Lag with history intact: every revision, once, in order.
    {
        let h = start_test_server_full().await;
        let (_tx, mut stream) = open_prefix_watch(&h.endpoint).await;
        let resyncs_before = resync_count();

        let last_rev = flood(&h.endpoint).await;

        let mut seen: Vec<i64> = Vec::with_capacity(PUTS);
        while seen.len() < PUTS {
            let resp = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("watch stalled before delivering every revision")
                .unwrap()
                .unwrap();
            assert!(!resp.canceled, "unexpected cancel: {}", resp.cancel_reason);
            seen.extend(
                resp.events
                    .iter()
                    .map(|e| e.kv.as_ref().unwrap().mod_revision),
            );
        }
        assert_eq!(seen.len(), PUTS, "duplicate events delivered");
        for pair in seen.windows(2) {
            assert_eq!(pair[1], pair[0] + 1, "revision gap or reorder in watch stream");
        }
        assert_eq!(*seen.last().unwrap(), last_rev);
        assert!(
            resync_count() > resyncs_before,
            "test did not actually make the watcher lag; raise PUTS"
        );
    }

    // --- 2. Lag past a compaction: the watch must be cancelled with
    // compact_revision, and nothing before the cancel may have a hole.
    {
        let h = start_test_server_full().await;
        let (_tx, mut stream) = open_prefix_watch(&h.endpoint).await;
        let cancels_before = lag_cancel_count();

        let last_rev = flood(&h.endpoint).await;
        let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
        kv.compact(pb::CompactionRequest {
            revision: last_rev,
            physical: true,
        })
        .await
        .unwrap();

        let mut seen: Vec<i64> = Vec::new();
        let cancel = loop {
            let resp = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("lagging watch was neither caught up nor cancelled")
                .unwrap()
                .unwrap();
            if resp.canceled {
                break resp;
            }
            seen.extend(
                resp.events
                    .iter()
                    .map(|e| e.kv.as_ref().unwrap().mod_revision),
            );
        };
        assert_eq!(cancel.compact_revision, last_rev);
        assert!(!cancel.cancel_reason.is_empty());
        assert!(seen.len() < PUTS, "a compacted-away lag cannot have been delivered in full");
        for pair in seen.windows(2) {
            assert_eq!(pair[1], pair[0] + 1, "revision gap before the cancel");
        }
        assert!(lag_cancel_count() > cancels_before);
    }
}
