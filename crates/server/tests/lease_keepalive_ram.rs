//! Keep-alives are renewed in the leader's RAM, not logged (fastetcd#92):
//! a renewal adds no raft entry, keeps a lease alive past its persisted
//! deadline, and once renewals stop the lease expires as before.

mod common;
use common::start_test_server_full_with_expiry_ticker;

use std::sync::atomic::Ordering;
use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

fn last_log_index(h: &common::TestServerHandles) -> u64 {
    h.state.raft.metrics().borrow().last_log_index.unwrap_or(0)
}

async fn value(kv: &mut KvClient<tonic::transport::Channel>, key: &[u8]) -> Option<Vec<u8>> {
    let r = kv
        .range(pb::RangeRequest { key: key.to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner();
    r.kvs.first().map(|kv| kv.value.clone())
}

#[tokio::test]
async fn keep_alives_add_no_log_entries_and_keep_a_lease_past_its_ttl() {
    let h = start_test_server_full_with_expiry_ticker().await;
    let mut lc = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let id = lc.lease_grant(pb::LeaseGrantRequest { ttl: 3, id: 0 }).await.unwrap().into_inner().id;
    kv.put(pb::PutRequest { key: b"k".to_vec(), value: b"v".to_vec(), lease: id, ..Default::default() })
        .await
        .unwrap();
    let before = last_log_index(&h);

    // Renew every second for 8 s: well past the 3 s the grant persisted.
    let (tx, rx) = mpsc::channel(4);
    let mut answers = lc.lease_keep_alive(ReceiverStream::new(rx)).await.unwrap().into_inner();
    for _ in 0..8 {
        tx.send(pb::LeaseKeepAliveRequest { id }).await.unwrap();
        let a = answers.next().await.unwrap().unwrap();
        assert_eq!((a.id, a.ttl), (id, 3));
        sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(last_log_index(&h), before, "a keep-alive added a raft log entry");
    assert_eq!(h.state.lessor.stats().renewed_in_ram.load(Ordering::Relaxed), 8);
    assert_eq!(value(&mut kv, b"k").await.as_deref(), Some(b"v".as_ref()), "renewed lease expired");
    let ttl = lc
        .lease_time_to_live(pb::LeaseTimeToLiveRequest { id, keys: false })
        .await
        .unwrap()
        .into_inner();
    assert!(ttl.ttl >= 1 && ttl.granted_ttl == 3, "TimeToLive sees the renewal: {ttl:?}");

    // Renewals stop: the lease expires and takes its key with it.
    drop(tx);
    let deadline = Instant::now() + Duration::from_secs(10);
    while value(&mut kv, b"k").await.is_some() {
        assert!(Instant::now() < deadline, "the lease never expired once renewals stopped");
        sleep(Duration::from_millis(200)).await;
    }
    let ttl = lc
        .lease_time_to_live(pb::LeaseTimeToLiveRequest { id, keys: false })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ttl.ttl, -1, "revoked: {ttl:?}");
}

#[tokio::test]
async fn a_keep_alive_of_a_missing_lease_answers_ttl_0() {
    let h = start_test_server_full_with_expiry_ticker().await;
    let mut lc = LeaseClient::connect(h.endpoint.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(1);
    let mut answers = lc.lease_keep_alive(ReceiverStream::new(rx)).await.unwrap().into_inner();
    tx.send(pb::LeaseKeepAliveRequest { id: 4242 }).await.unwrap();
    let a = answers.next().await.unwrap().unwrap();
    assert_eq!((a.id, a.ttl), (4242, 0));
}
