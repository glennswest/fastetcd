//! The background rewrite on a serving member (fastetcd#177): due once
//! enough of the data file is free pages, it rewrites and switches while
//! the member serves, and says so in its metrics.

mod common;
mod spawned;

use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_server::space::{RewriteConfig, SpaceConfig};

use common::start_test_server_with_space;

fn config() -> SpaceConfig {
    SpaceConfig {
        interval: Duration::from_secs(3600), // no background ticker in tests
        rewrite: RewriteConfig {
            free_percent: 50,
            min_file_bytes: 1 << 20,
            interval: Duration::from_secs(3600),
            pace: 1,
        },
        ..SpaceConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fragmented_store_is_rewritten_while_it_serves() {
    let h = start_test_server_with_space(Some(config())).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let value = vec![b'v'; 16 << 10];
    for i in 0..1000 {
        kv.put(pb::PutRequest { key: format!("k{i:05}").into_bytes(), value: value.clone(), ..Default::default() })
            .await
            .unwrap();
    }
    // Delete most of it and compact the history away: the pages are free.
    let rev = kv
        .delete_range(pb::DeleteRangeRequest { key: b"k00000".to_vec(), range_end: b"k00700".to_vec(), ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .header
        .unwrap()
        .revision;
    kv.compact(pb::CompactionRequest { revision: rev, physical: true }).await.unwrap();
    // The checkpoint writes the deletes into the data file.
    h.state.sm.mvcc().engine().sync().await.unwrap();

    let engine = h.state.sm.mvcc().engine().clone();
    let before = engine.size_on_disk().await.unwrap();

    // Writes and reads go on through the rewrite.
    let traffic = {
        let mut kv = kv.clone();
        tokio::spawn(async move {
            let mut n = 0;
            let started = tokio::time::Instant::now();
            while started.elapsed() < Duration::from_secs(3) {
                kv.put(pb::PutRequest { key: format!("live{n:05}").into_bytes(), value: b"x".to_vec(), ..Default::default() })
                    .await
                    .unwrap();
                n += 1;
            }
            n
        })
    };
    fastetcd_server::space::rewrite_if_due(&h.state, &config()).await.unwrap();
    let written = traffic.await.unwrap();
    let r = &h.state.space.rewrites;
    assert_eq!(r.ok.load(std::sync::atomic::Ordering::Relaxed), 1, "the rewrite was not due or did not run");
    let after = engine.size_on_disk().await.unwrap();
    assert!(after * 2 < before, "data file {before} -> {after} bytes");

    let count = |key: &[u8], end: &[u8]| {
        let (mut kv, key, end) = (kv.clone(), key.to_vec(), end.to_vec());
        async move {
            kv.range(pb::RangeRequest { key, range_end: end, count_only: true, ..Default::default() })
                .await
                .unwrap()
                .into_inner()
                .count
        }
    };
    assert_eq!(count(b"k", b"l").await, 300, "the keys kept");
    assert_eq!(count(b"live", b"livf").await, written, "every write during the rewrite");

    // Not due again: the file is compact now.
    fastetcd_server::space::rewrite_if_due(&h.state, &config()).await.unwrap();
    assert_eq!(r.ok.load(std::sync::atomic::Ordering::Relaxed), 1);

    // The metrics say what it did.
    let listener = tokio::net::TcpListener::bind((spawned::random_loopback(), 0)).await.unwrap();
    let bound = listener.local_addr().unwrap();
    drop(listener);
    fastetcd_server::metrics::spawn_server(bound, fastetcd_server::metrics::Metrics::new(), h.state.clone());
    let url = format!("http://{bound}/metrics");
    let mut body = String::new();
    for _ in 0..50 {
        if let Ok(r) = reqwest::get(&url).await {
            body = r.text().await.unwrap();
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(body.contains("fastetcd_defrag_rewrites_total{result=\"ok\"} 1"), "{body}");
    assert!(body.contains(&format!("fastetcd_defrag_rewrite_last_after_bytes {after}")), "{body}");
}

#[test]
fn when_a_rewrite_is_due() {
    use fastetcd_server::space::rewrite_decision;
    let cfg = config().rewrite;
    let mib = 1u64 << 20;
    // 100 MiB file, 30 live: 70% free.
    assert_eq!(rewrite_decision(&cfg, 80, 100 * mib, 30 * mib, 200 * mib, 1000 * mib), Ok(()));
    assert!(rewrite_decision(&cfg, 80, 100 * mib, 60 * mib, 200 * mib, 1000 * mib).is_err(), "40% free");
    assert!(rewrite_decision(&cfg, 80, mib / 2, 0, 0, 0).is_err(), "small file");
    // The copy (30 MiB) would take the volume past 80%.
    assert!(rewrite_decision(&cfg, 80, 100 * mib, 30 * mib, 780 * mib, 1000 * mib).is_err());
    let off = RewriteConfig { free_percent: 0, ..cfg };
    assert!(rewrite_decision(&off, 80, 100 * mib, 1, 0, 0).is_err());
}
