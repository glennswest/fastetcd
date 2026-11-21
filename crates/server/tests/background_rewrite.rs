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
    let tables_before = tables(&engine).await;
    let usage_before = engine.usage().await.unwrap();

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
    let usage_after = engine.usage().await.unwrap();
    let tables_after = tables(&engine).await;
    eprintln!(
        "data file {before} -> {after} bytes; usage {usage_before:?} -> {usage_after:?}\n\
         tables before (records, bytes): {tables_before:?}\ntables after: {tables_after:?}"
    );
    // This harness keeps its raft log in the data file (production's is
    // the WAL, #85), so the live data is that log as much as the keys. The
    // new file holds them in what redb can cut its file to (whole regions):
    // smaller, and no longer due (checked below).
    assert!(after < usage_before.file_bytes, "data file {before} -> {after} bytes");
    assert!(
        usage_after.reclaimable_bytes() * 100 < usage_after.file_bytes * config().rewrite.free_percent as u64,
        "the rewritten file is still mostly free pages: {usage_after:?}"
    );

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

/// fastetcd#221 (finding 4475547ca4da): a store at the NOSPACE mark whose
/// keys were deleted. The background check never admits the copy (it would
/// stay above the high-water mark it is already above); under pressure the
/// copy is admitted because it fits on the filesystem, and what is left
/// after it is below the alarm's clear mark.
#[test]
fn under_pressure_a_rewrite_is_admitted_when_the_copy_fits_on_the_filesystem() {
    use fastetcd_server::space::{pressure_rewrite_decision, rewrite_decision};
    let cfg = config();
    let mib = 1u64 << 20;
    // A 1000 MiB volume, 960 used (96%: above the 95% alarm); the data
    // file is 900 MiB, 20 of it live.
    let (fs_total, used, file, in_use) = (1000 * mib, 960 * mib, 900 * mib, 20 * mib);
    let fs_available = fs_total - used;
    assert!(
        rewrite_decision(&cfg.rewrite, cfg.high_water_percent, file, in_use, used, fs_total).is_err(),
        "the background check keeps the copy under the high-water mark"
    );
    // 20 MiB + an eighth, and a 10 MiB reserve, fit in the 40 MiB free.
    assert_eq!(pressure_rewrite_decision(&cfg, file, in_use, fs_total, fs_available), Ok(()));
    let after = used - file + in_use;
    assert!(after * 100 < fs_total * cfg.clear_percent as u64, "{after} left: below the clear mark");
    // A copy that does not fit with its reserve: 35 MiB + 4.4 + 10 > 40.
    assert!(pressure_rewrite_decision(&cfg, file, 35 * mib, fs_total, fs_available).is_err());
    // A small volume: the reserve is at least 1 MiB.
    assert!(pressure_rewrite_decision(&cfg, 60 * mib, 3 * mib, 64 * mib, 4 * mib).is_err());
    assert_eq!(pressure_rewrite_decision(&cfg, 60 * mib, 2 * mib, 64 * mib, 4 * mib), Ok(()));
    // No filesystem probe, nothing to come back, live bytes unmeasured, off.
    assert!(pressure_rewrite_decision(&cfg, file, in_use, 0, 0).is_err());
    assert!(pressure_rewrite_decision(&cfg, file, file - mib, fs_total, fs_available).is_err());
    assert!(pressure_rewrite_decision(&cfg, file, 0, fs_total, fs_available).is_err());
    let off = SpaceConfig { rewrite: RewriteConfig { free_percent: 0, ..cfg.rewrite.clone() }, ..cfg.clone() };
    assert!(pressure_rewrite_decision(&off, file, in_use, fs_total, fs_available).is_err());
}

/// The same on a serving member (#221): a quota of one byte puts the store
/// over every mark while the filesystem has room. The background check
/// refuses the rewrite; the reclaim under the alarm does it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reclaim_under_the_alarm_rewrites_when_the_copy_fits_on_the_filesystem() {
    let cfg = SpaceConfig { quota_backend_bytes: 1, alarm_percent: 95, clear_percent: 70, ..config() };
    let h = start_test_server_with_space(Some(cfg.clone())).await;
    let mut kv = KvClient::connect(h.endpoint.clone()).await.unwrap();
    let value = vec![b'v'; 16 << 10];
    for i in 0..1000 {
        kv.put(pb::PutRequest { key: format!("k{i:05}").into_bytes(), value: value.clone(), ..Default::default() })
            .await
            .unwrap();
    }
    kv.delete_range(pb::DeleteRangeRequest { key: b"k00000".to_vec(), range_end: b"k00700".to_vec(), ..Default::default() })
        .await
        .unwrap();
    h.state.sm.mvcc().engine().sync().await.unwrap();
    let stats = h.state.space.refresh(&h.state).await;
    assert!(stats.nospace, "over the one-byte quota: {stats:?}");
    assert!(stats.fs_total_bytes > 0, "the build VM's filesystem is probed: {stats:?}");

    let ok = || h.state.space.rewrites.ok.load(std::sync::atomic::Ordering::Relaxed);
    fastetcd_server::space::rewrite_if_due(&h.state, &cfg).await.unwrap();
    assert_eq!(ok(), 0, "the background check admitted a copy above the high-water mark");

    let engine = h.state.sm.mvcc().engine().clone();
    let before = engine.size_on_disk().await.unwrap();
    fastetcd_server::space::reclaim(&h.state, &cfg).await.unwrap();
    let after = engine.size_on_disk().await.unwrap();
    eprintln!("data file {before} -> {after} bytes; usage after {:?}", engine.usage().await.unwrap());
    assert_eq!(ok(), 1, "the reclaim under the alarm did not rewrite");
    assert!(after < before, "data file {before} -> {after} bytes");
    let left = kv
        .range(pb::RangeRequest { key: b"k".to_vec(), range_end: b"l".to_vec(), count_only: true, ..Default::default() })
        .await
        .unwrap()
        .into_inner()
        .count;
    assert_eq!(left, 300, "the keys kept");
}

/// Each table's records and key + value bytes.
async fn tables(engine: &std::sync::Arc<dyn fastetcd_storage::KvStore>) -> Vec<(String, usize, usize)> {
    let snap = engine.snapshot().await.unwrap();
    let mut out = Vec::new();
    for name in snap.table_names().await.unwrap() {
        let rows = snap.range(&name, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, 0).await.unwrap();
        let bytes = rows.iter().map(|(k, v)| k.len() + v.len()).sum();
        out.push((name, rows.len(), bytes));
    }
    out
}
