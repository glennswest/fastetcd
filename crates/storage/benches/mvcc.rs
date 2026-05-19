//! Microbenchmarks for the MVCC layer over the redb engine.
//!
//! Measures four core operations:
//!   - single-key Put (small value)        -> mvcc_put_single
//!   - single-key Range (current rev)      -> mvcc_range_single
//!   - batched Put (100 keys)              -> mvcc_put_batch_100
//!   - DeleteRange across 100 keys         -> mvcc_delete_range_100
//!
//! Each benchmark uses unique keys per iteration so the underlying
//! engine state grows over the measurement window. Numbers come from
//! `cargo bench -p fastetcd-storage`. The `iouring` engine, once
//! implemented, will be added as a second group for side-by-side.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tokio::runtime::Runtime;

use fastetcd_storage::mvcc::{Mutation, MvccStore};
use fastetcd_storage::redb_engine::RedbEngine;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn shared_mvcc(rt: &Runtime) -> (tempfile::TempDir, MvccStore) {
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let engine: Arc<dyn fastetcd_storage::KvStore> =
            Arc::new(RedbEngine::open(dir.path().join("mvcc.redb")).unwrap());
        let mvcc = MvccStore::open(engine).await.unwrap();
        (dir, mvcc)
    })
}

#[cfg(feature = "wal-engine")]
fn shared_mvcc_wal(rt: &Runtime) -> (tempfile::TempDir, MvccStore) {
    use fastetcd_storage::wal_engine::WalEngine;
    rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let engine: Arc<dyn fastetcd_storage::KvStore> =
            Arc::new(WalEngine::open(dir.path().join("mvcc.wal")).await.unwrap());
        let mvcc = MvccStore::open(engine).await.unwrap();
        (dir, mvcc)
    })
}

fn put_value(key: &[u8], value: &[u8]) -> Mutation {
    Mutation::Put {
        key: key.to_vec(),
        value: value.to_vec(),
        lease: 0,
        ignore_value: false,
        ignore_lease: false,
        prev_kv: false,
    }
}

fn bench_put_single(c: &mut Criterion) {
    let rt = rt();
    let mut group = c.benchmark_group("mvcc_put_single");
    group.throughput(Throughput::Elements(1));

    let (_dir, mvcc) = shared_mvcc(&rt);
    let counter = AtomicU64::new(0);
    group.bench_function("redb", |b| {
        b.iter(|| {
            let n = counter.fetch_add(1, Ordering::Relaxed);
            let key = format!("k-{n}");
            rt.block_on(async {
                mvcc.apply(&[put_value(key.as_bytes(), b"v")])
                    .await
                    .unwrap();
            });
        });
    });

    #[cfg(feature = "wal-engine")]
    {
        let (_dir2, mvcc2) = shared_mvcc_wal(&rt);
        let counter2 = AtomicU64::new(0);
        group.bench_function("wal", |b| {
            b.iter(|| {
                let n = counter2.fetch_add(1, Ordering::Relaxed);
                let key = format!("k-{n}");
                rt.block_on(async {
                    mvcc2.apply(&[put_value(key.as_bytes(), b"v")])
                        .await
                        .unwrap();
                });
            });
        });
    }

    group.finish();
}

fn bench_range_single(c: &mut Criterion) {
    let rt = rt();
    let (_dir, mvcc) = shared_mvcc(&rt);
    rt.block_on(async {
        mvcc.apply(&[put_value(b"k", b"v")]).await.unwrap();
    });
    let mut group = c.benchmark_group("mvcc_range_single");
    group.throughput(Throughput::Elements(1));
    group.bench_function("redb", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = mvcc.range(b"k", b"", 0, 0, false, false).await.unwrap();
            });
        });
    });
    group.finish();
}

fn bench_put_batch_100(c: &mut Criterion) {
    let rt = rt();
    let (_dir, mvcc) = shared_mvcc(&rt);
    let counter = AtomicU64::new(0);
    let mut group = c.benchmark_group("mvcc_put_batch_100");
    group.throughput(Throughput::Elements(100));
    group.bench_function("redb", |b| {
        b.iter(|| {
            let batch_id = counter.fetch_add(1, Ordering::Relaxed);
            let muts: Vec<Mutation> = (0..100u32)
                .map(|i| put_value(format!("b{batch_id}-{i}").as_bytes(), b"value"))
                .collect();
            rt.block_on(async {
                mvcc.apply(&muts).await.unwrap();
            });
        });
    });
    group.finish();
}

fn bench_delete_range_100(c: &mut Criterion) {
    let rt = rt();
    let (_dir, mvcc) = shared_mvcc(&rt);
    let counter = AtomicU64::new(0);
    let mut group = c.benchmark_group("mvcc_delete_range_100");
    group.throughput(Throughput::Elements(100));
    group.bench_function("redb", |b| {
        b.iter(|| {
            let batch_id = counter.fetch_add(1, Ordering::Relaxed);
            // Each iteration creates 100 keys in a unique namespace,
            // then deletes that whole namespace.
            rt.block_on(async {
                let prefix = format!("d{batch_id}-");
                let muts: Vec<Mutation> = (0..100u32)
                    .map(|i| put_value(format!("{prefix}{i:04}").as_bytes(), b"v"))
                    .collect();
                mvcc.apply(&muts).await.unwrap();
                // Range deletes everything starting with prefix.
                let mut end = prefix.clone().into_bytes();
                *end.last_mut().unwrap() += 1; // successor of '-' is '.'
                mvcc.apply(&[Mutation::DeleteRange {
                    key: prefix.into_bytes(),
                    range_end: end,
                    prev_kv: false,
                }])
                .await
                .unwrap();
            });
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_put_single,
    bench_range_single,
    bench_put_batch_100,
    bench_delete_range_100,
);
criterion_main!(benches);
