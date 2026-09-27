//! What redb's two-phase commit would cost on every fastetcd write
//! (fastetcd#37). Not enabled: see the changelog for v1.4.0. This
//! records the measurement behind that decision.
//!
//! Ignored by default because it measures the disk, not the code:
//!
//! ```text
//! cargo test --release -p fastetcd-storage --test two_phase_commit_cost -- --ignored --nocapture
//! ```
//!
//! Each commit is one small key, the shape of a single etcd put, and
//! redb fsyncs every commit either way; two-phase commit adds a second
//! fsync to flip the commit slot.

#![cfg(feature = "redb-engine")]

use std::time::{Duration, Instant};

use redb::{Database, TableDefinition};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("t");
const COMMITS: usize = 500;

fn run(two_phase: bool) -> Vec<Duration> {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("cost.redb")).unwrap();
    let value = [7u8; 256];
    let mut samples = Vec::with_capacity(COMMITS);
    for i in 0..COMMITS {
        let started = Instant::now();
        let mut txn = db.begin_write().unwrap();
        txn.set_two_phase_commit(two_phase);
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(format!("key/{i:06}").as_bytes(), value.as_slice()).unwrap();
        }
        txn.commit().unwrap();
        samples.push(started.elapsed());
    }
    samples.sort();
    samples
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

#[test]
#[ignore = "measures the disk; run explicitly with --ignored --nocapture"]
fn two_phase_commit_cost() {
    // Warm up the filesystem and page cache once.
    let _ = run(false);
    for two_phase in [false, true, false, true] {
        let s = run(two_phase);
        let mean = s.iter().sum::<Duration>() / s.len() as u32;
        println!(
            "two_phase_commit={two_phase:<5} commits={COMMITS} mean={mean:?} p50={:?} p99={:?} commits/s={:.0}",
            pct(&s, 0.50),
            pct(&s, 0.99),
            1.0 / mean.as_secs_f64()
        );
    }
}
