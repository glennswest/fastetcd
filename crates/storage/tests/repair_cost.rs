//! What redb's repair of a data file not closed cleanly costs, by file
//! size, cold (fastetcd#176), and what redb's quick-repair commits would
//! change. Records a measurement; changes nothing.
//!
//! Ignored by default because it measures the disk, not the code:
//!
//! ```text
//! cargo test --release -p fastetcd-storage --test repair_cost -- --ignored --nocapture
//! ```
//!
//! For each size, a child process (this test binary again) writes a file
//! as fastetcd does (1000-record batches committed non-durably, every
//! 20th durably, as a checkpoint) and exits without closing it, as a
//! kill or a power cut leaves it. The parent drops the file from the page
//! cache (`posix_fadvise(DONTNEED)`: what a reboot does) and opens it,
//! timing redb's repair and counting the bytes it read from the device.
//! With `quick` the child's durable commits are redb quick-repair commits
//! (two-phase commit + the allocator state saved), and their cost is
//! reported too.
//!
//! The file goes in cargo's per-target temp dir (on the build disk), or
//! `$REPAIR_COST_DIR`, never `/tmp` (often a tmpfs).

#![cfg(all(feature = "redb-engine", target_os = "linux"))]

use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use redb::{Database, Durability, TableDefinition};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("mvcc_kv");
const VALUE: usize = 1024;
const BATCH: usize = 1000;
const CHECKPOINT_EVERY: usize = 20;
const CACHE: usize = 256 << 20;

fn base_dir() -> PathBuf {
    std::env::var_os("REPAIR_COST_DIR")
        .map(Into::into)
        .unwrap_or_else(|| env!("CARGO_TARGET_TMPDIR").into())
}

/// The child: write `mib` MiB of records into `path`, then exit without
/// dropping the database. Prints the durable commits' median duration.
#[test]
#[ignore]
fn repair_cost_child() {
    let Ok(spec) = std::env::var("REPAIR_COST_CHILD") else { return };
    let mut parts = spec.split(',');
    let path = PathBuf::from(parts.next().unwrap());
    let mib: usize = parts.next().unwrap().parse().unwrap();
    let quick = parts.next().unwrap() == "quick";
    let db = Database::builder().set_cache_size(CACHE).create(&path).unwrap();
    let records = (mib << 20) / VALUE;
    let value = vec![7u8; VALUE];
    let mut durable = Vec::new();
    let mut rev = 0u64;
    for batch in 0..records.div_ceil(BATCH) {
        let checkpoint = batch % CHECKPOINT_EVERY == CHECKPOINT_EVERY - 1;
        let started = Instant::now();
        let mut txn = db.begin_write().unwrap();
        if checkpoint {
            txn.set_quick_repair(quick);
        } else {
            txn.set_durability(Durability::None);
        }
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for _ in 0..BATCH {
                rev += 1;
                t.insert(rev.to_be_bytes().as_slice(), value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        if checkpoint {
            durable.push(started.elapsed());
        }
    }
    // A last durable commit, so the file holds everything: the crash comes
    // after a checkpoint, with nothing more to lose.
    let started = Instant::now();
    let mut txn = db.begin_write().unwrap();
    txn.set_quick_repair(quick);
    txn.commit().unwrap();
    durable.push(started.elapsed());
    durable.sort();
    println!("DURABLE_MEDIAN_US {}", durable[durable.len() / 2].as_micros());
    // As a kill leaves it: no Drop, so redb never marks the file clean.
    std::process::exit(0);
}

fn read_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("read_bytes:").map(|v| v.trim().parse().unwrap()))
        .unwrap()
}

fn drop_from_page_cache(path: &Path) {
    let f = std::fs::File::open(path).unwrap();
    f.sync_all().unwrap();
    let rc = unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    assert_eq!(rc, 0, "posix_fadvise");
}

struct Run {
    file_mib: u64,
    open: Duration,
    read_mib: f64,
    repaired: bool,
    durable_median: Duration,
}

fn run(mib: usize, quick: bool) -> Run {
    let dir = tempfile::tempdir_in(base_dir()).unwrap();
    let path = dir.path().join("fastetcd.redb");
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "repair_cost_child", "--ignored", "--nocapture"])
        .env("REPAIR_COST_CHILD", format!("{},{mib},{}", path.display(), if quick { "quick" } else { "full" }))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "child: {stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    let durable_us: u64 = stdout
        .lines()
        .find_map(|l| l.strip_prefix("DURABLE_MEDIAN_US "))
        .unwrap_or_else(|| panic!("child printed no timing: {stdout}"))
        .trim()
        .parse()
        .unwrap();
    let file_mib = std::fs::metadata(&path).unwrap().len() >> 20;
    drop_from_page_cache(&path);
    let repaired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let before = read_bytes();
    let started = Instant::now();
    let flag = repaired.clone();
    let db = Database::builder()
        .set_cache_size(CACHE)
        .set_repair_callback(move |_| flag.store(true, std::sync::atomic::Ordering::Relaxed))
        .create(&path)
        .unwrap();
    let open = started.elapsed();
    let read_mib = (read_bytes() - before) as f64 / (1 << 20) as f64;
    drop(db);
    Run {
        file_mib,
        open,
        read_mib,
        repaired: repaired.load(std::sync::atomic::Ordering::Relaxed),
        durable_median: Duration::from_micros(durable_us),
    }
}

#[test]
#[ignore]
fn repair_cost() {
    if std::env::var("REPAIR_COST_CHILD").is_ok() {
        return;
    }
    let sizes: Vec<usize> = std::env::var("REPAIR_COST_MIB")
        .map(|s| s.split(',').map(|v| v.parse().unwrap()).collect())
        .unwrap_or_else(|_| vec![64, 256, 512]);
    println!("records {VALUE} B, batches of {BATCH}, a durable commit every {CHECKPOINT_EVERY}; cold open after a kill");
    println!("{:>9} {:>6} {:>9} {:>11} {:>9} {:>14}", "data MiB", "mode", "file MiB", "open", "read MiB", "durable p50");
    for mib in sizes {
        for quick in [false, true] {
            let r = run(mib, quick);
            assert_eq!(r.repaired, !quick, "a full repair only without quick-repair commits");
            println!(
                "{:>9} {:>6} {:>9} {:>11.3?} {:>9.1} {:>14.3?}",
                mib,
                if quick { "quick" } else { "full" },
                r.file_mib,
                r.open,
                r.read_mib,
                r.durable_median
            );
        }
    }
}
