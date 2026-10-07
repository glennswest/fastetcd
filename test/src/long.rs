//! `long` (the night window): waves of fastetcd's main workload, KV, on a
//! member this suite starts from the commit's binary, sized from what the
//! pod was given (its CPUs and its memory limit, read from its cgroup),
//! never assumed. Each wave ramps (writes its keys), holds (updates,
//! linearizable reads, watches, lease churn), and drains (deletes,
//! compacts, defragments); what is measured across waves is the point:
//! throughput and p99 of the ramp, and what the member keeps after the
//! drain (RSS, file descriptors, data file size, leases). A wave slower
//! than the first of its size, or a residue that grows, fails the trend
//! even when every operation passed. (The standard's container and VM
//! waves are cluster workloads; fastetcd's is the datastore's own.)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::time::Instant;

use crate::env::Env;
use crate::member::Member;
use crate::report::Report;

struct Capacity {
    cpus: usize,
    memory: u64,
}

fn capacity() -> Capacity {
    let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let read = |p: &str| std::fs::read_to_string(p).ok().map(|s| s.trim().to_string());
    let limit = read("/sys/fs/cgroup/memory.max")
        .or_else(|| read("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&b| b < (1 << 50));
    let available = read("/proc/meminfo").and_then(|s| {
        s.lines()
            .find(|l| l.starts_with("MemAvailable:"))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|kb| kb.parse::<u64>().ok())
            .map(|kb| kb * 1024)
    });
    let memory = match (limit, available) {
        (Some(l), Some(a)) => l.min(a),
        (Some(l), None) => l,
        (None, Some(a)) => a,
        (None, None) => 1 << 30,
    };
    Capacity { cpus, memory }
}

#[derive(Clone, Copy)]
struct Wave {
    factor: f64,
    ramp_ops: f64,
    p99_ms: f64,
    hold_ops: u64,
    rss: u64,
    fds: u64,
    db: u64,
    leases_left: usize,
}

fn rss_of(pid: u32) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<u64>().ok())
        })
        .map_or(0, |kb| kb * 1024)
}

fn fds_of(pid: u32) -> u64 {
    std::fs::read_dir(format!("/proc/{pid}/fd")).map_or(0, |d| d.count() as u64)
}

pub async fn run(env: &Env, rep: &mut Report) -> anyhow::Result<()> {
    let cap = capacity();
    let scale: f64 = std::env::var("FASTETCD_TEST_LONG_SCALE").ok().and_then(|s| s.parse().ok()).unwrap_or(1.0);
    let hold = Duration::from_secs(
        std::env::var("FASTETCD_TEST_LONG_HOLD_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(120),
    );
    let max_waves: usize = std::env::var("FASTETCD_TEST_LONG_WAVES").ok().and_then(|s| s.parse().ok()).unwrap_or(usize::MAX);
    // A quarter of the pod's memory for the wave's data, ~2 KiB a key.
    let base_keys = (((cap.memory / 4 / 2048) as f64 * scale) as usize).clamp(1000, 500_000);
    let writers = ((cap.cpus * 16) as f64 * scale.min(1.0)).clamp(4.0, 512.0) as usize;
    eprintln!(
        "long: {} cpus, {} MiB, waves of up to {base_keys} keys with {writers} writers, hold {hold:?}",
        cap.cpus,
        cap.memory >> 20
    );

    let mut m = Member::single(&env.bin, &env.work, "long", &[]).await?;
    let mut waves: Vec<Wave> = Vec::new();
    let factors = [1.0, 0.5, 0.75];
    let mut n = 0;
    loop {
        // Stop while a wave like the longest so far still fits.
        let reserve = Duration::from_secs(600);
        if n >= max_waves || env.left() < reserve + hold * 2 {
            break;
        }
        let factor = factors[n % factors.len()];
        let keys = ((base_keys as f64) * factor) as usize;
        let t = Instant::now();
        match wave(&m, keys, writers, hold).await {
            Ok(mut w) => {
                w.factor = factor;
                let pid = m.pid().unwrap_or(0);
                w.rss = rss_of(pid);
                w.fds = fds_of(pid);
                w.db = std::fs::metadata(m.data_file()).map_or(0, |md| md.len());
                rep.line(
                    &format!("wave-{}", n + 1),
                    if w.leases_left == 0 { "pass" } else { "fail" },
                    t.elapsed().as_millis(),
                    &format!("{keys} keys x{factor}"),
                    Some(json!({
                        "keys": keys, "writers": writers, "ramp_ops_per_s": w.ramp_ops.round(),
                        "ramp_p99_ms": (w.p99_ms * 10.0).round() / 10.0, "hold_ops": w.hold_ops,
                        "rss_bytes": w.rss, "fds": w.fds, "db_bytes": w.db, "leases_left": w.leases_left,
                    })),
                );
                waves.push(w);
            }
            Err(e) => {
                rep.line(&format!("wave-{}", n + 1), "fail", t.elapsed().as_millis(), &format!("{e:#}"), None);
                break;
            }
        }
        n += 1;
    }
    m.kill().await;
    let _ = std::fs::remove_dir_all(&m.dir);

    let t = Instant::now();
    match trend(&waves) {
        Ok(d) => rep.line("wave-trend", if waves.len() >= 2 { "pass" } else { "skip" }, t.elapsed().as_millis(), &d, None),
        Err(d) => rep.line("wave-trend", "fail", t.elapsed().as_millis(), &d, None),
    }
    Ok(())
}

/// The first regression, if any: a full-size wave whose ramp is under 70%
/// of the first full wave's, or a residue that grew past the first wave's.
fn trend(waves: &[Wave]) -> Result<String, String> {
    if waves.len() < 2 {
        return Ok(format!("{} wave(s): no trend to compare", waves.len()));
    }
    let first_full = waves.iter().find(|w| w.factor == 1.0).copied().unwrap_or(waves[0]);
    let w0 = waves[0];
    for (i, w) in waves.iter().enumerate().skip(1) {
        let n = i + 1;
        if w.factor == 1.0 && w.ramp_ops < 0.7 * first_full.ramp_ops {
            return Err(format!(
                "wave {n} ramped at {:.0} ops/s, under 70% of wave 1's {:.0}",
                w.ramp_ops, first_full.ramp_ops
            ));
        }
        if w.rss > w0.rss + w0.rss / 2 + (64 << 20) {
            return Err(format!("wave {n} left {} MiB RSS after its drain, wave 1 {} MiB", w.rss >> 20, w0.rss >> 20));
        }
        if w.fds > w0.fds + 32 {
            return Err(format!("wave {n} left {} file descriptors open, wave 1 {}", w.fds, w0.fds));
        }
        if w.db > w0.db + w0.db / 2 + (16 << 20) {
            return Err(format!("wave {n} left a {} MiB data file after defragment, wave 1 {} MiB", w.db >> 20, w0.db >> 20));
        }
    }
    Ok(format!(
        "{} waves: no full wave under 70% of the first's ramp ({:.0} ops/s); residue after drain stayed within wave 1's (RSS {} MiB, {} fds, data file {} MiB)",
        waves.len(),
        first_full.ramp_ops,
        w0.rss >> 20,
        w0.fds,
        w0.db >> 20
    ))
}

async fn wave(m: &Member, keys: usize, writers: usize, hold: Duration) -> anyhow::Result<Wave> {
    let c = m.clients().await?;
    let value = Arc::new(vec![b'w'; 1024]);

    // Ramp: every key written once, split across the writers.
    let t = Instant::now();
    let tasks: Vec<_> = (0..writers)
        .map(|w| {
            let (mut c, value) = (c.clone(), value.clone());
            tokio::spawn(async move {
                let mut lat = Vec::new();
                let mut k = w;
                while k < keys {
                    let s = Instant::now();
                    c.put(format!("/w/{k:07}").as_bytes(), &value).await?;
                    lat.push(s.elapsed().as_micros() as u64);
                    k += writers;
                }
                anyhow::Ok(lat)
            })
        })
        .collect();
    let mut lat = Vec::new();
    for t in tasks {
        lat.extend(t.await??);
    }
    let ramp = t.elapsed();
    lat.sort_unstable();
    let p99 = lat.get(lat.len() * 99 / 100).copied().unwrap_or(0) as f64 / 1000.0;

    // Hold: updates and linearizable reads, watches counting, lease churn.
    let stop = Instant::now() + hold;
    let ops = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for w in 0..writers {
        let (mut c, value, ops) = (c.clone(), value.clone(), ops.clone());
        tasks.push(tokio::spawn(async move {
            let mut i = w;
            while Instant::now() < stop {
                let k = format!("/w/{:07}", (i * 7919) % keys);
                if w % 2 == 0 {
                    c.put(k.as_bytes(), &value).await?;
                } else {
                    c.get(k.as_bytes()).await?;
                }
                ops.fetch_add(1, Ordering::Relaxed);
                i += writers;
            }
            anyhow::Ok(())
        }));
    }
    for _ in 0..4 {
        let mut c = c.clone();
        tasks.push(tokio::spawn(async move {
            let (_tx, mut s) = c.watch_prefix(b"/w/", 0).await?;
            while Instant::now() < stop {
                if tokio::time::timeout(Duration::from_secs(1), s.message()).await.is_ok_and(|m| m.is_err()) {
                    anyhow::bail!("a watch stream failed during the hold");
                }
            }
            anyhow::Ok(())
        }));
    }
    {
        let mut c = c.clone();
        tasks.push(tokio::spawn(async move {
            let mut i = 0u64;
            while Instant::now() < stop {
                let id = c.grant(30).await?;
                c.put_lease(format!("/lease/{i}").as_bytes(), b"l", id).await?;
                c.revoke(id).await?;
                i += 1;
            }
            anyhow::Ok(())
        }));
    }
    for t in tasks {
        t.await??;
    }

    // Drain: everything the wave made goes, and the space with it.
    let mut c = c;
    c.delete_prefix(b"/w/").await?;
    c.delete_prefix(b"/lease/").await?;
    let rev = c.status().await?.header.map_or(0, |h| h.revision);
    c.compact(rev).await?;
    c.defragment().await?;
    let leases = c
        .lease
        .lease_leases(fastetcd_proto::etcdserverpb::LeaseLeasesRequest {})
        .await?
        .into_inner()
        .leases
        .len();
    let left = c.count(b"/").await?;
    if left != 0 {
        anyhow::bail!("{left} keys left after the drain");
    }
    Ok(Wave {
        factor: 0.0,
        ramp_ops: keys as f64 / ramp.as_secs_f64().max(0.001),
        p99_ms: p99,
        hold_ops: ops.load(Ordering::Relaxed),
        rss: 0,
        fds: 0,
        db: 0,
        leases_left: leases,
    })
}
