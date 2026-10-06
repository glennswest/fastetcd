//! Minimal concurrent load generator for fastetcd — throughput and
//! latency for put / linearizable-get / serializable-get, and the read
//! latency under write load of fastetcd#71 (`read-under-load`), and a
//! crash test (`durability-write` / `durability-check`, fastetcd#90).
//! Not a full benchmark suite; enough to characterize a cluster. Speaks
//! the plain etcd v3 API, so it runs against upstream etcd as well.

use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use tokio::sync::Mutex;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    endpoint: String,
    /// put | get-lin | get-ser | read-under-load | durability-write |
    /// durability-check
    #[arg(long, default_value = "put")]
    mode: String,
    /// durability-check: the `acked <client> <count>` lines
    /// durability-write printed.
    #[arg(long)]
    acked_file: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = 64)]
    conns: usize,
    #[arg(long, default_value_t = 50_000)]
    total: usize,
    #[arg(long, default_value_t = 256)]
    val_bytes: usize,
    /// Number of distinct keys to spread over.
    #[arg(long, default_value_t = 10_000)]
    keys: usize,
    /// read-under-load: how long to run the load.
    #[arg(long, default_value_t = 20)]
    duration_secs: u64,
    /// read-under-load: sequential probe reads of each kind.
    #[arg(long, default_value_t = 200)]
    probes: usize,
}

/// Percentiles of a latency sample in microseconds, as a line in ms.
fn summary(mut l: Vec<u64>) -> String {
    if l.is_empty() {
        return "no samples".into();
    }
    l.sort_unstable();
    let n = l.len();
    let pct = |p: f64| l[((n as f64 * p) as usize).min(n - 1)] as f64 / 1000.0;
    let mean = l.iter().sum::<u64>() as f64 / n as f64 / 1000.0;
    format!(
        "n={n} mean={mean:.2} p50={:.2} p99={:.2} max={:.2} ms",
        pct(0.50),
        pct(0.99),
        l[n - 1] as f64 / 1000.0
    )
}

/// fastetcd#71's load, as rustkube's get-latency.sh makes it: `conns`
/// clients, each looping GET + compare-and-swap Put (a Txn on
/// mod_revision) of its own key, with a prefix Range every fifth loop.
/// Meanwhile one probe makes `probes` sequential linearizable Ranges of
/// one key, then as many serializable ones.
async fn read_under_load(args: &Args) -> anyhow::Result<()> {
    use pb::compare::{CompareResult, CompareTarget, TargetUnion};
    use pb::request_op::Request;

    let value = vec![b'x'; args.val_bytes];
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // (when the write started, its latency in µs), for the stall list.
    let writes: Arc<Mutex<Vec<(std::time::SystemTime, u64)>>> = Arc::default();
    let mut handles = Vec::new();
    for w in 0..args.conns {
        let (endpoint, value, stop, writes) =
            (args.endpoint.clone(), value.clone(), stop.clone(), writes.clone());
        handles.push(tokio::spawn(async move {
            let mut c = KvClient::connect(endpoint).await.unwrap();
            let key = format!("/load/{w}").into_bytes();
            let mut local = Vec::new();
            let mut i = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                i += 1;
                let got = c
                    .range(pb::RangeRequest { key: key.clone(), ..Default::default() })
                    .await
                    .unwrap()
                    .into_inner();
                let rev = got.kvs.first().map_or(0, |kv| kv.mod_revision);
                let (t, at) = (Instant::now(), std::time::SystemTime::now());
                c.txn(pb::TxnRequest {
                    compare: vec![pb::Compare {
                        result: CompareResult::Equal as i32,
                        target: CompareTarget::Mod as i32,
                        key: key.clone(),
                        target_union: Some(TargetUnion::ModRevision(rev)),
                        ..Default::default()
                    }],
                    success: vec![pb::RequestOp {
                        request: Some(Request::RequestPut(pb::PutRequest {
                            key: key.clone(),
                            value: value.clone(),
                            ..Default::default()
                        })),
                    }],
                    failure: vec![],
                })
                .await
                .unwrap();
                local.push((at, t.elapsed().as_micros() as u64));
                if i % 5 == 0 {
                    c.range(pb::RangeRequest {
                        key: b"/load/".to_vec(),
                        range_end: b"/load0".to_vec(),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                }
            }
            writes.lock().await.extend(local);
        }));
    }

    // Let the load settle, then probe.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let mut probe = KvClient::connect(args.endpoint.clone()).await?;
    probe
        .put(pb::PutRequest { key: b"/probe".to_vec(), value: value.clone(), ..Default::default() })
        .await?;
    let started = Instant::now();
    let mut lin = Vec::new();
    let mut ser = Vec::new();
    for serializable in [false, true] {
        for _ in 0..args.probes {
            let t = Instant::now();
            probe
                .range(pb::RangeRequest { key: b"/probe".to_vec(), serializable, ..Default::default() })
                .await?;
            let us = t.elapsed().as_micros() as u64;
            if serializable { ser.push(us) } else { lin.push(us) }
        }
    }
    let remaining = std::time::Duration::from_secs(args.duration_secs).saturating_sub(started.elapsed());
    tokio::time::sleep(remaining).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.await?;
    }
    let window = started.elapsed().as_secs_f64() + 2.0;
    let mut writes = Arc::try_unwrap(writes).unwrap().into_inner();
    println!("mode=read-under-load conns={} val={}B", args.conns, args.val_bytes);
    println!(
        "  writes: {:.0}/s, {}",
        writes.len() as f64 / window,
        summary(writes.iter().map(|w| w.1).collect())
    );
    // When the slowest writes started (UTC, as the members' logs print
    // it), so a stall can be matched to an election or a slow fsync in
    // the logs (fastetcd#83).
    writes.sort_unstable_by_key(|w| std::cmp::Reverse(w.1));
    let slow: Vec<String> = writes
        .iter()
        .take(5)
        .filter(|w| w.1 >= 200_000)
        .map(|w| format!("{} {:.2} s", utc_time(w.0), w.1 as f64 / 1e6))
        .collect();
    if !slow.is_empty() {
        let over = |ms: u64| writes.iter().filter(|w| w.1 >= ms * 1000).count();
        println!(
            "  slow writes: {} >= 200 ms, {} >= 1 s; slowest started at {}",
            over(200),
            over(1000),
            slow.join(", ")
        );
    }
    println!("  linearizable range: {}", summary(lin));
    println!("  serializable range: {}", summary(ser));
    Ok(())
}

/// `HH:MM:SS.mmm` UTC of `t`.
fn utc_time(t: std::time::SystemTime) -> String {
    let ms = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    let s = ms / 1000 % 86_400;
    format!("{:02}:{:02}:{:02}.{:03}", s / 3600, s / 60 % 60, s % 60, ms % 1000)
}

fn dur_key(client: usize, i: u64) -> Vec<u8> {
    format!("/dur/{client:04}/{i:010}").into_bytes()
}

/// `conns` clients each put `/dur/<client>/<i>` for i = 0, 1, ... until a
/// put fails (the server was killed) or `duration_secs` pass. Prints, per
/// client, how many puts were acknowledged: `acked <client> <count>`. A
/// put counts only once its response arrived.
async fn durability_write(args: &Args) -> anyhow::Result<()> {
    let value = vec![b'x'; args.val_bytes];
    let deadline = Instant::now() + std::time::Duration::from_secs(args.duration_secs);
    let mut handles = Vec::new();
    for w in 0..args.conns {
        let (endpoint, value) = (args.endpoint.clone(), value.clone());
        handles.push(tokio::spawn(async move {
            let Ok(mut c) = KvClient::connect(endpoint).await else { return 0u64 };
            let mut acked = 0u64;
            while Instant::now() < deadline {
                let put = pb::PutRequest { key: dur_key(w, acked), value: value.clone(), ..Default::default() };
                match tokio::time::timeout(std::time::Duration::from_secs(10), c.put(put)).await {
                    Ok(Ok(_)) => acked += 1,
                    _ => break,
                }
            }
            acked
        }));
    }
    let mut total = 0;
    for (w, h) in handles.into_iter().enumerate() {
        let n = h.await?;
        total += n;
        println!("acked {w} {n}");
    }
    println!("total_acked {total}");
    Ok(())
}

/// After a restart: time until a linearizable read succeeds
/// (`recovery_ms`), then, for every client in `--acked-file`, how many of
/// its acknowledged keys are missing (`lost`).
async fn durability_check(args: &Args) -> anyhow::Result<()> {
    let path = args.acked_file.as_ref().ok_or_else(|| anyhow::anyhow!("--acked-file is required"))?;
    let mut acked: Vec<(usize, u64)> = Vec::new();
    for line in std::fs::read_to_string(path)?.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() == 3 && f[0] == "acked" {
            acked.push((f[1].parse()?, f[2].parse()?));
        }
    }
    let started = Instant::now();
    let mut c = loop {
        if started.elapsed() > std::time::Duration::from_secs(300) {
            anyhow::bail!("no linearizable read within 300 s of the restart");
        }
        if let Ok(mut c) = KvClient::connect(args.endpoint.clone()).await {
            let probe = pb::RangeRequest { key: b"/dur/".to_vec(), count_only: true, ..Default::default() };
            if c.range(probe).await.is_ok() {
                break c;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    let recovery_ms = started.elapsed().as_millis();
    let (mut lost, mut total) = (0u64, 0u64);
    for (w, n) in acked {
        if n == 0 {
            continue;
        }
        let got = c
            .range(pb::RangeRequest {
                key: dur_key(w, 0),
                range_end: dur_key(w, n),
                count_only: true,
                ..Default::default()
            })
            .await?
            .into_inner()
            .count as u64;
        total += n;
        lost += n.saturating_sub(got);
    }
    println!("recovery_ms {recovery_ms}");
    println!("acked {total}");
    println!("lost {lost}");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arc::new(Args::parse());
    match args.mode.as_str() {
        "read-under-load" => return read_under_load(&args).await,
        "durability-write" => return durability_write(&args).await,
        "durability-check" => return durability_check(&args).await,
        _ => {}
    }
    let value = vec![b'x'; args.val_bytes];

    // Pre-seed keys for read modes so gets hit existing data.
    if args.mode.starts_with("get") {
        let mut c = KvClient::connect(args.endpoint.clone()).await?;
        for i in 0..args.keys {
            c.put(pb::PutRequest {
                key: format!("/bench/{i}").into_bytes(),
                value: value.clone(),
                ..Default::default()
            })
            .await?;
        }
    }

    let per = args.total / args.conns;
    let lat: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::with_capacity(args.total)));
    let start = Instant::now();

    let mut handles = Vec::new();
    for w in 0..args.conns {
        let (args, value, lat) = (args.clone(), value.clone(), lat.clone());
        handles.push(tokio::spawn(async move {
            let mut c = KvClient::connect(args.endpoint.clone()).await.unwrap();
            let mut local = Vec::with_capacity(per);
            for i in 0..per {
                let key = format!("/bench/{}", (w * per + i) % args.keys).into_bytes();
                let t = Instant::now();
                match args.mode.as_str() {
                    "put" => {
                        c.put(pb::PutRequest { key, value: value.clone(), ..Default::default() })
                            .await
                            .unwrap();
                    }
                    "get-lin" => {
                        c.range(pb::RangeRequest { key, ..Default::default() }).await.unwrap();
                    }
                    "get-ser" => {
                        c.range(pb::RangeRequest { key, serializable: true, ..Default::default() })
                            .await
                            .unwrap();
                    }
                    other => panic!("unknown mode {other}"),
                }
                local.push((at, t.elapsed().as_micros() as u64));
            }
            lat.lock().await.extend(local);
        }));
    }
    for h in handles {
        h.await?;
    }
    let elapsed = start.elapsed();

    let mut l = Arc::try_unwrap(lat).unwrap().into_inner();
    l.sort_unstable();
    let n = l.len();
    let pct = |p: f64| l[((n as f64 * p) as usize).min(n - 1)] as f64 / 1000.0;
    println!(
        "mode={} conns={} ops={} val={}B",
        args.mode, args.conns, n, args.val_bytes
    );
    println!("  throughput: {:.0} ops/sec", n as f64 / elapsed.as_secs_f64());
    println!(
        "  latency ms: p50={:.2} p90={:.2} p99={:.2} max={:.2}",
        pct(0.50),
        pct(0.90),
        pct(0.99),
        l[n - 1] as f64 / 1000.0
    );
    Ok(())
}
