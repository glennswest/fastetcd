//! Minimal concurrent load generator for fastetcd — throughput and
//! latency for put / linearizable-get / serializable-get, and the read
//! latency under write load of fastetcd#71 (`read-under-load`). Not a
//! full benchmark suite; enough to characterize a cluster.

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
    /// put | get-lin | get-ser | read-under-load
    #[arg(long, default_value = "put")]
    mode: String,
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
    let writes: Arc<Mutex<Vec<u64>>> = Arc::default();
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
                let t = Instant::now();
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
                local.push(t.elapsed().as_micros() as u64);
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
    let writes = Arc::try_unwrap(writes).unwrap().into_inner();
    println!("mode=read-under-load conns={} val={}B", args.conns, args.val_bytes);
    println!("  writes: {:.0}/s, {}", writes.len() as f64 / window, summary(writes));
    println!("  linearizable range: {}", summary(lin));
    println!("  serializable range: {}", summary(ser));
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arc::new(Args::parse());
    if args.mode == "read-under-load" {
        return read_under_load(&args).await;
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
                local.push(t.elapsed().as_micros() as u64);
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
