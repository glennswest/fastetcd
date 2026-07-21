//! Minimal concurrent load generator for fastetcd — throughput and
//! latency for put / linearizable-get / serializable-get. Not a full
//! benchmark suite; enough to characterize a cluster.

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
    /// put | get-lin | get-ser
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arc::new(Args::parse());
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
