//! Minimal concurrent load generator for fastetcd — throughput and
//! latency for put / linearizable-get / serializable-get, and the read
//! latency under write load of fastetcd#71 (`read-under-load`), and a
//! crash test (`durability-write` / `durability-check`, fastetcd#90), and
//! lease keep-alive throughput (`keepalive`, fastetcd#92).
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
    /// durability-check | keepalive
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

/// Failed RPC attempts by gRPC code, and operations that never succeeded
/// (fastetcd#104): a leader change makes forwarded writes `Unavailable`
/// for a moment, which used to panic the whole bench.
#[derive(Default)]
struct Errors {
    by_code: std::sync::Mutex<std::collections::BTreeMap<String, u64>>,
    failed: std::sync::atomic::AtomicU64,
}

impl Errors {
    fn add(&self, what: &str) {
        *self.by_code.lock().unwrap().entry(what.to_string()).or_default() += 1;
    }
    /// `  errors: ...` for the summary, or nothing when there were none.
    fn line(&self) -> Option<String> {
        let by = self.by_code.lock().unwrap();
        if by.is_empty() {
            return None;
        }
        let codes: Vec<String> = by.iter().map(|(c, n)| format!("{c} {n}")).collect();
        Some(format!(
            "  errors: {} attempts failed ({}); {} operations gave up",
            by.values().sum::<u64>(),
            codes.join(", "),
            self.failed.load(std::sync::atomic::Ordering::Relaxed)
        ))
    }
}

/// Worth trying again: the cluster is between leaders, or the connection
/// broke (a member restarting).
fn transient(s: &tonic::Status) -> bool {
    use tonic::Code;
    matches!(s.code(), Code::Unavailable | Code::DeadlineExceeded | Code::Aborted)
        || (s.code() == Code::Unknown && s.message().contains("transport error"))
}

const RETRIES: u32 = 8;

/// Run `f` until it succeeds, retrying transient errors with a growing
/// backoff (50 ms, 100 ms, ... up to `RETRIES` tries), each failed try
/// counted by its code. `None` once it gives up, counted as failed.
async fn retry<T, F, Fut>(errors: &Errors, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, tonic::Status>>,
{
    for attempt in 1..=RETRIES {
        match f().await {
            Ok(v) => return Some(v),
            Err(s) => {
                errors.add(&format!("{:?}", s.code()));
                if !transient(&s) || attempt == RETRIES {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50 * attempt as u64)).await;
            }
        }
    }
    errors.failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    None
}

/// A KV client, retrying the connect as [`retry`] does.
async fn connect_kv(endpoint: &str, errors: &Errors) -> Option<KvClient<tonic::transport::Channel>> {
    for attempt in 1..=RETRIES {
        match KvClient::connect(endpoint.to_string()).await {
            Ok(c) => return Some(c),
            Err(_) => {
                errors.add("connect");
                tokio::time::sleep(std::time::Duration::from_millis(100 * attempt as u64)).await;
            }
        }
    }
    errors.failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    None
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
    let errors: Arc<Errors> = Arc::default();
    let mut handles = Vec::new();
    for w in 0..args.conns {
        let (endpoint, value, stop, writes, errors) =
            (args.endpoint.clone(), value.clone(), stop.clone(), writes.clone(), errors.clone());
        handles.push(tokio::spawn(async move {
            let Some(mut c) = connect_kv(&endpoint, &errors).await else { return };
            let key = format!("/load/{w}").into_bytes();
            let mut local = Vec::new();
            let mut i = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                i += 1;
                let get = pb::RangeRequest { key: key.clone(), ..Default::default() };
                let Some(got) = retry(&errors, || {
                    let mut c = c.clone();
                    let get = get.clone();
                    async move { c.range(get).await }
                })
                .await
                else {
                    continue;
                };
                let rev = got.into_inner().kvs.first().map_or(0, |kv| kv.mod_revision);
                let (t, at) = (Instant::now(), std::time::SystemTime::now());
                let txn = pb::TxnRequest {
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
                };
                let wrote = retry(&errors, || {
                    let mut c = c.clone();
                    let txn = txn.clone();
                    async move { c.txn(txn).await }
                })
                .await;
                if wrote.is_some() {
                    local.push((at, t.elapsed().as_micros() as u64));
                }
                if i % 5 == 0 {
                    let list = pb::RangeRequest {
                        key: b"/load/".to_vec(),
                        range_end: b"/load0".to_vec(),
                        ..Default::default()
                    };
                    retry(&errors, || {
                        let mut c = c.clone();
                        let list = list.clone();
                        async move { c.range(list).await }
                    })
                    .await;
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
    if let Some(l) = errors.line() {
        println!("{l}");
    }
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
/// `conns` clients, each with its own lease (TTL 60 s) and keep-alive
/// stream, sending a keep-alive and awaiting its answer, `total` in all:
/// etcd's `benchmark lease-keepalive` (fastetcd#92).
async fn keepalive(args: &Args) -> anyhow::Result<()> {
    use fastetcd_proto::etcdserverpb::lease_client::LeaseClient;
    let per = args.total / args.conns;
    let mut handles = Vec::new();
    let start = Instant::now();
    let errors: Arc<Errors> = Arc::default();
    for _ in 0..args.conns {
        let (endpoint, errors) = (args.endpoint.clone(), errors.clone());
        handles.push(tokio::spawn(async move {
            let mut local = Vec::with_capacity(per);
            let mut tries = 0;
            let lc = loop {
                match LeaseClient::connect(endpoint.clone()).await {
                    Ok(c) => break c,
                    Err(e) => {
                        errors.add("connect");
                        tries += 1;
                        if tries >= RETRIES {
                            return Err(anyhow::anyhow!("connect: {e}"));
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                }
            };
            let Some(granted) = retry(&errors, || {
                let mut lc = lc.clone();
                async move { lc.lease_grant(pb::LeaseGrantRequest { ttl: 60, id: 0 }).await }
            })
            .await
            else {
                return anyhow::Ok(local);
            };
            let id = granted.into_inner().id;
            // A broken stream (a member restarting) is opened again.
            let mut opens = 0;
            while local.len() < per && opens < RETRIES {
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                let mut lc2 = lc.clone();
                let mut answers = match lc2.lease_keep_alive(tokio_stream::wrappers::ReceiverStream::new(rx)).await {
                    Ok(a) => a.into_inner(),
                    Err(s) => {
                        errors.add(&format!("{:?}", s.code()));
                        opens += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(100 * opens as u64)).await;
                        continue;
                    }
                };
                while local.len() < per {
                    let t = Instant::now();
                    if tx.send(pb::LeaseKeepAliveRequest { id }).await.is_err() {
                        break;
                    }
                    match tokio_stream::StreamExt::next(&mut answers).await {
                        Some(Ok(a)) if a.ttl > 0 => local.push(t.elapsed().as_micros() as u64),
                        Some(Ok(a)) => anyhow::bail!("lease {id} answered TTL {}", a.ttl),
                        Some(Err(s)) => {
                            errors.add(&format!("{:?}", s.code()));
                            opens += 1;
                            break;
                        }
                        None => {
                            errors.add("stream ended");
                            opens += 1;
                            break;
                        }
                    }
                }
            }
            anyhow::Ok(local)
        }));
    }
    let mut lat = Vec::with_capacity(args.total);
    for h in handles {
        lat.extend(h.await??);
    }
    let elapsed = start.elapsed();
    println!("mode=keepalive conns={} ops={}", args.conns, lat.len());
    println!("  throughput: {:.0} ops/sec", lat.len() as f64 / elapsed.as_secs_f64());
    println!("  latency: {}", summary(lat));
    if let Some(e) = errors.line() {
        println!("{e}");
    }
    Ok(())
}

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
        "keepalive" => return keepalive(&args).await,
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

    if !matches!(args.mode.as_str(), "put" | "get-lin" | "get-ser") {
        anyhow::bail!("unknown mode {}", args.mode);
    }
    let errors: Arc<Errors> = Arc::default();
    let mut handles = Vec::new();
    for w in 0..args.conns {
        let (args, value, lat, errors) = (args.clone(), value.clone(), lat.clone(), errors.clone());
        handles.push(tokio::spawn(async move {
            let Some(c) = connect_kv(&args.endpoint, &errors).await else { return };
            let mut local = Vec::with_capacity(per);
            for i in 0..per {
                let key = format!("/bench/{}", (w * per + i) % args.keys).into_bytes();
                let t = Instant::now();
                let done = match args.mode.as_str() {
                    "put" => {
                        let req = pb::PutRequest { key, value: value.clone(), ..Default::default() };
                        retry(&errors, || {
                            let (mut c, req) = (c.clone(), req.clone());
                            async move { c.put(req).await.map(|_| ()) }
                        })
                        .await
                    }
                    mode => {
                        let req = pb::RangeRequest { key, serializable: mode == "get-ser", ..Default::default() };
                        retry(&errors, || {
                            let (mut c, req) = (c.clone(), req.clone());
                            async move { c.range(req).await.map(|_| ()) }
                        })
                        .await
                    }
                };
                if done.is_some() {
                    local.push(t.elapsed().as_micros() as u64);
                }
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
    if n == 0 {
        anyhow::bail!("no operation succeeded{}", errors.line().map(|e| format!(":\n{e}")).unwrap_or_default());
    }
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
    if let Some(e) = errors.line() {
        println!("{e}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retry_counts_transient_errors_and_goes_on() {
        let errors = Errors::default();
        let mut n = 0;
        let got = retry(&errors, || {
            n += 1;
            let k = n;
            async move {
                if k <= 2 {
                    Err(tonic::Status::unavailable("forwarded write to leader 3: has to forward"))
                } else {
                    Ok(k)
                }
            }
        })
        .await;
        assert_eq!(got, Some(3));
        assert_eq!(errors.line().unwrap(), "  errors: 2 attempts failed (Unavailable 2); 0 operations gave up");
    }

    #[tokio::test]
    async fn retry_gives_up_on_a_lasting_or_final_error() {
        let errors = Errors::default();
        let got: Option<()> = retry(&errors, || async { Err(tonic::Status::permission_denied("no")) }).await;
        assert_eq!(got, None, "not transient: no retry");
        let got: Option<()> = retry(&errors, || async { Err(tonic::Status::unavailable("down")) }).await;
        assert_eq!(got, None);
        let line = errors.line().unwrap();
        assert_eq!(line, format!("  errors: {} attempts failed (PermissionDenied 1, Unavailable {RETRIES}); 2 operations gave up", RETRIES + 1));
    }
}
