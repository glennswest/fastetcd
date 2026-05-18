use clap::Parser;

/// Minimal etcdctl-compatible smoke client.
///
/// Not a replacement for upstream etcdctl — just enough surface to
/// integration-test fastetcd end-to-end without depending on the Go toolchain.
#[derive(Debug, Parser)]
#[command(name = "fastetcd-ctl", version, about)]
struct Args {
    /// Server endpoint(s) — comma-separated.
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    endpoints: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let _args = Args::parse();
    anyhow::bail!("fastetcd-ctl is a skeleton — implementation lands with KV service in task #5");
}
