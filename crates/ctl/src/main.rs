//! Minimal etcdctl-compatible client. Not a full etcdctl
//! replacement — just enough surface to drive end-to-end smoke
//! tests against fastetcd without needing the Go toolchain.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tokio_stream::StreamExt;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::maintenance_client::MaintenanceClient;

#[derive(Debug, Parser)]
#[command(name = "fastetcd-ctl", version, about)]
struct Args {
    /// Server endpoint, e.g. `http://127.0.0.1:2379`.
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    endpoint: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Put a key.
    Put { key: String, value: String },
    /// Get a key (optionally with `--prefix`).
    Get {
        key: String,
        /// Treat `key` as a prefix.
        #[arg(long, default_value_t = false)]
        prefix: bool,
    },
    /// Delete a key (optionally with `--prefix`).
    Del {
        key: String,
        #[arg(long, default_value_t = false)]
        prefix: bool,
    },
    /// Stream `Maintenance.Snapshot` to a local file.
    SnapshotSave {
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    match args.cmd {
        Cmd::Put { key, value } => {
            let mut c = KvClient::connect(args.endpoint).await?;
            let resp = c
                .put(pb::PutRequest {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                    ..Default::default()
                })
                .await?
                .into_inner();
            println!("OK rev={}", resp.header.map(|h| h.revision).unwrap_or(0));
        }
        Cmd::Get { key, prefix } => {
            let mut c = KvClient::connect(args.endpoint).await?;
            let mut range_end = Vec::new();
            if prefix {
                range_end = prefix_range_end(key.as_bytes());
            }
            let resp = c
                .range(pb::RangeRequest {
                    key: key.into_bytes(),
                    range_end,
                    ..Default::default()
                })
                .await?
                .into_inner();
            for kv in resp.kvs {
                println!("{}", String::from_utf8_lossy(&kv.key));
                println!("{}", String::from_utf8_lossy(&kv.value));
            }
        }
        Cmd::Del { key, prefix } => {
            let mut c = KvClient::connect(args.endpoint).await?;
            let mut range_end = Vec::new();
            if prefix {
                range_end = prefix_range_end(key.as_bytes());
            }
            let resp = c
                .delete_range(pb::DeleteRangeRequest {
                    key: key.into_bytes(),
                    range_end,
                    ..Default::default()
                })
                .await?
                .into_inner();
            println!("deleted {}", resp.deleted);
        }
        Cmd::SnapshotSave { path } => {
            let mut c = MaintenanceClient::connect(args.endpoint).await?;
            let mut stream = c
                .snapshot(pb::SnapshotRequest {})
                .await?
                .into_inner();
            let mut out = tokio::fs::File::create(&path).await?;
            let mut total: usize = 0;
            while let Some(msg) = stream.next().await {
                let chunk = msg?;
                if chunk.blob.is_empty() {
                    continue;
                }
                tokio::io::AsyncWriteExt::write_all(&mut out, &chunk.blob).await?;
                total += chunk.blob.len();
            }
            tokio::io::AsyncWriteExt::flush(&mut out).await?;
            println!("wrote {} bytes to {}", total, path.display());
        }
    }
    Ok(())
}

/// Build the etcd-style range_end that selects every key with
/// `prefix` as its leading bytes: increment the last byte, or
/// fall back to `[0]` if the prefix is all 0xff.
fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            end[i] += 1;
            return end[..=i].to_vec();
        }
    }
    vec![0u8]
}
