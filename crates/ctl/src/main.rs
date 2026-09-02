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
    /// Report member status, including disk occupancy and any alarms.
    Status,
    /// Rewrite the backend so space freed by deletes, compaction and
    /// log purge goes back to the filesystem. Not gated on the NOSPACE
    /// alarm — it is meant to work on a store that is already refusing
    /// writes.
    Defrag,
    /// Discard MVCC history below `revision`. Bounds the store's growth
    /// and, unlike a put, is still accepted under a NOSPACE alarm.
    Compact {
        revision: i64,
    },
    /// List raised alarms, or clear them with `--disarm`.
    Alarm {
        /// Clear the alarms instead of listing them.
        #[arg(long, default_value_t = false)]
        disarm: bool,
    },
}

/// Render a byte count the way an operator reads it.
fn human_bytes(n: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
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
        Cmd::Status => {
            let mut c = MaintenanceClient::connect(args.endpoint).await?;
            let r = c.status(pb::StatusRequest {}).await?.into_inner();
            println!("version:      {}", r.version);
            println!("member:       {:x}  leader: {:x}", r.header.map(|h| h.member_id).unwrap_or(0), r.leader);
            println!("raft:         term {} index {} applied {}", r.raft_term, r.raft_index, r.raft_applied_index);
            println!("db size:      {}", human_bytes(r.db_size));
            println!("db in use:    {}", human_bytes(r.db_size_in_use));
            println!(
                "reclaimable:  {}  (run `fastetcd-ctl defrag` to return it)",
                human_bytes(r.db_size - r.db_size_in_use)
            );
            if r.db_size_quota > 0 {
                let pct = (r.db_size as f64 / r.db_size_quota as f64) * 100.0;
                println!(
                    "capacity:     {} ({pct:.1}% used)",
                    human_bytes(r.db_size_quota)
                );
            }
            if r.errors.is_empty() {
                println!("alarms:       none");
            } else {
                println!("alarms:       {}", r.errors.join(", "));
            }
        }
        Cmd::Defrag => {
            let mut c = MaintenanceClient::connect(args.endpoint).await?;
            let before = c.status(pb::StatusRequest {}).await?.into_inner().db_size;
            c.defragment(pb::DefragmentRequest {}).await?;
            let after = c.status(pb::StatusRequest {}).await?.into_inner().db_size;
            println!(
                "defrag: {} -> {} ({} returned to the filesystem)",
                human_bytes(before),
                human_bytes(after),
                human_bytes((before - after).max(0))
            );
        }
        Cmd::Compact { revision } => {
            let mut c = KvClient::connect(args.endpoint).await?;
            c.compact(pb::CompactionRequest {
                revision,
                physical: false,
            })
            .await?;
            println!("compacted to revision {revision}");
        }
        Cmd::Alarm { disarm } => {
            let mut c = MaintenanceClient::connect(args.endpoint).await?;
            let action = if disarm {
                pb::alarm_request::AlarmAction::Deactivate
            } else {
                pb::alarm_request::AlarmAction::Get
            };
            let r = c
                .alarm(pb::AlarmRequest {
                    action: action as i32,
                    member_id: 0,
                    alarm: pb::AlarmType::None as i32,
                })
                .await?
                .into_inner();
            if r.alarms.is_empty() {
                println!("no alarms raised");
            } else {
                for a in r.alarms {
                    println!(
                        "memberID:{:x} alarm:{}",
                        a.member_id,
                        pb::AlarmType::try_from(a.alarm)
                            .map(|t| t.as_str_name().to_string())
                            .unwrap_or_else(|_| a.alarm.to_string())
                    );
                }
            }
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
