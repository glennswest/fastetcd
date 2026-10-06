//! Minimal etcdctl-compatible client. Not a full etcdctl
//! replacement — just enough surface to drive end-to-end smoke
//! tests against fastetcd without needing the Go toolchain.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tokio_stream::StreamExt;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::auth_client::AuthClient;
use fastetcd_proto::etcdserverpb::cluster_client::ClusterClient;
use fastetcd_proto::etcdserverpb::maintenance_client::MaintenanceClient;
use fastetcd_proto::fastetcd_admin as apb;
use fastetcd_proto::fastetcd_admin::fastetcd_admin_client::FastetcdAdminClient;
use tonic::metadata::MetadataValue;

#[derive(Debug, Parser)]
#[command(name = "fastetcd-ctl", version, about)]
struct Args {
    /// Server endpoint, e.g. `http://127.0.0.1:2379`.
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    endpoint: String,

    /// `name:password` to authenticate as, when auth is on. Used by the
    /// `auth` commands, which need root.
    #[arg(long)]
    user: Option<String>,

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
    /// Stream `Maintenance.Snapshot` to a local file: a backup of the
    /// whole member that `fastetcd restore` restores (fastetcd#61).
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
    /// Replicated auth (fastetcd#32): inspect and converge members.
    Auth {
        #[command(subcommand)]
        cmd: AuthCmd,
    },
}

#[derive(Debug, Subcommand)]
enum AuthCmd {
    /// Show every member's auth state: version, users, roles, whether
    /// auth is on, and the digest compared before replicating.
    Members,
    /// Replicate one member's auth state to every member, replacing
    /// theirs. For members that held different auth state before
    /// replication. `member` is a member name or hex ID, as `etcdctl
    /// member list` prints them.
    Adopt { member: String },
}

/// Authenticate as `name:password`, returning the token.
async fn login(endpoint: &str, user: &Option<String>) -> anyhow::Result<Option<String>> {
    let Some(user) = user else { return Ok(None) };
    let (name, password) = user
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("--user must be name:password"))?;
    let mut c = AuthClient::connect(endpoint.to_string()).await?;
    let r = c
        .authenticate(pb::AuthenticateRequest {
            name: name.to_string(),
            password: password.to_string(),
        })
        .await?
        .into_inner();
    Ok(Some(r.token))
}

/// Attaches the auth token, if any, to every request.
#[derive(Clone)]
struct WithToken(Option<MetadataValue<tonic::metadata::Ascii>>);

impl tonic::service::Interceptor for WithToken {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(t) = &self.0 {
            req.metadata_mut().insert("token", t.clone());
        }
        Ok(req)
    }
}

fn print_member(m: &apb::AuthMember) {
    if !m.error.is_empty() {
        println!("member {:x}: {}", m.member_id, m.error);
        return;
    }
    println!(
        "member {:x}: version {}, auth {}, digest {}",
        m.member_id,
        m.version,
        if m.enabled { "on" } else { "off" },
        &m.digest[..16.min(m.digest.len())]
    );
    println!("  users: {}", m.users.join(", "));
    println!("  roles: {}", m.roles.join(", "));
}

async fn auth_cmd(endpoint: String, user: Option<String>, cmd: AuthCmd) -> anyhow::Result<()> {
    let token = login(&endpoint, &user)
        .await?
        .map(|t| t.parse())
        .transpose()
        .map_err(|e| anyhow::anyhow!("token: {e}"))?;
    let channel = tonic::transport::Endpoint::from_shared(endpoint)?.connect().await?;
    let auth = WithToken(token);
    let mut admin = FastetcdAdminClient::with_interceptor(channel.clone(), auth.clone());
    match cmd {
        AuthCmd::Members => {
            let r = admin.auth_members(apb::AuthMembersRequest {}).await?.into_inner();
            for m in &r.members {
                print_member(m);
            }
            if r.identical {
                println!("all members hold the same auth state");
            } else {
                println!(
                    "members differ: auth changes are refused until one is adopted \
                     (fastetcd-ctl auth adopt <member>)"
                );
            }
        }
        AuthCmd::Adopt { member } => {
            let mut cluster = ClusterClient::with_interceptor(channel, auth);
            let list = cluster
                .member_list(pb::MemberListRequest { linearizable: false })
                .await?
                .into_inner();
            let id = list
                .members
                .iter()
                .find(|m| m.name == member)
                .map(|m| m.id)
                .or_else(|| u64::from_str_radix(member.trim_start_matches("0x"), 16).ok())
                .ok_or_else(|| anyhow::anyhow!("no member named {member}, and not a hex ID"))?;
            let r = admin
                .auth_adopt(apb::AuthAdoptRequest { member_id: id })
                .await?
                .into_inner();
            println!("adopted on every member:");
            if let Some(m) = r.adopted {
                print_member(&m);
            }
        }
    }
    Ok(())
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
            // Written beside the destination and renamed into place once
            // whole and checked, as etcdctl does.
            let mut part = path.clone().into_os_string();
            part.push(".part");
            let part = PathBuf::from(part);
            let mut out = tokio::fs::File::create(&part).await?;
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
            out.sync_all().await?;
            drop(out);
            if let Err(e) = check_backup(&part) {
                let _ = std::fs::remove_file(&part);
                anyhow::bail!("the snapshot received is not usable: {e}");
            }
            std::fs::rename(&part, &path)?;
            println!(
                "wrote {} bytes to {} (checksum ok); restore it with `fastetcd restore {}` \
                 on a stopped member",
                total,
                path.display(),
                path.display()
            );
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
                "recoverable:  up to {}  (`fastetcd-ctl defrag`; an upper bound)",
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
        Cmd::Auth { cmd } => auth_cmd(args.endpoint, args.user, cmd).await?,
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

/// Check a `Maintenance.Snapshot` file: fastetcd's backup format (magic
/// `FEBACKUP`, ..., SHA-256 of everything before it), fastetcd#61. An
/// upstream etcd's snapshot is a BoltDB file and is not checked here.
fn check_backup(path: &std::path::Path) -> anyhow::Result<()> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let len = f.metadata()?.len();
    let mut magic = [0u8; 8];
    if len < 44 || f.read_exact(&mut magic).is_err() || &magic != b"FEBACKUP" {
        anyhow::bail!("not a fastetcd backup file ({len} bytes)");
    }
    let mut hash = Sha256::new();
    hash.update(magic);
    let mut body = (&f).take(len - 32 - 8);
    std::io::copy(&mut body, &mut hash)?;
    let mut stored = [0u8; 32];
    f.read_exact(&mut stored)?;
    if hash.finalize().as_slice() != stored {
        anyhow::bail!("checksum mismatch");
    }
    Ok(())
}
