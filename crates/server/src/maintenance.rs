//! Implementation of the etcd `Maintenance` gRPC service.
//!
//! - `Status` — real values: cluster_id, member_id, raft_term, raft_index,
//!   leader (from openraft metrics), revision, version, dbSize.
//! - `Hash` / `HashKV` — SHA-256 over the `mvcc_kv` table (deterministic
//!   across nodes at the same revision; etcd uses CRC32 but client tools
//!   treat it as an opaque hash). At a revision, HashKV restricts to
//!   `mod_revision <= revision`.
//! - `Alarm` — empty list; fastetcd does not surface alarm-shaped state
//!   yet. `AlarmRequest` with `action != Get` is a no-op success.
//! - `Defragment` — no-op success today (`redb` has no online compact
//!   that's safe to invoke from here). Re-enable once we wire it.
//! - `Snapshot` — streams the bincode-serialized state machine snapshot
//!   to the client in 64 KiB chunks.
//! - `MoveLeader` — `Status::unimplemented` until peer transport lands.
//! - `Downgrade` — `Status::unimplemented`.

use std::pin::Pin;
use std::sync::Arc;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::maintenance_server::Maintenance;
use openraft::storage::RaftStateMachine;
use openraft::RaftSnapshotBuilder;
use sha2::{Digest, Sha256};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::state::{response_header, ServerState};

/// Server version string carried in `StatusResponse.version`. Reported
/// as the etcd version that fastetcd targets so clients that gate
/// behavior on the protocol version (e.g., etcdctl's downgrade check)
/// see a compatible value. The build-time fastetcd version goes into
/// `dbSizeInUse` reporting and metrics, not the wire response.
const ETCD_COMPAT_VERSION: &str = "3.6.0";

#[derive(Clone)]
pub struct MaintenanceService {
    state: Arc<ServerState>,
}

impl MaintenanceService {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Maintenance for MaintenanceService {
    async fn alarm(
        &self,
        _request: Request<pb::AlarmRequest>,
    ) -> Result<Response<pb::AlarmResponse>, Status> {
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        Ok(Response::new(pb::AlarmResponse {
            header: Some(header),
            alarms: Vec::new(),
        }))
    }

    async fn status(
        &self,
        _request: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let revision = self.state.sm.mvcc().current_revision().await;
        let metrics = self.state.raft.metrics().borrow().clone();

        let db_size = self
            .state
            .sm
            .mvcc()
            .engine()
            .size_on_disk()
            .await
            .map_err(|e| Status::internal(format!("size_on_disk: {e}")))?;

        let header = response_header(&self.state, revision).await;
        Ok(Response::new(pb::StatusResponse {
            header: Some(header),
            version: ETCD_COMPAT_VERSION.to_string(),
            db_size: db_size as i64,
            leader: metrics.current_leader.unwrap_or(0),
            raft_index: metrics.last_log_index.unwrap_or(0),
            raft_term: metrics.current_term,
            raft_applied_index: metrics.last_applied.map(|l| l.index).unwrap_or(0),
            errors: Vec::new(),
            db_size_in_use: db_size as i64,
            is_learner: false,
            storage_version: ETCD_COMPAT_VERSION.to_string(),
            db_size_quota: 0,
            downgrade_info: None,
        }))
    }

    async fn defragment(
        &self,
        _request: Request<pb::DefragmentRequest>,
    ) -> Result<Response<pb::DefragmentResponse>, Status> {
        self.state
            .sm
            .mvcc()
            .engine()
            .defragment()
            .await
            .map_err(|e| Status::internal(format!("defragment: {e}")))?;
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        Ok(Response::new(pb::DefragmentResponse {
            header: Some(header),
        }))
    }

    async fn hash(
        &self,
        _request: Request<pb::HashRequest>,
    ) -> Result<Response<pb::HashResponse>, Status> {
        let (revision, hash) = hash_kv_table(&self.state, 0)
            .await
            .map_err(|e| Status::internal(format!("hash: {e}")))?;
        let header = response_header(&self.state, revision).await;
        Ok(Response::new(pb::HashResponse {
            header: Some(header),
            hash,
        }))
    }

    async fn hash_kv(
        &self,
        request: Request<pb::HashKvRequest>,
    ) -> Result<Response<pb::HashKvResponse>, Status> {
        let req = request.into_inner();
        let (revision, hash) = hash_kv_table(&self.state, req.revision)
            .await
            .map_err(|e| Status::internal(format!("hash_kv: {e}")))?;
        let header = response_header(&self.state, revision).await;
        Ok(Response::new(pb::HashKvResponse {
            header: Some(header),
            hash,
            compact_revision: 0,
            hash_revision: revision,
        }))
    }

    type SnapshotStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::SnapshotResponse, Status>> + Send>>;

    async fn snapshot(
        &self,
        _request: Request<pb::SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        // Build a snapshot of the MVCC state.
        let mut sm = self.state.sm.clone();
        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder
            .build_snapshot()
            .await
            .map_err(|e| Status::internal(format!("snapshot build: {e}")))?;
        let data: Vec<u8> = snap.snapshot.into_inner();
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<pb::SnapshotResponse, Status>>(4);
        tokio::spawn(async move {
            const CHUNK: usize = 64 * 1024;
            let total = data.len();
            let mut sent = 0usize;
            while sent < total {
                let end = (sent + CHUNK).min(total);
                let blob = data[sent..end].to_vec();
                sent = end;
                let resp = pb::SnapshotResponse {
                    header: Some(header),
                    remaining_bytes: (total - sent) as u64,
                    blob,
                    version: ETCD_COMPAT_VERSION.to_string(),
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return;
                }
            }
            // etcd ends the stream with a final empty message that
            // confirms `remaining_bytes == 0`. We've already sent
            // that as the last chunk above; the channel close itself
            // is the EOS signal for tonic.
            drop(tx);
        });

        let stream: Self::SnapshotStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }

    async fn move_leader(
        &self,
        request: Request<pb::MoveLeaderRequest>,
    ) -> Result<Response<pb::MoveLeaderResponse>, Status> {
        let req = request.into_inner();
        // Validate target is a current voter — etcd does the same check.
        let metrics = self.state.raft.metrics().borrow().clone();
        let voters: Vec<_> = metrics.membership_config.voter_ids().collect();
        if !voters.contains(&req.target_id) {
            return Err(Status::failed_precondition(format!(
                "MoveLeader target {} is not a voting member",
                req.target_id
            )));
        }
        // openraft 0.9 doesn't expose an explicit `transfer_leader`
        // primitive (added in 0.10). The closest correct behavior is
        // to surface that limitation rather than do something racy.
        // Upgrade-to-0.10 lifts this restriction.
        Err(Status::unimplemented(
            "MoveLeader is not yet supported on openraft 0.9; \
             pending upgrade to openraft 0.10 which exposes \
             trigger().transfer_leader()",
        ))
    }

    async fn downgrade(
        &self,
        _request: Request<pb::DowngradeRequest>,
    ) -> Result<Response<pb::DowngradeResponse>, Status> {
        Err(Status::unimplemented(
            "Downgrade is not implemented in fastetcd",
        ))
    }
}

/// Compute SHA-256 over the `mvcc_kv` table. If `revision > 0`, only
/// records with `mod_revision <= revision` are hashed. The output is
/// folded into a `u32` (lower-32 of the hash) to match the wire shape
/// expected by `HashResponse.hash`.
async fn hash_kv_table(
    state: &ServerState,
    revision: i64,
) -> Result<(i64, u32), Box<dyn std::error::Error + Send + Sync>> {
    use fastetcd_storage::mvcc::KvRecord;
    use std::ops::Bound;

    let engine = state.sm.mvcc().engine().clone();
    let snap = engine.snapshot().await?;
    let entries = snap
        .range("mvcc_kv", Bound::Unbounded, Bound::Unbounded, 0)
        .await?;

    let mut h = Sha256::new();
    for (k, v) in entries {
        if revision > 0 {
            // Filter on mod_revision; deserialize so we can read it.
            let rec: KvRecord = bincode::deserialize(&v)?;
            if rec.mod_revision > revision {
                continue;
            }
        }
        h.update((k.len() as u32).to_be_bytes());
        h.update(&k);
        h.update((v.len() as u32).to_be_bytes());
        h.update(&v);
    }
    let digest = h.finalize();
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&digest[..4]);
    let folded = u32::from_be_bytes(bytes);
    let current = state.sm.mvcc().current_revision().await;
    Ok((current, folded))
}
