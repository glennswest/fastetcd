//! The etcd v3 API as the tests use it: fastetcd's own generated clients,
//! the wire any etcd client speaks.

use std::time::Duration;

use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

use fastetcd_proto::etcdserverpb as pb;
use pb::auth_client::AuthClient;
use pb::kv_client::KvClient;
use pb::lease_client::LeaseClient;
use pb::maintenance_client::MaintenanceClient;
use pb::watch_client::WatchClient;

#[derive(Clone)]
pub struct Clients {
    pub kv: KvClient<Channel>,
    pub watch: WatchClient<Channel>,
    pub lease: LeaseClient<Channel>,
    pub maint: MaintenanceClient<Channel>,
    pub auth: AuthClient<Channel>,
    token: Option<String>,
}

/// The end of the range holding every key that starts with `prefix`.
pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

impl Clients {
    pub async fn connect(endpoint: &str) -> anyhow::Result<Clients> {
        let ch = Channel::from_shared(endpoint.to_string())?
            .connect_timeout(Duration::from_secs(5))
            // Generous: a long wave's compaction or defragment of a large
            // file on a slow disk takes a while.
            .timeout(Duration::from_secs(120))
            .connect()
            .await?;
        Ok(Clients {
            kv: KvClient::new(ch.clone()),
            watch: WatchClient::new(ch.clone()),
            lease: LeaseClient::new(ch.clone()),
            maint: MaintenanceClient::new(ch.clone()),
            auth: AuthClient::new(ch),
            token: None,
        })
    }

    /// The same connection, as the user `token` was issued to.
    pub fn as_user(&self, token: &str) -> Clients {
        let mut c = self.clone();
        c.token = Some(token.to_string());
        c
    }

    pub fn req<T>(&self, msg: T) -> Request<T> {
        let mut r = Request::new(msg);
        if let Some(t) = &self.token {
            if let Ok(v) = MetadataValue::try_from(t.as_str()) {
                r.metadata_mut().insert("token", v);
            }
        }
        r
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<i64, tonic::Status> {
        self.put_lease(key, value, 0).await
    }

    pub async fn put_lease(&mut self, key: &[u8], value: &[u8], lease: i64) -> Result<i64, tonic::Status> {
        let r = self.req(pb::PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            lease,
            ..Default::default()
        });
        Ok(self.kv.put(r).await?.into_inner().header.map_or(0, |h| h.revision))
    }

    /// The value of `key` (linearizable), if it exists.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, tonic::Status> {
        let r = self.req(pb::RangeRequest { key: key.to_vec(), ..Default::default() });
        Ok(self.kv.range(r).await?.into_inner().kvs.pop().map(|kv| kv.value))
    }

    pub async fn count(&mut self, prefix: &[u8]) -> Result<i64, tonic::Status> {
        let r = self.req(pb::RangeRequest {
            key: prefix.to_vec(),
            range_end: prefix_end(prefix),
            count_only: true,
            ..Default::default()
        });
        Ok(self.kv.range(r).await?.into_inner().count)
    }

    pub async fn serializable_count(&mut self, prefix: &[u8]) -> Result<i64, tonic::Status> {
        let r = self.req(pb::RangeRequest {
            key: prefix.to_vec(),
            range_end: prefix_end(prefix),
            count_only: true,
            serializable: true,
            ..Default::default()
        });
        Ok(self.kv.range(r).await?.into_inner().count)
    }

    pub async fn delete_prefix(&mut self, prefix: &[u8]) -> Result<i64, tonic::Status> {
        let r = self.req(pb::DeleteRangeRequest {
            key: prefix.to_vec(),
            range_end: prefix_end(prefix),
            ..Default::default()
        });
        Ok(self.kv.delete_range(r).await?.into_inner().deleted)
    }

    pub async fn status(&mut self) -> Result<pb::StatusResponse, tonic::Status> {
        let r = self.req(pb::StatusRequest {});
        Ok(self.maint.status(r).await?.into_inner())
    }

    pub async fn alarms(&mut self) -> Result<Vec<pb::AlarmMember>, tonic::Status> {
        let r = self.req(pb::AlarmRequest {
            action: pb::alarm_request::AlarmAction::Get as i32,
            ..Default::default()
        });
        Ok(self.maint.alarm(r).await?.into_inner().alarms)
    }

    pub async fn grant(&mut self, ttl: i64) -> Result<i64, tonic::Status> {
        let r = self.req(pb::LeaseGrantRequest { ttl, id: 0 });
        Ok(self.lease.lease_grant(r).await?.into_inner().id)
    }

    pub async fn revoke(&mut self, id: i64) -> Result<(), tonic::Status> {
        let r = self.req(pb::LeaseRevokeRequest { id });
        self.lease.lease_revoke(r).await.map(|_| ())
    }

    pub async fn time_to_live(&mut self, id: i64) -> Result<pb::LeaseTimeToLiveResponse, tonic::Status> {
        let r = self.req(pb::LeaseTimeToLiveRequest { id, keys: true });
        Ok(self.lease.lease_time_to_live(r).await?.into_inner())
    }

    /// One keep-alive of `id`; its TTL (0: the lease is gone).
    pub async fn keep_alive_once(&mut self, id: i64) -> anyhow::Result<i64> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(pb::LeaseKeepAliveRequest { id }).await?;
        let r = self.req(tokio_stream::wrappers::ReceiverStream::new(rx));
        let mut s = self.lease.lease_keep_alive(r).await?.into_inner();
        let resp = s.message().await?.ok_or_else(|| anyhow::anyhow!("keep-alive stream ended"))?;
        Ok(resp.ttl)
    }

    pub async fn compact(&mut self, revision: i64) -> Result<(), tonic::Status> {
        let r = self.req(pb::CompactionRequest { revision, physical: true });
        self.kv.compact(r).await.map(|_| ())
    }

    pub async fn defragment(&mut self) -> Result<(), tonic::Status> {
        let r = self.req(pb::DefragmentRequest {});
        self.maint.defragment(r).await.map(|_| ())
    }

    /// Stream `Maintenance.Snapshot` into `path`; its size.
    pub async fn snapshot_to(&mut self, path: &std::path::Path) -> anyhow::Result<u64> {
        let r = self.req(pb::SnapshotRequest {});
        let mut s = self.maint.snapshot(r).await?.into_inner();
        let mut bytes = Vec::new();
        while let Some(m) = s.message().await? {
            bytes.extend_from_slice(&m.blob);
        }
        std::fs::write(path, &bytes)?;
        Ok(bytes.len() as u64)
    }

    /// Open a watch of every key under `prefix` from `start_revision` (0:
    /// from now); the stream of responses after the "created" one.
    pub async fn watch_prefix(
        &mut self,
        prefix: &[u8],
        start_revision: i64,
    ) -> anyhow::Result<(tokio::sync::mpsc::Sender<pb::WatchRequest>, tonic::Streaming<pb::WatchResponse>)> {
        use pb::watch_request::RequestUnion;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(pb::WatchRequest {
            request_union: Some(RequestUnion::CreateRequest(pb::WatchCreateRequest {
                key: prefix.to_vec(),
                range_end: prefix_end(prefix),
                start_revision,
                prev_kv: true,
                ..Default::default()
            })),
        })
        .await?;
        let r = self.req(tokio_stream::wrappers::ReceiverStream::new(rx));
        let mut s = self.watch.watch(r).await?.into_inner();
        let first = s.message().await?.ok_or_else(|| anyhow::anyhow!("watch stream ended at create"))?;
        if !first.created {
            anyhow::bail!("the first watch response was not `created`: {first:?}");
        }
        if first.canceled {
            // A compacted start revision is answered created + canceled.
            return Err(anyhow::anyhow!(
                "watch canceled at create: compact_revision {} ({})",
                first.compact_revision,
                first.cancel_reason
            ));
        }
        Ok((tx, s))
    }
}

/// The next `n` events of a watch, waiting at most `within`.
pub async fn next_events(
    s: &mut tonic::Streaming<pb::WatchResponse>,
    n: usize,
    within: Duration,
) -> anyhow::Result<Vec<fastetcd_proto::mvccpb::Event>> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + within;
    while out.len() < n {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(left, s.message()).await {
            Ok(Ok(Some(m))) => {
                if m.canceled {
                    anyhow::bail!("watch canceled: {} (compact_revision {})", m.cancel_reason, m.compact_revision);
                }
                out.extend(m.events);
            }
            Ok(Ok(None)) => anyhow::bail!("watch stream ended after {} of {n} events", out.len()),
            Ok(Err(e)) => anyhow::bail!("watch stream error after {} of {n} events: {e}", out.len()),
            Err(_) => anyhow::bail!("{} of {n} events within {within:?}", out.len()),
        }
    }
    Ok(out)
}
