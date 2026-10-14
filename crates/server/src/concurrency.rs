//! etcd's Lock and Election services (fastetcd#64).
//!
//! In etcd (v3.6 `server/etcdserver/api/v3lock`, `v3election`) both are
//! thin servers over clientv3's `concurrency` package, run in-process
//! against the server's own KV and Lease APIs with the caller's context.
//! Here they run the same steps through [`KvService`] and
//! [`LeaseService`] with the caller's identity, so authorization is the
//! caller's, as it is in etcd:
//!
//! - **Session:** the request's lease, or with lease 0 a new one of
//!   60 s (`concurrency.NewSession`); etcd orphans the session at once,
//!   so nothing keeps it alive here either.
//! - **Lock** (`Mutex.Lock`): key `name/<lease hex>`; if it does not
//!   exist, put it with the lease; the owner is the key under `name/`
//!   with the lowest create revision; until that is ours, wait for the
//!   key created last before ours to be deleted, again. Then read the key
//!   back: gone means the session expired. **Unlock:** delete the key.
//! - **Election:** Campaign (as a lock, with the value; a different
//!   value on a key already ours is proclaimed), Proclaim and Resign (on
//!   the key only while its create revision is the leader's), Leader (the
//!   oldest key), Observe (the leader, then its changes, then the next).
//!
//! Waiting for a delete follows this member's applied events, subscribed
//! to before the read, so none is missed. A Lock or Campaign whose client
//! goes away while it waits removes its key, as etcd's do.
//!
//! The owner queries need Range's sort and create-revision filters
//! (#154).

use std::pin::Pin;
use std::sync::Arc;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_server::Kv;
use fastetcd_proto::etcdserverpb::lease_server::Lease;
use fastetcd_proto::mvccpb;
use fastetcd_proto::v3electionpb as epb;
use fastetcd_proto::v3lockpb as lpb;
use fastetcd_storage::mvcc::EventKind;
use tokio::sync::broadcast::error::RecvError;
use tonic::{Request, Response, Status};

use crate::authz::UserIdentity;
use crate::kv::KvService;
use crate::lease::LeaseService;
use crate::state::ServerState;

/// etcd's `defaultSessionTTL`.
const SESSION_TTL: i64 = 60;

/// The errors clientv3's `concurrency` package returns, which etcd's
/// servers return as they are (gRPC `Unknown`, the text as the message).
fn concurrency_error(msg: &str) -> Status {
    Status::unknown(msg)
}

/// The KV and Lease calls of one request, made as its caller.
#[derive(Clone)]
struct Caller {
    kv: KvService,
    lease: LeaseService,
    state: Arc<ServerState>,
    user: Option<UserIdentity>,
}

fn range_end_of_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

/// `name/` as the concurrency package names a prefix.
fn prefix(name: &[u8]) -> Vec<u8> {
    let mut p = name.to_vec();
    p.push(b'/');
    p
}

/// `pfx` + the lease id as Go's `%x` prints it.
fn lease_key(pfx: &[u8], lease: i64) -> Vec<u8> {
    let hex = if lease < 0 { format!("-{:x}", lease.unsigned_abs()) } else { format!("{lease:x}") };
    [pfx, hex.as_bytes()].concat()
}

fn create_rev_is(key: &[u8], rev: i64) -> pb::Compare {
    pb::Compare {
        result: pb::compare::CompareResult::Equal as i32,
        target: pb::compare::CompareTarget::Create as i32,
        key: key.to_vec(),
        range_end: Vec::new(),
        target_union: Some(pb::compare::TargetUnion::CreateRevision(rev)),
    }
}

fn op(request: pb::request_op::Request) -> pb::RequestOp {
    pb::RequestOp { request: Some(request) }
}

fn op_put(key: &[u8], value: &[u8], lease: i64) -> pb::RequestOp {
    op(pb::request_op::Request::RequestPut(pb::PutRequest {
        key: key.to_vec(),
        value: value.to_vec(),
        lease,
        ..Default::default()
    }))
}

fn op_get(key: &[u8]) -> pb::RequestOp {
    op(pb::request_op::Request::RequestRange(pb::RangeRequest { key: key.to_vec(), ..Default::default() }))
}

/// clientv3 `WithFirstCreate` on a prefix: the oldest key under it.
fn first_create(pfx: &[u8]) -> pb::RangeRequest {
    pb::RangeRequest {
        key: pfx.to_vec(),
        range_end: range_end_of_prefix(pfx),
        limit: 1,
        sort_order: pb::range_request::SortOrder::Ascend as i32,
        sort_target: pb::range_request::SortTarget::Create as i32,
        ..Default::default()
    }
}

fn range_of(resp: &pb::TxnResponse, i: usize) -> Vec<mvccpb::KeyValue> {
    match resp.responses.get(i).and_then(|r| r.response.as_ref()) {
        Some(pb::response_op::Response::ResponseRange(r)) => r.kvs.clone(),
        _ => Vec::new(),
    }
}

impl Caller {
    fn req<T>(&self, msg: T) -> Request<T> {
        let mut r = Request::new(msg);
        if let Some(u) = &self.user {
            r.extensions_mut().insert(u.clone());
        }
        r
    }

    async fn txn(&self, t: pb::TxnRequest) -> Result<pb::TxnResponse, Status> {
        Ok(Kv::txn(&self.kv, self.req(t)).await?.into_inner())
    }

    async fn range(&self, r: pb::RangeRequest) -> Result<pb::RangeResponse, Status> {
        Ok(Kv::range(&self.kv, self.req(r)).await?.into_inner())
    }

    async fn delete(&self, key: &[u8]) -> Result<pb::DeleteRangeResponse, Status> {
        let d = pb::DeleteRangeRequest { key: key.to_vec(), ..Default::default() };
        Ok(Kv::delete_range(&self.kv, self.req(d)).await?.into_inner())
    }

    /// The session's lease: `lease`, or a new one of 60 s for 0.
    async fn session(&self, lease: i64) -> Result<i64, Status> {
        if lease != 0 {
            return Ok(lease);
        }
        let g = pb::LeaseGrantRequest { ttl: SESSION_TTL, id: 0 };
        let resp = Lease::lease_grant(&self.lease, self.req(g)).await?.into_inner();
        if !resp.error.is_empty() {
            return Err(Status::unknown(resp.error));
        }
        Ok(resp.id)
    }

    /// clientv3 `waitDeletes`: until no key under `pfx` was created at or
    /// below `max_create_rev`, wait for the last of them to be deleted.
    async fn wait_deletes(&self, pfx: &[u8], max_create_rev: i64) -> Result<(), Status> {
        loop {
            // Subscribed first: a delete applied after the read below is
            // in this stream, one applied before it is not in the read.
            let mut events = self.state.sm.mvcc().subscribe();
            let read = pb::RangeRequest {
                key: pfx.to_vec(),
                range_end: range_end_of_prefix(pfx),
                limit: 1,
                sort_order: pb::range_request::SortOrder::Descend as i32,
                sort_target: pb::range_request::SortTarget::Create as i32,
                max_create_revision: max_create_rev,
                ..Default::default()
            };
            let resp = self.range(read).await?;
            let Some(last) = resp.kvs.first() else {
                return Ok(());
            };
            let read_at = resp.header.map(|h| h.revision).unwrap_or(0);
            // clientv3 `waitDelete`: the key's first DELETE after the read.
            loop {
                match events.recv().await {
                    Ok(batch) if batch.revision > read_at => {
                        if batch.events.iter().any(|e| e.kind == EventKind::Delete && e.kv.key == last.key) {
                            break;
                        }
                    }
                    Ok(_) => {}
                    // Fell behind: read again rather than miss the delete.
                    Err(RecvError::Lagged(_)) => break,
                    Err(RecvError::Closed) => return Err(Status::unavailable("the store is closing")),
                }
            }
        }
    }
}

/// Undoes a Lock or Campaign whose client goes away while it waits, as
/// etcd's do: the lock key is deleted; a candidate resigns.
struct Abandon {
    caller: Caller,
    key: Vec<u8>,
    rev: Option<i64>,
    armed: bool,
}

impl Drop for Abandon {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (caller, key, rev) = (self.caller.clone(), self.key.clone(), self.rev);
        tokio::spawn(async move {
            let _ = match rev {
                // Resign: only while the key is still the candidate's.
                Some(rev) => caller
                    .txn(pb::TxnRequest {
                        compare: vec![create_rev_is(&key, rev)],
                        success: vec![op(pb::request_op::Request::RequestDeleteRange(pb::DeleteRangeRequest {
                            key: key.clone(),
                            ..Default::default()
                        }))],
                        failure: Vec::new(),
                    })
                    .await
                    .map(|_| ()),
                None => caller.delete(&key).await.map(|_| ()),
            };
        });
    }
}

/// etcd's `v3lockpb.Lock` service.
#[derive(Clone)]
pub struct LockService {
    kv: KvService,
    lease: LeaseService,
    state: Arc<ServerState>,
}

impl LockService {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { kv: KvService::new(state.clone()), lease: LeaseService::new(state.clone()), state }
    }

    fn caller<T>(&self, req: &Request<T>) -> Caller {
        Caller {
            kv: self.kv.clone(),
            lease: self.lease.clone(),
            state: self.state.clone(),
            user: req.extensions().get::<UserIdentity>().cloned(),
        }
    }
}

#[tonic::async_trait]
impl lpb::lock_server::Lock for LockService {
    async fn lock(&self, request: Request<lpb::LockRequest>) -> Result<Response<lpb::LockResponse>, Status> {
        let c = self.caller(&request);
        let req = request.into_inner();
        let lease = c.session(req.lease).await?;
        let pfx = prefix(&req.name);
        let key = lease_key(&pfx, lease);
        // Mutex.tryAcquire.
        let resp = c
            .txn(pb::TxnRequest {
                compare: vec![create_rev_is(&key, 0)],
                success: vec![op_put(&key, b"", lease), op(pb::request_op::Request::RequestRange(first_create(&pfx)))],
                failure: vec![op_get(&key), op(pb::request_op::Request::RequestRange(first_create(&pfx)))],
            })
            .await?;
        let header_rev = resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
        let my_rev = if resp.succeeded {
            header_rev
        } else {
            range_of(&resp, 0).first().map(|kv| kv.create_revision).unwrap_or(header_rev)
        };
        let owner = range_of(&resp, 1);
        if owner.first().is_none_or(|kv| kv.create_revision == my_rev) {
            return Ok(Response::new(lpb::LockResponse { header: resp.header, key }));
        }
        let mut abandon = Abandon { caller: c.clone(), key: key.clone(), rev: None, armed: true };
        c.wait_deletes(&pfx, my_rev - 1).await?;
        let got = c.range(pb::RangeRequest { key: key.clone(), ..Default::default() }).await?;
        abandon.armed = false;
        if got.kvs.is_empty() {
            return Err(concurrency_error("mutex: session is expired"));
        }
        Ok(Response::new(lpb::LockResponse { header: got.header, key }))
    }

    async fn unlock(&self, request: Request<lpb::UnlockRequest>) -> Result<Response<lpb::UnlockResponse>, Status> {
        let c = self.caller(&request);
        let resp = c.delete(&request.into_inner().key).await?;
        Ok(Response::new(lpb::UnlockResponse { header: resp.header }))
    }
}

/// etcd's `v3electionpb.Election` service.
#[derive(Clone)]
pub struct ElectionService {
    lock: LockService,
}

impl ElectionService {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { lock: LockService::new(state) }
    }
}

const NOT_LEADER: &str = "election: not leader";
const NO_LEADER: &str = "election: no leader";
const MISSING_LEADER: &str = "\"leader\" field must be provided";

/// Election.Proclaim on a key the caller leads.
async fn proclaim(c: &Caller, leader: &epb::LeaderKey, value: &[u8]) -> Result<pb::TxnResponse, Status> {
    let resp = c
        .txn(pb::TxnRequest {
            compare: vec![create_rev_is(&leader.key, leader.rev)],
            success: vec![op_put(&leader.key, value, leader.lease)],
            failure: Vec::new(),
        })
        .await?;
    if !resp.succeeded {
        return Err(concurrency_error(NOT_LEADER));
    }
    Ok(resp)
}

async fn resign(c: &Caller, leader: &epb::LeaderKey) -> Result<pb::TxnResponse, Status> {
    c.txn(pb::TxnRequest {
        compare: vec![create_rev_is(&leader.key, leader.rev)],
        success: vec![op(pb::request_op::Request::RequestDeleteRange(pb::DeleteRangeRequest {
            key: leader.key.clone(),
            ..Default::default()
        }))],
        failure: Vec::new(),
    })
    .await
}

type ObserveStream = Pin<Box<dyn futures::Stream<Item = Result<epb::LeaderResponse, Status>> + Send>>;

#[tonic::async_trait]
impl epb::election_server::Election for ElectionService {
    async fn campaign(
        &self,
        request: Request<epb::CampaignRequest>,
    ) -> Result<Response<epb::CampaignResponse>, Status> {
        let c = self.lock.caller(&request);
        let req = request.into_inner();
        let lease = c.session(req.lease).await?;
        let pfx = prefix(&req.name);
        let key = lease_key(&pfx, lease);
        let resp = c
            .txn(pb::TxnRequest {
                compare: vec![create_rev_is(&key, 0)],
                success: vec![op_put(&key, &req.value, lease)],
                failure: vec![op_get(&key)],
            })
            .await?;
        let mut leader = epb::LeaderKey {
            name: req.name.clone(),
            key: key.clone(),
            rev: resp.header.as_ref().map(|h| h.revision).unwrap_or(0),
            lease,
        };
        if !resp.succeeded {
            let kv = range_of(&resp, 0).into_iter().next().ok_or_else(|| concurrency_error(NOT_LEADER))?;
            leader.rev = kv.create_revision;
            if kv.value != req.value {
                if let Err(e) = proclaim(&c, &leader, &req.value).await {
                    let _ = resign(&c, &leader).await;
                    return Err(e);
                }
            }
        }
        let mut abandon = Abandon { caller: c.clone(), key, rev: Some(leader.rev), armed: true };
        c.wait_deletes(&pfx, leader.rev - 1).await?;
        abandon.armed = false;
        Ok(Response::new(epb::CampaignResponse { header: resp.header, leader: Some(leader) }))
    }

    async fn proclaim(
        &self,
        request: Request<epb::ProclaimRequest>,
    ) -> Result<Response<epb::ProclaimResponse>, Status> {
        let c = self.lock.caller(&request);
        let req = request.into_inner();
        let leader = req.leader.ok_or_else(|| concurrency_error(MISSING_LEADER))?;
        let resp = proclaim(&c, &leader, &req.value).await?;
        Ok(Response::new(epb::ProclaimResponse { header: resp.header }))
    }

    async fn leader(&self, request: Request<epb::LeaderRequest>) -> Result<Response<epb::LeaderResponse>, Status> {
        let c = self.lock.caller(&request);
        let resp = c.range(first_create(&prefix(&request.into_inner().name))).await?;
        let kv = resp.kvs.into_iter().next().ok_or_else(|| concurrency_error(NO_LEADER))?;
        Ok(Response::new(epb::LeaderResponse { header: resp.header, kv: Some(kv) }))
    }

    type ObserveStream = ObserveStream;

    async fn observe(&self, request: Request<epb::LeaderRequest>) -> Result<Response<Self::ObserveStream>, Status> {
        let c = self.lock.caller(&request);
        let pfx = prefix(&request.into_inner().name);
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<epb::LeaderResponse, Status>>(16);
        tokio::spawn(async move {
            if let Err(e) = observe(&c, &pfx, &tx).await {
                let _ = tx.send(Err(e)).await;
            }
        });
        Ok(Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))))
    }

    async fn resign(&self, request: Request<epb::ResignRequest>) -> Result<Response<epb::ResignResponse>, Status> {
        let c = self.lock.caller(&request);
        let leader = request.into_inner().leader.ok_or_else(|| concurrency_error(MISSING_LEADER))?;
        let resp = resign(&c, &leader).await?;
        Ok(Response::new(epb::ResignResponse { header: resp.header }))
    }
}

/// clientv3 `Election.observe`: the leader, then every change to its key,
/// then, once it is deleted, the next leader. Ends when the client goes.
async fn observe(
    c: &Caller,
    pfx: &[u8],
    tx: &tokio::sync::mpsc::Sender<Result<epb::LeaderResponse, Status>>,
) -> Result<(), Status> {
    let header = |rev: i64| {
        let state = c.state.clone();
        async move { crate::state::response_header(&state, rev).await }
    };
    loop {
        let mut events = c.state.sm.mvcc().subscribe();
        let resp = c.range(first_create(pfx)).await?;
        let read_at = resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
        let (mut at, leader) = match resp.kvs.into_iter().next() {
            Some(kv) => (read_at, kv),
            // No leader yet: the first key put under the prefix.
            None => loop {
                match events.recv().await {
                    Ok(b) if b.revision > read_at => {
                        let put = b.events.iter().find(|e| e.kind == EventKind::Put && e.kv.key.starts_with(pfx));
                        if let Some(e) = put {
                            break (e.kv.mod_revision, crate::conv::record_to_kv(&e.kv));
                        }
                    }
                    Ok(_) => {}
                    Err(RecvError::Lagged(_)) => break (0, mvccpb::KeyValue::default()),
                    Err(RecvError::Closed) => return Ok(()),
                }
            },
        };
        if at == 0 {
            continue; // lagged: read again
        }
        if tx.send(Ok(epb::LeaderResponse { header: Some(header(at).await), kv: Some(leader.clone()) })).await.is_err() {
            return Ok(());
        }
        // The leader's changes until it is deleted.
        loop {
            match events.recv().await {
                Ok(b) if b.revision > at => {
                    at = b.revision;
                    let mut deleted = false;
                    for e in b.events.iter().filter(|e| e.kv.key == leader.key) {
                        if e.kind == EventKind::Delete {
                            deleted = true;
                            break;
                        }
                        let kv = crate::conv::record_to_kv(&e.kv);
                        let msg = epb::LeaderResponse { header: Some(header(b.revision).await), kv: Some(kv) };
                        if tx.send(Ok(msg)).await.is_err() {
                            return Ok(());
                        }
                    }
                    if deleted {
                        break;
                    }
                }
                Ok(_) => {}
                Err(RecvError::Lagged(_)) => break,
                Err(RecvError::Closed) => return Ok(()),
            }
            if tx.is_closed() {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_prefixes_are_named_as_clientv3_names_them() {
        assert_eq!(prefix(b"lk"), b"lk/".to_vec());
        assert_eq!(lease_key(b"lk/", 0x694d71ab), b"lk/694d71ab".to_vec());
        assert_eq!(lease_key(b"lk/", -1), b"lk/-1".to_vec());
        assert_eq!(range_end_of_prefix(b"lk/"), b"lk0".to_vec());
        assert_eq!(range_end_of_prefix(&[b'a', 0xff]), b"b".to_vec());
    }
}
