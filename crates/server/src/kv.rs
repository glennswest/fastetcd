//! Implementation of the etcd `KV` gRPC service.
//!
//! Range:
//!   - `serializable=true` → direct read from the local MvccStore
//!     (may be stale on a follower; the client opted into that).
//!   - `serializable=false` (etcd default, linearizable) → a read
//!     barrier first: on the leader, `Raft::ensure_linearizable`
//!     (ReadIndex + wait for apply); on a follower, forward the range
//!     to the leader (see `ServerState::linearize_read`). This is what
//!     stops a lagging follower or unconfirmed leader from returning
//!     stale data (#10). On a cluster-of-one the barrier is trivially
//!     satisfied, so single-node reads stay a direct local read.
//!
//! Put / DeleteRange / Compact / Txn:
//!   - Built into a `FastetcdLogEntry`, proposed through
//!     `Raft::client_write`. The state-machine response carries the
//!     new revision and any `prev_kv`s.

use std::sync::Arc;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_server::Kv;
use fastetcd_raft::{FastetcdLogEntry, FastetcdLogResponse};
use fastetcd_storage::mvcc::{RangeResult, TxnOpResult, TxnResult};
use tonic::{Request, Response, Status};

use crate::authz::{authorize, authorize_all, Access, RequiredPerm, UserIdentity};
use crate::conv;
use crate::state::{response_header, ServerState};

#[derive(Clone)]
pub struct KvService {
    state: Arc<ServerState>,
}

impl KvService {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    async fn propose(
        &self,
        entry: FastetcdLogEntry,
    ) -> Result<FastetcdLogResponse, Status> {
        self.state.propose(entry).await
    }
}

/// True if a txn writes anything new. A txn that only reads or only
/// deletes is allowed under the NOSPACE alarm: refusing it would remove
/// the one way a client has to free space (fastetcd#14).
fn txn_consumes_space(req: &pb::TxnRequest) -> bool {
    fn op_consumes(op: &pb::RequestOp) -> bool {
        match &op.request {
            Some(pb::request_op::Request::RequestPut(_)) => true,
            // A nested txn can hide a put.
            Some(pb::request_op::Request::RequestTxn(t)) => txn_consumes_space(t),
            Some(pb::request_op::Request::RequestRange(_))
            | Some(pb::request_op::Request::RequestDeleteRange(_))
            | None => false,
        }
    }
    req.success.iter().any(op_consumes) || req.failure.iter().any(op_consumes)
}

/// Every key access a put makes: write on the key, and read too when
/// it asks for the previous value back (etcd `isPutPermitted`).
fn put_accesses<'a>(p: &'a pb::PutRequest, out: &mut Vec<Access<'a>>) {
    out.push(Access { perm: RequiredPerm::Write, key: &p.key, range_end: b"" });
    if p.prev_kv {
        out.push(Access { perm: RequiredPerm::Read, key: &p.key, range_end: b"" });
    }
}

/// Every key access a delete makes: write on the range, and read too
/// when it returns the deleted pairs (etcd `isDeleteRangePermitted`).
fn delete_accesses<'a>(d: &'a pb::DeleteRangeRequest, out: &mut Vec<Access<'a>>) {
    out.push(Access { perm: RequiredPerm::Write, key: &d.key, range_end: &d.range_end });
    if d.prev_kv {
        out.push(Access { perm: RequiredPerm::Read, key: &d.key, range_end: &d.range_end });
    }
}

/// Every key access a txn can make, whichever branch it takes: read on
/// each compare target, and the accesses of every op in *both* the
/// success and failure lists, recursing into nested txns. Both branches
/// are checked because which one runs is decided at apply time, after
/// authorization; checking only the taken branch would let a compare
/// the client controls choose an op the role does not cover. This is
/// etcd's `checkTxnReqsPermission` (fastetcd#22).
fn txn_accesses<'a>(t: &'a pb::TxnRequest, out: &mut Vec<Access<'a>>) {
    for c in &t.compare {
        out.push(Access { perm: RequiredPerm::Read, key: &c.key, range_end: &c.range_end });
    }
    for op in t.success.iter().chain(t.failure.iter()) {
        match &op.request {
            Some(pb::request_op::Request::RequestRange(r)) => out.push(Access {
                perm: RequiredPerm::Read,
                key: &r.key,
                range_end: &r.range_end,
            }),
            Some(pb::request_op::Request::RequestPut(p)) => put_accesses(p, out),
            Some(pb::request_op::Request::RequestDeleteRange(d)) => delete_accesses(d, out),
            Some(pb::request_op::Request::RequestTxn(n)) => txn_accesses(n, out),
            None => {}
        }
    }
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn range(
        &self,
        request: Request<pb::RangeRequest>,
    ) -> Result<Response<pb::RangeResponse>, Status> {
        let user = request.extensions().get::<UserIdentity>().cloned();
        let req = request.into_inner();
        authorize(
            self.state.sm.mvcc().engine(),
            &self.state.auth,
            user.as_ref(),
            RequiredPerm::Read,
            &req.key,
            &req.range_end,
        )
        .await?;
        let result = serve_range(&self.state, &req).await?;
        let revision = self.state.sm.mvcc().current_revision().await;
        let header = response_header(&self.state, revision).await;
        Ok(Response::new(range_result_to_response(header, result, req.count_only)))
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::PutResponse>, Status> {
        let user = request.extensions().get::<UserIdentity>().cloned();
        let req = request.into_inner();
        let mut accesses = Vec::with_capacity(2);
        put_accesses(&req, &mut accesses);
        authorize_all(self.state.sm.mvcc().engine(), &self.state.auth, user.as_ref(), &accesses)
            .await?;
        // Under the NOSPACE alarm a put is refused so the store keeps
        // enough room to compact, snapshot and defragment its way out.
        // Reads and deletes are deliberately still served (fastetcd#14).
        self.state.space.check_write()?;
        let mutation = conv::put_request_to_mutation(&req);
        let resp = self
            .propose(FastetcdLogEntry::Apply {
                mutations: vec![mutation],
            })
            .await?;

        let (revision, mut results) = match resp {
            FastetcdLogResponse::Apply { revision, results } => (revision, results),
            other => return Err(Status::internal(format!("unexpected response: {other:?}"))),
        };
        let result = results
            .pop()
            .ok_or_else(|| Status::internal("Apply returned no results"))?;
        let header = response_header(&self.state, revision).await;
        Ok(Response::new(pb::PutResponse {
            header: Some(header),
            prev_kv: result.prev_kvs.first().map(conv::record_to_kv),
        }))
    }

    async fn delete_range(
        &self,
        request: Request<pb::DeleteRangeRequest>,
    ) -> Result<Response<pb::DeleteRangeResponse>, Status> {
        let user = request.extensions().get::<UserIdentity>().cloned();
        let req = request.into_inner();
        let mut accesses = Vec::with_capacity(2);
        delete_accesses(&req, &mut accesses);
        authorize_all(self.state.sm.mvcc().engine(), &self.state.auth, user.as_ref(), &accesses)
            .await?;
        let mutation = conv::delete_request_to_mutation(&req);
        let resp = self
            .propose(FastetcdLogEntry::Apply {
                mutations: vec![mutation],
            })
            .await?;

        let (revision, mut results) = match resp {
            FastetcdLogResponse::Apply { revision, results } => (revision, results),
            other => return Err(Status::internal(format!("unexpected response: {other:?}"))),
        };
        let result = results
            .pop()
            .ok_or_else(|| Status::internal("Apply returned no results"))?;
        let header = response_header(&self.state, revision).await;
        Ok(Response::new(pb::DeleteRangeResponse {
            header: Some(header),
            deleted: result.n,
            prev_kvs: result.prev_kvs.iter().map(conv::record_to_kv).collect(),
        }))
    }

    async fn txn(
        &self,
        request: Request<pb::TxnRequest>,
    ) -> Result<Response<pb::TxnResponse>, Status> {
        let user = request.extensions().get::<UserIdentity>().cloned();
        let req = request.into_inner();
        // Authorize every compare and every op in both branches before
        // anything is proposed; one denied access fails the whole txn.
        let mut accesses = Vec::new();
        txn_accesses(&req, &mut accesses);
        authorize_all(self.state.sm.mvcc().engine(), &self.state.auth, user.as_ref(), &accesses)
            .await?;
        if txn_consumes_space(&req) {
            self.state.space.check_write()?;
        }
        let mut compares = Vec::with_capacity(req.compare.len());
        for c in &req.compare {
            compares.push(conv::compare_from_proto(c)?);
        }
        let mut success = Vec::with_capacity(req.success.len());
        for op in &req.success {
            success.push(conv::request_op_from_proto(op)?);
        }
        let mut failure = Vec::with_capacity(req.failure.len());
        for op in &req.failure {
            failure.push(conv::request_op_from_proto(op)?);
        }

        let resp = self
            .propose(FastetcdLogEntry::Txn {
                compares,
                success,
                failure,
            })
            .await?;

        let result = match resp {
            FastetcdLogResponse::Txn(t) => t,
            other => return Err(Status::internal(format!("unexpected response: {other:?}"))),
        };

        Ok(Response::new(txn_result_to_response(&self.state, &req, result).await?))
    }

    async fn compact(
        &self,
        request: Request<pb::CompactionRequest>,
    ) -> Result<Response<pb::CompactionResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .propose(FastetcdLogEntry::Compact { rev: req.revision })
            .await?;
        let compact_rev = match resp {
            FastetcdLogResponse::Compact { compact_rev } => compact_rev,
            other => return Err(Status::internal(format!("unexpected response: {other:?}"))),
        };
        let header = response_header(&self.state, compact_rev).await;
        Ok(Response::new(pb::CompactionResponse {
            header: Some(header),
        }))
    }
}

async fn serve_range(
    state: &ServerState,
    req: &pb::RangeRequest,
) -> Result<RangeResult, Status> {
    // A default (linearizable) read must not return state older than a
    // completed write. On a cluster, reading local state without a
    // barrier lets a lagging follower — or a leader that has silently
    // lost leadership — return stale data, which shows up as spurious
    // CAS failures in read-modify-write loops (#10). `serializable=true`
    // opts out and reads local state directly, matching etcd.
    if !req.serializable {
        let read = fastetcd_raft::ForwardedRead {
            key: req.key.clone(),
            range_end: req.range_end.clone(),
            limit: req.limit.max(0) as u64,
            revision: req.revision,
            keys_only: req.keys_only,
            count_only: req.count_only,
        };
        // On a follower this returns the leader's result directly.
        if let Some(result) = state.linearize_read(&read).await? {
            return Ok(result);
        }
    }
    state
        .sm
        .mvcc()
        .range(
            &req.key,
            &req.range_end,
            req.limit.max(0) as usize,
            req.revision,
            req.keys_only,
            req.count_only,
        )
        .await
        .map_err(mvcc_error_to_status)
}

fn range_result_to_response(
    header: pb::ResponseHeader,
    result: RangeResult,
    count_only: bool,
) -> pb::RangeResponse {
    pb::RangeResponse {
        header: Some(header),
        kvs: if count_only {
            Vec::new()
        } else {
            result.kvs.iter().map(conv::record_to_kv).collect()
        },
        more: result.more,
        count: result.count,
    }
}

/// Build the gRPC response for an applied txn.
///
/// Each `ResponseOp` takes its variant from the request op at the same
/// position in the branch that ran, as etcd guarantees. The applied
/// result alone cannot say: a single-key `DeleteRange` and a `Put`
/// both produce one `MutationResult` with `n == 1`, and inferring the
/// kind from that shape is what turned deletes into `ResponsePut`s
/// (fastetcd#18). The request is still in hand here, so nothing about
/// the kind needs to travel through Raft.
async fn txn_result_to_response(
    state: &ServerState,
    req: &pb::TxnRequest,
    txn: TxnResult,
) -> Result<pb::TxnResponse, Status> {
    use pb::request_op::Request;
    use pb::response_op::Response as Resp;

    let header = response_header(state, txn.revision).await;
    let ops = if txn.succeeded { &req.success } else { &req.failure };
    if ops.len() != txn.op_results.len() {
        return Err(Status::internal(format!(
            "txn: {} op results for {} request ops",
            txn.op_results.len(),
            ops.len()
        )));
    }
    let mut responses = Vec::with_capacity(ops.len());
    for (op, result) in ops.iter().zip(txn.op_results) {
        let response = match (&op.request, result) {
            (Some(Request::RequestRange(r)), TxnOpResult::Range(res)) => {
                Resp::ResponseRange(range_result_to_response(header, res, r.count_only))
            }
            (Some(Request::RequestPut(_)), TxnOpResult::Mutation(m)) => {
                Resp::ResponsePut(pb::PutResponse {
                    header: Some(header),
                    prev_kv: m.prev_kvs.first().map(conv::record_to_kv),
                })
            }
            (Some(Request::RequestDeleteRange(_)), TxnOpResult::Mutation(m)) => {
                Resp::ResponseDeleteRange(pb::DeleteRangeResponse {
                    header: Some(header),
                    deleted: m.n,
                    prev_kvs: m.prev_kvs.iter().map(conv::record_to_kv).collect(),
                })
            }
            (req_op, _) => {
                return Err(Status::internal(format!(
                    "txn: op result does not match request op {req_op:?}"
                )))
            }
        };
        responses.push(pb::ResponseOp { response: Some(response) });
    }
    Ok(pb::TxnResponse {
        header: Some(header),
        succeeded: txn.succeeded,
        responses,
    })
}

fn mvcc_error_to_status(e: fastetcd_storage::mvcc::MvccError) -> Status {
    use fastetcd_storage::mvcc::MvccError;
    match e {
        MvccError::Compacted { .. } => {
            // etcd uses gRPC code OutOfRange for ErrCompacted.
            Status::out_of_range(e.to_string())
        }
        MvccError::FutureRevision { .. } => Status::out_of_range(e.to_string()),
        MvccError::Storage(_) | MvccError::Internal(_) => Status::internal(e.to_string()),
    }
}

