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
use fastetcd_storage::mvcc::{MutationResult, RangeResult, TxnOpResult, TxnResult};
use tonic::{Request, Response, Status};

use crate::authz::{authorize, RequiredPerm, UserIdentity};
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
        authorize(
            self.state.sm.mvcc().engine(),
            &self.state.auth,
            user.as_ref(),
            RequiredPerm::Write,
            &req.key,
            b"",
        )
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
        authorize(
            self.state.sm.mvcc().engine(),
            &self.state.auth,
            user.as_ref(),
            RequiredPerm::Write,
            &req.key,
            &req.range_end,
        )
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
        let req = request.into_inner();
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

        Ok(Response::new(txn_result_to_response(&self.state, result).await))
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

async fn txn_result_to_response(
    state: &ServerState,
    txn: TxnResult,
) -> pb::TxnResponse {
    let header = response_header(state, txn.revision).await;
    let mut responses = Vec::with_capacity(txn.op_results.len());
    for op_result in txn.op_results {
        let pb_resp = match op_result {
            TxnOpResult::Range(r) => {
                // Construct an inner RangeResponse with the same header.
                let header = pb::ResponseHeader {
                    revision: txn.revision,
                    ..header
                };
                pb::ResponseOp {
                    response: Some(pb::response_op::Response::ResponseRange(
                        range_result_to_response(header, r, false),
                    )),
                }
            }
            TxnOpResult::Mutation(m) => mutation_result_to_response_op(&header, m),
        };
        responses.push(pb_resp);
    }
    pb::TxnResponse {
        header: Some(header),
        succeeded: txn.succeeded,
        responses,
    }
}

fn mutation_result_to_response_op(
    header: &pb::ResponseHeader,
    m: MutationResult,
) -> pb::ResponseOp {
    // We don't know inside a Txn whether the mutation was a Put or a
    // DeleteRange; pick by the shape of the result. `n == 1` and
    // exactly one prev_kv (or none) indicates a Put; any other shape
    // is treated as DeleteRange (matches etcd's choice when result
    // counts don't disambiguate — DeleteRange is the "generic" case).
    //
    // In practice the gRPC service knows because the request op
    // already discriminated; the trade here is to keep TxnResult
    // engine-agnostic. We could carry an op tag in TxnOpResult to
    // make this exact — TODO for a follow-up.
    let looks_like_put = m.n == 1 && m.prev_kvs.len() <= 1;
    if looks_like_put {
        pb::ResponseOp {
            response: Some(pb::response_op::Response::ResponsePut(pb::PutResponse {
                header: Some(*header),
                prev_kv: m.prev_kvs.first().map(conv::record_to_kv),
            })),
        }
    } else {
        pb::ResponseOp {
            response: Some(pb::response_op::Response::ResponseDeleteRange(
                pb::DeleteRangeResponse {
                    header: Some(*header),
                    deleted: m.n,
                    prev_kvs: m.prev_kvs.iter().map(conv::record_to_kv).collect(),
                },
            )),
        }
    }
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

