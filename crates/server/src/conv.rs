//! Converters between etcd v3 wire types and fastetcd's internal MVCC
//! types. Keeps the gRPC handlers focused on dispatch rather than
//! marshaling.

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::mvccpb;
use fastetcd_storage::mvcc::{
    Compare, CompareOp, CompareTarget, KvRecord, Mutation, RangeOp, TxnOp,
};
use tonic::Status;

/// Convert an internal `KvRecord` to the wire `mvccpb::KeyValue`. We
/// follow etcd's convention: tombstones are never returned to clients
/// from Range; the caller is expected to skip them.
pub fn record_to_kv(rec: &KvRecord) -> mvccpb::KeyValue {
    mvccpb::KeyValue {
        key: rec.key.clone(),
        create_revision: rec.create_revision,
        mod_revision: rec.mod_revision,
        version: rec.version,
        value: rec.value.clone(),
        lease: rec.lease,
    }
}

/// Build a `Mutation::Put` from a `PutRequest` proto.
pub fn put_request_to_mutation(req: &pb::PutRequest) -> Mutation {
    Mutation::Put {
        key: req.key.clone(),
        value: req.value.clone(),
        lease: req.lease,
        ignore_value: req.ignore_value,
        ignore_lease: req.ignore_lease,
        prev_kv: req.prev_kv,
    }
}

/// Build a `Mutation::DeleteRange` from a `DeleteRangeRequest`.
pub fn delete_request_to_mutation(req: &pb::DeleteRangeRequest) -> Mutation {
    Mutation::DeleteRange {
        key: req.key.clone(),
        range_end: req.range_end.clone(),
        prev_kv: req.prev_kv,
    }
}

/// Build an internal `RangeOp` from a `RangeRequest`. Used by Txn
/// (which wraps Range as a sub-op).
pub fn range_request_to_op(req: &pb::RangeRequest) -> RangeOp {
    RangeOp {
        key: req.key.clone(),
        range_end: req.range_end.clone(),
        limit: req.limit.max(0) as usize,
        revision: req.revision,
        keys_only: req.keys_only,
        count_only: req.count_only,
    }
}

/// Translate a proto `Compare` into our internal `Compare`. Returns
/// a `Status::invalid_argument` if the discriminants are unknown or
/// the target union is missing.
pub fn compare_from_proto(c: &pb::Compare) -> Result<Compare, Status> {
    use pb::compare::{CompareResult, CompareTarget as PbTarget, TargetUnion};
    let result = CompareResult::try_from(c.result).map_err(|_| {
        Status::invalid_argument(format!("unknown CompareResult enum: {}", c.result))
    })?;
    let target_enum = PbTarget::try_from(c.target).map_err(|_| {
        Status::invalid_argument(format!("unknown CompareTarget enum: {}", c.target))
    })?;
    let op = match result {
        CompareResult::Equal => CompareOp::Equal,
        CompareResult::Greater => CompareOp::Greater,
        CompareResult::Less => CompareOp::Less,
        CompareResult::NotEqual => CompareOp::NotEqual,
    };
    let target_union = c
        .target_union
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("Compare.target_union missing"))?;

    // The wire format permits any TargetUnion variant regardless of
    // target_enum, but in practice etcd uses them coherently. Be
    // strict and check that they match — clients sending inconsistent
    // pairs are malformed.
    let target = match (target_enum, target_union) {
        (PbTarget::Version, TargetUnion::Version(v)) => CompareTarget::Version(*v),
        (PbTarget::Create, TargetUnion::CreateRevision(v)) => CompareTarget::CreateRevision(*v),
        (PbTarget::Mod, TargetUnion::ModRevision(v)) => CompareTarget::ModRevision(*v),
        (PbTarget::Value, TargetUnion::Value(v)) => CompareTarget::Value(v.clone()),
        (PbTarget::Lease, TargetUnion::Lease(v)) => CompareTarget::Lease(*v),
        (te, _) => {
            return Err(Status::invalid_argument(format!(
                "Compare.target ({te:?}) does not match target_union variant"
            )))
        }
    };
    Ok(Compare {
        key: c.key.clone(),
        range_end: c.range_end.clone(),
        op,
        target,
    })
}

/// Translate a proto `RequestOp` (used inside a TxnRequest) into our
/// internal `TxnOp`. Nested Txn is rejected at this layer — we'd need
/// to flatten or refuse depth > 1. v0.1 refuses.
pub fn request_op_from_proto(op: &pb::RequestOp) -> Result<TxnOp, Status> {
    use pb::request_op::Request;
    let Some(req) = &op.request else {
        return Err(Status::invalid_argument("RequestOp.request missing"));
    };
    match req {
        Request::RequestRange(r) => Ok(TxnOp::Range(range_request_to_op(r))),
        Request::RequestPut(p) => Ok(TxnOp::Mutation(put_request_to_mutation(p))),
        Request::RequestDeleteRange(d) => Ok(TxnOp::Mutation(delete_request_to_mutation(d))),
        Request::RequestTxn(_) => Err(Status::unimplemented(
            "nested Txn in Txn ops is not yet supported",
        )),
    }
}
