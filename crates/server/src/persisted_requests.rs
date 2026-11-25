//! `fastetcd persisted-requests`: the client requests a member's raft
//! log holds, in log order, as JSON lines (fastetcd#195).
//!
//! etcd's robustness suite (tests/robustness, `report.PersistedRequests`)
//! reads every member's etcd WAL after a run: the requests it finds are
//! what may have been applied even when the client saw an error, and the
//! model it replays to check watches and serializable reads. fastetcd's
//! log is its own format, so the suite's patch in `tests/robustness/`
//! runs this instead and converts each line as the suite's
//! `parseEntryNormal` converts an etcd raft request.
//!
//! The log is read with [`fastetcd_raft::wal_log_store::read_history`]:
//! from its first entry, without changing it, while the member runs. It
//! is whole only on a member started with `--wal-keep-purged` that never
//! installed a snapshot; otherwise the command fails, as the suite's
//! reader fails on an etcd WAL that does not start at the beginning.
//!
//! Each line is one object with one key (bytes are base64):
//!
//! - `{"put": {"key", "value", "lease"}}`: a Put.
//! - `{"delete": {"key", "range_end"}}`: a DeleteRange.
//! - `{"txn": {"compare": [..], "success": [..], "failure": [..]}}`: a
//!   Txn that can write (several mutations in one entry come as a txn
//!   with no compares). An op is `{"range": {"key", "range_end",
//!   "limit"}}`, `{"put": ..}`, `{"delete": ..}` or `{"txn": ..}`; a
//!   compare is `{"key", "range_end", "target", "result", "value"}`,
//!   `target` one of `version`, `create`, `mod`, `value` (the value then
//!   base64), `lease`, and `result` one of `equal`, `not_equal`,
//!   `greater`, `less`.
//! - `{"lease_grant": {"id"}}`: the id the grant was given. A grant
//!   asking for id 0 gets one when it is applied (the next of a counter
//!   that also follows explicit ids), so it is worked out here the same
//!   way, in log order, from the first entry.
//! - `{"lease_revoke": {"id"}}`, `{"compact": {"revision"}}`.
//!
//! Left out, as etcd's WAL has no such requests: read-only txns (etcd
//! serves them without raft; fastetcd logs them), keep-alives (etcd
//! renews leases off the log), and fastetcd's own entries (no-ops,
//! membership, auth, cluster id, start revision).

use base64::Engine;
use openraft::{Entry, EntryPayload};
use serde_json::{json, Value};

use fastetcd_raft::{FastetcdLogEntry, TypeConfig};
use fastetcd_storage::mvcc::{Compare, CompareOp, CompareTarget, Mutation, NestedTxn, TxnOp};

/// The requests in `entries` (the log from its first entry), in order.
pub fn requests(entries: &[Entry<TypeConfig>]) -> Vec<Value> {
    let mut out = Vec::new();
    let mut leases = LeaseIds::default();
    for entry in entries {
        if let EntryPayload::Normal(e) = &entry.payload {
            push(e, &mut leases, &mut out);
        }
    }
    out
}

/// Lease ids as the store allocates them at apply
/// (`MvccStore::apply_lease_grant`): id 0 takes the counter's next
/// value; an explicit id above the counter moves it up.
#[derive(Default)]
struct LeaseIds {
    next: i64,
}

impl LeaseIds {
    fn grant(&mut self, id: i64) -> i64 {
        if id == 0 {
            self.next += 1;
            self.next
        } else {
            self.next = self.next.max(id);
            id
        }
    }
}

fn push(entry: &FastetcdLogEntry, leases: &mut LeaseIds, out: &mut Vec<Value>) {
    match entry {
        FastetcdLogEntry::Apply { mutations } => match mutations.as_slice() {
            [] => {}
            [one] => out.push(mutation(one)),
            many => out.push(json!({ "txn": {
                "compare": [],
                "success": many.iter().map(mutation).collect::<Vec<_>>(),
                "failure": [],
            }})),
        },
        FastetcdLogEntry::Txn { compares, success, failure } => {
            if writes(success) || writes(failure) {
                out.push(json!({ "txn": txn(compares, success, failure) }));
            }
        }
        FastetcdLogEntry::Compact { rev } => out.push(json!({ "compact": { "revision": rev } })),
        FastetcdLogEntry::LeaseGrant { id, ttl_secs, .. } => {
            // A TTL the store refuses allocates nothing (the server
            // raises a TTL <= 0 before proposing, so none is logged).
            if *ttl_secs > 0 {
                out.push(json!({ "lease_grant": { "id": leases.grant(*id) } }));
            }
        }
        FastetcdLogEntry::LeaseRevoke { id } => out.push(json!({ "lease_revoke": { "id": id } })),
        FastetcdLogEntry::Batch(entries) => {
            for e in entries {
                push(e, leases, out);
            }
        }
        FastetcdLogEntry::LeaseKeepAlive { .. }
        | FastetcdLogEntry::Noop
        | FastetcdLogEntry::Auth(_)
        | FastetcdLogEntry::ClusterId(_)
        | FastetcdLogEntry::StartRevision
        | FastetcdLogEntry::PutVersions => {}
    }
}

/// Whether any op (nested txns included) writes.
fn writes(ops: &[TxnOp]) -> bool {
    ops.iter().any(|op| match op {
        TxnOp::Range(_) => false,
        TxnOp::Mutation(_) => true,
        TxnOp::Txn(t) => writes(&t.success) || writes(&t.failure),
    })
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn mutation(m: &Mutation) -> Value {
    match m {
        Mutation::Put { key, value, lease, .. } => {
            json!({ "put": { "key": b64(key), "value": b64(value), "lease": lease } })
        }
        Mutation::DeleteRange { key, range_end, .. } => {
            json!({ "delete": { "key": b64(key), "range_end": b64(range_end) } })
        }
    }
}

fn txn(compares: &[Compare], success: &[TxnOp], failure: &[TxnOp]) -> Value {
    json!({
        "compare": compares.iter().map(compare).collect::<Vec<_>>(),
        "success": success.iter().map(op).collect::<Vec<_>>(),
        "failure": failure.iter().map(op).collect::<Vec<_>>(),
    })
}

fn op(op: &TxnOp) -> Value {
    match op {
        TxnOp::Range(r) => json!({ "range": {
            "key": b64(&r.key),
            "range_end": b64(&r.range_end),
            "limit": r.limit,
        }}),
        TxnOp::Mutation(m) => mutation(m),
        TxnOp::Txn(NestedTxn { compares, success, failure }) => {
            json!({ "txn": txn(compares, success, failure) })
        }
    }
}

fn compare(c: &Compare) -> Value {
    let result = match c.op {
        CompareOp::Equal => "equal",
        CompareOp::NotEqual => "not_equal",
        CompareOp::Greater => "greater",
        CompareOp::Less => "less",
    };
    let (target, value) = match &c.target {
        CompareTarget::Version(v) => ("version", json!(v)),
        CompareTarget::CreateRevision(v) => ("create", json!(v)),
        CompareTarget::ModRevision(v) => ("mod", json!(v)),
        CompareTarget::Value(v) => ("value", json!(b64(v))),
        CompareTarget::Lease(v) => ("lease", json!(v)),
    };
    json!({
        "key": b64(&c.key),
        "range_end": b64(&c.range_end),
        "target": target,
        "result": result,
        "value": value,
    })
}

/// Print the requests of the member in `data_dir`, one JSON object per
/// line.
pub fn cmd_persisted_requests(data_dir: &std::path::Path) -> anyhow::Result<()> {
    use std::io::Write;
    let entries = fastetcd_raft::wal_log_store::read_history(data_dir)?;
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    for r in requests(&entries) {
        serde_json::to_writer(&mut out, &r)?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastetcd_storage::mvcc::RangeOp;
    use openraft::{CommittedLeaderId, LogId};

    fn entry(index: u64, payload: EntryPayload<TypeConfig>) -> Entry<TypeConfig> {
        Entry { log_id: LogId::new(CommittedLeaderId::new(1, 1), index), payload }
    }

    fn normal(index: u64, e: FastetcdLogEntry) -> Entry<TypeConfig> {
        entry(index, EntryPayload::Normal(e))
    }

    fn put(key: &[u8], lease: i64) -> Mutation {
        Mutation::Put {
            key: key.to_vec(),
            value: b"v".to_vec(),
            lease,
            ignore_value: false,
            ignore_lease: false,
            prev_kv: false,
        }
    }

    fn range(key: &[u8]) -> TxnOp {
        TxnOp::Range(RangeOp { key: key.to_vec(), ..Default::default() })
    }

    #[test]
    fn requests_come_in_log_order_in_the_suites_shapes() {
        let log = vec![
            entry(0, EntryPayload::Blank),
            normal(1, FastetcdLogEntry::StartRevision),
            normal(2, FastetcdLogEntry::Apply { mutations: vec![put(b"a", 0)] }),
            normal(
                3,
                FastetcdLogEntry::Batch(vec![
                    FastetcdLogEntry::Apply {
                        mutations: vec![Mutation::DeleteRange {
                            key: b"a".to_vec(),
                            range_end: vec![],
                            prev_kv: false,
                        }],
                    },
                    FastetcdLogEntry::LeaseKeepAlive { id: 9, now_unix: 0 },
                    FastetcdLogEntry::Compact { rev: 3 },
                ]),
            ),
            normal(
                4,
                FastetcdLogEntry::Txn {
                    compares: vec![Compare {
                        key: b"k".to_vec(),
                        range_end: vec![],
                        op: CompareOp::Equal,
                        target: CompareTarget::ModRevision(2),
                    }],
                    success: vec![TxnOp::Mutation(put(b"k", 0))],
                    failure: vec![range(b"k")],
                },
            ),
            normal(5, FastetcdLogEntry::Noop),
        ];
        let got = requests(&log);
        let k = b64(b"k");
        assert_eq!(
            got,
            vec![
                json!({ "put": { "key": b64(b"a"), "value": b64(b"v"), "lease": 0 } }),
                json!({ "delete": { "key": b64(b"a"), "range_end": "" } }),
                json!({ "compact": { "revision": 3 } }),
                json!({ "txn": {
                    "compare": [{ "key": k, "range_end": "", "target": "mod", "result": "equal", "value": 2 }],
                    "success": [{ "put": { "key": k, "value": b64(b"v"), "lease": 0 } }],
                    "failure": [{ "range": { "key": k, "range_end": "", "limit": 0 } }],
                }}),
            ]
        );
    }

    #[test]
    fn read_only_txns_are_left_out_as_etcd_never_logs_them() {
        let nested_read = TxnOp::Txn(NestedTxn { compares: vec![], success: vec![range(b"x")], failure: vec![] });
        let nested_write =
            TxnOp::Txn(NestedTxn { compares: vec![], success: vec![], failure: vec![TxnOp::Mutation(put(b"x", 0))] });
        let log = vec![
            normal(1, FastetcdLogEntry::Txn { compares: vec![], success: vec![range(b"a")], failure: vec![] }),
            normal(2, FastetcdLogEntry::Txn { compares: vec![], success: vec![nested_read], failure: vec![] }),
            normal(3, FastetcdLogEntry::Txn { compares: vec![], success: vec![], failure: vec![nested_write] }),
        ];
        let got = requests(&log);
        assert_eq!(got.len(), 1, "{got:?}");
        assert!(got[0]["txn"]["failure"][0]["txn"].is_object());
    }

    #[test]
    fn lease_grants_carry_the_id_apply_gives_them() {
        let grant = |id| FastetcdLogEntry::LeaseGrant { id, ttl_secs: 10, now_unix: 0 };
        let log = vec![
            normal(1, grant(0)),
            normal(2, grant(0)),
            normal(3, grant(100)),
            normal(4, FastetcdLogEntry::Batch(vec![grant(0), grant(50)])),
            normal(5, grant(0)),
            normal(6, FastetcdLogEntry::LeaseRevoke { id: 2 }),
        ];
        let ids: Vec<Value> = requests(&log)
            .iter()
            .map(|r| r.get("lease_grant").or(r.get("lease_revoke")).unwrap()["id"].clone())
            .collect();
        assert_eq!(ids, vec![json!(1), json!(2), json!(100), json!(101), json!(50), json!(102), json!(2)]);
    }

    /// The ids worked out here are the ones the store gives.
    #[tokio::test]
    async fn lease_ids_match_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let engine = fastetcd_storage::redb_engine::RedbEngine::open(dir.path().join("t.redb")).unwrap();
        let mvcc = fastetcd_storage::mvcc::MvccStore::open(std::sync::Arc::new(engine)).await.unwrap();
        let mut ids = LeaseIds::default();
        for id in [0, 0, 7, 3, 0, 20, 0] {
            let applied = mvcc.apply_lease_grant(id, 10, 0).await.unwrap().id;
            assert_eq!(ids.grant(id), applied, "grant of id {id}");
        }
    }
}
