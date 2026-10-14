//! A Range's sort and revision filters, as etcd applies them
//! (fastetcd#154): `sort_order`, `sort_target`, `min_mod_revision`,
//! `max_mod_revision`, `min_create_revision`, `max_create_revision`.
//!
//! etcd's `executeRange` (v3.6 `server/etcdserver/txn/range.go`): with a
//! sort or any filter, the store is read with no limit; records outside
//! the min/max revisions are dropped; what is left is sorted by the
//! target; then it is cut to `limit`, with `more` set if anything was
//! cut. `count` is the store's, before the filters. clientv3's
//! `concurrency.Mutex` and `Election` find their owner this way
//! (`WithFirstCreate`, `WithLastCreate` + `WithMaxCreateRev`), so without
//! it a lock was not mutually exclusive.
//!
//! Applied in the server to the store's result, the same for a local
//! read, a read forwarded to the leader and a Range in a Txn: nothing in
//! the storage, the raft log or the peer protocol changes.

use std::cmp::Ordering;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_storage::mvcc::{KvRecord, RangeResult};

use pb::range_request::{SortOrder, SortTarget};

/// The order etcd sorts in for `req`, or `None` for the store's own
/// (ascending key) order: NONE with a target other than KEY means
/// ascending, and KEY ascending is the store's order already.
fn effective_order(req: &pb::RangeRequest) -> Option<(SortTarget, bool)> {
    let target = SortTarget::try_from(req.sort_target).unwrap_or(SortTarget::Key);
    let order = SortOrder::try_from(req.sort_order).unwrap_or(SortOrder::None);
    match (target, order) {
        (SortTarget::Key, SortOrder::None | SortOrder::Ascend) => None,
        (t, SortOrder::None | SortOrder::Ascend) => Some((t, true)),
        (t, SortOrder::Descend) => Some((t, false)),
    }
}

fn filters(req: &pb::RangeRequest) -> bool {
    req.min_mod_revision != 0
        || req.max_mod_revision != 0
        || req.min_create_revision != 0
        || req.max_create_revision != 0
}

/// Whether `req` needs the whole range read and then processed here.
/// A count-only read does not: the count is taken before the filters.
pub fn applies(req: &pb::RangeRequest) -> bool {
    !req.count_only && (effective_order(req).is_some() || filters(req))
}

/// The limit to read the store with: none when this module will sort or
/// filter, else the request's.
pub fn store_limit(req: &pb::RangeRequest) -> usize {
    if applies(req) {
        0
    } else {
        req.limit.max(0) as usize
    }
}

fn compare(target: SortTarget, a: &KvRecord, b: &KvRecord) -> Ordering {
    match target {
        SortTarget::Key => a.key.cmp(&b.key),
        SortTarget::Version => a.version.cmp(&b.version),
        SortTarget::Create => a.create_revision.cmp(&b.create_revision),
        SortTarget::Mod => a.mod_revision.cmp(&b.mod_revision),
        SortTarget::Value => a.value.cmp(&b.value),
    }
}

/// Apply `req`'s filters, sort and limit to `result`, read with
/// [`store_limit`]. A request this module does not apply to is returned
/// as it is.
pub fn apply(req: &pb::RangeRequest, mut result: RangeResult) -> RangeResult {
    if !applies(req) {
        return result;
    }
    let keep = |kv: &KvRecord| {
        (req.max_mod_revision == 0 || kv.mod_revision <= req.max_mod_revision)
            && (req.min_mod_revision == 0 || kv.mod_revision >= req.min_mod_revision)
            && (req.max_create_revision == 0 || kv.create_revision <= req.max_create_revision)
            && (req.min_create_revision == 0 || kv.create_revision >= req.min_create_revision)
    };
    result.kvs.retain(keep);
    if let Some((target, ascending)) = effective_order(req) {
        // Stable: equal records keep the store's key order.
        if ascending {
            result.kvs.sort_by(|a, b| compare(target, a, b));
        } else {
            result.kvs.sort_by(|a, b| compare(target, b, a));
        }
    }
    let limit = req.limit.max(0) as usize;
    result.more = limit > 0 && result.kvs.len() > limit;
    if result.more {
        result.kvs.truncate(limit);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(key: &str, create: i64, modr: i64, version: i64, value: &str) -> KvRecord {
        KvRecord {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            create_revision: create,
            mod_revision: modr,
            version,
            lease: 0,
            deleted: false,
        }
    }

    /// Keys whose key order is not their creation order, as lock keys
    /// named by lease id are.
    fn store() -> RangeResult {
        RangeResult {
            kvs: vec![
                kv("l/1", 30, 31, 2, "c"),
                kv("l/2", 10, 40, 3, "a"),
                kv("l/3", 20, 20, 1, "b"),
            ],
            more: false,
            count: 3,
        }
    }

    fn keys(r: &RangeResult) -> Vec<String> {
        r.kvs.iter().map(|k| String::from_utf8(k.key.clone()).unwrap()).collect()
    }

    fn req(f: impl FnOnce(&mut pb::RangeRequest)) -> pb::RangeRequest {
        let mut r = pb::RangeRequest { key: b"l/".to_vec(), range_end: b"l0".to_vec(), ..Default::default() };
        f(&mut r);
        r
    }

    #[test]
    fn first_create_is_the_oldest_key_not_the_first_by_name() {
        // clientv3 WithFirstCreate: sort CREATE ascending, limit 1.
        let r = req(|r| {
            r.sort_target = SortTarget::Create as i32;
            r.sort_order = SortOrder::Ascend as i32;
            r.limit = 1;
        });
        assert_eq!(store_limit(&r), 0);
        let out = apply(&r, store());
        assert_eq!((keys(&out), out.more, out.count), (vec!["l/2".into()], true, 3));
    }

    #[test]
    fn last_create_below_a_revision_is_the_one_to_wait_for() {
        // clientv3 waitDeletes: WithLastCreate + WithMaxCreateRev(myRev - 1).
        let r = req(|r| {
            r.sort_target = SortTarget::Create as i32;
            r.sort_order = SortOrder::Descend as i32;
            r.limit = 1;
            r.max_create_revision = 29;
        });
        let out = apply(&r, store());
        // l/3 (20) and l/2 (10) are below 29: the newest of them, and
        // `more` for the one cut off, as etcd's limit sets it.
        assert_eq!(keys(&out), vec!["l/3".to_string()]);
        assert!(out.more);
    }

    #[test]
    fn every_target_sorts_both_ways() {
        for (target, asc) in [
            (SortTarget::Version, vec!["l/3", "l/1", "l/2"]),
            (SortTarget::Create, vec!["l/2", "l/3", "l/1"]),
            (SortTarget::Mod, vec!["l/3", "l/1", "l/2"]),
            (SortTarget::Value, vec!["l/2", "l/3", "l/1"]),
            (SortTarget::Key, vec!["l/1", "l/2", "l/3"]),
        ] {
            let up = apply(&req(|r| { r.sort_target = target as i32; r.sort_order = SortOrder::Ascend as i32 }), store());
            assert_eq!(keys(&up), asc, "{target:?} ascend");
            let down = apply(&req(|r| { r.sort_target = target as i32; r.sort_order = SortOrder::Descend as i32 }), store());
            let mut desc = asc.clone();
            desc.reverse();
            assert_eq!(keys(&down), desc, "{target:?} descend");
        }
    }

    #[test]
    fn none_with_a_target_is_ascending_and_key_ascending_is_the_store_order() {
        let none_mod = req(|r| r.sort_target = SortTarget::Mod as i32);
        assert_eq!(keys(&apply(&none_mod, store())), vec!["l/3", "l/1", "l/2"]);
        let key_asc = req(|r| { r.sort_order = SortOrder::Ascend as i32; r.limit = 2 });
        assert!(!applies(&key_asc));
        assert_eq!(store_limit(&key_asc), 2);
    }

    #[test]
    fn mod_and_create_filters_drop_records_and_count_stays_the_stores() {
        let r = req(|r| {
            r.min_mod_revision = 25;
            r.max_create_revision = 30;
        });
        let out = apply(&r, store());
        assert_eq!(keys(&out), vec!["l/1", "l/2"]);
        assert_eq!(out.count, 3, "etcd's count is before the filters");
        let r = req(|r| { r.min_create_revision = 15; r.max_mod_revision = 31 });
        assert_eq!(keys(&apply(&r, store())), vec!["l/1", "l/3"]);
    }

    #[test]
    fn a_count_only_read_is_left_alone() {
        let r = req(|r| { r.count_only = true; r.sort_target = SortTarget::Create as i32; r.limit = 1 });
        assert!(!applies(&r));
        assert_eq!(store_limit(&r), 1);
    }
}
