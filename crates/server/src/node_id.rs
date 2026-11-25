//! Which raft id this member runs as (fastetcd#232).
//!
//! `MemberAdd` gives a new member the id [`from_peer_url`] of its first
//! peer URL, and records that URL as its address in the membership. A
//! bootstrap member (`--initial-cluster-state new`) is [`from_name`] of
//! its `--name`, as every member derives the others' ids from the
//! `--initial-cluster` names. Before #232 a member started with
//! `--initial-cluster-state existing` also took [`from_name`], so a member
//! added with `member add` and started as etcd documents ran under an id
//! the membership did not have, unless `--node-id` was given.
//!
//! etcd finds a joining member's id in the member list by its own peer
//! URL. fastetcd does the same without asking the peers:
//!
//! - `--node-id` given: that id;
//! - `new`: [`from_name`], as before;
//! - `existing`, with a membership in this member's own raft state (a
//!   restart): the member whose address is this member's advertised peer
//!   URL (it may have moved with `member update`); else the `--name`
//!   id if the membership has it (a bootstrap member restarted with
//!   `existing`); else the `--name` id, with a warning, as before;
//! - `existing`, no raft state yet (it is joining): [`from_peer_url`] of
//!   its advertised peer URL, the id `MemberAdd` gave the member it
//!   added at that URL.

use fastetcd_raft::NodeId;

/// FNV-1a of `s`, as a non-zero 63-bit id.
fn fnv1a(s: &str) -> NodeId {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash & 0x7FFF_FFFF_FFFF_FFFF).max(1)
}

/// The id of a member named `name` in `--initial-cluster`.
pub fn from_name(name: &str) -> NodeId {
    fnv1a(name)
}

/// The id `MemberAdd` gives a member whose first peer URL is `url`.
pub fn from_peer_url(url: &str) -> NodeId {
    fnv1a(url)
}

/// The id of the `--initial-cluster` entry `name=url`: this member's own
/// id when `url` is its advertised peer URL (a member joining with
/// `existing` lists itself under its name, but runs as the id `member
/// add` gave it), else [`from_name`].
pub fn of_listed(name: &str, url: &str, own_id: NodeId, own_url: &str) -> NodeId {
    if same_url(url, own_url) {
        own_id
    } else {
        from_name(name)
    }
}

/// A peer URL compared as written, but for spaces and a trailing `/`.
fn same_url(a: &str, b: &str) -> bool {
    a.trim().trim_end_matches('/') == b.trim().trim_end_matches('/')
}

/// How the id was chosen, for the startup log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// `--node-id`.
    Flag,
    /// `--name`, for a member that bootstraps (`new`).
    Name,
    /// The member whose address is the advertised peer URL, in this
    /// member's own raft membership.
    Membership,
    /// `--name`, found in this member's own raft membership.
    NameInMembership,
    /// `--name`: the membership has neither this URL nor this name.
    NameNotInMembership,
    /// The advertised peer URL, as `MemberAdd` derives it (joining).
    PeerUrl,
}

/// This member's raft id. `advertised` is its first advertised peer URL
/// (`--initial-advertise-peer-urls`, else `--listen-peer-urls`), and
/// `membership` its own durable raft membership, if it has one: each
/// member's id and address.
pub fn resolve(
    flag: Option<NodeId>,
    name: &str,
    existing: bool,
    advertised: &str,
    membership: Option<&[(NodeId, String)]>,
) -> (NodeId, Source) {
    if let Some(id) = flag {
        return (id, Source::Flag);
    }
    if !existing {
        return (from_name(name), Source::Name);
    }
    match membership {
        Some(members) => {
            if let Some((id, _)) = members.iter().find(|(_, addr)| same_url(addr, advertised)) {
                return (*id, Source::Membership);
            }
            let by_name = from_name(name);
            if members.iter().any(|(id, _)| *id == by_name) {
                (by_name, Source::NameInMembership)
            } else {
                (by_name, Source::NameNotInMembership)
            }
        }
        None => (from_peer_url(advertised.trim()), Source::PeerUrl),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "http://10.0.0.4:2380";

    #[test]
    fn the_flag_wins_and_new_members_use_their_name() {
        assert_eq!(resolve(Some(7), "n4", true, URL, None), (7, Source::Flag));
        assert_eq!(resolve(None, "n1", false, URL, None), (from_name("n1"), Source::Name));
        // A bootstrap member's own data does not change it.
        let members = [(99, URL.to_string())];
        assert_eq!(resolve(None, "n1", false, URL, Some(&members)), (from_name("n1"), Source::Name));
    }

    /// fastetcd#232: a member joining after `member add` runs as the id
    /// MemberAdd gave the URL.
    #[test]
    fn a_joining_member_takes_the_id_member_add_gave_its_url() {
        let (id, source) = resolve(None, "n4", true, URL, None);
        assert_eq!(source, Source::PeerUrl);
        assert_eq!(id, crate::cluster::ClusterService::derive_node_id_from_url(URL));
        assert_ne!(id, from_name("n4"));
        assert_eq!(resolve(None, "n4", true, &format!(" {URL} "), None).0, id);
    }

    /// A restart reads the id from its own membership, by address: the
    /// URL may have moved (`member update`) since the id was given.
    #[test]
    fn a_restart_finds_itself_in_its_own_membership() {
        let members = [
            (from_name("n1"), "http://10.0.0.1:2380".to_string()),
            (from_peer_url("http://10.0.0.9:2380"), URL.to_string()),
        ];
        let (id, source) = resolve(None, "n4", true, &format!("{URL}/"), Some(&members));
        assert_eq!((id, source), (from_peer_url("http://10.0.0.9:2380"), Source::Membership));
        // A bootstrap member restarted with `existing` and another URL.
        assert_eq!(
            resolve(None, "n1", true, "http://n1.local:2380", Some(&members)),
            (from_name("n1"), Source::NameInMembership)
        );
        // `--initial-cluster` lists the joining member under its name, at
        // its URL: that entry is this member.
        assert_eq!(of_listed("n4", URL, 42, &format!("{URL}/")), 42);
        assert_eq!(of_listed("n1", "http://10.0.0.1:2380", 42, URL), from_name("n1"));
        // Neither: as before, by name.
        assert_eq!(
            resolve(None, "n7", true, "http://10.0.0.7:2380", Some(&members)),
            (from_name("n7"), Source::NameNotInMembership)
        );
    }
}
