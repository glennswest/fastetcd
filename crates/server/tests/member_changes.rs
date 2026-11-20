//! Member changes go through raft and reach every member (#208), on
//! three real `fastetcd` processes.
//!
//! - A learner removed through a follower leaves the raft membership:
//!   it is gone from every member's MemberList (it used to be dropped
//!   only from the answering member's local maps).
//! - A peer URL changed through a follower is the address every member
//!   dials: the member restarted on its new port still gets the writes,
//!   also from a leader that restarted with the old URL in
//!   `--initial-cluster` (it used to change only the answering member's
//!   map, so the leader kept dialing the old address).

use std::path::{Path, PathBuf};
use std::time::Duration;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::cluster_client::ClusterClient;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::maintenance_client::MaintenanceClient;
use tokio::process::{Child, Command};
use tokio::time::{sleep, Instant};

const WAIT: Duration = Duration::from_secs(30);

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Member {
    name: String,
    dir: PathBuf,
    client_port: u16,
    peer_port: u16,
    log: PathBuf,
    child: Option<Child>,
}

impl Member {
    fn client_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.client_port)
    }

    fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(40)..].join("\n")
    }

    /// Start (or restart) the member with `initial_cluster`, and wait for
    /// etcd's ready line.
    async fn start(&mut self, initial_cluster: &str) {
        let out = std::fs::OpenOptions::new().create(true).append(true).open(&self.log).unwrap();
        let ready_before = std::fs::read_to_string(&self.log).unwrap_or_default().matches("ready to serve").count();
        let peer = format!("http://127.0.0.1:{}", self.peer_port);
        let child = Command::new(env!("CARGO_BIN_EXE_fastetcd"))
            .arg("--name")
            .arg(&self.name)
            .arg("--data-dir")
            .arg(&self.dir)
            .arg("--listen-client-urls")
            .arg(self.client_url())
            .arg("--advertise-client-urls")
            .arg(self.client_url())
            .arg("--listen-peer-urls")
            .arg(&peer)
            .arg("--initial-advertise-peer-urls")
            .arg(&peer)
            .arg("--initial-cluster")
            .arg(initial_cluster)
            .arg("--listen-metrics-url")
            .arg("")
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .kill_on_drop(true)
            .spawn()
            .expect("spawn fastetcd");
        self.child = Some(child);
        let deadline = Instant::now() + WAIT;
        while std::fs::read_to_string(&self.log).unwrap_or_default().matches("ready to serve").count() <= ready_before {
            assert!(Instant::now() < deadline, "{} did not come up:\n{}", self.name, self.log_tail());
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            c.kill().await.unwrap();
            c.wait().await.unwrap();
        }
    }
}

async fn cluster(dir: &Path) -> (Vec<Member>, String) {
    let mut members: Vec<Member> = (1..=3)
        .map(|i| Member {
            name: format!("m{i}"),
            dir: dir.join(format!("m{i}")),
            client_port: free_port(),
            peer_port: free_port(),
            log: dir.join(format!("m{i}.log")),
            child: None,
        })
        .collect();
    let initial = members
        .iter()
        .map(|m| format!("{}=http://127.0.0.1:{}", m.name, m.peer_port))
        .collect::<Vec<_>>()
        .join(",");
    // All at once: each waits for the others to elect a leader.
    let starts: Vec<_> = members.iter_mut().map(|m| m.start(&initial)).collect();
    futures::future::join_all(starts).await;
    (members, initial)
}

async fn list(m: &Member) -> Vec<pb::Member> {
    let mut c = ClusterClient::connect(m.client_url()).await.unwrap();
    c.member_list(pb::MemberListRequest { linearizable: false }).await.unwrap().into_inner().members
}

/// The member id of each member, by index, and the index of the leader.
async fn ids_and_leader(members: &[Member]) -> (Vec<u64>, usize) {
    let all = list(&members[0]).await;
    let ids: Vec<u64> = members
        .iter()
        .map(|m| all.iter().find(|x| x.name == m.name).unwrap_or_else(|| panic!("{} not listed", m.name)).id)
        .collect();
    let deadline = Instant::now() + WAIT;
    loop {
        let mut mc = MaintenanceClient::connect(members[0].client_url()).await.unwrap();
        let leader = mc.status(pb::StatusRequest {}).await.unwrap().into_inner().leader;
        if let Some(i) = ids.iter().position(|id| *id == leader) {
            return (ids, i);
        }
        assert!(Instant::now() < deadline, "no leader");
        sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until `pred` holds for every member's MemberList.
async fn until_every_list(members: &[&Member], what: &str, pred: impl Fn(&[pb::Member]) -> bool) {
    let deadline = Instant::now() + WAIT;
    for m in members {
        loop {
            if pred(&list(m).await) {
                break;
            }
            assert!(Instant::now() < deadline, "{}'s MemberList never showed {what}: {:?}", m.name, list(m).await);
            sleep(Duration::from_millis(200)).await;
        }
    }
}

async fn put(m: &Member, key: &str) {
    let deadline = Instant::now() + WAIT;
    loop {
        let mut kv = KvClient::connect(m.client_url()).await.unwrap();
        let r = kv
            .put(pb::PutRequest { key: key.as_bytes().to_vec(), value: b"v".to_vec(), ..Default::default() })
            .await;
        if r.is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "put {key} on {}: {r:?}", m.name);
        sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until `m` has `key` in its own store (a serializable read).
async fn until_local(m: &Member, key: &str) {
    let deadline = Instant::now() + WAIT;
    loop {
        let mut kv = KvClient::connect(m.client_url()).await.unwrap();
        let r = kv
            .range(pb::RangeRequest { key: key.as_bytes().to_vec(), serializable: true, ..Default::default() })
            .await;
        if r.is_ok_and(|r| r.into_inner().count == 1) {
            return;
        }
        assert!(Instant::now() < deadline, "{} never got {key}:\n{}", m.name, m.log_tail());
        sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_learner_removed_through_a_follower_leaves_every_member() {
    let dir = tempfile::tempdir().unwrap();
    let (members, _) = cluster(dir.path()).await;
    let (_, leader) = ids_and_leader(&members).await;
    let follower = &members[(leader + 1) % 3];
    let mut c = ClusterClient::connect(follower.client_url()).await.unwrap();

    let added = c
        .member_add(pb::MemberAddRequest { peer_ur_ls: vec!["http://127.0.0.1:1".into()], is_learner: true })
        .await
        .unwrap()
        .into_inner()
        .member
        .unwrap();
    let all: Vec<&Member> = members.iter().collect();
    until_every_list(&all, "the learner", |l| l.iter().any(|m| m.id == added.id && m.is_learner)).await;

    c.member_remove(pb::MemberRemoveRequest { id: added.id }).await.expect("MemberRemove of a learner");
    until_every_list(&all, "the learner gone", |l| l.iter().all(|m| m.id != added.id) && l.len() == 3).await;

    let again = c.member_remove(pb::MemberRemoveRequest { id: added.id }).await.unwrap_err();
    assert_eq!(again.code(), tonic::Code::NotFound, "{again:?}");
    assert_eq!(again.message(), "etcdserver: member not found");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_url_updated_through_a_follower_is_where_every_member_dials() {
    let dir = tempfile::tempdir().unwrap();
    let (mut members, initial) = cluster(dir.path()).await;
    let (ids, leader) = ids_and_leader(&members).await;
    let follower = (leader + 1) % 3;
    let target = (leader + 2) % 3;

    // Move the target member's peer URL, through the follower.
    let new_port = free_port();
    let new_url = format!("http://127.0.0.1:{new_port}");
    let mut c = ClusterClient::connect(members[follower].client_url()).await.unwrap();
    let taken = format!("http://127.0.0.1:{}", members[leader].peer_port);
    let dup = c
        .member_update(pb::MemberUpdateRequest { id: ids[target], peer_ur_ls: vec![taken] })
        .await
        .unwrap_err();
    assert_eq!(dup.code(), tonic::Code::FailedPrecondition, "{dup:?}");
    let missing = c.member_update(pb::MemberUpdateRequest { id: 12345, peer_ur_ls: vec![new_url.clone()] }).await.unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound, "{missing:?}");
    c.member_update(pb::MemberUpdateRequest { id: ids[target], peer_ur_ls: vec![new_url.clone()] })
        .await
        .expect("MemberUpdate through a follower");
    let all: Vec<&Member> = members.iter().collect();
    until_every_list(&all, "the new URL", |l| {
        l.iter().any(|m| m.id == ids[target] && m.peer_ur_ls == vec![new_url.clone()])
    })
    .await;

    // The target now listens only on its new port (its --initial-cluster
    // still names the old one): the leader must dial the new address.
    members[target].kill().await;
    members[target].peer_port = new_port;
    members[target].start(&initial).await;
    put(&members[leader], "after-update").await;
    until_local(&members[target], "after-update").await;

    // A restart of the follower, with the old URL in --initial-cluster,
    // routes from the membership too: made leader, it reaches the target.
    members[follower].kill().await;
    members[follower].start(&initial).await;
    until_every_list(&[&members[follower]], "the new URL after restart", |l| {
        l.iter().any(|m| m.id == ids[target] && m.peer_ur_ls == vec![new_url.clone()])
    })
    .await;
    let deadline = Instant::now() + WAIT;
    loop {
        let (_, now_leader) = ids_and_leader(&members).await;
        if now_leader == follower {
            break;
        }
        let mut mc = MaintenanceClient::connect(members[now_leader].client_url()).await.unwrap();
        let _ = mc.move_leader(pb::MoveLeaderRequest { target_id: ids[follower] }).await;
        assert!(Instant::now() < deadline, "leadership never moved to {}", members[follower].name);
        sleep(Duration::from_millis(500)).await;
    }
    put(&members[follower], "after-restart").await;
    until_local(&members[target], "after-restart").await;
}
