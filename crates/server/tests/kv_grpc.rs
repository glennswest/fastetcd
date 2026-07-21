//! End-to-end gRPC test: spin up an in-process fastetcd server, drive
//! the KV service through a tonic gRPC client.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use openraft::{Config, Raft};
use tempfile::tempdir;
use tokio::time::sleep;
use tonic::transport::Channel;

use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::etcdserverpb::kv_client::KvClient;
use fastetcd_proto::etcdserverpb::kv_server::KvServer;
use fastetcd_raft::kv_log_store::KvLogStore;
use fastetcd_raft::types::{NodeId, TypeConfig};
use fastetcd_raft::FastetcdStateMachine;
use fastetcd_server::kv::KvService;
use fastetcd_server::ServerState;
use fastetcd_storage::mvcc::MvccStore;
use fastetcd_storage::redb_engine::RedbEngine;

#[derive(Clone)]
struct NopNet;

impl openraft::network::RaftNetworkFactory<TypeConfig> for NopNet {
    type Network = NopNetConn;
    async fn new_client(
        &mut self,
        _target: NodeId,
        _node: &openraft::BasicNode,
    ) -> Self::Network {
        NopNetConn
    }
}

struct NopNetConn;

fn err() -> openraft::error::RPCError<
    NodeId,
    openraft::BasicNode,
    openraft::error::RaftError<NodeId>,
> {
    openraft::error::RPCError::Network(openraft::error::NetworkError::new(&std::io::Error::other(
        "test: no peers",
    )))
}

impl openraft::network::RaftNetwork<TypeConfig> for NopNetConn {
    async fn append_entries(
        &mut self,
        _: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        Err(err())
    }
    async fn install_snapshot(
        &mut self,
        _: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            openraft::BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        Err(openraft::error::RPCError::Network(
            openraft::error::NetworkError::new(&std::io::Error::other(
                "test: no peers",
            )),
        ))
    }
    async fn vote(
        &mut self,
        _: openraft::raft::VoteRequest<NodeId>,
        _: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        Err(err())
    }
}

async fn wait_for_leader(raft: &Raft<TypeConfig>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut rx = raft.metrics();
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("did not become leader in 5s");
        }
        let m = rx.borrow_and_update().clone();
        if matches!(m.state, openraft::ServerState::Leader) {
            return;
        }
        tokio::time::timeout(Duration::from_millis(100), rx.changed())
            .await
            .ok();
    }
}

async fn start_test_server() -> (KvClient<Channel>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("kv.redb");
    let engine: Arc<dyn fastetcd_storage::KvStore> = Arc::new(RedbEngine::open(&path).unwrap());
    let mvcc = MvccStore::open(engine.clone()).await.unwrap();
    let sm = FastetcdStateMachine::open(mvcc, dir.path().join("snapshots")).await.unwrap();
    let log = KvLogStore::new(engine);
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 200,
            election_timeout_max: 500,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let raft = Raft::<TypeConfig>::new(1, config, NopNet, log.clone(), sm.clone())
        .await
        .unwrap();
    let mut members: BTreeSet<NodeId> = BTreeSet::new();
    members.insert(1);
    raft.initialize(members).await.unwrap();
    wait_for_leader(&raft).await;

    let auth_state = fastetcd_server::auth::AuthState::default();
    let forwarder = fastetcd_raft::WriteForwarder::new(fastetcd_raft::network::empty_peers());
    let state = Arc::new(ServerState::new(raft, sm, 7, 1, auth_state, forwarder));
    let _ = log;
    let kv = KvService::new(state);

    // Bind to an ephemeral port (async — tokio doesn't like
    // std::net listeners constructed outside its runtime).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServer::new(kv))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Give the server a moment to start accepting.
    sleep(Duration::from_millis(50)).await;
    let endpoint = format!("http://{addr}");
    let client = KvClient::connect(endpoint).await.unwrap();
    (client, dir)
}

#[tokio::test]
async fn put_then_range_round_trips() {
    let (mut client, _dir) = start_test_server().await;
    let put = client
        .put(pb::PutRequest {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let header = put.header.expect("put header");
    assert_eq!(header.cluster_id, 7);
    assert_eq!(header.member_id, 1);
    assert_eq!(header.revision, 1);

    let range = client
        .range(pb::RangeRequest {
            key: b"hello".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(range.kvs.len(), 1);
    assert_eq!(range.kvs[0].key, b"hello");
    assert_eq!(range.kvs[0].value, b"world");
    assert_eq!(range.kvs[0].create_revision, 1);
    assert_eq!(range.kvs[0].mod_revision, 1);
    assert_eq!(range.kvs[0].version, 1);
}

#[tokio::test]
async fn put_with_prev_kv_returns_prior_value() {
    let (mut client, _dir) = start_test_server().await;
    client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v0".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let put = client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            prev_kv: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let prev = put.prev_kv.expect("prev_kv");
    assert_eq!(prev.value, b"v0");
}

#[tokio::test]
async fn delete_range_returns_count_and_prev_kvs() {
    let (mut client, _dir) = start_test_server().await;
    for k in [b"k1", b"k2", b"k3"] {
        client
            .put(pb::PutRequest {
                key: k.to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    let del = client
        .delete_range(pb::DeleteRangeRequest {
            key: b"k1".to_vec(),
            range_end: b"k3".to_vec(),
            prev_kv: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(del.deleted, 2);
    assert_eq!(del.prev_kvs.len(), 2);
}

#[tokio::test]
async fn historical_range_returns_old_value() {
    let (mut client, _dir) = start_test_server().await;
    client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v0".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let r1 = client
        .range(pb::RangeRequest {
            key: b"k".to_vec(),
            revision: 1,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r1.kvs[0].value, b"v0");
}

#[tokio::test]
async fn txn_success_branch_runs_when_compare_holds() {
    let (mut client, _dir) = start_test_server().await;
    client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v0".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    let resp = client
        .txn(pb::TxnRequest {
            compare: vec![pb::Compare {
                key: b"k".to_vec(),
                result: pb::compare::CompareResult::Equal as i32,
                target: pb::compare::CompareTarget::Value as i32,
                target_union: Some(pb::compare::TargetUnion::Value(b"v0".to_vec())),
                ..Default::default()
            }],
            success: vec![pb::RequestOp {
                request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
                    key: b"k".to_vec(),
                    value: b"v1".to_vec(),
                    ..Default::default()
                })),
            }],
            failure: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.succeeded);
    let range = client
        .range(pb::RangeRequest {
            key: b"k".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(range.kvs[0].value, b"v1");
}

#[tokio::test]
async fn compact_then_old_revision_errors() {
    let (mut client, _dir) = start_test_server().await;
    client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v0".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .put(pb::PutRequest {
            key: b"k".to_vec(),
            value: b"v1".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .compact(pb::CompactionRequest {
            revision: 2,
            physical: false,
        })
        .await
        .unwrap();
    let err = client
        .range(pb::RangeRequest {
            key: b"k".to_vec(),
            revision: 1,
            ..Default::default()
        })
        .await
        .expect_err("revision 1 should be compacted");
    assert_eq!(err.code(), tonic::Code::OutOfRange);
}
