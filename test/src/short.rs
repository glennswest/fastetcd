//! `short` (< 2 min): fastetcd is up on the node and does its main job,
//! and the commit's own binary runs on this machine.
//!
//! The node's fastetcd is rustkube's live store. These checks only read
//! the cluster-wide state (health, status, alarms) and write keys under
//! `/storm-test/<run id>/`, removed at the end, and one lease of their
//! own, revoked. Nothing cluster-wide is changed: no compaction,
//! defragment, alarm, auth or membership change, ever.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::client::{next_events, Clients};
use crate::env::Env;
use crate::member::Member;
use crate::report::{fail, pass, Outcome, Report};
use fastetcd_proto::mvccpb::event::EventType;

/// What `GET /health` on the node's client port found.
enum Health {
    /// An HTTP answer: its status line and body.
    Http(String),
    /// The port took the connection but did not answer plain HTTP: TLS
    /// (stormcos#146 puts the client port on mutual TLS; the test gets no
    /// client certificate).
    NotPlain(String),
}

async fn health(host: &str, port: u16) -> anyhow::Result<Health> {
    let addr = format!("{host}:{port}");
    let mut s = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(&addr))
        .await
        .map_err(|_| anyhow::anyhow!("connecting to {addr}: timed out"))?
        .map_err(|e| anyhow::anyhow!("connecting to {addr}: {e}"))?;
    let req = format!("GET /health HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    if let Err(e) = s.write_all(req.as_bytes()).await {
        return Ok(Health::NotPlain(format!("write: {e}")));
    }
    let mut buf = Vec::new();
    match tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut buf)).await {
        Ok(Ok(_)) | Ok(Err(_)) if buf.starts_with(b"HTTP/1.") => {
            Ok(Health::Http(String::from_utf8_lossy(&buf).into_owned()))
        }
        Ok(Ok(_)) => Ok(Health::NotPlain(format!("{} bytes, not HTTP", buf.len()))),
        Ok(Err(e)) => Ok(Health::NotPlain(format!("read: {e}"))),
        Err(_) => Ok(Health::NotPlain("no answer within 5 s".into())),
    }
}

pub async fn run(env: &Env, rep: &mut Report) -> anyhow::Result<()> {
    node(env, rep).await?;
    rep.check("commit-binary-serves", commit_binary(env)).await;
    Ok(())
}

async fn node(env: &Env, rep: &mut Report) -> anyhow::Result<()> {
    const NODE_TESTS: [&str; 5] = ["node-health", "node-status", "node-kv-roundtrip", "node-watch", "node-lease"];
    let Some(host) = env.node.clone() else {
        for t in NODE_TESTS {
            rep.line(t, "skip", 0, "no STORM_NODE: no node to test", None);
        }
        return Ok(());
    };
    // Unreachable is the suite not being able to run (exit 2).
    let h = health(&host, env.node_port).await?;
    let body = match h {
        Health::Http(b) => b,
        Health::NotPlain(why) => {
            let d = format!(
                "{host}:{} does not answer plain HTTP/gRPC ({why}): a client port on mutual TLS \
                 (stormcos#146) needs a client certificate the test runner does not give",
                env.node_port
            );
            for t in NODE_TESTS {
                rep.line(t, "skip", 0, &d, None);
            }
            return Ok(());
        }
    };
    rep.check("node-health", async {
        let status_ok = body.starts_with("HTTP/1.1 200") || body.starts_with("HTTP/1.0 200");
        Ok(if status_ok && body.contains("\"health\":\"true\"") {
            pass("GET /health: {\"health\":\"true\"}")
        } else {
            fail(format!("GET /health: {}", body.lines().next().unwrap_or("")))
        })
    })
    .await;

    let endpoint = format!("http://{host}:{}", env.node_port);
    let c = Clients::connect(&endpoint).await?;
    rep.check("node-status", node_status(c.clone())).await;
    let prefix = env.prefix();
    rep.check("node-kv-roundtrip", node_kv(c.clone(), prefix.clone())).await;
    rep.check("node-watch", node_watch(c.clone(), prefix.clone())).await;
    rep.check("node-lease", node_lease(c.clone(), prefix.clone())).await;
    // Whatever happened above, leave nothing behind.
    rep.check("node-cleanup", async move {
        let mut c = c;
        c.delete_prefix(prefix.as_bytes()).await?;
        let left = c.count(prefix.as_bytes()).await?;
        Ok(if left == 0 { pass(format!("no key left under {prefix}")) } else { fail(format!("{left} keys left under {prefix}")) })
    })
    .await;
    Ok(())
}

async fn node_status(mut c: Clients) -> anyhow::Result<Outcome> {
    let st = c.status().await?;
    if st.leader == 0 {
        return Ok(fail(format!("no leader (version {}, raft index {})", st.version, st.raft_index)));
    }
    let alarms = c.alarms().await?;
    if !alarms.is_empty() {
        let names: Vec<String> = alarms.iter().map(|a| format!("{:?} on {:x}", a.alarm(), a.member_id)).collect();
        return Ok(fail(format!("alarms raised: {}", names.join(", "))));
    }
    Ok(pass(format!(
        "version {}, leader {:x}, raft index {}, db {} bytes, no alarms",
        st.version, st.leader, st.raft_index, st.db_size
    )))
}

async fn node_kv(mut c: Clients, prefix: String) -> anyhow::Result<Outcome> {
    use fastetcd_proto::etcdserverpb as pb;
    use pb::compare::{CompareResult, CompareTarget, TargetUnion};
    let k = format!("{prefix}kv").into_bytes();
    let rev = c.put(&k, b"one").await?;
    if c.get(&k).await?.as_deref() != Some(&b"one"[..]) {
        return Ok(fail("a linearizable read did not return the value just written"));
    }
    // Compare-and-swap on mod_revision, as the apiserver updates objects.
    let cas = |r: i64, v: &'static [u8]| pb::TxnRequest {
        compare: vec![pb::Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Mod as i32,
            key: k.clone(),
            target_union: Some(TargetUnion::ModRevision(r)),
            ..Default::default()
        }],
        success: vec![pb::RequestOp {
            request: Some(pb::request_op::Request::RequestPut(pb::PutRequest {
                key: k.clone(),
                value: v.to_vec(),
                ..Default::default()
            })),
        }],
        failure: vec![],
    };
    let r = c.req(cas(rev, b"two"));
    if !c.kv.txn(r).await?.into_inner().succeeded {
        return Ok(fail("a CAS at the current mod_revision did not succeed"));
    }
    let r = c.req(cas(rev, b"stale"));
    if c.kv.txn(r).await?.into_inner().succeeded {
        return Ok(fail("a CAS at a stale mod_revision succeeded"));
    }
    for i in 0..3 {
        c.put(format!("{prefix}list/{i}").as_bytes(), b"x").await?;
    }
    let n = c.count(format!("{prefix}list/").as_bytes()).await?;
    let deleted = c.delete_prefix(format!("{prefix}list/").as_bytes()).await?;
    let v = c.get(&k).await?;
    Ok(if n == 3 && deleted == 3 && v.as_deref() == Some(&b"two"[..]) {
        pass("put, linearizable get, CAS (current succeeds, stale refused), prefix range and delete")
    } else {
        fail(format!("range counted {n}, delete removed {deleted}, value {v:?}"))
    })
}

async fn node_watch(mut c: Clients, prefix: String) -> anyhow::Result<Outcome> {
    let p = format!("{prefix}watch/");
    let (_tx, mut s) = c.watch_prefix(p.as_bytes(), 0).await?;
    let k = format!("{p}w").into_bytes();
    c.put(&k, b"v").await?;
    c.delete_prefix(p.as_bytes()).await?;
    let ev = next_events(&mut s, 2, Duration::from_secs(10)).await?;
    let kinds: Vec<EventType> = ev.iter().map(|e| e.r#type()).collect();
    Ok(if kinds == [EventType::Put, EventType::Delete] {
        pass("a prefix watch saw the PUT and the DELETE, in order")
    } else {
        fail(format!("events: {kinds:?}"))
    })
}

async fn node_lease(mut c: Clients, prefix: String) -> anyhow::Result<Outcome> {
    let id = c.grant(60).await?;
    let k = format!("{prefix}leased").into_bytes();
    let r = async {
        c.put_lease(&k, b"l", id).await?;
        let ttl = c.keep_alive_once(id).await?;
        let t = c.time_to_live(id).await?;
        anyhow::Ok((ttl, t))
    }
    .await;
    // The lease goes whatever happened, and its key with it.
    let revoked = c.revoke(id).await;
    let (ttl, t) = r?;
    revoked?;
    let gone = c.get(&k).await?.is_none();
    Ok(if ttl > 0 && t.keys == vec![k.clone()] && gone {
        pass(format!("granted, attached, kept alive (TTL {ttl}), listed its key, revoked: key gone"))
    } else {
        fail(format!("keep-alive TTL {ttl}, keys {:?}, key gone after revoke: {gone}", t.keys))
    })
}

/// The commit's own fastetcd (`/fastetcd` in the image) starts on this
/// machine and serves a write and a read.
async fn commit_binary(env: &Env) -> anyhow::Result<Outcome> {
    let mut m = Member::single(&env.bin, &env.work, "short", &[]).await?;
    let mut c = m.clients().await?;
    c.put(b"/k", b"v").await?;
    let got = c.get(b"/k").await?;
    let version = c.status().await?.version;
    m.kill().await;
    let _ = std::fs::remove_dir_all(&m.dir);
    Ok(if got.as_deref() == Some(&b"v"[..]) {
        pass(format!("{} (etcd API {version}) started on loopback, wrote and read a key", env.bin.display()))
    } else {
        fail(format!("read back {got:?}"))
    })
}
