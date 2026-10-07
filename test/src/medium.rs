//! `medium` (~10 min, under the standard's 15): fastetcd's features and
//! failure paths, end to end, on members this suite starts itself from the
//! commit's binary. Nothing here touches the node's live store: compaction,
//! auth, alarms, crashes and leader loss are exactly what must never be
//! done to rustkube's datastore from a test.

use std::time::Duration;

use fastetcd_proto::authpb;
use fastetcd_proto::etcdserverpb as pb;
use fastetcd_proto::mvccpb::event::EventType;
use tokio::time::{sleep, Instant};
use tonic::Code;

use crate::client::{next_events, Clients};
use crate::env::Env;
use crate::member::{Cluster, Member};
use crate::report::{fail, pass, Outcome, Report};

pub async fn run(env: &Env, rep: &mut Report) -> anyhow::Result<()> {
    rep.check("history-watch-compaction", history(env)).await;
    rep.check("lease-expiry-and-keepalive", leases(env)).await;
    rep.check("auth-rbac", auth(env)).await;
    rep.check("nospace-alarm-and-recovery", nospace(env)).await;
    rep.check("snapshot-save-restore", snapshot_restore(env)).await;
    rep.check("sigkill-keeps-acknowledged-writes", sigkill(env)).await;
    rep.check("three-members-leader-loss", leader_loss(env)).await;
    Ok(())
}

/// Revisions, a watch replaying history with prev_kv, compaction, and
/// what reads and watches below the compacted revision get.
async fn history(env: &Env) -> anyhow::Result<Outcome> {
    let mut m = Member::single(&env.bin, &env.work, "history", &[]).await?;
    let mut c = m.clients().await?;
    let mut revs = Vec::new();
    for i in 0..5 {
        revs.push(c.put(b"/h/k", format!("v{i}").as_bytes()).await?);
    }
    // From the first revision: all five puts, each with its previous value.
    let (_tx, mut s) = c.watch_prefix(b"/h/", revs[0]).await?;
    let ev = next_events(&mut s, 5, Duration::from_secs(10)).await?;
    let replay_ok = ev.iter().all(|e| e.r#type() == EventType::Put)
        && ev.iter().skip(1).all(|e| e.prev_kv.is_some())
        && ev.last().and_then(|e| e.kv.as_ref()).map(|kv| kv.value.clone()) == Some(b"v4".to_vec());
    if !replay_ok {
        m.kill().await;
        return Ok(fail(format!("history replay: {} events, not 5 PUTs with prev_kv ending at v4", ev.len())));
    }
    c.compact(revs[3]).await?;
    let old = c
        .kv
        .range(pb::RangeRequest { key: b"/h/k".to_vec(), revision: revs[1], ..Default::default() })
        .await;
    let at_compacted = c
        .kv
        .range(pb::RangeRequest { key: b"/h/k".to_vec(), revision: revs[3], ..Default::default() })
        .await?
        .into_inner();
    let watch_old = c.watch_prefix(b"/h/", revs[1]).await;
    m.kill().await;
    let range_refused = matches!(&old, Err(s) if s.code() == Code::OutOfRange && s.message().contains("compacted"));
    let watch_refused = matches!(&watch_old, Err(e) if e.to_string().contains("compact_revision"));
    Ok(if range_refused && watch_refused && at_compacted.kvs.first().map(|kv| kv.value.clone()) == Some(b"v3".to_vec()) {
        pass("a watch from the first revision replays 5 PUTs with prev_kv; after compaction a read below it is OutOfRange, a watch below it is cancelled with compact_revision, and the compacted revision still reads")
    } else {
        fail(format!(
            "range below compaction: {:?}; watch below compaction: {}; read at compaction: {:?}",
            old.err().map(|s| (s.code(), s.message().to_string())),
            if watch_refused { "cancelled".to_string() } else { "not cancelled".to_string() },
            at_compacted.kvs.first().map(|kv| String::from_utf8_lossy(&kv.value).into_owned())
        ))
    })
}

/// A key on a 2 s lease goes once the lease lapses; keep-alives hold it.
async fn leases(env: &Env) -> anyhow::Result<Outcome> {
    let mut m = Member::single(&env.bin, &env.work, "leases", &[]).await?;
    let mut c = m.clients().await?;
    let lapsing = c.grant(2).await?;
    c.put_lease(b"/l/lapses", b"x", lapsing).await?;
    let kept = c.grant(2).await?;
    c.put_lease(b"/l/kept", b"x", kept).await?;
    // Keep `kept` alive for 6 s, three times its TTL.
    let t = Instant::now();
    let mut lapsed_at = None;
    while t.elapsed() < Duration::from_secs(6) {
        c.keep_alive_once(kept).await?;
        if lapsed_at.is_none() && c.get(b"/l/lapses").await?.is_none() {
            lapsed_at = Some(t.elapsed());
        }
        sleep(Duration::from_millis(500)).await;
    }
    let kept_there = c.get(b"/l/kept").await?.is_some();
    let gone_ttl = c.keep_alive_once(lapsing).await?;
    m.kill().await;
    Ok(match (lapsed_at, kept_there, gone_ttl) {
        (Some(at), true, 0) => pass(format!(
            "the 2 s lease's key was gone after {:.1} s; the kept-alive one is there after 6 s; a keep-alive of the lapsed lease answers TTL 0",
            at.as_secs_f64()
        )),
        _ => fail(format!("lapsed at {lapsed_at:?}, kept key there: {kept_there}, keep-alive of the lapsed lease: TTL {gone_ttl}")),
    })
}

/// RBAC with auth on: a user scoped to `/app/` writes there and nowhere
/// else, and cannot revoke a lease holding a key it cannot write (#47).
async fn auth(env: &Env) -> anyhow::Result<Outcome> {
    let mut m = Member::single(&env.bin, &env.work, "auth", &[]).await?;
    let mut c = m.clients().await?;
    let pw = "test-only"; // not a secret: test fixture
    for role in ["root", "app"] {
        c.auth.role_add(pb::AuthRoleAddRequest { name: role.into() }).await?;
    }
    c.auth
        .role_grant_permission(pb::AuthRoleGrantPermissionRequest {
            name: "app".into(),
            perm: Some(authpb::Permission { perm_type: 2, key: b"/app/".to_vec(), range_end: b"/app0".to_vec() }),
        })
        .await?;
    for (user, role) in [("root", "root"), ("alice", "app")] {
        c.auth
            .user_add(pb::AuthUserAddRequest { name: user.into(), password: pw.into(), ..Default::default() })
            .await?;
        c.auth.user_grant_role(pb::AuthUserGrantRoleRequest { user: user.into(), role: role.into() }).await?;
    }
    let lease = c.grant(600).await?;
    c.put_lease(b"/other/k", b"x", lease).await?;
    c.auth.auth_enable(pb::AuthEnableRequest {}).await?;
    let login = |user: &'static str| {
        let mut a = c.auth.clone();
        async move {
            anyhow::Ok(
                a.authenticate(pb::AuthenticateRequest { name: user.into(), password: pw.into() })
                    .await?
                    .into_inner()
                    .token,
            )
        }
    };
    let (root, alice) = (login("root").await?, login("alice").await?);
    let mut a = c.as_user(&alice);
    let in_scope = a.put(b"/app/k", b"v").await;
    let out_of_scope = a.put(b"/other/x", b"v").await;
    let anonymous = c.put(b"/app/anon", b"v").await;
    let revoke = a.revoke(lease).await;
    let mut r = c.as_user(&root);
    let req = r.req(pb::AuthDisableRequest {});
    r.auth.auth_disable(req).await?;
    m.kill().await;
    let denied = |x: &Result<i64, tonic::Status>| matches!(x, Err(s) if s.code() == Code::PermissionDenied);
    let unauth = matches!(&anonymous, Err(s) if s.code() == Code::Unauthenticated || s.code() == Code::InvalidArgument || s.code() == Code::PermissionDenied);
    let revoke_denied = matches!(&revoke, Err(s) if s.code() == Code::PermissionDenied);
    Ok(if in_scope.is_ok() && denied(&out_of_scope) && unauth && revoke_denied {
        pass("alice writes /app/ and is denied /other/; no token is refused; revoking a lease that holds /other/k is denied; root disables auth")
    } else {
        fail(format!(
            "in scope: {:?}; out of scope: {:?}; no token: {:?}; revoke: {:?}",
            in_scope.map_err(|s| s.code()),
            out_of_scope.map_err(|s| s.code()),
            anonymous.map_err(|s| s.code()),
            revoke.map_err(|s| s.code())
        ))
    })
}

/// A member on a small quota raises NOSPACE and refuses writes while
/// reads and deletes still work; after delete, compact and defragment the
/// alarm is disarmed and writes work again (#14).
async fn nospace(env: &Env) -> anyhow::Result<Outcome> {
    let quota = 24 * 1024 * 1024;
    let q = format!("--quota-backend-bytes={quota}");
    let mut m = Member::single(&env.bin, &env.work, "quota", &[&q, "--space-check-interval-secs=1"]).await?;
    let mut c = m.clients().await?;
    let value = vec![b'q'; 128 * 1024];
    let mut written = 0;
    let mut refused = None;
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(120) && written < 800 {
        match c.put(format!("/q/{written:05}").as_bytes(), &value).await {
            Ok(_) => written += 1,
            Err(s) if s.code() == Code::ResourceExhausted => {
                refused = Some(s.message().to_string());
                break;
            }
            Err(s) => return Err(s.into()),
        }
    }
    let Some(why) = refused else {
        m.kill().await;
        return Ok(fail(format!("{written} puts of 128 KiB on a {quota}-byte quota were never refused")));
    };
    let alarms = c.alarms().await?;
    let reads = c.count(b"/q/").await?;
    let deleted = c.delete_prefix(b"/q/").await?;
    let rev = c.status().await?.header.map_or(0, |h| h.revision);
    c.compact(rev).await?;
    c.defragment().await?;
    let id = c.status().await?.header.map_or(0, |h| h.member_id);
    c.maint
        .alarm(pb::AlarmRequest {
            action: pb::alarm_request::AlarmAction::Deactivate as i32,
            member_id: id,
            alarm: pb::AlarmType::Nospace as i32,
        })
        .await?;
    // Writes work again (the monitor may also clear it on its own).
    let t = Instant::now();
    let mut after = c.put(b"/q/after", b"x").await;
    while after.is_err() && t.elapsed() < Duration::from_secs(20) {
        sleep(Duration::from_millis(500)).await;
        after = c.put(b"/q/after", b"x").await;
    }
    m.kill().await;
    let nospace = alarms.iter().any(|a| a.alarm() == pb::AlarmType::Nospace);
    Ok(if nospace && reads == written as i64 && deleted == reads && after.is_ok() {
        pass(format!(
            "after {written} puts ({} MiB) writes were refused ({why}); NOSPACE raised; reads and the delete worked; after compact, defragment and disarm a write succeeded",
            written * 128 / 1024
        ))
    } else {
        fail(format!(
            "NOSPACE alarm: {nospace}; read {reads} of {written}; deleted {deleted}; write after recovery: {:?}",
            after.map_err(|s| s.message().to_string())
        ))
    })
}

/// A live snapshot (`etcdctl snapshot save`) restores with
/// `fastetcd restore` into a new data directory, keys and leases included
/// (#61, #41).
async fn snapshot_restore(env: &Env) -> anyhow::Result<Outcome> {
    let mut m = Member::single(&env.bin, &env.work, "snap", &[]).await?;
    let mut c = m.clients().await?;
    for i in 0..50 {
        c.put(format!("/s/{i:03}").as_bytes(), b"v").await?;
    }
    let lease = c.grant(600).await?;
    c.put_lease(b"/s/leased", b"l", lease).await?;
    let file = env.work.join("snap.db");
    let size = c.snapshot_to(&file).await?;
    m.kill().await;

    let mut restored = Member::prepare(&env.bin, &env.work.join("restored"), "snap", &[])?;
    let out = tokio::process::Command::new(&env.bin)
        .arg(format!("--data-dir={}", restored.data_dir().display()))
        .arg("restore")
        .arg(&file)
        .output()
        .await?;
    if !out.status.success() {
        return Ok(fail(format!("fastetcd restore failed: {}", String::from_utf8_lossy(&out.stderr).trim())));
    }
    restored.start().await?;
    restored.ready(Duration::from_secs(60)).await?;
    let mut r = restored.clients().await?;
    let n = r.count(b"/s/").await?;
    let ttl = r.time_to_live(lease).await?;
    restored.kill().await;
    Ok(if n == 51 && ttl.ttl > 0 && ttl.keys == vec![b"/s/leased".to_vec()] {
        pass(format!("a {size}-byte live snapshot restored: 51 keys, the lease and its key"))
    } else {
        fail(format!("restored {n} of 51 keys; lease TTL {}, keys {:?}", ttl.ttl, ttl.keys))
    })
}

/// SIGKILL under write load loses no acknowledged write.
async fn sigkill(env: &Env) -> anyhow::Result<Outcome> {
    const WRITERS: usize = 16;
    let mut m = Member::single(&env.bin, &env.work, "crash", &[]).await?;
    let c = m.clients().await?;
    let stop = Instant::now() + Duration::from_secs(4);
    let tasks: Vec<_> = (0..WRITERS)
        .map(|w| {
            let mut c = c.clone();
            tokio::spawn(async move {
                let mut acked = 0usize;
                while Instant::now() < stop {
                    if c.put(format!("/d/{w:02}/{acked:06}").as_bytes(), b"v").await.is_err() {
                        break;
                    }
                    acked += 1;
                }
                acked
            })
        })
        .collect();
    // Kill while the writers are still writing.
    sleep(Duration::from_millis(3500)).await;
    m.kill().await;
    let mut acked = Vec::new();
    for t in tasks {
        acked.push(t.await?);
    }
    m.start().await?;
    m.ready(Duration::from_secs(60)).await?;
    let mut c = m.clients().await?;
    let mut lost = Vec::new();
    for (w, &n) in acked.iter().enumerate() {
        if n > 0 && c.get(format!("/d/{w:02}/{:06}", n - 1).as_bytes()).await?.is_none() {
            lost.push(w);
        }
        let have = c.count(format!("/d/{w:02}/").as_bytes()).await?;
        if (have as usize) < n {
            lost.push(w);
        }
    }
    m.kill().await;
    let total: usize = acked.iter().sum();
    Ok(if lost.is_empty() && total > 0 {
        pass(format!("{total} acknowledged writes from {WRITERS} writers, SIGKILL mid-load, all there after restart"))
    } else {
        fail(format!("{total} acknowledged; writers missing acknowledged writes after restart: {lost:?}"))
    })
}

/// Three members: writes through a follower, linearizable reads on
/// another, the leader SIGKILLed, a new leader, nothing acknowledged lost,
/// and the old leader rejoins and catches up.
async fn leader_loss(env: &Env) -> anyhow::Result<Outcome> {
    let mut cl = Cluster::start(&env.bin, &env.work, "c", 3).await?;
    let l = cl.leader(Duration::from_secs(30)).await?;
    let (f1, f2) = match l {
        0 => (1, 2),
        1 => (0, 2),
        _ => (0, 1),
    };
    let mut w = cl.members[f1].clients().await?;
    for i in 0..100 {
        w.put(format!("/c/{i:03}").as_bytes(), b"v").await?;
    }
    let seen = cl.members[f2].clients().await?.count(b"/c/").await?;
    cl.members[l].kill().await;
    let t = Instant::now();
    let nl = cl.leader(Duration::from_secs(30)).await?;
    let elected = t.elapsed();
    let survivor = if nl == f1 { f2 } else { f1 };
    let mut s = cl.members[survivor].clients().await?;
    let mut extra = 0;
    let t = Instant::now();
    while extra < 50 && t.elapsed() < Duration::from_secs(30) {
        match s.put(format!("/c/after/{extra:03}").as_bytes(), b"v").await {
            Ok(_) => extra += 1,
            Err(_) => sleep(Duration::from_millis(200)).await,
        }
    }
    let after = s.count(b"/c/").await?;
    cl.members[l].start().await?;
    cl.members[l].ready(Duration::from_secs(60)).await?;
    let mut old = cl.members[l].clients().await?;
    let t = Instant::now();
    let mut caught = old.serializable_count(b"/c/").await?;
    while caught < 150 && t.elapsed() < Duration::from_secs(30) {
        sleep(Duration::from_millis(200)).await;
        caught = old.serializable_count(b"/c/").await?;
    }
    for m in cl.members.iter_mut() {
        m.kill().await;
    }
    Ok(if seen == 100 && nl != l && extra == 50 && after == 150 && caught == 150 {
        pass(format!(
            "100 writes through a follower, read linearizably on the other; leader SIGKILLed, a new one in {:.1} s; 50 more writes; the old leader rejoined with all 150",
            elected.as_secs_f64()
        ))
    } else {
        fail(format!(
            "linearizable read on a follower saw {seen} of 100; new leader {nl} (old {l}); {extra} of 50 writes after; {after} of 150 on a survivor; old leader caught up to {caught}"
        ))
    })
}
