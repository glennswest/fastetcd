# CLAUDE.md — fastetcd

Project-specific context. Cross-project rules live in `../CLAUDE.md`.

## Project summary

Rust implementation of the etcd v3 wire protocol. Wire-compatible gRPC.
Multi-node Raft from day one. Importer for existing etcd BoltDB data.
Focused on low resource overhead and predictable latency.

## Version

**`1.6.1`** — A Range's header revision is the revision its contents
were read at (#50, P0). It was read after the range, so a concurrent
commit made a Kubernetes LIST + WATCH skip that write for good
(rustkube#146). `MvccStore::range_with_revision` returns the read's
revision (engine snapshot taken under the write-state lock), and
`ForwardRead` replies carry the leader's read revision after the result
(older followers ignore it; from an older leader the follower falls
back to its own revision and warns once).

Previous: **`1.6.0`** — Under `--client-cert-auth`, a client certificate's Common
Name is its etcd user (#20), as in etcd: no token → the verified leaf's
CN (`authz::identify`, used by the interceptor and the Auth service); a
token wins; an invalid token is refused, not replaced. Roles now scope
mTLS clients (one user per ClusterMesh cluster). New dep `x509-parser`.

Previous: **`1.5.2`** — A `Put` naming a lease that does not exist is refused
(#19) with etcd's `etcdserver: requested lease not found`. Checked on
the leader just before proposing (`crates/raft/src/precheck.rs`), not at
apply as etcd does, so members cannot diverge while snapshots lack the
lease tables (#41). Found alongside: #49 (P0), an `ignore_value` put on a
missing key fails inside apply and stops every member.

Previous: **`1.5.1`** — Admin RPCs are root-only while auth is on (#31). Any
logged-in user could grant itself root, disable auth, stream a snapshot
or change membership. Rules from etcd's release-3.5 source, not the
issue's list: all Auth changes plus `AuthStatus`/`UserList`/`RoleList`
(self `UserGet` and held-role `RoleGet` allowed, own password change
still root); Maintenance `Snapshot`/`Defragment`/`Hash`/`HashKV`/
`MoveLeader`/`Downgrade`/`Alarm` change; member add/remove/update/
promote. `Compact`, `Status`, `Alarm` list, `MemberList` stay open.
Remaining auth gap: lease RPCs (#47).

Previous: **`1.5.0`** — Auth state is replicated through Raft (#32). Auth changes
used to commit to the serving member only and tokens were per member, so
RBAC held on some members and not others. Now every change and every
`Authenticate` token is a log entry each member validates and applies
(`FastetcdLogEntry::Auth`, `MvccStore::apply_auth`), snapshots carry the
auth tables in a compatible trailer, and a gate (`crates/server/src/
auth_sync.rs`, peer `AuthSync` RPC) refuses auth changes while any member
is older or unreachable, or while members hold different tables (kept
from 1.4.x). The operator converges those with `fastetcd-ctl auth
members` / `auth adopt <member>` (owner's decision: compare, refuse if
they differ, never pick automatically). Docs: `docs/03-deploy.md` § Auth
on a multi-member cluster. Remaining auth gap: #31.

Previous: **`1.4.2`** — `Watch` is authorized (#33). A watch create had no
permission check, so with auth enabled any authenticated user could
watch any key or range (history and `prev_kv` included) and read what
RBAC denies to `Range`. Each create now needs read on `[key, range_end)`;
a denied create gets etcd's answer (`created` + `canceled`, `watch_id`
-1, `etcdserver: permission denied`) and the stream stays usable.
Checked at create only, as in etcd. Auth gaps left: #31, #32.

Previous: **`1.4.1`** — Peer TLS is honoured, with its own identity and trust root
(#23). The `--peer-*` TLS flags were parsed and discarded: the peer port
served the client identity whenever `--cert-file` was set, and members
dialled each other in plaintext, so a TLS multi-member cluster could not
form. Now (`crates/server/src/tls.rs`) the peer port takes its identity
and CA only from `--peer-*`, members dial each other over mutual TLS
against `--peer-trusted-ca-file`, `--peer-client-cert-auth` refuses any
caller without a peer-CA cert (a client-CA cert included), and every
peer URL's scheme must match (`https://` with peer TLS), or startup
fails naming the flag. Behaviour change: `--cert-file` alone no longer
puts TLS on the peer port. Also hardened the multi-node test harnesses
against a loaded build box (#44).

Previous: **`1.4.0`** — Survive a corrupt data file (#37). After a power cut on a
device that lost fsync'd writes, redb refused to open the store and the
node crash-looped with nothing to recover from. Now: periodic checksummed
backups to `--backup-dir` (off unless set; belongs on a separate volume),
and a lone member whose file is corrupt restores the newest backup that
verifies, keeps the corrupt file, and raises a CORRUPT alarm until
disarmed. A multi-member node refuses (its raft vote was in the file)
and prints the member remove / re-add remedy. redb 2.6.3 sometimes
*panics* on a damaged file instead of erroring; that panic is caught
and treated as corruption. redb two-phase commit is deliberately off
(it makes repair unable to roll back a torn commit). Docs:
`docs/05-backup-and-recovery.md`. Also: a `Range` in a `Txn` sees the
txn's own earlier writes (#35).

Previous: **`1.3.0`** — A deployment-distinct, stable `cluster_id` (#17). The
id in every `ResponseHeader` was `--cluster-id`, undocumented and
defaulting to `1` for every deployment, so a client pinning it could
not tell a mis-pointed endpoint from the right one. Now
(`crates/server/src/cluster_id.rs`) it is chosen on a store's first
start and persisted in a node-local `node_meta` table (not carried by
raft snapshots): explicit `--cluster-id` (0 rejected), else FNV-1a-64
of `--initial-cluster-token` masked to 63 bits and never 0, else `1`
with a startup warning. Later starts use the persisted id. A store
older than this keeps `1` unless `--cluster-id` is given, so an upgrade
never changes the id clients see. Members still choose independently;
replicating it (etcd-style, distinct by default) is #38. Also fixed a
port race in the multinode test harness (#40).

Previous: **`1.2.4`** — Raft snapshots move through files, not RAM (#30).
`SnapshotData` was `Cursor<Vec<u8>>`, so a lagging follower cost the
leader a full database copy of RAM per transfer and the follower up to
~3x. It is now `SnapshotFile` (`crates/raft/src/snapshot_data.rs`): a
retained `.snap` opened read-only (send), a temp file chunks are
received into (renamed into place on install, deleted on drop), or an
in-memory fallback used only when the disk cannot hold it. Build
serializes straight into the file. Install decodes from the file. The
on-disk and wire formats are unchanged. Chunk I/O is synchronous on
purpose: `tokio::fs::File` reports a failed write on a *later* call,
which would make the ENOSPC fallback lose a chunk. Also fixed along the
way: `get_current_snapshot` returned `None` after a failed persist,
but openraft had already purged the log against that snapshot, and its
replication turns `None` into a storage error. A node with applied
state now always serves a snapshot, rebuilt on demand. Still in RAM
during build and install: the decoded tables (needs a record-streamed
format, a separate change).

Previous: **`1.2.3`** — Txn response ops have the variant of their request op
(#18). The kind of each `ResponseOp` used to be guessed from the
result's shape (`n == 1` → Put), so a `DeleteRange` that removed one
key came back as a `ResponsePut`. The handler now builds the response
by pairing each op in the branch that ran with its result, by position.
No storage or raft wire change. Found alongside it: #35, a `Range`
inside a txn doesn't see that txn's own earlier writes.

Previous: **`1.2.2`** — `Txn` is authorized (#22). With auth enabled, `Txn` had
no permission check at all, so any authenticated user could read or
write any key by wrapping the operation in a transaction. A txn now
needs read on every compare target, plus the permission of every op in
*both* branches (nested txns included), checked all-or-nothing against
one snapshot of the auth tables before anything is proposed (etcd's
`checkTxnReqsPermission`). `prev_kv` on `Put`/`DeleteRange` now also
needs read, as in etcd. Tests: `crates/server/tests/authz_txn.rs`.
Auth is still **not** a security boundary. Three gaps found while doing
this are filed as P1: #31 (admin RPCs don't require root, so any user
can grant itself root or `AuthDisable`), #32 (auth state is written
locally, not through Raft), #33 (Watch is unauthorized).

Previous: **`1.2.1`** — Watch never silently skips a revision (#16). A watch
stream that fell behind the 1024-batch event broadcast logged `Lagged`
and kept going, dropping events with no error, so consumer caches
(Cilium/flowsdn-style) drifted forever. Each watcher now has a
`next_rev` cursor; on lag, or on a gap in batch revisions (a follower
installing a raft snapshot emits no events), it is caught up from MVCC
history, or cancelled with `compact_revision` if that history is gone.
The cursor also fixed create-time replay racing live delivery.
Regression test `crates/server/tests/watch_lag.rs` forces real lag.

Previous: **`1.2.0`** — Volume sizing you can provision against. "How big should the
volume be?" had no answer, and getting it wrong deadlocks the store
(#14). `fastetcd sizing --nodes N [--pods-per-node P]` turns a cluster
shape into a number *and shows its arithmetic*, so the estimate can be
argued with. The ladder at 30 pods/node: 512 MiB for 1-10 nodes, 1 GiB
at 100, 2 GiB at 500, 4 GiB at 1000, 16 GiB at 5000.

The headline is that the volume runs roughly **10x the live data** — a
100-node cluster stores 63 MiB of Kubernetes objects and wants 1 GiB —
and that this is not waste. In order of size the multipliers are: MVCC
history between compactions (+30%), the engine's copy-on-write overhead
(x1.6), a raft snapshot (*a full copy of the database*, the term people
forget), the raft log, and up to `--upgrade-backup-retain` safety
backups at a full copy each. Sizing for the 63 MiB is how a volume
fills.

Two facts the model makes visible: clusters below ~50 nodes all land in
the same bucket, because fixed cluster overhead (CRDs, RBAC, API
priority-and-fairness config — none of it proportional to nodes)
dominates until the pod term takes over around 100 nodes; and pod
*density*, not node count, drives the variable part.

`--expected-nodes N` runs the same check against the real volume at
startup and logs the shortfall while the store is still empty and the
answer is actionable — warning, not refusing, because a control plane
that will not boot is worse than one that needs attention later. It also
enables auto-compaction if it was left off, scaled to hold a roughly
constant ~16-minute window at any cluster size.

Filed #15 off the back of this (streaming snapshots out of a pinned
engine read transaction to drop the on-disk copy). **Declined
2026-09-24**: priority for this component is safety first, performance
next, disk savings last. A pinned read transaction makes a healthy
leader's compaction and defragment wait on a lagging follower, and the
saving is ~14% of the minimum volume (~80 MiB at 100 nodes), not the
~40% first estimated. The on-disk snapshot stays. The performance half —
stop buffering the whole snapshot in RAM on both ends of a transfer by
moving it through files — is #30 (to be implemented by stormcentral).

Previous: **`1.1.0`** — Bounded on-disk footprint (#14). A fixed-size data volume
must stay bounded and must never wedge the store. On the reported
node a 64 MB volume filled after ~a day of ordinary control-plane
traffic and then deadlocked in both directions: the snapshot write
failed with ENOSPC, openraft surfaced that storage error on the
linearizable read barrier *and* on every proposal, so even deleting
keys to make room was refused — every process healthy, cluster down.
Four changes:

- **Snapshots are a retained set with roll-off.** They used to be one
  file overwritten in place, which still needed room for two copies
  during the write. `--max-snapshots` (default 1) is now enforced
  oldest-first *before* the new snapshot is written, so a write never
  needs room for `retain + 1`. Temp files and half-written pairs are
  reclaimed on startup; the pre-1.1 `current.snap` layout migrates in
  place. An ENOSPC on the write discards every retained snapshot and
  retries — openraft rebuilds one, and a node with no snapshot beats a
  node that cannot write one.
- **Occupancy is measured against the device, not a quota.** The
  monitor samples the data file, the retained snapshots and the
  filesystem's real free space (`statvfs`); effective capacity is the
  smaller of `--quota-backend-bytes` and what the volume can actually
  hold. A 2 GiB quota on a 64 MB volume is a fiction, and it was that
  fiction that let every threshold report healthy right up to ENOSPC.
- **Reclaim at a high-water mark** (80%): compact MVCC history to
  `--space-reclaim-retention` (even with auto-compaction off), trigger
  a raft snapshot so the log can be purged, then defragment — the only
  step that actually returns pages to the filesystem, since a
  copy-on-write B-tree never shrinks its file on its own.
- **NOSPACE alarm at 95%, raised while the store still works.** Writes
  (`Put`, a `Txn` containing a put, `LeaseGrant`) are refused with
  `ResourceExhausted` / `etcdserver: mvcc: database space exceeded`;
  reads, deletes, compaction and defragment are deliberately not gated,
  because refusing those is what makes a full volume unrecoverable.
  Clears itself below 70% or via `etcdctl alarm disarm`.

Also: `Maintenance.Alarm`/`Status` report real alarms, `dbSizeInUse`
and `dbSizeQuota`; seven occupancy metrics on `/metrics`; offline
`fastetcd defrag` (no quorum, no read barrier, no snapshot write —
works on an already-full volume); `fastetcd-ctl status/defrag/compact/
alarm`; `docs/04-disk-space.md`. Changelog backfilled for 0.8.2–1.0.7,
which had drifted.

An end-to-end test on a real bounded volume found three defects the unit
tests had missed: `usage()` reported the key/value payload rather than
the pages holding it (advertising 22 MB recoverable where a defragment
freed zero); an online defragment failed outright on any loaded node,
because redb refuses to compact while a read transaction is live and a
snapshot holds one for as long as its caller does; and `Status` could
report more bytes in use than the file contained.

Previous: **`1.0.7`** — Tooling release: ships `fastetcd-bench`, a small concurrent
gRPC load generator (put / linearizable-get / serializable-get,
throughput + latency percentiles), now bundled in the release tarball
(work-plan #14). No server/behavior change from 1.0.6. Reference numbers
on 8-core/15G/SSD, single node, 256B values: put ~5.2k ops/s (p50 11ms,
fsync-bound), linearizable get ~61k ops/s (p50 0.9ms), serializable get
~57k ops/s; RSS flat at ~274 MB. Follower serializable reads ~64k ops/s.

Previous: **`1.0.6`** — Bounded memory + log-size management (#13, the sustaining
half). Four changes so a node's memory and raft log stay bounded 24/7
regardless of data size, and snapshot+purge keeps up instead of
stalling:
- **Snapshots live on disk, not RAM.** The state machine used to hold
  the entire serialized database as a `Vec` in `current_snapshot` (why
  an idle node sat at gigabytes) and lost it on restart (why purge
  stalled). Now the body is written to `<data-dir>/snapshots/current.snap`
  and only the small `SnapshotMeta` is kept in memory; it's reloaded on
  restart so openraft can purge the log immediately.
- **Bounded log purge.** `purge`/`truncate` used a range read that loaded
  the whole purged prefix (key+value) into RAM — a large part of the
  2.1 GB. They now use a `delete_range` (keys only).
- **Auto-compaction** (opt-in, `--auto-compaction-retention <revs>`,
  revision mode): a leader-side ticker proposes `Compact` through Raft
  to bound MVCC history, and therefore snapshot cost. Off by default —
  under Kubernetes the apiserver drives compaction itself.
- **Tunable snapshot policy**: `--snapshot-count` (default 5000) and
  `--max-in-snapshot-log-to-keep` (default 1000) now configure openraft's
  snapshot/purge, matching etcd's `--snapshot-count`.

Previous: **`1.0.5`** — Startup no longer loads the whole raft log into RAM (#13,
the acute half). On the g8 3-master cluster every member hung right
after the "starting" line — the peer port (:2380) never opened, so no
election, no quorum, control plane down. Cause: `get_log_state` (called
by openraft on every startup) did `range(Unbounded, Unbounded)` over
`raft_log`, materializing all ~93k un-purged entries (2.1 GB) just to
read the last one. Added `Snapshot::last` (an O(log n) reverse B-tree
lookup in redb; a slow default for other engines) and used it in
`get_log_state`. Also gated + windowed the #11 membership-recovery log
scan so a healthy cluster never scans the log on startup and a legacy
recovery scan is bounded to the last 20k entries. Startup is now bounded
regardless of log size. NOTE: this fixes *restart*; it does not stop the
log from growing — that's the snapshot/purge work below (still needed
for 24/7 operation, since fastetcd's whole-DB in-RAM snapshot build
stalls at scale, which is why the log reached 93k).

Previous: **`1.0.4`** — Startup-robustness fix. In v1.0.3 the automatic
pre-version safety backup was fatal: `backup_before_version(...).await?`
meant that if the backup copy failed — no disk space for a second copy
of a large db, an unwritable/misowned `backups/` dir — fastetcd exited
and systemd crash-looped it, so a *failed safety net* could take a
control-plane node down (the opposite of its purpose). The backup is now
best-effort: on failure it logs an error with the remedy (free space /
fix permissions / `FASTETCD_UPGRADE_BACKUP=false`) and the node starts
anyway. Nothing else about the backup changed.

Previous: **`1.0.3`** — Data-directory safety toolkit, so an upgrade can never
be a one-way door. (1) **Automatic pre-version backup**: the fastetcd
version that last opened a data dir is recorded in `mvcc_meta`; on
startup, if the running binary is a different version and there is data,
a full copy of `fastetcd.redb` is written to `<data-dir>/backups/`
*before* any recovery/conversion writes (on by default;
`--upgrade-backup`/`--upgrade-backup-dir`). (2) New offline
subcommands (server must be stopped; each opens the redb file
exclusively and refuses if it's locked): `fastetcd backup --out <path>`
(consistent single-file copy), `fastetcd restore <backup> [--force]`
(refuses to overwrite a newer dir without `--force`, keeps the
pre-restore file as `fastetcd.redb.replaced-*`), and `fastetcd fsck
[--repair]` (checks structural integrity, `format_version`, raft
membership / `last_applied`, and MVCC counter sanity; `--repair` reuses
the #11 recovery path — deep MVCC key-index damage is reported, not
auto-fixed, so restore from a backup). The #9/#11 recovery logic is now
shared between server startup and `fsck --repair`. Restoring a backup
onto a *new* node identity needs `--force-new-cluster` on first start,
as with etcd.

Previous: **`1.0.2`** — Makes the #11 upgrade a faithful in-place conversion. The
v1.0.1 recovery rebuilt the voter set from `--initial-cluster`, which is
only a guess if the live membership had diverged from bootstrap. v1.0.2
first recovers the *actual* membership from the retained raft log (scans
for the newest `Membership` entry — the true voter set as of the last
on-disk membership change), and falls back to `--initial-cluster` only
when the log has been purged clean of it. The startup log states which
source was used. Standing rule going forward: any on-disk-incompatible
change ships an automatic in-place upgrade; never tell an operator to
wipe/rebuild.

Previous: **`1.0.1`** — Critical upgrade fix (#11). A rolling upgrade of a
long-running cluster from v0.8.2 → v1.0.0 could strand it: every member
came up with an empty voter set and never elected a leader (MVCC data
survived; only membership/leadership was lost). Root cause was the #9
recovery path: v0.8.2 never persisted raft membership, so once its log
had been purged the membership entry (at the head of the log) was gone
from everywhere, and `recover_applied_floor` adopted the purge floor as
`last_applied` — telling openraft not to replay — without recovering
the membership. (v0.8.2 was itself already latently crash-looping on
any restart-after-purge, the #9 bug; the upgrade just forced the
restart.) Fix: detect the pre-v1.0.1 on-disk format (no `format_version`
marker in `mvcc_meta`) and, when the restored membership is empty but
data is present, rebuild the voter set from `--initial-cluster` and
persist it durably before Raft starts — an in-place upgrade. Added a
`--force-new-cluster` escape hatch (etcd parity) that rebuilds
single-node membership from self while preserving the redb data, for
arbitrarily stranded dirs. New `mvcc_meta` `format_version` marker
(FORMAT_VERSION = 1) suppresses re-recovery. Note: downgrading below
v1.0.1 after it (or v0.8.2 after v1.0.0) is unsafe — the old binary
crash-loops on the purged log; treat 1.0.1 as a one-way upgrade from
0.8.x.

Previous: **`1.0.0`** — First stable release. The wire-compat bar is met end to
end: unmodified etcd v3 clients (etcdctl, client-go, kubeadm) drive
KV/Watch/Lease/Cluster/Maintenance/Auth through Raft, single- and
multi-node, with linearizable reads by default. Shipped as the #10 fix
below closes the last known correctness gap surfaced by the rustkube
soak tests.

Headline fix — linearizable reads (#10). A single-client
read-modify-write loop (GET `mod_revision` → txn
`Compare::mod_revision(Equal)` + put) saw ~25% spurious CAS failures on
a cluster with no concurrent writer. Root cause: fastetcd never
implemented linearizable reads — `serve_range` ignored `serializable`
(`let _ = req.serializable;`) and always read local state, so a
follower whose state machine lags, or a leader that has silently lost
leadership, answered a default read with stale data; the client's next
GET then returned a `mod_revision` older than committed, and the
following CAS compared against it and failed. Fix: a default
(non-serializable) Range now runs a read barrier first — on the leader
`Raft::ensure_linearizable` (ReadIndex + wait-for-apply), on a follower
forward the whole Range to the leader over a new `RaftPeer.ForwardRead`
RPC. Cluster-of-one satisfies the barrier trivially, so single-node
reads stay a direct local read; `serializable=true` still opts out.
The single-machine test harness applies too fast to show the
staleness, so the regression test asserts the barrier itself: a node
that can't confirm leadership must *fail* a linearizable read rather
than serve stale local state (verified it fails without the fix).

Previous: **`0.8.3`** — Three durability/compat fixes found by the rustkube
cluster tests.

`#9` — a single node couldn't survive a reboot. `FastetcdStateMachine`
rebuilt `last_applied_log_id`/`last_membership` as `None` on *every*
open (`main.rs` called `new(mvcc)`, which had no restore path), so
openraft saw an empty state machine next to a populated MVCC store and
replayed the log from index 0. That crash-looped once a snapshot had
purged the early entries ("expected index [0, N), got [None, None)")
and, when the log happened to be intact, silently double-applied every
mutation — the second failure mode wasn't in the report. Both fields
now persist into `mvcc_meta`, staged before an entry is dispatched and
folded into that entry's own `WriteBatch`, so the mutation and the log
id describing it commit in one fsync. Note this was never log
corruption: the raft log was fine, the state machine just didn't know
where it was.

Data dirs already in the bad state have no applied position, so
startup adopts the log's `last_purged_log_id` as a floor (openraft
only purges what it has applied *and* snapshotted). **This re-applies
a bounded tail and can advance the revision past what clients
previously observed** — logged loudly at `warn`. Strictly better than
the old workaround (wiping the data dir), but not lossless.

`#8` — a rejoined member stayed at an old MVCC revision. Not a
snapshot-transfer problem: `rebuild_mvcc` replaces every MVCC table by
writing straight through the engine, but `MvccStore` caches
`current_rev`/`compact_rev`/`next_lease_id` from open. The snapshot
landed on disk while the handle served the stale counters — reads
clamp to `current_rev` (hiding every key above it, the reported "504
of 1004") and new writes allocate from `current_rev + 1`, colliding
with the snapshot's revisions. Added `MvccStore::reload_write_state`.
v0.8.2 fixed the *leader* side of this path (torn snapshot build);
this is the follower side, which is why #8 stayed open.

`#7` — `etcdctl member add/remove` against a follower returned
openraft's raw `ForwardToLeader` error. Membership changes go through
openraft's own APIs rather than the replicated log, so they can't ride
on the `ForwardWrite` RPC added for #4; added a sibling
`RaftPeer.ForwardMembership` over the same peer channel plus
`ServerState::propose_add_learner`/`propose_set_voters`.

Each regression test was confirmed to fail without its fix, not merely
pass with it.

Previous: **`0.8.2`** — Atomic snapshot build: capture the MVCC handle
and `last_applied` together under the state-machine lock, so a
concurrent apply can't produce a snapshot whose data predates its own
`last_applied_log_id`.

Previous: **`0.8.1`** — Security fix (#6): `--client-cert-auth` was never
actually enforced. The flag had no `env` binding and no entry in the
`ETCD_*`→`FASTETCD_*` compat shim, so `ETCD_CLIENT_CERT_AUTH=true`
(the documented drop-in config) was silently ignored — the flag
stayed `false`, TLS was built with no client-CA verifier, and any
client could complete the handshake and read/write anonymously on
both the gRPC port and the `/health` route. Wired the `env` binding +
compat pair (same for `peer-client-cert-auth`) and set
`client_auth_optional(false)` explicitly. Verified end to end:
certless TLS clients are now rejected at the handshake; CA-signed
clients still succeed.

Previous: **`0.8.0`** — Found and fixed the real reason releases had stalled:
GitHub Actions is disabled at the repo-settings level (confirmed off
since 2026-05-24, not a workflow bug or billing issue) — every push
and tag since then, including `v0.6.0`/`v0.7.0`, silently ran
nothing. Decision: leave Actions off; `dev.g8.lo` is now the sole
build+test path, scripted via `deploy/packaging/run-tests.sh` and
`deploy/packaging/build-release.sh` (the now-dead
`.github/workflows/ci.yml` was deleted). Published the missing
`v0.7.0` GitHub Release from `dev.g8.lo`, and verified its rpm/deb
end to end there (systemd unit, `fastetcd-ctl put`/`get` through
Raft) — noting a Fedora + SELinux `Enforcing` quirk with foreign
`dpkg` maintainer scripts that isn't a package defect.

Previous: **`0.7.0`** — Fixed a multi-node blocker (#4): client writes sent to
a non-leader now actually forward to the leader over the existing
peer channel (new `RaftPeer.ForwardWrite` RPC / `WriteForwarder` in
`crates/raft/src/network.rs`), instead of relaying openraft's
`ForwardToLeader` error straight to the client with an empty
address. `raft.initialize()` also now carries real peer-URL
`BasicNode` addresses. Added etcd-compat `GET /health` (+ `/livez`,
`/readyz`) on the client gRPC port itself via tonic 0.12's
`Routes`↔`axum::Router` interop (#5), so LB/k8s health probes work
without a second port.

Earlier: **`0.6.0`** — `ETCD_*` env var drop-in compat (falls back to etcd's
env names when `FASTETCD_*` is unset), a systemd unit
(`deploy/systemd/fastetcd.service`, autostart + unconditional
auto-restart) and rpm/deb packaging (`crates/server/Cargo.toml`
`[package.metadata.deb]` / `[package.metadata.generate-rpm]`) that
bundle all three binaries as static `x86_64-unknown-linux-musl`
builds — avoids baking in a glibc-version requirement from the
packaging host. Verified end to end on dev.g8.lo (Fedora 43):
`dnf install` / `dpkg -i` both create the `fastetcd` system user,
enable, and start the service automatically.

Older: **`0.5.0`** — Kubernetes-ready: grpc.health.v1.Health for
service-mesh probes, a Helm chart at `deploy/charts/fastetcd`,
real `fastetcd-ctl` client (put/get/del/snapshot-save), README
rewrite. Only remaining gap from the v0.1.0 era is the openraft
0.10 upgrade for real `MoveLeader`.

Version locations (keep in sync):
- `Cargo.toml` workspace `[workspace.package] version`
- This file (the line above)
- Tags `vX.Y.Z`

## Architecture pillars

- **Storage**: trait-first abstraction (`KvStore`) with two first-class
  engines selectable at runtime: `redb` (default, cross-platform) and
  `iouring` (Linux, `glommio` + `O_DIRECT` + custom WAL; behind cargo
  feature `iouring`).
- **Consensus**: `openraft` — core, not optional. Single-node is a
  cluster-of-one.
- **gRPC**: `tonic` + `prost`, generated from vendored etcd `.proto` files.
- **Async runtime**: `tokio` for the gRPC frontend and Raft node;
  `glommio` runtime is internal to the iouring engine.
- **Logging/tracing**: `tracing` + `tracing-subscriber`.

## Repo layout

```
crates/
  proto/      # tonic-generated etcd v3 stubs
  storage/    # MVCC state machine over redb
  raft/       # openraft glue: log storage, state machine adapter, transport
  server/     # binary: gRPC frontend + raft node + lifecycle
  migrate/    # binary: read etcd BoltDB → write into fastetcd
  ctl/        # binary: minimal etcdctl-compat smoke client
docs/
  00-design.md
benches/
```

## Work plan

Tracked live in the Claude task system. Snapshot of the order:

1. Project scaffolding (done)
2. Cargo workspace + crate skeletons
3. Vendor etcd protos, wire up tonic codegen (done)
4. Design `KvStore` trait + implement redb engine
5. MVCC state machine over `KvStore`
6. openraft integration (log storage trait + redb impl, state machine adapter)
7. Raft peer transport (gRPC) + discovery
8. KV gRPC service
9. Watch service
10. Lease service
11. Maintenance + Cluster services
12. iouring engine implementing `KvStore` (Linux-only, cargo feature)
13. Migration tool from etcd BoltDB
14. Benchmarks: redb engine vs iouring engine vs upstream etcd
15. **Bounded on-disk footprint (#14) — done, shipped in v1.1.0.** A bounded data
    volume must never wedge the store. Work items:
    - [x] `KvStore::usage()` (file bytes / in-use bytes / fragmented
      bytes) + filesystem free-space probe.
    - [x] Space monitor task: high-water reclaim (compact → snapshot →
      purge → defragment) and a NOSPACE alarm at the alarm water mark.
    - [x] Writes rejected with `ResourceExhausted` under the alarm while
      reads/deletes/compaction/defrag stay available (etcd parity).
    - [x] Snapshot writes survive a full disk: retained-set roll-off
      before each write, stale `.tmp`/orphan cleanup on open, and an
      ENOSPC retry that frees every retained snapshot first.
    - [x] `Maintenance.Alarm` returns real alarms; `Status` reports
      `dbSizeInUse` and `dbSizeQuota`.
    - [x] Occupancy metrics on `/metrics`.
    - [x] Offline `fastetcd defrag` escape hatch that works on a full
      volume, plus `fastetcd-ctl defrag/compact/alarm`.

16. **Watch never silently skips a revision (#16) — done, shipped in v1.2.1.** A
    watcher that falls behind the event broadcast used to log `Lagged`
    and carry on, dropping events with no error. Work items:
    - [x] Per-watcher `next_rev` cursor; live delivery skips anything
      the watcher already has (also fixes create/replay duplicates and
      out-of-order delivery, and honours a future `start_revision`).
    - [x] On `Lagged`, or a gap in the stream's batch revisions (e.g. a
      follower installing a raft snapshot, which emits no events),
      resync each watcher from MVCC history up to the current revision.
    - [x] If that history is compacted, cancel the watch with
      `compact_revision` set (etcd `ErrCompacted`); on a read error,
      cancel with a reason. Never drop events silently.
    - [x] Regression test that forces lag and asserts every revision
      arrives exactly once, in order; docs + changelog.

17. **Txn is authorized like every other KV path (#22) — done, shipped in v1.2.2.**
    `Txn` had no authorization check, so RBAC could be bypassed by
    wrapping any read or write in a transaction. Work items:
    - [x] `authorize_all`: check a list of accesses against one
      snapshot of the auth tables, all-or-nothing.
    - [x] `Txn` authorizes read on every compare and every op in *both*
      branches (recursing into nested txns), as etcd's
      `checkTxnReqsPermission` does.
    - [x] `prev_kv` on `Put`/`DeleteRange` (standalone and in a txn)
      also needs read, as in etcd.
    - [x] Regression tests (`crates/server/tests/authz_txn.rs`); docs +
      changelog. Remaining auth gaps filed: #31 (admin RPCs not root-only), #32 (auth
      state not replicated), #33 (Watch unauthorized).

18. **Txn response ops match their request ops (#18) — done, shipped in v1.2.3.**
    Each `ResponseOp` in a `TxnResponse` was a guess from the
    result's shape (`n == 1` → Put), so a single-key `DeleteRange` came
    back as a `ResponsePut`. Work items:
    - [x] Build the response in the gRPC handler from the request ops
      of the branch that ran (`succeeded` → success, else failure),
      zipped by position with the op results. No storage or raft wire
      change.
    - [x] Error rather than guess if the counts or kinds disagree.
    - [x] Regression tests in `kv_grpc.rs` (single-key delete, zero-hit
      delete, multi-key delete, put, range, failure branch); changelog.
    - Found alongside: a `Range` in a txn doesn't see the txn's own
      earlier writes (etcd's does) — #35.

19. **Snapshots move through files, not RAM (#30) — done, shipped in v1.2.4.**
    `SnapshotData` was `Cursor<Vec<u8>>`, so every build, send, receive
    and install held a whole database copy (or three) in RAM. The
    on-disk `.snap` format and the wire format stay exactly as they are.
    Work items:
    - [x] `SnapshotData` = `SnapshotFile`: a retained `.snap` opened
      read-only (send), an incoming temp file (receive, removed on drop
      unless installed), or an in-memory fallback. Synchronous chunk I/O
      so a write error is exact, never deferred.
    - [x] Build: `bincode::serialize_into` a temp file, fsync, rename,
      then meta; the serialized `Vec` never exists. Roll off before
      writing, ENOSPC → discard all and retry, then fall back to memory.
    - [x] Receive: roll off before writing, chunks into a temp file;
      ENOSPC → discard all and retry, then switch to memory. A receive
      never surfaces a `StorageError`.
    - [x] Install: fsync, `deserialize_from` the file, apply, rename the
      file into place as the retained snapshot (no second write).
    - [x] `get_current_snapshot` never answers "none" while the state
      machine has applied state: a missing file → rebuild on demand
      (openraft's replication treats `None` as a storage error).
    - [x] `Maintenance.Snapshot` streams from the file.
    - [x] Tests: build/send/receive/install round trip through files,
      cut-off transfer leaves no temp file and the live DB untouched,
      missing-file fallback, memory fallback; docs + changelog.
    - [x] End-to-end: a late learner caught up by a real chunked
      `InstallSnapshot` over gRPC (`snapshot_transfer_grpc.rs`).
    - Not done (needs a new on-disk format + automatic upgrade): a
      record-streamed snapshot, so build and install stop holding the
      decoded tables as `Vec`s.

20. **A deployment-distinct, stable `cluster_id` (#17) — done, shipped in v1.3.0.**
    `ResponseHeader.cluster_id` was `--cluster-id`, undocumented and
    defaulting to `1` for every deployment, so a client pinning it could
    not tell a mis-pointed endpoint from the right one. Decided
    (2026-09-26) rules, in order:
    - [x] Explicit `--cluster-id` wins; `0` is rejected at startup.
    - [x] Else FNV-1a-64 of `--initial-cluster-token`, masked to 63 bits,
      never 0.
    - [x] Else `1`, with a startup warning.
    - [x] Persisted in the data dir (node-local `node_meta` table, not
      in snapshots) on first start and used from then on. A store that
      pre-dates this keeps `1` unless `--cluster-id` is given, so an
      upgrade never changes the id clients see.
    - [x] Tests, docs (`--cluster-id` documented), changelog.
    - [x] Replicated / distinct-by-default id filed as #38.

21. **Survive a corrupt DB (#37, P0) — done, shipped in v1.4.0.** After a power cut
    on a device that loses fsync'd writes (stormblock#171), redb refused
    to open ("All roots are corrupted") and the node crash-looped with
    nothing to recover from. Decided 2026-09-27:
    - **No redb two-phase commit.** In redb 2.6.3 a 2PC commit makes
      repair trust the primary slot and never fall back to the previous
      commit, so on a lying device a torn commit becomes an unopenable
      file (full restore) instead of a one-write rollback. Measure the
      fsync cost anyway and record it.
    - **Backups are off unless `--backup-dir` is set** (keeps the #14
      sizing model; warn when it is on the data volume).
    - **Only a lone member restores locally.** A member of a multi-node
      cluster must never forget its raft vote, so it refuses to start,
      leaves the corrupt file, and prints the member remove / re-add
      remedy.
    Work items:
    - [x] `StorageError::Corrupted`, mapped from redb's corruption error
      on open; `Snapshot::table_names()` (redb `list_tables`).
    - [x] Online backup: every table from one engine snapshot, one
      checksummed file (magic + header + tables + sha256), temp file +
      fsync + rename + dir fsync, keep the newest `--backup-retain` (4).
      Every `--backup-interval-secs` (900) and every
      `--backup-every-revisions` (10000), and on a membership change.
    - [x] Startup: on `Corrupted`, if lone member and
      `--on-corruption=restore` (default): move the file aside
      (`fastetcd.redb.corrupt.<ts>`, never deleted) and the retained
      raft snapshots with it, restore the newest backup whose checksum
      verifies, record the recovery in `node_meta`. Else refuse, file
      untouched.
    - [x] CORRUPT alarm through `Maintenance.Alarm`/`Status` until
      disarmed; metrics `fastetcd_recovered_from_backup_total`,
      `fastetcd_recovered_revision`. Reads and writes are not gated.
    - [x] Offline `fastetcd restore` accepts the new backup files.
    - [x] Tests: zeroed roots → restore + alarm, bad checksum skipped,
      multi-member refuses, no backup refuses; 2PC cost measurement.
    - [x] Docs (`docs/05-backup-and-recovery.md`), changelog; stormcos
      issue for a backup volume + `--backup-dir`; file the lease-tables
      snapshot gap found while reading the snapshot payload (filed: #41).
    - Status 2026-09-27: redb 2.6.3 *panics* (`unreachable!` in
      `btree.rs` `get_helper`) opening a file with zeroed pages rather
      than returning "All roots are corrupted" (#42); `RedbEngine::open`
      catches it as `Corrupted` (8fb2124, reason says "corrupt": ec30653,
      #43). Restore swap made crash-safe (6c64183). sc-build of ec30653:
      whole workspace green, `corruption_recovery` 10/10. 2PC cost: tmpfs
      ~4%, the contended build disk too noisy to say (changelog).
    - Not verified here: stormcentral's power-cut check (needs the golden
      plus stormcos#132, a backup volume and `--backup-dir`).

22. **A Range in a Txn sees the txn's own earlier writes (#35) — done, shipped in v1.4.0.**
    `MvccStore::txn` ran every `Range` against the pre-txn snapshot and
    applied the mutations afterwards, so `[Put(b), Range(b)]` returned
    the old value; etcd returns the new one. Work items:
    - [x] Apply a txn's ops strictly in order in one `ApplyContext`;
      a `Range` after a change reads at the pending revision
      (`current_rev + 1`), as etcd's `storeTxnWrite.Range` does.
    - [x] The in-batch record cache is used only for the revision it
      holds, so a `Range` with an explicit older `revision` still reads
      history.
    - [x] Tests: put→range, delete→range, range→put→range, explicit
      older revision; fix the unit test that asserted the old
      behaviour; gRPC regression test; changelog.
    - Stored state and sub-revisions are unchanged (only range results
      differ), so a mixed-version cluster cannot diverge.
    - Shipped with #37 in v1.4.0.

23. **Peer TLS is honoured, with its own identity and trust root (#23) — done, shipped in v1.4.1.**
    `--peer-cert-file`/`--peer-key-file`/`--peer-trusted-ca-file`/
    `--peer-client-cert-auth` were parsed and discarded; the peer port
    served the *client* identity whenever `--cert-file` was set, and
    peers dialled each other with no TLS at all, so a multi-node cluster
    with `--cert-file` could not form. flowsdn#275: raft peers span pods,
    so this is not "within one pod". Decided (etcd semantics):
    - Peer TLS comes only from the `--peer-*` flags; `--cert-file` no
      longer leaks onto the peer port. Client TLS on with peer TLS off
      logs a warning that peer traffic is plaintext.
    - Peer TLS on: `--peer-trusted-ca-file` is required (peers verify
      each other against it); outbound peer dials present the peer
      cert as their client cert. `--peer-client-cert-auth` makes the
      peer port refuse any caller without a cert from that CA.
    - Scheme must match: with peer TLS every listen/advertise/
      initial-cluster peer URL is `https://`, without it `http://`;
      anything else is a startup error, and a runtime dial to a
      mismatched URL fails with an error naming it. Never silent.
    Work items:
    - [x] `crates/server/src/tls.rs`: server + peer-client TLS builders
      and URL-scheme check, in the lib so tests use the same code.
    - [x] raft crate: `GrpcNetworkFactory`/`WriteForwarder` take the
      peer `ClientTlsConfig`; one shared dial helper.
    - [x] main.rs: separate client and peer server TLS; drop the discard.
    - [x] Tests: peer mTLS cluster replicates; peer port rejects a
      certless caller and one with a client-CA cert; config errors.
    - [x] Docs (flags, chart values), changelog; release; close #23.
    - Verifying it, sc-build hit multi-node test flakes (#44): leader
      churn on the loaded build box, not #23. Harness election timeouts
      widened, polls instead of fixed sleeps, and the #10 CAS loop
      retries `Unavailable`. A separate snapshot-transfer flake is #45.

24. **Watch is authorized (#33) — done, shipped in v1.4.2.** With auth enabled any
    authenticated user could watch any key or range (with a past
    `start_revision` and `prev_kv`) and receive values RBAC denies to
    `Range`. etcd parity (`isWatchPermitted`):
    - Each `CreateRequest` needs read on `[key, range_end)`, checked
      against the user captured from the stream request.
    - Denied: the create is answered `created: true, canceled: true`,
      `watch_id: -1` (clientv3 `InvalidWatchID`), `cancel_reason:
      "etcdserver: permission denied"`; nothing is registered or
      replayed. The stream stays open for other creates.
    - Checked at create only, as in etcd: a permission revoked later
      does not cancel a live watch (documented).
    Work items:
    - [x] watch.rs: capture `UserIdentity`, authorize each create.
    - [x] Tests `crates/server/tests/authz_watch.rs`: out-of-scope
      key/range/prefix denied with no events; in-scope allowed and
      delivers; denied create doesn't break the stream; root and
      auth-disabled unaffected; etcd-client sees the cancel.
    - [x] Docs, changelog; release; close #33.
    - Verified: the 5 denial tests fail with the check stubbed out.

25. **Auth state is replicated through Raft (#32) — done, shipped in v1.5.0.** Every Auth mutation commits
    straight to the local engine, tokens are a node-local in-memory set,
    and raft snapshots carry only the MVCC tables. So users, roles,
    `AuthEnable` and tokens differ per member. Plan (etcd parity):
    - New `FastetcdLogEntry::Auth` variant (see "Entries are intents"
      below). `AuthState.enabled` follows the applied value.
    - Tokens: etcd's default "simple" tokens, replicated. `Authenticate`
      checks the password, then proposes the token; every member adds
      it on apply. Not in snapshots, lost on a member restart (clients
      re-authenticate), as in etcd. JWT is not in scope.
    - Snapshots: auth tables go in a trailer after today's payload.
      bincode 1 ignores trailing bytes, so an old node still decodes a
      new payload, and a new node takes "no trailer" as "leave auth
      tables alone" (today's behaviour).
    - Mixed versions: an old member cannot decode the new entries (its
      AppendEntries fails and it stalls, and two stalled members in
      three means no quorum). Auth mutations and `Authenticate` are
      refused with `Unavailable` until every member reports >= this
      version (new peer `AuthSync` RPC; `Unimplemented` = old).
    - Entries are intents (UserAdd, RoleGrantPermission, ...), validated
      and applied deterministically in the state machine, as etcd does;
      the apply result (ok / not found / exists / precondition) goes
      back in the response. The password is hashed before proposing.
      The serving member waits until it has applied the entry itself,
      so a token or change is usable on it as soon as the call returns.
    - **Converging members upgraded from <= 1.4.2 (owner, 2026-09-28:
      "Compare; refuse if they differ").** Before the first auth write,
      the member compares every member's auth-table digest (peer
      `AuthSync` RPC). Identical (normally all empty) → replicate from
      then on. Different → auth writes stay refused (FailedPrecondition
      naming the members), logged, `fastetcd_auth_diverged` = 1, until
      the operator runs `fastetcd-ctl auth adopt <member>`, which
      replicates that member's tables to all (an `Adopt` entry).
    Work items:
    - [x] storage: `AuthOp`, `MvccStore::apply_auth`, in-memory
      `AuthMemory` (enabled + tokens) owned by the store, export /
      digest / replace of the auth tables; unit tests.
    - [x] raft: `FastetcdLogEntry::Auth`, apply returns log index;
      snapshot trailer with the auth tables (old payloads still decode
      both ways); peer `AuthSync` RPC (status + export).
    - [x] server: every Auth mutation and `Authenticate` through Raft,
      behind the version / divergence gate; `FastetcdAdmin.AuthAdopt`
      on the client port (root only when auth is on); ctl `auth adopt`.
    - [x] Tests: 3-node — change on one member enforced on the others,
      token from one member accepted by another, learner caught up by
      snapshot gets auth, restart keeps it; gate refuses with a member
      down; diverged members refused, adopt converges them.
    - [x] Docs (auth section, upgrade note), changelog; release; close #32.
    - Verified: negative checks fail as they should (no auth tables in
      the snapshot → learner test fails; gate forced open → the
      unreachable / older / diverged tests fail). Restart is covered at
      the store level (enabled flag reloads at open; tables are on
      disk), not by a full member restart. Token expiry: #46.

26. **Admin RPCs are root-only (#31) — done, shipped in v1.5.1.** With auth on, any
    authenticated user could grant itself root, disable auth, stream a
    snapshot or change membership. Rules taken from etcd's source
    (release-3.5 `apply_auth.go` `needAdminPermission`,
    `v3rpc/maintenance.go` `authMaintenanceServer`, `server.go`
    `checkMembershipOperationPermission`; `main` agrees), not from the
    issue's list, which differed in three places (noted when closing):
    - Auth: every mutation, plus `AuthStatus`, `UserList`, `RoleList`,
      needs root. `UserGet` of yourself and `RoleGet` of a role you
      hold are allowed. `Authenticate` is open. Changing *your own*
      password still needs root, as in etcd.
    - Maintenance: `Defragment`, `Snapshot`, `Hash`, `HashKV`,
      `MoveLeader`, `Downgrade`, and `Alarm` other than GET need root;
      `Status` and `Alarm` GET need only a login.
    - Cluster: `MemberAdd`/`Remove`/`Update`/`Promote` need root;
      `MemberList` does not.
    - `KV.Compact` stays open to any authenticated user, as in etcd.
    - Checked at the API layer against the serving member's applied
      auth state, as etcd checks membership changes (no log format
      change). With auth off, everything is open, as in etcd.
    Work items:
    - [x] authz: `require_root` (from #32) + `holds_role`; Auth service
      resolves the caller from its `token` (it is not behind the
      interceptor, so `Authenticate` works without one).
    - [x] Gate each RPC above; tests `crates/server/tests/authz_admin.rs`
      (non-root denied, root allowed, no token unauthenticated, the
      self exceptions, open calls stay open).
    - [x] Docs (auth section, security-boundary note), changelog;
      release; close #31. File the lease-RPC authorization gap (#47).
    - Verified: with `require_root` stubbed, the 4 denial tests fail.
      Existing tests that made admin calls tokenless after enabling auth
      were updated (#48).

27. **A Put with a lease that does not exist is refused (#19) — done,
    shipped in v1.5.2.** etcd refuses it (`etcdserver: requested lease not found`,
    NotFound); fastetcd stored the dangling lease id. etcd checks at
    apply. Here the check runs **on the leader, just before proposing**,
    not at apply, because an apply-time check would let members
    diverge: raft snapshots do not carry the lease tables yet (#41), and
    a member older than the check would accept what newer ones refuse.
    - Leader only: a follower can lag a lease just granted. A follower
      forwards (ForwardWrite) and the leader checks. On a miss, the
      leader runs a read barrier (`ensure_linearizable`) and looks
      again, so a newly elected leader that has not applied everything
      yet does not refuse a live lease.
    - Put with `lease != 0` and not `ignore_lease`; in a Txn, the puts of
      the branch the compares select, as etcd checks the branch taken.
    - Left: a lease that expires between the check and the apply still
      attaches (etcd refuses that too). Moving the check to apply is for
      after #41, with a version gate.
    Work items:
    - [x] storage: `lease_exists`, public compare evaluation.
    - [x] raft: `check_leases(entry)`, called before `client_write` on
      the leader (ServerState::propose and ForwardWrite).
    - [x] Tests: single node (put / txn branch / revoked / ignore_lease)
      and 3-node (grant on the leader, put via a follower at once).
    - [x] Docs, changelog; release; close #19.
    Found alongside: #49 (P0), an `ignore_value` put on a missing key
    fails inside apply and stops every member.

28. **A client certificate's CN is the etcd user (#20) — done, shipped in v1.6.0.**
    etcd (`AuthInfoFromCtx` → `AuthInfoFromTLS`): a `token` in the
    metadata wins; with none, and `--client-cert-auth` on, the caller is
    the Common Name of the verified client certificate's leaf. No
    `Authenticate` call, no password (such users are usually
    `--no-password`); the usual permission checks apply, and a CN with
    no matching user gets PermissionDenied. An invalid token is refused,
    never replaced by the CN. Empty CN = no identity.
    Work items:
    - [x] authz: CN from the peer certificate (`x509-parser`), one
      `identity(request)` used by the interceptor and the Auth service
      (which is not behind the interceptor).
    - [x] `ServerState::client_cert_auth`, interceptor flag; main wires
      `--client-cert-auth`.
    - [x] Tests over a real mTLS client port built like main's (routes →
      axum → tonic): CN-scoped reads/writes/watch, root by CN, unknown
      CN denied, token beats CN, off without `--client-cert-auth`.
    - [x] Docs (auth section), changelog; release; close #20.

29. **/metrics shows traffic (#29) — in progress.** The console
    (stormconsole#20) cannot tell busy from idle or see anything falling
    behind. etcd's names, so existing dashboards work:
    - `etcd_debugging_mvcc_{put,delete,range,txn}_total`: counted in
      `MvccStore` (puts/deletes as applied on every member, including
      inside txns and lease-revoke cascades; ranges where served; txn
      per Txn executed).
    - `grpc_server_started_total` / `grpc_server_handled_total{grpc_type,
      grpc_service,grpc_method,grpc_code}`: an axum middleware on the
      client port reading `grpc-status` from headers or trailers
      (`Canceled` if the stream is dropped first).
    - `etcd_debugging_mvcc_watch_stream_total`, `..._watcher_total`,
      `..._slow_watcher_total` (gauges). Slow = watchers of a stream
      that is resyncing from history or blocked on a full outbound
      buffer. Watch tasks now also end when the client goes away.
    - `etcd_server_proposals_{committed,applied}_total` (index gauges;
      committed from `save_committed`), `etcd_server_proposals_pending`.
    - `etcd_server_is_leader`, `etcd_server_id{server_id=<hex>}`,
      `fastetcd_engine_info{engine=…}` (was documented, never registered).
    Work items:
    - [ ] storage op counters; raft committed index; ServerState traffic.
    - [ ] watch gauges; grpc middleware; metrics registration.
    - [ ] Tests (`crates/server/tests/metrics.rs`), docs, changelog;
      release; close #29.

30. **A Range's header revision is its snapshot's revision (#50, P0) — done, shipped in v1.6.1.**
    `KvService::range` read `current_revision()` *after* the read, so a
    commit in between stamped the response with a later revision than
    its contents; a Kubernetes LIST then WATCHes from after that and
    never sees the write in between (rustkube#146). A forwarded read
    also took the serving member's local revision, not the leader's.
    etcd: the header is the read txn's revision.
    - [x] storage: `MvccStore::range_with_revision` returns the
      revision the read was taken at (the `current_rev` it copied under
      the write-state lock, which is what its contents are filtered to).
    - [x] raft `ForwardRead`: the leader appends that revision to its
      reply (`(Result<RangeResult,String>, i64)`; bincode 1 ignores the
      trailing bytes, so an older follower still decodes it). An older
      leader's reply has no revision: fall back to the local revision
      (the old behaviour), logged once — transient during an upgrade.
    - [x] server: header from that revision; txn headers already come
      from the txn's own revision (under the lock).
    - [x] Tests: concurrent writers + LIST readers, single node and
      through a follower (every acknowledged write <= header is in the
      list, none above); wire compat both ways; docs, changelog;
      release; close #50.
    - Verified: sc-build of 1335734, whole workspace green. With the old
      header logic patched back in on the build box, the gRPC tests fail
      as the issue did: single node 89 of 132 LISTs missing a write,
      3-member 62 of 119.

## Constraints & rules

- **Wire compatibility is the bar.** If unmodified etcd v3 clients don't
  work, the feature isn't done.
- **No v2 HTTP API.** Deprecated upstream; not in scope.
- **Linearizable reads by default** (read-index), serializable opt-in.
  Match etcd's semantics, not "close enough."
- **All writes go through Raft.** No direct state-machine writes from gRPC
  handlers, even for "internal" things like lease expiry — those go
  through Raft too so they survive failover.
- **Compaction-correctness is non-negotiable.** Revision history truncation
  must coordinate with watchers and raft log compaction.
