# Changelog

## [Unreleased]

### 2026-10-06
- **chore:** test-fixture credentials marked `not a secret` (inline, or `.github/secret_scanning.yml` for files that cannot hold a comment) — owner
<!-- New unreleased changes go here -->

## [v1.27.0] — 2026-10-08

### Added
- On server3 fastetcd wrote nothing for 70+ minutes and nothing
  said which operation had hung. Now a WAL fdatasync or a checkpoint that
  has run for 10 s is reported every 10 s at WARN (target
  `fastetcd::stall`), naming which and for how long, with the entries or
  applied index, and `fastetcd_wal_fsync_inflight_seconds` /
  `fastetcd_checkpoint_inflight_seconds` show it on `/metrics`. From the
  metrics captured during the stall (write-behind layers empty, the
  checkpoint's durable index 5 behind applied): a checkpoint's fsync of
  the data file was the one that never returned.

### Documentation
- 03 (a stuck fsync, the metrics).

## [v1.26.0] — 2026-10-08

### Added
- `--auth-token-ttl` (seconds, default 300, as etcd;
  `FASTETCD_AUTH_TOKEN_TTL` / `ETCD_AUTH_TOKEN_TTL`; 0 refused). A token
  expires that long after its last use on a member, as etcd's simple
  tokens do (`simpleTokenKeeper`): each use renews it, each member times
  its own copy (the token is replicated, its timer is not), expiry needs
  no raft entry, and expired tokens are purged at most once a second, so
  memory no longer grows with every login. An expired token gets
  `etcdserver: invalid auth token` (#105), on which clients authenticate
  again. They used to live until the user was deleted, its password
  changed, or the member restarted.

### Documentation
- 01 (the flag), 03 (auth: token lifetime; the no-token /
  bad-token codes since #105).

### 2026-10-08 — the test container on a test machine (#36)
- 02-testing: the first run through stormcentral's pipeline on
  a test machine (3070765391, pvetest2) passed; the node checks skip
  while the client port is mutual TLS and the Job has no client pair
  (stormcentral#555).

### 2026-10-08 — how the golden ships (#81)
- 03-deploy § How fastetcd reaches a StormCOS node, the README
  and the CLAUDE.md pillar said the golden is built from `/root/fastetcd`
  on dev at whatever `main` is. It is a special golden staged by
  stormcentral (`stormcentral component stage fastetcd`, stormcos's
  `build-goldens.sh` in stage mode, on a build VM, from fetched trees at
  `main`'s tip; no `/root`). Rewritten from stormcos 27add14: the stormd
  argv (mutual TLS on 2379, metrics on 0.0.0.0:2381, `FASTETCD_DATA_DIR`),
  the TCP liveness probe, the `fastetcd-data` (1G) and `fastetcd-backup`
  (2G) volumes with `FASTETCD_BACKUP_DIR`, the 30-kube start on
  sno/master. dev.g8.lo is retired: sc-build runs on build VMs (README,
  02, 03).

## [v1.25.1] — 2026-10-08

### Fixed
- An auth change made through one member right after one made through
  another could be refused as divergence (`FailedPrecondition`, "members
  hold different auth state", `fastetcd_auth_diverged` 1) when a member
  had not applied the first change yet: the gate compared digests once.
  It now surveys again, every 50 ms for up to 3 s, before calling it
  divergence (a member behind catches up; tables that differ from before
  1.5 do not). Found by the new rolling-upgrade test setting up auth
  through etcdctl over three endpoints (#65). Test
  `auth_replication::a_member_behind_by_an_entry_is_not_divergence`
  (a member answering two surveys late; without the re-survey, refused).

- The pre-version safety backup (`<data-dir>/backups`, taken when a new
  version first opens a data dir) is taken before anything writes again.
  From 1.12 it came after the raft log had moved into the WAL, which
  clears the data file's own log, so the copy meant for rolling back
  lacked the log entries not yet applied. Found by the rolling-upgrade
  test (the members' logs had the move first), which now checks the
  order (#65).

### Added
- `tests/rolling_upgrade.sh <from> [<to>]`: a real rolling upgrade or
  downgrade of three member processes under writes, with auth and a
  lease, checked end to end (#65). Run: v1.9.0 → this, v1.24.1 → this,
  this → v1.24.1.

### Documentation
- 02-testing (the script), 03 (tested rolling upgrades).

## [v1.25.0] — 2026-10-07

### Changed
- `/health`, `/livez` and `/readyz` check what etcd's check (#69,
  `crates/server/src/health.rs`; etcd release-3.5 `etcdhttp/health.go`).
  They always answered healthy. `/health`: an alarm on this member
  (`ALARM NOSPACE` / `ALARM CORRUPT`, `?exclude=` to skip), no leader
  (`RAFT NO LEADER`, unless `?serializable=true`), then a keys-only read,
  linearizable unless serializable, within 5 s + 2x the election timeout
  (`RANGE ERROR:…`): 503 `{"health":"false","reason":…}`, else 200
  `{"health":"true","reason":""}`. `/livez`: `serializable_read`.
  `/readyz`: `data_corruption`, `serializable_read`, `linearizable_read`,
  `non_learner`; per-check routes, `?exclude=`, `?verbose`; 503 with
  `[-]<check> failed: …` lines. A member restored from a backup (CORRUPT,
  #37) is unhealthy and not ready until disarmed, as etcd's.
- `grpc.health.v1` follows `/readyz` (every service and `""`, checked
  every second); it was SERVING from start to end.
- Helm chart: liveness `GET /livez`, readiness `GET /readyz` (were gRPC
  health probes, which would now restart members on quorum loss).

### Added
- `etcd_server_health_success_total` / `etcd_server_health_failures_total`.
- Tests `health_checks.rs` (healthy, NOSPACE, CORRUPT, no leader,
  excludes, verbose, per-check routes); `health_http.rs` checks the
  binary's bodies and grpc.health.v1.

### Documentation
- 03 § Health checks; 01, README.

## [v1.24.1] — 2026-10-07

### Fixed
- Write-behind back-pressure no longer waits behind the checkpoint's
  fsync (#93). The checkpoint (`WriteBehind::sync`) held the flush lock
  through redb's durable commit, an fsync of everything it had just
  written, so an apply over `--write-behind-bytes` (and the client
  waiting on it) waited for that whole fsync. Now it flushes the layers
  under the lock, fdatasyncs the data file with no lock held
  (`KvStore::presync`: redb 2.6.3's non-durable commit has already
  written its pages into the file), then flushes what came meanwhile and
  commits durably, which has little left to write. A commit over the
  budget waits only for a write-out. Test: with an engine whose fsync
  takes 1 ms per KiB written since the last (a 2 s checkpoint), the
  slowest over-budget commit during it is well under 0.5 s; with the
  old `sync` it waited 1.96 s.
- Back-pressure also when more than 1024 layers are held
  (`MAX_LAYERS`): every commit copies the layer list and every read walks
  it, so a checkpoint that could not keep up let thousands pile up.

### Added
- Metrics `fastetcd_write_behind_backpressure_seconds_total`,
  `fastetcd_write_behind_presync_seconds_total`; `tests/read_latency.sh`'s
  METRICS=1 prints the write-behind lines.

### Documentation
- 00 (write-behind), 03 (writes, metrics).

## [v1.24.0] — 2026-10-07

### Added
- etcd's process metrics on `/metrics`:
  `process_resident_memory_bytes`, `process_virtual_memory_bytes`,
  `process_cpu_seconds_total`, `process_start_time_seconds`,
  `process_open_fds`, `process_max_fds` (`crates/server/src/
  process_metrics.rs`, from `/proc/self`), so a member's RSS can be
  watched off the node (#84) and etcd dashboards' process panels work.
- `READ_ONLY=1` for `tests/bench/blade_getlatency.py`: GETs and
  LISTs of kube-system's Leases only, for a member that cannot write.
- `tests/bench/blade_getlatency.py <node>`: rustkube's
  get-latency / lease-churn load (40 clients renewing their own Lease and
  listing) against a test node's apiserver, with GET and renewal
  percentiles and fastetcd's WAL / checkpoint / write-behind metrics read
  off the node (:2381). Its first run on server3 found #138.

### Documentation
- `docs/benchmarks/blade-server3.md` § Reads: apiserver GET of
  the cilium-operator lease on server3, p99 2.9–6.4 ms idle, 9.4–12.1 ms
  at 4 300–4 700 GETs/s; value cache 99.8% hits over the day. 03 §
  Memory: the process metrics.

## [v1.23.1] — 2026-10-07

### Fixed
- Auth errors use etcd's codes and texts (#105, #127), so clientv3,
  etcdctl and client-go recognise them (they match by message). A token
  a member does not know (it restarted, or has not applied that
  `Authenticate` yet) is `Unauthenticated` `etcdserver: invalid auth
  token`, and no token `InvalidArgument` `etcdserver: user name is
  empty`: clientv3 now re-authenticates and retries by itself, where it
  used to fail for good. A denied call is `etcdserver: permission denied`
  (detail at debug); a bad name or password `InvalidArgument`
  `ErrAuthFailed`; `Authenticate` with auth off `ErrAuthNotEnabled`; a
  password change during `Authenticate` `ErrAuthOldRevision` (clientv3
  retries); user/role not found or existing and no root user on
  `AuthEnable`: etcd's `FailedPrecondition` texts; empty user/role name
  and a missing permission: etcd's `InvalidArgument` texts
  (`crates/server/src/etcd_errors.rs`). Behaviour changes, as etcd: a
  tokenless call is `InvalidArgument` (was `Unauthenticated`; on the
  gateway 400, was 401), and `Authenticate` is refused while auth is off.

### Added
- `tests/clientv3_reauth.sh [baseline-ref]`: etcd's Go client (v3.5.17)
  keeps one client across a member restart that loses every token. This
  tree passes; v1.23.0 fails with its own text.

### Documentation
- 03 § Auth errors.

## [v1.23.0] — 2026-10-07

### Changed
- A lease keep-alive is no longer a raft proposal (a WAL fsync
  and an apply each). As etcd's lessor: the leader confirms it still
  leads and renews the lease in RAM (`crates/raft/src/lessor.rs`); a
  follower forwards it (`ForwardWrite`, renewed there by a 1.23 leader);
  TimeToLive is answered by the leader (new peer RPC `LeaseTimeToLive`;
  a follower of an older leader answers itself, as before); expiry uses
  the later of the persisted deadline and the RAM renewal, and is still a
  `LeaseRevoke` through Raft.
- A new leader, or a restarted single
  member, gives every lease a full TTL from when it leads (etcd's
  `Promote`), since renewals live only in the old leader's RAM. A lease
  can outlive its TTL by up to one TTL across a leader change, which
  etcd's lease contract allows; it never expires early.

### Added
- RAM renewal waits until every member answers `ConfirmLeader`
  with 1.23 or later (the #56 version gate, now
  `fastetcd_raft::version_gate`): an older member made leader would
  expire every lease kept alive in RAM. Until then keep-alives go through
  Raft. Metrics `fastetcd_lease_renewals_total{path}`,
  `fastetcd_lease_promotions_total`.
- `fastetcd-bench --mode keepalive`, `tests/keepalive_bench.sh`
  (etcd's `benchmark lease-keepalive`); `BENCH_ARGS` for
  `tests/read_latency.sh`.
- `tests/bench/blade_pods.py`, the 5-pod run against a node's
  apiserver and fastetcd metrics.

### Documentation
- 00 (design rule 5), 03 (leases, mixed versions, metrics),
  presentation.

### 2026-10-07 — #95 measured on an X9 blade (#101)
- `docs/benchmarks/blade-server3.md`: the kubelet's status
  report on server3's spinning disk with fastetcd v1.18.0 (release
  11.91), 6 runs of 5 pods: p50 39.5 ms (v1.12: 236–1,040 ms; target
  < 30 ms not met), 7 of 30 at 0.29–0.82 s, each in a window of 300–550 ms
  WAL fsyncs; fastetcd not queueing (1.0–1.4 proposals per fsync).
  Follow-up: #135. Raw data in `docs/benchmarks/data/blade-server3/`.

## [v1.22.0] — 2026-10-07

### Added
- A `Txn` whose success or failure ops include a `Txn` runs, as
  in etcd since 3.3, instead of answering `Unimplemented`. As etcd's
  release-3.5 `applyTxn`: every compare, nested ones included, is
  judged on the state before the txn; the ops run in order in one write,
  at one revision, sub-revisions in execution order, and a Range sees
  earlier writes inside or outside a nested txn; a nested `ResponseTxn`
  carries its `succeeded` and its branch's responses. A refusal anywhere
  inside refuses the whole txn. The leader's precheck (#19, #49) follows
  the nested branches. Storage: `TxnOp::Txn(NestedTxn)` and
  `TxnOpResult::Txn`, appended last.
- A member older than 1.22 cannot decode a nested txn, so one
  is proposed only once every member answers `ConfirmLeader` with 1.22
  or later (`version_gate.rs`, cached per membership): an older member
  gets `FailedPrecondition` naming it, an unreachable one `Unavailable`.
  Flat txns are not gated.

### Documentation
- 00, 03 (mixed versions), README, presentation.

## [v1.21.0] — 2026-10-07

### Added
- `fastetcd-ctl` takes etcdctl's `--cacert`, `--cert` and
  `--key` (and `ETCDCTL_CACERT`/`_CERT`/`_KEY`), so `status`, `defrag`,
  `alarm`, `snapshot-save` and `auth members`/`adopt` work against a
  member with `--cert-file`, and with `--client-cert-auth` (the
  certificate's CN is the user when auth is on). `--cacert` is required
  for `https://`; TLS options with `http://`, or `--cert` without
  `--key`, are refused naming the flag. A scheme-less endpoint is
  `https://` with TLS options, else `http://`, as in etcdctl. All
  commands share one connection, and `--user`'s token now goes on every
  command (it went only on `auth`). `tests/ctl_tls.sh`: real binaries,
  an openssl CA, 31 checks.

### Documentation
- 01 (ctl options), 02 (`ctl_tls.sh`; #45 no longer a known
  flake), 03 (TLS note), presentation.

## [v1.20.1] — 2026-10-07

### Fixed
- Beginning to receive a snapshot rolled off the retained one,
  before anything was known about the transfer (#45). openraft resends a
  snapshot whose `InstallSnapshot` timed out, even once the first copy is
  installed, and then drops the duplicate (`snapshot last_log_id <=
  committed`), so the resend deleted the snapshot just installed and the
  member was left with none (rebuilt on demand, at a cost). Caught in the
  logs of `snapshot_transfer_grpc` on dev (the "flaky" test, ~1 in 5 on a
  loaded box): install took 1.4 s, the leader's RPCs timed out and
  resent, `rolled off an old snapshot index=199`, then `No need to
  install snapshot`. Retention is now enforced when a received snapshot
  is adopted; a full disk is still handled by the receive's ENOSPC path
  (discard retained, write again). A member receiving a snapshot holds
  one extra copy while the disk has room.

## [v1.20.0] — 2026-10-07

### 2026-10-07 — older members kept from a batched log (#77)
- **feat:** A member records that its log has held a batched entry
  (`mvcc_meta` `batched`, set by the first applied batch, carried by
  snapshots, never cleared; gauge `fastetcd_log_has_batched`), so "has
  this cluster batched?" has an answer (#77). Only docs guarded against
  putting a member older than 1.10, which cannot decode a batch and stops
  at the first, behind such a log.
- **feat:** With that set, `MemberAdd` asks the new member `ConfirmLeader`
  first and refuses one older than 1.10 (FailedPrecondition, naming it);
  one not reachable yet is added as a learner (the usual `member add`,
  then start order), and `MemberPromote` (or adding it as a voter) needs
  it to answer as 1.10 or later.
- **feat:** The leader asks every member every 30 s while the log holds
  batches; one answering as older (a downgrade) is logged as an error and
  counted in `fastetcd_members_unable_to_read_batches`.

## [v1.19.0] — 2026-10-07

### 2026-10-07 — fastetcd-migrate imports leases and auth (#60)
- **fix:** `fastetcd-migrate` read only etcd's `key` bucket (#60): keys
  kept lease ids no lease backed, so they never expired, and users, roles
  and `auth enable` were lost. It now grants every lease in the `lease`
  bucket (id kept; the TTL etcd restores with, remaining if recorded,
  else full; at least 2 s) before the keys, imports a key whose lease the
  snapshot lacks without one (counted, warned), and imports roles,
  permissions, users (roles, `no_password`, bcrypt hash as is) and the
  enabled flag through the server's own auth operations. The summary
  line gives leases, users, roles and whether auth is on.
- **feat:** `Authenticate` also verifies bcrypt password hashes (etcd's),
  so users migrated from etcd keep their passwords; passwords set in
  fastetcd stay argon2. New dependency `bcrypt` 0.15.
- **fix:** `fastetcd-migrate` could not read any file etcd wrote
  (`open bolt: invalid database`); its tests passed only because their
  BoltDB files were written by the same crate that read them. The
  bbolt-rs crate uses its own magic number unless built with `compat`,
  and even then refuses a file without a freelist ("PGID_NO_FREE_LIST not
  currently supported"), which is how etcd always writes (`NoFreelistSync`).
  The tool now reads BoltDB itself (`crates/migrate/src/bolt.rs`,
  read-only: meta pages with their checksum, branch and leaf pages,
  overflow pages, inline buckets; positioned reads, so a large snapshot is
  not held in memory). bbolt-rs only writes the tests' fixtures, with Go
  bbolt's magic. And `etcdctl snapshot save` writes the database followed
  by its 32-byte SHA-256: as etcd's own restore does, a file whose
  size % 512 == 32 has its hash checked (a mismatch is refused) and cut
  off, through a temporary copy; a file without one (a member's
  `member/snap/db`) is read as it is.
- **fix:** `--preserve-revisions` attached a key to every lease any of
  its records named, deleted generations included; `bulk_load_records`
  now attaches only a live key's latest record.
- **test:** `tests/migrate_e2e.sh` (real etcd v3.5 → snapshot → migrate →
  fastetcd, checked with etcdctl); round-trip test with leases, an
  orphaned lease id and auth, in both modes; bcrypt verify unit test.

## [v1.18.0] — 2026-10-07

### 2026-10-07 — `--snapshot-count` counts writes (#80)
- **fix:** `--snapshot-count` and `--max-in-snapshot-log-to-keep` count
  writes (proposals), as etcd's do, not raft log entries (#80). Since
  1.10 an entry can be a batch of hundreds of writes, and openraft's
  policy counts entries, so the log between snapshots, and what a purge
  kept, held far more than the flags (and `fastetcd sizing`) say. The
  state machine now counts the writes in each entry it applies
  (`snapshot_policy::ProposalLog`), and a task on every member triggers a
  snapshot once `--snapshot-count` writes are applied since the last,
  and after it purges the log to about `--max-in-snapshot-log-to-keep`
  writes (`Raft::trigger().snapshot()` / `purge_log`). openraft's
  entry-counted policy stays as a backstop. Test: 300 writes batched into
  few entries snapshot at 200 writes and purge to >= 100.

### 2026-10-07 — election timeouts are settable (#103)
- **feat:** `--heartbeat-interval` and `--election-timeout` (ms, etcd's
  names; `ETCD_HEARTBEAT_INTERVAL` / `ETCD_ELECTION_TIMEOUT`), defaults
  unchanged (250, 1000; the election range is [t, 2t)). The timeouts were
  fixed, and a 3-member cluster lost its leader under heavy writes on a
  stalling disk (#103). The cause, from openraft 0.9.24: a leader's
  heartbeats come from RaftCore's tick, and RaftCore waits for each
  append's fsync, so a leader is silent for as long as its fsync stalls,
  and past the followers' timeout plus openraft's leader lease (3–4 s by
  default) they elect another. Raising `--election-timeout` above the
  disk's worst stall keeps the leader. The election timeout must be at
  least 2x the heartbeat (etcd asks 5x; the defaults predate it) and at
  most 50 s.
- **feat:** A WAL fsync longer than the election timeout is logged as
  such (`slow raft WAL fdatasync, longer than the election timeout`),
  naming the flag.
- **test:** `multinode_fastpath`: with the leader's fsyncs stalled 6 s
  (`WalLogStore::set_sync_delay`), a 1 s election timeout elects again;
  a 10 s one keeps the leader and term, with the leader's disk alone or
  every member's stalled. (With every disk stalled at a 1 s timeout the
  outcome depends on timing, from no election to 40 terms of churn with
  no leader, so it is not asserted.)

### 2026-10-07 — 3-member write maxima attributed (#83)
- **docs:** The multi-second write maxima on a 3-member cluster under
  `tests/read_latency.sh` are the shared build disk's, not fastetcd's
  (#83). Measured on main (v1.17.1), disk vs tmpfs interleaved, twice:
  on disk the max was 1.57–2.18 s with every member's longest WAL fsync
  0.80–1.13 s at the same moment; on tmpfs (`DATA_ROOT=/dev/shm`) the max
  was 32–81 ms through the leader and a follower alike, at 4–7k
  writes/s, with no election. A write pays the leader's fsync and then a
  follower's in series (#97), which is why a stall shows doubled.

## [v1.17.1] — 2026-10-07

### 2026-10-07 — online defragment after a non-durable commit (#119)
- **fix:** `Maintenance.Defragment` (and the space monitor's reclaim)
  failed with "A transaction is still in progress" whenever the newest
  redb commit was non-durable, which every apply has been since #71:
  always on 1.9–1.11, and on a member taking writes since 1.12 (until
  the next checkpoint). redb refuses to compact then. The defragment now
  makes one empty durable commit under its exclusive lock before
  compacting. Found by the new test container's `medium` and `long`
  (#36).

### 2026-10-07 — test containers (#36)
- **test:** fastetcd's test container per stormcentral's test standard
  (#36): `test/` (crate `fastetcd-test`, a workspace member but not a
  default member, so the golden's build is unchanged), `test/build.sh`
  (static musl test binary + the commit's static `fastetcd`),
  `test/Containerfile` (`FROM scratch`), `test/requires.toml` (nothing
  beyond the namespace), `test/README.md`. `short`: the node's fastetcd
  (health, status, no alarms; KV, watch and lease round trips under
  `/storm-test/<run id>/`, removed; never anything cluster-wide on the
  live store) and the commit's binary serving. `medium`: compaction and
  watch history, lease expiry, RBAC, NOSPACE recovery, snapshot restore,
  SIGKILL durability, 3-member leader loss, all on the suite's own
  members. `long`: night waves (ramp, hold, drain) sized from the pod's
  cgroup, with a throughput and residue trend. `tests/test_container.sh`
  runs all three in an sc-build job.

## [v1.17.0] — 2026-10-07

### 2026-10-06 — a batch takes what queued, bounded by bytes (#94)
- **perf:** A batch of proposals takes everything that queued, up to
  4096 (was 256) or 512 KiB (unchanged), so with 1000 clients waiting
  one fsync carries hundreds of writes instead of at most 256 (#94, P1;
  etcd carries ~350–450 there). Test: 1000 writers on a 200 ms fsync.
- **fix:** A follower far behind on large log entries could never catch
  up: openraft put as many entries as the log reader returned (up to
  300) in one AppendEntries, the peer port decoded at most 4 MiB, the
  follower refused the message, and the leader sent it again, forever
  (8 full batches of 512 KiB were enough); and openraft gives each
  AppendEntries one heartbeat interval (250 ms) to be sent, appended and
  fsynced, resending a slower one at the same size. The WAL reader now
  hands replication at most 1 MiB of entries per AppendEntries (always at
  least one entry), and the peer
  port decodes up to 16 MiB (`PEER_MAX_DECODE_BYTES`), for one client
  write near the client port's own 4 MiB limit.

## [v1.16.1] — 2026-10-06

### 2026-10-06 — lease RPCs are authorized (#47)
- **fix:** With auth on, the lease RPCs are checked against the keys
  attached to the lease, as etcd's are (#47, P1). Any logged-in user
  could revoke any lease (deleting keys it cannot write), renew it, list
  its keys or every lease, and attach keys to a lease whose other keys it
  cannot write. Now, root exempt: `LeaseRevoke` and `LeaseKeepAlive` need
  write on every attached key (a denied keep-alive ends the stream);
  `LeaseTimeToLive` with `keys: true` needs read on each; `LeaseLeases`
  needs read on every key of every lease; a `Put` naming a lease, and
  every put in either branch of a `Txn`, needs write on the keys already
  on it. Rules from etcd release-3.5's source (`checkLeaseRenew` and
  `checkLeaseLeases` were not in the issue's list). New
  `MvccStore::lease_attached_keys` / `all_lease_attached_keys`. Tests:
  `crates/server/tests/authz_lease.rs`.

## [v1.16.0] — 2026-10-06

### 2026-10-06 — leases in raft snapshots (#41)
- **fix:** Raft snapshots carry the lease tables (#41, P1). A snapshot
  held the MVCC and auth tables only, so a member caught up by one had
  no record of leases granted before it and kept stale ones of its own:
  its keys on those leases never expired there, and a revoke deleted
  them on every other member but not on it. A snapshot now carries
  `lease` and `lease_keys` (`LeaseTables`) in a second trailer after the
  auth trailer, and an install replaces every lease row the member had.
  Compatible both ways: a 1.5–1.15 member reads the payload and the auth
  trailer and ignores the new one; a snapshot from an older leader
  leaves the lease tables alone, as before. Members caught up by a
  snapshot before this keep wrong lease tables until re-added
  (03-deploy § Leases in raft snapshots). Moving #19's lease check to
  apply is #115.

### 2026-10-06
- **docs:** The presentation's open-issues slide no longer lists #61.

## [v1.15.0] — 2026-10-06

### 2026-10-06 — a live snapshot can be restored (#61)
- **fix:** A live snapshot can be restored (#61, P1). `Maintenance.Snapshot`
  (`etcdctl snapshot save`, `fastetcd-ctl snapshot-save`) streamed the
  raft snapshot body, which `fastetcd restore`, `fastetcd-migrate` and
  etcdctl all refuse, and which lacks leases and node metadata anyway.
  It now streams the `--backup-dir` backup format: every table (through
  the write-behind layer, so every applied write), one point in time,
  SHA-256 at the end, encoded straight into the stream (no temp file,
  no second copy in memory). `fastetcd restore <file>` restores it.
  `fastetcd-ctl snapshot-save` writes `<file>.part`, checks the
  checksum, then renames it. `fastetcd restore` on an old raft-snapshot
  file says what it is. **Changed:** the bytes `Maintenance.Snapshot`
  streams are a different format (neither was BoltDB).

## [v1.14.1] — 2026-10-06

### 2026-10-06 — boolean flags take values; the chart's pods start (#53)
- **fix:** Boolean flags take values as etcd's do (#53, P1). They were
  clap switches, so `--auto-defrag=false` and `--upgrade-backup=false`
  were parse errors (only the env var set to exactly `false` turned
  them off), and the Helm chart's `--auto-defrag=true` stopped every pod
  at argument parsing; `--client-cert-auth=true` failed the same way.
  Now `--flag` alone is true, `--flag=true|false|1|0` sets it, and the
  env vars take true/false, 1/0, yes/no, on/off. For
  `--auto-defrag`, `--upgrade-backup`, `--enable-grpc-gateway`,
  `--client-cert-auth`, `--peer-client-cert-auth`,
  `--force-new-cluster` and `--enable-pprof`. Every form accepted
  before still works (`--enable-grpc-gateway false` too).
- **test:** `tests/chart_args.sh` renders the Helm chart and starts the
  rendered pod command (defaults, `space.autoDefrag=false`, bool flags
  in `extraArgs`), checking it serves.

## [v1.14.0] — 2026-10-06

### 2026-10-06 — a request that cannot apply stops no member (#49)
- **fix:** A request that cannot apply no longer stops every member
  (#49, P0). A put with `ignore_value`/`ignore_lease` on a missing key
  (alone, in a Txn, or in a batch with other clients' writes), `Compact`
  at a future revision, at 0 or below the compacted one, a keep-alive of
  a lease that does not exist, a `LeaseGrant` with TTL <= 0 and a Txn
  `Range` at a compacted or future revision failed inside raft apply,
  which openraft turns into a storage error that stops the state machine
  on every member, and again on every restart as the entry replays. Each
  is now a refusal (`Refusal`, raised before anything is written; a new
  `FastetcdLogResponse::Refused`): the state machine answers it and keeps
  applying, the rest of a batch included. The leader also refuses them
  before proposing (`precheck.rs`). Clients get etcd's errors:
  `etcdserver: key not found` (InvalidArgument), `etcdserver: mvcc:
  required revision has been compacted` / `is a future revision`
  (OutOfRange, also on a plain Range, which used to carry fastetcd's
  own text), lease not found (NotFound); a keep-alive of a missing lease
  answers TTL 0, and a grant of TTL <= 0 gets 2 s, as in etcd. A member
  older than 1.14 still stops on such an entry (03-deploy).
- **feat:** A raft WAL fdatasync of 1 s or more is logged at WARN
  (`slow raft WAL fdatasync`, `took_ms`), as etcd logs a slow fdatasync,
  so a write stall can be matched to the disk (#83).
- **test:** `fastetcd-bench --mode read-under-load` prints when its
  slowest writes started (UTC); `tests/read_latency.sh` with `METRICS=1`
  prints leader-change and proposal metrics and the members' election
  and slow-fsync lines, and `DATA_ROOT=/dev/shm` runs it on tmpfs (#83).

### 2026-10-05
- **docs:** `docs/presentation.md`, a 12-slide Marp deck of fastetcd's
  purpose and functionality (#27): the problem, its place in StormCOS
  (stormcentral's graph), one architecture diagram, what works today
  from the code, measured performance, interfaces, how it ships (special
  golden, stage mode) and runs (stormd), open issues that matter, and
  planned work on its own slide. Linked from the README.

## [v1.13.0] — 2026-10-03

### 2026-10-03 — group commit on a slow disk (#95, #94)
- **perf:** Group commit: concurrent writes share one WAL fsync. openraft
  0.9's RaftCore appends one log entry at a time and waits for its fsync
  (`append_to_log` awaits `LogFlushed`; #85 assumed it did not), so the
  proposer is where writes are grouped. It now keeps one batch in RaftCore
  (was three): the next is formed when the previous is answered, from
  everything that arrived meanwhile. With three, batches queued in
  RaftCore behind each other, RaftCore could not answer one while it
  waited on the next one's fsync, and a write waited ~4 fsyncs; on a slow
  disk 20 writers got ~4 writes per fsync, now ~10.
- **perf:** Checkpoints are paced: after one that took `d`, the next waits
  at least `4·d`. On a slow disk a checkpoint took about the 100 ms
  interval, so they ran back to back and every WAL fsync queued behind
  the data file's writes (fsync 35 → 119 ms on benchslow). Unchanged on
  a fast disk.
- **feat:** Metrics `fastetcd_wal_entries_synced_total`,
  `fastetcd_wal_proposals_synced_total` (writes per fsync = their rate /
  the fsync rate) and `fastetcd_checkpoint_pace_seconds`.

### 2026-10-03 — upstream etcd vs fastetcd, side by side (#90)
- **docs:** `docs/benchmarks/etcd-vs-fastetcd.md`: etcd v3.7.2 vs fastetcd
  v1.11.0 and v1.12.0 on three disks (build box SSD, a pve VM on SSD, a
  pve VM throttled to ~6 fsyncs/s), 1 and 3 members: etcd's own
  `benchmark` workloads, SIGKILL durability, resources, and rustkube's
  apiserver rig. Raw CSVs and summaries in `docs/benchmarks/data/`.
  In short: under Kubernetes-shaped load v1.12's GET p99 is 10–800x lower
  than etcd's (1.0 vs 835 ms on the slow disk) at a similar write rate;
  etcd keeps more raw write throughput (1.3–6x), most on the slow disk
  (#94), and much faster lease keepalives (#92). No server lost an
  acknowledged write in 21 crash-restarts.
- **test:** `tests/bench/compare.sh <etcd-version>` (downloads and checks
  etcd, builds its `benchmark` with a downloaded Go, builds fastetcd tags;
  `MEMBERS`, `PARTS`, `BENCH_TIMEOUT`, sizes), `tests/bench/extract.py`,
  `tests/bench/report.py`. `fastetcd-bench --mode durability-write` /
  `durability-check`.
- **chore:** `.gitignore` no longer hides `docs/benchmarks/data/`.

## [v1.12.0] — 2026-10-02

### 2026-10-02 — streaming writes: the raft log is a sequential WAL (#85, P1)
- **perf:** The raft log moved out of the data file into a write-ahead
  log in `<data-dir>/wal/`: checksummed records appended to segments
  written full of zeros in the background before use
  (`--wal-segment-bytes`, 16 MiB), so a sync converts no extents. A client's write now waits
  for one sequential append-and-`fdatasync` instead of a durable redb
  commit, which also wrote every B-tree page the applies since the last
  one had dirtied (seeks on a spinning disk). One writer thread syncs
  everything appended while the previous sync ran (group commit), and
  openraft is told each append is durable afterwards, so RaftCore does
  not wait on the disk.
- **perf:** Applies no longer touch the data file. A write-behind layer
  in front of redb (`crates/storage/src/write_behind.rs`) holds each
  applied batch in RAM, visible to reads at once (a snapshot is the
  held batches plus a redb snapshot, merged). A checkpoint every
  `--wal-checkpoint-interval-ms` (100) or `--wal-checkpoint-entries`
  (10000) applied entries writes them into redb as one commit and makes
  it durable; applies never wait on that fsync. Past
  `--write-behind-bytes` (64 MiB) held, an apply writes them out first.
  A crash loses at most the applies since the last checkpoint, and the
  WAL replays them. WAL segments are deleted only once a checkpoint
  covers every entry in them.
- **feat:** `--wal-cache-bytes` (64 MiB): recent raft entries kept in RAM
  for replication. Metrics `fastetcd_wal_*`, `fastetcd_checkpoint*` and
  `fastetcd_write_behind_*`. `tests/read_latency.sh` with `METRICS=1`
  prints them after a run.
- **perf:** Measured with `tests/read_latency.sh v1.11.0` on dev's disk,
  one member, two rounds: writes 601 → 1662/s and 693 → 1513/s, write
  p50 44 → 18 ms, p99 135 → 111 and 125 → 135 ms, linearizable read p99
  50 → 0.6 ms.
- **feat:** In-place upgrade: on the first start the log is copied from
  redb's `raft_log`/`raft_meta` into a new WAL (built in `wal.tmp/`,
  renamed), then `raft_log` is cleared. One-way: do not downgrade a
  member below this version afterwards. The vote and purge point stay
  mirrored in `raft_meta`, so a backup restores to a consistent log.
- **fix:** Restoring a data file (corruption recovery, `fastetcd restore`)
  moves `wal/` and `snapshots/` aside with the replaced file
  (`*.corrupt.<ts>` / `*.replaced-<ts>`); `fastetcd restore` used to leave
  newer raft snapshots in place.
- **docs:** 00-design, 01-configuration (new flags), 03-deploy (storage
  layout, write path, upgrade note), 04-disk-space (three WAL segments
  in the sizing model; ladder unchanged), 05-backup-and-recovery, README.

## [v1.11.0] — 2026-10-02

### 2026-10-02 — a GET of a hot key reads nothing from disk (#82, P1)
- **perf:** Every key's index is resident in RAM, as etcd's `treeIndex`
  is: loaded from `mvcc_idx` at open (in chunks), changed only after the
  engine commit it describes and under the store's write lock, so it is
  never ahead of the disk. Range, Txn compares, apply and compaction use
  it; compaction no longer reads the whole index table. A Range with a
  limit now reads only the records it returns.
- **feat:** A latest-value cache: the newest record of recently used
  keys, a sharded LRU bounded by `--value-cache-bytes` (default the
  smaller of 128 MiB and 5% of the cgroup limit or RAM; `0` = off).
  Writes put what they wrote in it, deletes remove it, a latest-revision
  read that missed fills it. An entry is used only at the exact revision
  the index names, so a read can come out faster but never different.
  Values over `--value-cache-max-entry-bytes` (256 KiB) are not cached.
- **feat:** `--engine-cache-bytes` sets redb's page cache, 256 MiB by
  default (redb's own default is 1 GiB), so memory is about the two
  budgets plus the index.
- **feat:** Metrics `fastetcd_value_cache_{hits,misses,evictions}_total`,
  `fastetcd_value_cache_{bytes,entries,budget_bytes}`,
  `fastetcd_key_index_{keys,bytes}`, and the histogram
  `fastetcd_mvcc_get_duration_seconds{cache=hit|miss}`.
- **fix:** A raft snapshot install commits the new tables and reloads
  the revision counters (and now the index) under the store's write
  lock (`MvccStore::install_tables`); before, a read could run between
  the commit and the reload and see new data at the old revision.
- **perf:** The leader's lease precheck (#19) no longer holds the
  store's write lock while it evaluates a Txn's compares, and skips them
  when neither branch names a lease. Holding it queued every CAS Txn with
  the apply loop (found benchmarking this change: writes -15%, 4-6 s
  stalls).
- No change to the data file or the wire: any version opens the file.
- **docs:** `docs/03-deploy.md` § Memory, `docs/01-configuration.md`
  § Memory, README.

### 2026-10-02 — documentation from the code, since 2026-09-25
- **docs:** `--snapshot-count` counts raft log entries, and since 1.10
  an entry can be a batch of up to 256 writes, so it is not etcd's
  per-proposal count (`docs/01-configuration.md`, `docs/04-disk-space.md`).
- **docs:** the sizing model's raft-log term assumes 2 KiB per entry;
  batched entries can be up to 512 KiB, so a busy cluster can carry more
  log between snapshots than `fastetcd sizing` shows (filed #80).
- **docs:** README architecture shows the read index (`ConfirmLeader`)
  and the proposer (group commit); CLAUDE.md architecture, layout and
  plan status (#24, #26 shipped in v1.9.0; #21's decision is #68).

## [v1.10.0] — 2026-10-01

### 2026-10-01 — multi-member reads off RaftCore, batched proposals (#75, P1)
- **perf:** A leader with other voters serves linearizable reads without
  openraft's RaftCore queue. It confirms leadership itself over a new
  peer RPC, `ConfirmLeader`, which each member answers from the vote term
  its log store last saved (openraft saves a vote before granting it, so
  a quorum answering a term no newer than the leader's means no newer
  leader existed when the read arrived). One round at a time; reads that
  arrive meanwhile share the next. Followers' forwarded reads use it too.
  Any failure, older member or lost quorum falls back to
  `ensure_linearizable`.
- **perf:** The read index is the first index of the leader's term, not
  the commit index: the leader answers a write only after applying it,
  and earlier terms' writes lie below its blank entry. Reads no longer
  wait for the apply pipeline (about one fsync per read on one data
  file). Single members too.
- **perf:** Group commit. Proposals go through a `Proposer`: up to three
  `client_write`s in flight; what queues meanwhile goes as one
  `FastetcdLogEntry::Batch` (≤ 256 proposals, ≤ 512 KiB), one append and
  one fsync, each proposal at its own revision with its own answer.
  Forwarded writes are batched on the leader. Batches only once every
  member answers `ConfirmLeader`; re-checked on membership changes.
- **feat:** A batch's apply records `(index, done)` in `mvcc_meta` with
  each proposal's commit, so a crash mid-batch replays exactly what did
  not reach disk. New metrics: `fastetcd_read_index_total{path}`,
  `fastetcd_proposal_batches_total`, `fastetcd_proposals_batched_total`,
  `fastetcd_proposals_single_total`.
- **BREAKING (mixed versions):** once batched entries are in the log, a
  member older than this release cannot read it: do not add one, do not
  downgrade below it (docs/03-deploy.md § Multi-node). Rolling upgrades
  are safe: nothing is batched until every member is upgraded.
- **test:** `crates/raft/tests/batch_apply.rs`,
  `crates/server/tests/multinode_fastpath.rs`; `MEMBERS=3
  tests/read_latency.sh` for a local three-member cluster.
- Measured on dev (`tests/read_latency.sh v1.9.0`, 40 GET+CAS clients):
  one member, writes 170 → 540/s (mean 235 → 58 ms); three members,
  writes 141 → ~300/s (mean ~250 → ~97 ms), linearizable Range p50
  31.8/17.2 → 0.69/0.55 ms and p99 298 → 66/57 ms through two members.

## [v1.9.0] — 2026-10-01

### 2026-10-01 — linearizable reads stop queueing behind writes (#71, P0)
- **perf:** A linearizable Range on a single-member cluster no longer
  waits for the writes queued ahead of it. It went through openraft's
  `ensure_linearizable`, a message to RaftCore, which handles one client
  write at a time and awaits that write's log fsync before the next
  message; under ~58 writes/s that was p50 157 ms, p99 6.9 s, with
  serializable reads at 0.2 ms (rustkube#177). A leader that is the only
  voter now takes its read index from the log store (the larger of the
  committed index and the last durable log index) and waits on the
  state machine's own applied index, never entering RaftCore
  (`crates/raft/src/read_index.rs`). Leadership, a single-voter
  non-joint membership and no unshown membership change are checked
  first; otherwise the read uses openraft's read-index as before.
- **perf:** A write costs one fsync instead of three. State-machine
  applies (with the `last_applied_log_id` folded into each) and openraft's
  committed index commit without an fsync (redb `Durability::None`):
  visible at once, persisted by the next durable commit, normally the
  next log append. A crash loses at most the last few applies and their
  applied position together, and they replay from the durable log into
  the same state. Snapshot installs, bulk loads and startup recovery
  stay durable. `KvStore::sync` on redb is now a real flush, run on a
  ctrl-c shutdown.
- **perf:** `RedbEngine::commit` opens its write transaction inside
  `spawn_blocking`; redb's `begin_write` blocks the thread until the
  previous writer finishes, which used to stall a tokio worker.
- **feat:** `fastetcd-bench --mode read-under-load` (the #71 load: GET +
  CAS Txn loops, a probe of linearizable and serializable Ranges) and
  `tests/read_latency.sh [baseline-ref]` to run it on a real disk.
- **test:** `crates/raft/tests/read_barrier.rs` (slow-fsync engine, 30
  queued writes: the local barrier stays under 3 fsyncs and sees every
  acknowledged write; openraft's own barrier, the control, queues for
  seconds); `crates/server/tests/apply_replay.rs` (SIGKILL after
  acknowledged puts loses unsynced applies on disk; a restart replays
  them with the same revisions).

### 2026-09-29 — documentation from the code (#26)
- **docs:** `docs/01-configuration.md`: every server flag with its
  default, `FASTETCD_*` variable and `ETCD_*` fallback, the ports, the
  offline subcommands and the other binaries, taken from the binaries'
  own `--help`.
- **docs:** README: current status (v1.8.0) and what is not implemented;
  ports; the server is redb-only (it claimed runtime-selectable engines);
  no CI claim; real test count (271 in 34 binaries).
- **docs:** 00-design marks design vs code (storage engines, apply-thread
  pinning, OTLP, migration scope, wire-compat table); 02-testing
  describes sc-build and the real test files; 03-deploy: storage engine,
  backups that actually restore, multi-node flags, a Helm chart section
  (PVCs are stormblock volumes on StormCOS), peer-TLS fix version
  (v1.4.1, not v1.5.0).
- **docs:** `--help` text: `--initial-advertise-peer-urls` and
  `--advertise-client-urls` are used (help said they were not);
  `--log-level` / `--max-request-bytes` say they are ignored; the
  subcommand list is complete. `fastetcd-migrate`'s module doc.
- **docs:** Filed what the docs promised and the code does not do: #53
  (the Helm chart's `--auto-defrag=true` stops every pod), #54, #55, #56,
  #57, #58, #59, #60, #61 (a live snapshot cannot be restored).

### 2026-09-29
- **docs:** Deploy docs say how fastetcd really ships, and drop GHCR,
  which this platform deliberately does not use (#24).
  `docs/03-deploy.md` § How fastetcd reaches a StormCOS node describes
  the golden stormcos's `build-goldens.sh` builds from `main` (`cargo
  build --release --locked`, musl, three binaries, the `fastetcd` and
  `fastetcd-data` goldens and the flags it runs with), what that asks of
  this repo, and links stormcos `docs/goldens.md`. The GHCR push steps
  are gone; container images are build-your-own. The packages section
  no longer claims every release is on GitHub Releases (they stop at
  v1.2.0).
- **chore:** Helm chart 0.2.0: the default image is `fastetcd` (build
  and push your own) instead of `ghcr.io/glennswest/fastetcd`;
  `appVersion` 1.8.0 (was 0.4.0).

## [v1.8.0] — 2026-09-29

### 2026-09-29
- **feat:** etcd's v3 JSON gateway on the client port (#28). `POST
  /v3/kv/range`, `/v3/maintenance/status`, `/v3/cluster/member/list`
  and every other KV, Lease, Watch, Cluster, Maintenance and Auth
  method, at etcd v3.6's routes and with its JSON (proto field names,
  64-bit numbers as strings, base64 bytes, enums by name). Errors map to
  grpc-gateway's HTTP statuses with `{"code","message"}` bodies.
  Snapshot, watch and keepalive stream a JSON line per message. Each
  call goes through the same auth interceptor and service code as gRPC,
  with the token in the `Authorization` header, and counts in
  `grpc_server_*_total`. `--enable-grpc-gateway` (default on,
  `ETCD_ENABLE_GRPC_GATEWAY`). Docs: `docs/03-deploy.md` § v3 JSON
  gateway.
- **feat:** The gRPC API also accepts the auth token as `authorization`
  metadata, as etcd does, not only `token`.

## [v1.7.1] — 2026-09-29

### 2026-09-29
- **build:** `Cargo.lock` lists `fastetcd-server`'s `http-body` dependency (added for #29 without a lock update), so `cargo build --locked` works again.

## [v1.7.0] — 2026-09-29

### 2026-09-29
- **feat:** `/metrics` shows traffic (#29), with etcd's names so
  existing dashboards work:
  - `grpc_server_started_total` / `grpc_server_handled_total{grpc_type,
    grpc_service,grpc_method[,grpc_code]}` for every call on the client
    port (an axum middleware reads `grpc-status` from the headers or
    trailers; a call whose client leaves first is `Canceled`).
  - `etcd_debugging_mvcc_{put,delete,range,txn}_total`, counted in
    `MvccStore` (writes as applied on every member).
  - `etcd_debugging_mvcc_watch_stream_total`, `…_watcher_total`,
    `…_slow_watcher_total` (resyncing from history, or blocked on a
    client that is not reading).
  - `etcd_server_proposals_committed_total` / `…_applied_total` (the
    raft indexes; committed from the log store's `save_committed`),
    `etcd_server_proposals_pending`, `etcd_server_is_leader`,
    `etcd_server_id{server_id}`.
  - `fastetcd_engine_info{engine}`, documented before but never
    registered; `fastetcd_watch_resyncs_total` /
    `fastetcd_watch_lag_cancels_total` now registered too.
- **fix:** A watch stream's tasks end as soon as its client goes away,
  not at the next event they fail to send, which might never come.
- **docs:** Metrics reference in `docs/03-deploy.md` § Metrics.

## [v1.6.1] — 2026-09-29

### 2026-09-29
- **fix:** A Range's header revision is the revision its contents were
  read at (#50, P0). The handler read `current_revision()` *after* the
  range, so a commit in between stamped the response with a later
  revision than its contents. Kubernetes uses that header as its LIST
  snapshot and WATCHes from after it, so the write in between was
  skipped for good (rustkube's LIST probe: 165 of 255 LISTs
  inconsistent). A follower's forwarded read also took the follower's
  own revision instead of the leader's.
  - `MvccStore::range_with_revision` returns the revision the read was
    taken at; the engine snapshot is now taken under the write-state
    lock, so a compaction can't land between the two.
  - `ForwardRead` replies carry the leader's read revision after the
    result, so a follower older than this still decodes them. A reply
    from an older leader has none: the follower falls back to its own
    revision (the old answer) and warns once, only during an upgrade.
  - Tests: concurrent writers + LIST readers against MVCC, a single
    node over gRPC, and a 3-member cluster (leader and follower) with
    LIST → WATCH from the next revision on the same member; the
    forwarded-read wire both ways between versions.

## [v1.6.0] — 2026-09-28

### 2026-09-28
- **feat:** A client certificate's Common Name is its etcd user (#20).
  Under `--client-cert-auth`, a request with no `token` is made by the
  user named by the verified client certificate's CN, as in etcd
  (`AuthInfoFromTLS`). No `Authenticate` call or password is needed,
  and roles scope what each mTLS client may read, write and watch. This
  is the ClusterMesh model: one etcd user per remote cluster.
  - A token still wins over the certificate, and an invalid token is
    refused rather than replaced by the certificate.
  - A CN with no user is refused, and a certificate with no CN names
    no one.
  - The Auth service, which is not behind the interceptor, resolves
    its caller the same way.
  - New dependency: `x509-parser`.

  Tests: `crates/server/tests/client_cert_auth.rs`, over a real mTLS
  client port built as `main.rs` builds it. Docs: `docs/03-deploy.md`
  § Users by client certificate.

## [v1.5.2] — 2026-09-28

### 2026-09-28
- **fix:** A `Put` naming a lease that does not exist is refused (#19),
  with etcd's `etcdserver: requested lease not found` (NotFound).
  Nothing is written. It used to be accepted, and the key recorded the
  dangling lease id and never expired. This is the error a client uses
  to learn its lease expired between grant and use. Details:
  - Applies to a standalone `Put` and to the puts of the branch a
    `Txn` takes (the other branch is not checked, as in etcd).
    `ignore_lease` puts are not checked.
  - The check runs on the leader just before proposing
    (`crates/raft/src/precheck.rs`). A follower forwards, and on a miss
    the leader runs a read barrier first, so a lease granted through
    any member can be used at once through any other.
  - etcd checks at apply; that would let members diverge while raft
    snapshots lack the lease tables (#41) and while older members
    accept such puts. The one difference left: a lease that expires
    between the check and the apply still gets the key attached.

  Tests: `crates/server/tests/lease_put.rs` and
  `a_put_names_a_lease_through_any_member` in `multinode_grpc.rs`.
  Found alongside: #49 (P0).

## [v1.5.1] — 2026-09-28

### 2026-09-28
- **fix:** Admin RPCs are root-only while auth is on (#31). Any
  authenticated user could grant itself the root role, disable auth,
  change root's password, stream a snapshot of the whole keyspace, or
  add and remove members. The rules now match etcd's release-3.5 source
  (`needAdminPermission`, `authMaintenanceServer`,
  `checkMembershipOperationPermission`):
  - Auth: every change, `AuthStatus`, `UserList` and `RoleList` need
    root. A user may `UserGet` itself and `RoleGet` a role it holds.
    `Authenticate` is open.
  - Maintenance: `Snapshot`, `Defragment`, `Hash`, `HashKV`,
    `MoveLeader`, `Downgrade`, and `Alarm` other than list need root.
    `Status` and `Alarm` list need a login.
  - Cluster: member add / remove / update / promote need root.
    `MemberList` needs a login.
  - `Compact` stays open to any logged-in user, as in etcd. Changing
    your own password needs root, as in etcd.

  The Auth service now resolves its caller from the `token` metadata
  itself, since it is not behind the interceptor. With auth off,
  everything stays open. Tests: `crates/server/tests/authz_admin.rs`.
  Docs: `docs/03-deploy.md` § What needs root. The remaining gap, lease
  RPCs, is filed as #47.

## [v1.5.0] — 2026-09-28

### 2026-09-28
- **feat:** Auth state is replicated through Raft (#32). Every auth
  change used to commit to the serving member's engine only, and tokens
  were a per-member set, so on a multi-member cluster RBAC held on some
  members and not others, and a token from one member was rejected by
  the next. Now, as in etcd:
  - every change (users, roles, permissions, `AuthEnable`/`Disable`) is
    a log entry (`FastetcdLogEntry::Auth`) that every member validates
    and applies, with the same result everywhere. Passwords are hashed
    before proposing;
  - `Authenticate` proposes its token, so any member accepts it. The
    entry carries the hash the password was checked against, and is
    refused if the password changed meanwhile. Tokens stay in memory
    only, as with etcd's "simple" tokens;
  - raft snapshots carry the auth tables, in a trailer after the
    unchanged payload. An older member still decodes the payload, and
    a snapshot from one leaves the auth tables alone;
  - the serving member waits until it has applied the change itself,
    so it is in effect there when the call returns.
- **feat:** A gate in front of replicated auth (#32). Before the first
  auth change, each member asks every other one over a new peer RPC
  (`AuthSync`):
  - a member that is older (it answers `Unimplemented`) or unreachable
    refuses the change with `Unavailable`, naming it. An older member
    cannot decode the entry, and two such in three would lose quorum;
  - members holding different auth tables (kept from 1.4.x, when each
    wrote its own) refuse changes with `FailedPrecondition`, and
    `fastetcd_auth_diverged` is 1. New client-port service
    `fastetcd.admin.FastetcdAdmin` and `fastetcd-ctl auth members` /
    `auth adopt <member>` (root only while auth is on) show each
    member's state and replicate the chosen one to all. Nothing is
    chosen automatically.
- **refactor:** (internal crate API, not published) `ServerState::new`
  no longer takes an `AuthState`. It uses the store's own
  (`MvccStore::auth_memory`). `AuthService::new` takes only the state.
  `AuthState` is now `AuthMemory` from the storage crate.
- **perf:** `Authenticate` is now a raft write, one fsync per login, as
  in etcd.
- **test:** `crates/server/tests/auth_replication.rs` covers:
  - a change made on a follower is enforced on every member;
  - a token from one member is accepted by the others;
  - revoking a role or deleting a user takes effect everywhere;
  - a snapshot-installed learner gets the auth state;
  - an unreachable member and an older one both refuse changes;
  - diverged members are refused until an adopt;
  - adopt needs root.

  Also unit tests for apply and for snapshot trailer compatibility in
  both directions.
- **docs:** `docs/03-deploy.md` § Auth on a multi-member cluster;
  README.

## [v1.4.2] — 2026-09-28

### 2026-09-28
- **fix:** `Watch` is authorized (#33). A watch create had no permission
  check, so with auth enabled any authenticated user could watch any key
  or range, with a past `start_revision` and `prev_kv`, and receive
  values RBAC denies to `Range`. Each create now needs read on
  `[key, range_end)`, checked against the user who opened the stream
  (etcd's `isWatchPermitted`). A denied create is answered as etcd
  answers it: `created` and `canceled`, `watch_id` -1, `cancel_reason`
  `etcdserver: permission denied`. Nothing is registered or replayed,
  and the stream stays open for other creates. As in etcd, the check is
  made at create only. Tests: `crates/server/tests/authz_watch.rs`.
  Docs: `docs/03-deploy.md`.

## [v1.4.1] — 2026-09-28

### 2026-09-28
- **test:** `membership_changes_forward_from_a_follower` raced the
  initial membership commit (#44): a follower learns the leader from its
  first AppendEntries, before membership index 0 is committed, and a
  MemberAdd sent then is refused with "already undergoing a configuration
  change". The test now waits for the leader to apply its first entry.
  All of #44 comes down to leadership moving mid-test on a loaded
  build box (per-job volumes, six builds at once, slow fsync) under the
  harness's 400-900 ms election timeout. The multi-node harnesses
  (`multinode_grpc`, `snapshot_transfer_grpc`, `peer_tls`) now use
  1.5-3 s, the single-node leader wait allows 30 s, and the replication
  test polls followers instead of sleeping a fixed 300 ms, and the #10
  CAS loop retries an `Unavailable` (as an etcd client does) while still
  failing on any CAS conflict. Test-only;
  server defaults are unchanged.

### 2026-09-27
- **fix:** Peer TLS is honoured, with its own identity and trust root
  (#23). `--peer-cert-file`, `--peer-key-file`, `--peer-trusted-ca-file`
  and `--peer-client-cert-auth` (and their `ETCD_PEER_*` env vars) were
  parsed and discarded. The peer port served the *client* identity
  whenever `--cert-file` was set, so any certificate trusted by the
  client port was trusted by raft too. Members also dialled each other
  with no TLS at all, so a multi-member cluster with `--cert-file` could
  not form. Now, as in etcd:
  - the peer port's TLS comes only from the `--peer-*` flags.
    `--peer-client-cert-auth` refuses any caller without a certificate
    from `--peer-trusted-ca-file`, including one the client CA signed;
  - members dial each other over TLS, verifying against the peer CA and
    presenting their peer certificate, which gives mutual TLS between
    members. `--peer-trusted-ca-file` is required with peer TLS;
  - peer URLs must be `https://` with peer TLS and `http://` without
    it. A mismatch is a startup error naming the flag and URL, never a
    silent fallback;
  - client TLS on with peer TLS off logs a warning that raft traffic is
    plaintext;
  - Helm chart: a separate `peerTls` block.
  **Behaviour change:** `--cert-file` alone no longer puts TLS on the
  peer port. No known deployment relied on it: multi-member clusters
  could not form with it, and every known one uses `http://` peer URLs.
  Tests: `crates/server/tests/peer_tls.rs`. Docs: `docs/03-deploy.md`.

## [v1.4.0] — 2026-09-27

### Added
- **feat:** Survive a corrupt data file (#37). After a power cut on a
  device that lost fsync'd writes, redb refused to open the store ("All
  roots are corrupted") and the node crash-looped with nothing to
  recover from. Now:
  - **Periodic backups** with `--backup-dir` (off unless set; put it on
    a separate volume, a warning says so when it is not): every table of
    the store from one engine snapshot, one checksummed file (SHA-256),
    temp file + fsync + rename + directory fsync. Taken every
    `--backup-interval-secs` (900) and every `--backup-every-revisions`
    (10000) when anything changed, and on every raft membership change.
    The newest `--backup-retain` (4) are kept.
  - **Restore instead of crash-looping.** A lone member whose data file
    is corrupt restores the newest backup whose checksum verifies,
    keeps the corrupt file as `fastetcd.redb.corrupt.<ms>` (never
    deleted), and raises the CORRUPT alarm (`Maintenance.Alarm`,
    `Status` errors) until `etcdctl alarm disarm`. Metrics
    `fastetcd_recovered_from_backup_total`, `fastetcd_recovered_revision`.
    A member of a multi-node cluster refuses and leaves the file: its
    raft vote was in it. `--on-corruption=refuse` never restores.
  - A damaged file is reported as corruption, not an I/O error,
    including an existing empty file (redb would have silently created a
    fresh store over it) and a file whose repair makes redb 2.6.3
    **panic** (`unreachable!` walking a zeroed page), which it does
    rather than returning an error when a device loses whole pages.
    The reason recorded with the recovery says "corrupt" in both cases.
  - The restore is crash-safe: the new store, recovery record included,
    is complete before anything is renamed, and a restore cut short
    between renames is finished on the next start rather than leaving
    an empty store.
  - The offline `fastetcd restore` accepts the new backup files.
  - redb two-phase commit is **not** enabled: in 2.6.3 it makes repair
    trust the primary commit slot, so on a device that lies about fsync
    a torn commit becomes an unopenable file instead of a one-commit
    rollback. See `docs/05-backup-and-recovery.md`.
    Measured with `crates/storage/tests/two_phase_commit_cost.rs` on
    dev.g8.lo (500 single-key commits, twice each): on tmpfs, where
    fsync is free, 2PC adds ~4% (p50 11.1 → 11.5 µs), which is pure
    bookkeeping; on the shared ext4 build disk, per-commit fsync ran
    0.36-2.4 s p50 under other builds' load and swamped any difference
    (the runs did not even order consistently). No trustworthy disk
    number yet: its cost is one extra fsync per write, at most doubling
    commit latency on a quiet disk. The decision does not rest on it.
### Fixed
- **fix:** A `Range` inside a `Txn` now sees the txn's own earlier
  writes, as in etcd (#35). The ops of the branch that runs are applied
  strictly in order, and once the txn has changed anything a range
  reads at the txn's pending revision (etcd's `storeTxnWrite.Range`),
  so `[Put(b, "v"), Range(b)]` returns `v` with `mod_revision` equal to
  the txn's revision, and `[DeleteRange(b), Range(b)]` returns nothing.
  A range with an explicit older `revision` still reads history. Stored
  state, sub-revisions and the raft wire format are unchanged; only the
  range results in a `TxnResponse` differ.

## [v1.3.0] — 2026-09-26

### Added
- **cluster-id:** A deployment-distinct, stable `cluster_id` (#17). The
  `cluster_id` in every response header was `--cluster-id`, which
  defaulted to `1` for every deployment, so a client pinning it could
  not notice an endpoint repointed at a different store. It is now
  chosen on a store's first start and persisted in the data directory:
  an explicit `--cluster-id`, else FNV-1a-64 of
  `--initial-cluster-token` masked to 63 bits and never 0, else `1`
  with a startup warning. Later starts use the persisted id, so a
  restart or an upgrade never changes it. Stores created before this
  keep reporting `1` unless `--cluster-id` is given.

### Fixed
- **cluster-id:** `--cluster-id 0` is rejected at startup; etcd treats
  0 as "no cluster id".
- **tests:** The multinode test harness keeps each reserved peer port
  bound until its node takes it, instead of closing and rebinding it,
  which another process could win on a shared build box (#40).

### Documentation
- `docs/03-deploy.md` documents `--cluster-id` and how the id is
  chosen. Replicating it, so members cannot disagree and it is
  distinct by default, is #38.

## [v1.2.4] — 2026-09-25

### Changed
- **perf:** Raft snapshots move through files instead of RAM (#30).
  `SnapshotData` was a `Cursor<Vec<u8>>`, so a lagging follower cost the
  leader a whole database copy of RAM per transfer and the follower up
  to ~3x. Now a snapshot is serialized straight into its file, sent by
  reading that file a chunk at a time, received into a temp file, and
  installed by decoding from the file and renaming it into place. The
  on-disk format and the wire format are unchanged, so there is no
  upgrade step and mixed-version clusters keep working.
- **perf:** `Maintenance.Snapshot` (`etcdctl snapshot save`) streams
  from the snapshot file instead of copying it into memory first.

### Fixed
- **raft:** A node never reports "no snapshot" while it has applied
  state. openraft purges the log against every snapshot it is handed,
  even one that could not be written to disk, and then fails
  replication to a lagging follower with a storage error when the
  snapshot is missing. A missing or unwritten snapshot is now rebuilt
  on demand, and held in memory if the disk still cannot take it.
- **raft:** Snapshot bodies and metas are fsynced before they are
  renamed into place, and the directory after, so a snapshot that is
  reported written survives a power loss.

### Documentation
- `docs/04-disk-space.md` describes how snapshots move and the two
  transient disk costs.

## [v1.2.3] — 2026-09-24

### Fixed
- **kv:** Each op in a `TxnResponse` now has the variant of the request
  op at the same position in the branch that ran (#18). The variant
  used to be guessed from the result's shape, so a `DeleteRange` that
  removed exactly one key came back as a `ResponsePut` (with no
  `deleted` count), misleading any client that switches on the
  variant. A mismatch between request and result is now an internal
  error instead of a guess.

### Known issues
- A `Range` inside a txn reads the state from before the txn, not the
  txn's own earlier writes as etcd does (#35).

## [v1.2.2] — 2026-09-24

### Fixed
- **auth:** `Txn` is now authorized (#22). It had no permission check,
  so any authenticated user could read or write keys outside their
  roles by wrapping the operation in a transaction. A txn now needs
  read on every compare target and the permission of every op in both
  the success and failure branches (nested txns included), checked
  all-or-nothing before anything is proposed, as in etcd.
- **auth:** `prev_kv` on `Put`/`DeleteRange` (standalone or in a txn)
  now also requires read permission, as in etcd: returning the old
  value is a read.

### Documentation
- `docs/03-deploy.md` documents what each operation needs and that
  auth is not yet a security boundary (#31, #32, #33).

## [v1.2.1] — 2026-09-24

### Fixed
- **watch:** A watch no longer silently drops revisions when it falls
  behind (#16). A stream whose client read too slowly overflowed the
  1024-batch event broadcast; the forwarder logged `Lagged` and carried
  on, so consumer caches drifted with no error. Each watcher now keeps a
  revision cursor, and on lag — or on a gap in batch revisions, such as
  a follower installing a raft snapshot — is caught up from MVCC history
  to the current revision. If that history has been compacted the watch
  is cancelled with `compact_revision` set (etcd `ErrCompacted`), so the
  client re-lists. Consumers no longer need a periodic full re-list.
- **watch:** Creating a watch with a past `start_revision` could deliver
  live events ahead of, or duplicated in, the history replay; replay now
  runs under the stream lock and hands off at an exact revision. A
  future `start_revision` now withholds events below it.

### Documentation
- #15 (stream snapshots from a pinned read transaction to drop the
  on-disk copy) declined — safety first, performance next, disk savings
  last; the saving is ~14% of the minimum volume, not ~40%. Performance
  follow-up filed as #30 (file-backed snapshot transfer, no RAM-whole
  buffering).
- `docs/00-design.md`: "Watch delivery guarantee".

## [v1.2.0] — 2026-09-02

### Added
- **sizing:** `fastetcd sizing --nodes N [--pods-per-node P]` turns a
  cluster shape into a recommended volume size and prints the
  arithmetic behind it, so the estimate can be argued with rather than
  trusted. The ladder at default density (30 pods/node): **512 MiB** for
  1-10 nodes, **1 GiB** at 100, **2 GiB** at 500, **4 GiB** at 1000,
  **16 GiB** at 5000.
- **sizing:** `--expected-nodes N` / `--expected-pods-per-node P`. At
  startup the server checks the real volume against that estimate —
  while the store is still empty and the answer is actionable — and
  logs an error naming the shortfall if it is too small. It warns
  rather than refuses: an operator who knows their cluster better than
  the model should not be blocked from booting a control plane. It also
  enables revision-mode auto-compaction when
  `--auto-compaction-retention` was left at `0`, scaled so the retained
  window is roughly constant in *time* (~16 minutes) at any cluster
  size, since node leases write ~6 revisions per node per minute.
- **deploy:** the chart exposes `space.expectedNodes` /
  `space.expectedPodsPerNode` and documents `persistence.size` against
  the ladder; the systemd `EnvironmentFile` example gains the same.

### Documentation
- **docs:** `docs/04-disk-space.md` gains a sizing section explaining
  why the volume runs ~10x the live data (MVCC history, copy-on-write
  overhead, a raft snapshot that is a *full copy* of the database, the
  raft log, and the upgrade backups), and why clusters below ~50 nodes
  all land in the same bucket — the fixed cluster overhead (CRDs, RBAC,
  API priority-and-fairness config) dominates until the pod term takes
  over around 100 nodes.

## [v1.1.0] — 2026-09-02

### Added
- **space (#14):** disk-space accounting, pressure reclaim and etcd's
  `NOSPACE` alarm, so a bounded data volume stays bounded. Occupancy —
  data file, retained snapshots, and the filesystem's real free space
  via `statvfs` — is sampled every `--space-check-interval-secs`
  (default 30). Effective capacity is the smaller of
  `--quota-backend-bytes` and what the volume can actually hold, so an
  oversized quota can no longer hide a small volume. Above
  `--space-high-water-percent` (80) the store reclaims: compact MVCC
  history to `--space-reclaim-retention` revisions (even with
  auto-compaction off), trigger a raft snapshot so the log can be
  purged, then defragment if enough would come back.
- **space (#14):** raft snapshots are now a retained set with roll-off.
  `--max-snapshots` (default 1, etcd's flag name) is enforced
  oldest-first *before* a new snapshot is written, so a write never
  needs room for `retain + 1` copies. Leftover temp files and
  half-written pairs are reclaimed on startup, and the pre-v1.1
  single-file `current.snap` layout is migrated in place.
- **admin (#14):** `fastetcd defrag --data-dir <dir>` — an offline
  escape hatch that works on an already-full volume. It needs no
  quorum, no read barrier and no snapshot write, which is exactly what
  fails when the volume is full, and redb's compaction shrinks the file
  in place rather than needing free space.
- **maintenance (#14):** `Alarm` returns the real alarm set and honours
  `DEACTIVATE`; `Status` reports `dbSizeInUse` (live bytes, measured
  behind a 60s cache) and `dbSizeQuota` (effective capacity), and lists
  `NOSPACE` under `errors`.
- **metrics (#14):** `etcd_mvcc_db_total_size_in_use_in_bytes`,
  `etcd_server_quota_backend_bytes`,
  `fastetcd_store_snapshot_size_in_bytes`, `fastetcd_disk_total_bytes`,
  `fastetcd_disk_available_bytes`, `fastetcd_store_space_used_ratio`,
  `fastetcd_nospace_alarm_active`.
- **storage (#14):** `KvStore::usage()` reports file / live /
  fragmented bytes (redb via an aborted write-txn stats walk), and
  `fs_space::probe()` reads the data directory's real free space.
- **ctl (#14):** `fastetcd-ctl status`, `defrag`, `compact <rev>`,
  `alarm [--disarm]`.
- **docs:** `docs/04-disk-space.md` — what grows, what fastetcd does
  about it, every flag and metric, and the dig-out procedures.
- **sizing:** `fastetcd sizing --nodes N [--pods-per-node P]` turns a
  cluster shape into a recommended volume and shows the arithmetic. The
  ladder at default density: 512 MiB for 1-10 nodes, 1 GiB at 100,
  2 GiB at 500, 4 GiB at 1000, 16 GiB at 5000. The volume is roughly 10x
  the live data and that is not waste — MVCC history, copy-on-write
  overhead, a raft snapshot that is a full copy of the database, the
  raft log, and the upgrade backups each multiply it. Small clusters all
  land in the same bucket because fixed cluster overhead dominates below
  ~50 nodes.
- **sizing:** `--expected-nodes N` (with `--expected-pods-per-node`)
  checks the real volume against that estimate at startup, while the
  store is still empty and the answer is actionable, and enables
  auto-compaction when it was left off. Warns rather than refuses.

### Changed
- **BEHAVIOR (#14):** above `--space-alarm-percent` (95) the `NOSPACE`
  alarm is raised and writes — `Put`, a `Txn` containing a put, and
  `LeaseGrant` — are refused with gRPC `ResourceExhausted` and etcd's
  message `etcdserver: mvcc: database space exceeded`. Reads, deletes,
  compaction and defragment are deliberately **not** gated: refusing
  those is what turns a full volume into an unrecoverable one. The
  alarm clears itself below `--space-clear-percent` (70), or via
  `etcdctl alarm disarm`.
- **raft (#14):** a snapshot write that still hits `ENOSPC` now
  discards every retained snapshot and retries, instead of returning a
  storage error that openraft surfaces on every subsequent read and
  write. openraft rebuilds a snapshot on its next tick; a node with no
  snapshot is worth more than a node that cannot write one.
- `--quota-backend-bytes` is no longer a no-op compat flag.

### Fixed
- **storage (#14):** `usage()` reported redb's `stored_bytes` — the key
  and value payload — as bytes in use, rather than the pages holding
  them. A store full of live 4 KiB values looked three-quarters empty,
  so `Status`, `/metrics` and `fastetcd defrag` told an operator 22 MB
  was recoverable from a store where a defragment freed nothing. Now
  `allocated_pages * page_size`, matching etcd's `dbSizeInUse`
  semantics. (redb releases a delete's pages on a later commit, so the
  figure lags a large delete by a commit or two; the reclaim path
  compacts before it measures.)
- **storage (#14):** an online defragment failed outright with "a
  transaction is still in progress" on any loaded node. redb refuses to
  compact while a read transaction is live, and a snapshot holds one for
  as long as its caller does — long after the lock used to create it was
  released — so under steady traffic one essentially always was.
  Defragment now takes the engine's exclusive lock (stopping new
  snapshots) and waits for the outstanding readers to drain, with a 30s
  timeout that names the reader count and points at the offline path.
- **space (#14):** `Maintenance.Status` could report more bytes in use
  than the file contained, because the cached live-bytes measurement
  described the pre-defragment file. Both defragment paths now
  invalidate it, and the reported figure is clamped to the file size.
- **#14:** a full data volume wedged the store in both directions —
  every read failed at the linearizable barrier behind a pending
  snapshot write, every write failed at the same place, and the one
  recovery a client could attempt (deleting keys so the next snapshot
  fits) was refused for the same reason. Every process healthy, cluster
  down. The store now refuses writes early, while it still works.

## [v1.0.7] — 2026-07-21

### Added
- **ctl:** `fastetcd-bench`, a concurrent gRPC load generator (put /
  linearizable get / serializable get; throughput and latency
  percentiles), bundled in the release tarball. No server change.

## [v1.0.6] — 2026-07-20

### Changed
- **raft (#13):** bounded memory and log size. Snapshot bodies live on
  disk instead of RAM (only `SnapshotMeta` is kept in memory) and are
  reloaded on restart so purge is not stalled; `purge`/`truncate` use a
  keys-only `delete_range` instead of loading the purged prefix;
  `--snapshot-count` and `--max-in-snapshot-log-to-keep` are now real
  flags.

### Added
- **server:** `--auto-compaction-retention` (revision mode) and
  `--auto-compaction-interval-secs`; a leader-side ticker proposes
  `Compact` through Raft. Off by default.

## [v1.0.5] — 2026-07-20

### Fixed
- **raft (#13):** startup no longer loads the whole raft log into RAM.
  `get_log_state` did an unbounded range over `raft_log` just to read
  the last entry, which hung every member of a 3-master cluster before
  it could bind its peer port. Added `Snapshot::last` (an O(log n)
  reverse B-tree lookup in redb) and gated/windowed the #11
  membership-recovery scan so a healthy cluster never scans on startup.

## [v1.0.4] — 2026-07-20

### Fixed
- **server:** the automatic pre-version safety backup was fatal on
  failure, so a full disk or an unwritable `backups/` directory
  crash-looped the node — a failed safety net taking down the control
  plane. It is now best-effort: log the error with the remedy and start.

## [v1.0.3] — 2026-07-19

### Added
- **admin:** data-directory safety toolkit. Automatic pre-version
  backup before any recovery/conversion write, plus offline `fastetcd
  backup`, `restore` and `fsck [--repair]` (each opens the redb file
  exclusively and refuses if the server holds it).

## [v1.0.2] — 2026-07-19

### Fixed
- **server (#11):** the upgrade recovery now recovers the *actual*
  membership from the retained raft log (newest `Membership` entry) and
  falls back to `--initial-cluster` only when the log has been purged
  clean of it. The startup log states which source was used.

## [v1.0.1] — 2026-07-19

### Fixed
- **server (#11):** a rolling upgrade from v0.8.2 could strand a
  cluster — every member came up with an empty voter set and never
  elected a leader. Detects the pre-v1.0.1 on-disk format and rebuilds
  the voter set in place, persisting it before Raft starts. Added
  `--force-new-cluster` (etcd parity) and a `format_version` marker.

## [v1.0.0] — 2026-07-19

### Fixed
- **kv (#10):** linearizable reads. `serve_range` ignored
  `serializable` and always read local state, so a lagging follower or
  a leader that had silently lost leadership answered a default read
  with stale data — surfacing as ~25% spurious CAS failures in
  read-modify-write loops. A default Range now runs a read barrier
  first: `ensure_linearizable` on the leader, `RaftPeer.ForwardRead` to
  the leader on a follower. `serializable=true` still opts out.

## [v0.8.3] — 2026-07-18

### Fixed
- **raft (#9):** a single node could not survive a reboot —
  `last_applied_log_id` / `last_membership` were rebuilt as `None` on
  every open, so openraft replayed from index 0 (crash-looping once a
  snapshot had purged the early entries, or silently double-applying
  every mutation). Both now persist into `mvcc_meta`, folded into the
  same `WriteBatch` as the mutation they describe.
- **raft (#8):** a rejoined member stayed at an old MVCC revision:
  `rebuild_mvcc` wrote through the engine while `MvccStore` kept
  serving the counters it cached at open. Added
  `MvccStore::reload_write_state`.
- **cluster (#7):** `member add/remove` against a follower returned
  openraft's raw `ForwardToLeader`; added `RaftPeer.ForwardMembership`.

## [v0.8.2] — 2026-07-16

### Fixed
- **raft (#8):** atomic snapshot build — the MVCC handle and
  `last_applied` are captured together under the state-machine lock, so
  a concurrent apply cannot produce a snapshot whose data predates its
  own `last_applied_log_id`.

## [v0.8.1] — 2026-07-06

### Fixed
- **security (#6):** `--client-cert-auth` is now actually enforced.
  The flag had no `env` binding, and the `ETCD_*`→`FASTETCD_*` compat
  shim had no entry for it, so `ETCD_CLIENT_CERT_AUTH=true` (and
  `FASTETCD_CLIENT_CERT_AUTH`) were silently ignored — the flag stayed
  `false`, TLS was built with no client-CA verifier, and any client
  could complete the handshake and read/write anonymously. Wired
  `env = "FASTETCD_CLIENT_CERT_AUTH"` onto the flag, added the
  `ETCD_CLIENT_CERT_AUTH` compat pair (same fix for
  `peer-client-cert-auth`), and set `client_auth_optional(false)`
  explicitly so mandatory client auth doesn't depend on a tonic
  default. Verified: certless TLS clients are now rejected at the
  handshake on both the gRPC port and the `/health` route; clients
  presenting a CA-signed cert still succeed.

## [v0.8.0] — 2026-07-06

### Changed
- **ci:** Root cause of the stalled release pipeline found: GitHub
  Actions is disabled at the repo-settings level
  (`repos/.../actions/permissions` → `enabled: false`), not broken —
  confirmed off since 2026-05-24, so the `v0.6.0`/`v0.7.0` tag
  pushes never ran anything and `v0.7.0` shipped no GitHub Release.
  Decision: leave Actions disabled; `dev.g8.lo` is now the sole
  build+test path. Deleted `.github/workflows/ci.yml`. Added
  `deploy/packaging/run-tests.sh` (workspace + `wal-engine` +
  `iouring` feature tests) and `deploy/packaging/build-release.sh`
  (rpm/deb/tarball) to script that path. Published the missing
  `v0.7.0` GitHub Release from `dev.g8.lo`.

### Documentation
- **test:** Verified the v0.7.0 rpm and deb end-to-end on
  `dev.g8.lo`: both create the `fastetcd` system user, enable, and
  start the service; `fastetcd-ctl put`/`get` round-trips through
  Raft. Documented a Fedora-specific caveat — `dpkg`'s maintainer
  scripts fail under SELinux `Enforcing` on Fedora (missing SELinux
  context for dpkg scripts, not a package defect); works under
  `setenforce 0` and is a non-issue on real Debian/Ubuntu.

## [v0.7.0] — 2026-07-01

### Fixed
- **fix(raft):** Multi-node client writes sent to a non-leader no
  longer fail. `raft.initialize()` was defaulting every member's
  `BasicNode.addr` to empty (a bare `BTreeSet<NodeId>` instead of a
  `BTreeMap<NodeId, BasicNode>`), and — more fundamentally — fastetcd
  never actually forwarded a write to the leader; it just relayed
  openraft's `ForwardToLeader` error (with that empty address baked
  in) straight to the client. Added a `RaftPeer.ForwardWrite` RPC
  (`crates/raft/src/network.rs`'s new `WriteForwarder`) that hands
  the write off to the leader over the same peer channel already
  used for AppendEntries/Vote/InstallSnapshot, and a single shared
  `ServerState::propose()` that every write path (KV, Lease grant/
  revoke/keepalive) now goes through. Regression test: a `PUT` sent
  to a follower in the 3-node integration test now succeeds and
  replicates. Closes #4.

### Added
- **feat(server):** Serve etcd's plain-HTTP `GET /health` (and
  `/livez`, `/readyz`) on the client gRPC port itself, so load
  balancers and Kubernetes httpGet probes already pointed at etcd's
  client port work unchanged against fastetcd — no second port
  needed. Built on tonic 0.12's `Routes`↔`axum::Router` interop
  (`tonic::service::Routes::builder()...routes().into_axum_router()`),
  preserving TLS. Closes #5.

### 2026-07-01
- **ci:** Add a `build-packages` job that builds the rpm/deb/tarball
  (static `x86_64-unknown-linux-musl`) on every `v*` tag push and
  publishes them straight to the GitHub Release via
  `softprops/action-gh-release`, mirroring the manual v0.6.0 release
  process. Future tags no longer need a manual build-and-upload
  pass.

### 2026-06-30
- **docs:** Replace the stale hand-written systemd unit template in
  `docs/03-deploy.md` with the real `deploy/systemd/fastetcd.service`
  shipped in the rpm/deb, document the rpm/deb/tarball install paths
  released to GitHub Releases, and update the `README.md` deployment
  quick-paths list to match.

## [v0.6.0] — 2026-06-30

### Added
- **feat(server):** Read `ETCD_*` environment variables as a fallback
  for every `FASTETCD_*`-prefixed CLI arg (name, data-dir, listen/
  advertise URLs, initial-cluster*, TLS cert/key/CA paths, metrics
  URL, snapshot-count, quota-backend-bytes, max-request-bytes,
  log-level). An unmodified etcd `EnvironmentFile` (systemd,
  container, Kubernetes) now boots a fastetcd cluster identically —
  `FASTETCD_*` still takes precedence when both are set. Closes #2.
- **feat(deploy):** Add a systemd unit (`deploy/systemd/fastetcd.service`)
  with `Restart=always` and an unbounded restart-rate limit
  (`StartLimitIntervalSec=0` in `[Unit]`) so the service both
  autostarts (`WantedBy=multi-user.target`) and keeps retrying after
  any exit, plus an example `EnvironmentFile` at
  `deploy/systemd/fastetcd.conf.example`. Closes #3.
- **feat(deploy):** rpm (`cargo-generate-rpm`) and deb (`cargo-deb`)
  packaging metadata in `crates/server/Cargo.toml`, bundling
  `fastetcd` / `fastetcd-ctl` / `fastetcd-migrate` into a single
  `fastetcd` package built statically against
  `x86_64-unknown-linux-musl` — avoids baking in a glibc-version
  requirement tied to whichever host built the package. Maintainer/
  post-install scripts (`deploy/packaging/debian/`, inline rpm
  scriptlets) create the `fastetcd` system user, own
  `/var/lib/fastetcd`, and `enable --now` the unit on install.
  Verified end to end on dev.g8.lo (Fedora 43): both `dnf install`
  and `dpkg -i` bring up a working single-node server with
  `fastetcd-ctl put`/`get` round-tripping through it.

### 2026-06-29
- **docs:** Add a detailed "fastetcd vs etcd" comparison section to
  `README.md` covering runtime (Go/GC vs Rust/no-GC), storage engines
  (BoltDB vs pluggable redb/iouring), latency profile, the v3-only
  compatibility boundary, migration, and a decision guide. v3 clients
  (etcdctl, kube-apiserver) use gRPC natively, so no HTTP gateway is
  in scope; only the deprecated v2 HTTP API is called out as out of
  scope.

### 2026-05-24
- **fix(storage):** iouring tests now skip gracefully when
  io_uring is blocked in the test environment (most commonly
  docker's default seccomp profile rejecting `io_uring_setup`
  with EPERM) rather than panicking. Tests use a new
  `iouring_available()` probe at entry. Closes #1.

### 2026-05-23
- **chore:** Note that fastetcd is now mirrored on a local Forgejo
  instance (`forcicd.g8.lo:3000`) for faster CI feedback on the LAN.
  Github Actions remain authoritative; the mirror runs the same
  `.github/workflows/ci.yml` unmodified. Local runner uses
  forgejo/runner:7 (supports node24-based actions like
  Swatinem/rust-cache@v2).

### 2026-05-19
- **feat(server):** etcd-compat CLI flag aliases. fastetcd now
  accepts etcd's plural URL forms (`--listen-client-urls`,
  `--listen-peer-urls`, `--advertise-client-urls`,
  `--initial-advertise-peer-urls`) — comma-separated lists with
  the singular forms kept as aliases. Each is parsed for the
  first entry to bind. Also accepts the no-op flags etcd's e2e /
  robustness harness passes when launching the binary
  (`--snapshot-count`, `--quota-backend-bytes`,
  `--max-request-bytes`, `--log-level`, `--log-outputs`,
  `--logger`, `--metrics`, `--enable-pprof`,
  `--initial-cluster-token`, peer-TLS flags
  `--peer-cert-file`/`--peer-key-file`/`--peer-trusted-ca-file`/
  `--peer-client-cert-auth`); their values are logged at debug
  level but otherwise ignored. This makes fastetcd a drop-in
  binary for any tooling that already knows how to launch etcd
  (including downstream robustness suites).

## [v0.5.0] — 2026-05-19

Kubernetes-ready additions on top of v0.4.0's production hardening.

### Added

- **gRPC health service** (`grpc.health.v1.Health`) on the client
  port. All fastetcd services (KV / Cluster / Maintenance / Watch
  / Lease / Auth) report `SERVING` at startup. Service meshes and
  k8s `readinessProbe.grpc` / `livenessProbe.grpc` work out of
  the box.
- **Helm chart** at `deploy/charts/fastetcd/`. StatefulSet of N
  replicas with stable peer DNS via a headless Service;
  auto-generates `--initial-cluster` from the replica set
  (override-able). Persistent `volumeClaimTemplates` per replica.
  Optional TLS via a referenced Secret. Optional `ServiceMonitor`
  for the Prometheus operator.
- **Real `fastetcd-ctl`** with subcommands `put`, `get [--prefix]`,
  `del [--prefix]`, `snapshot-save <path>` (streams
  `Maintenance.Snapshot` to a local file). Useful for end-to-end
  smoke without needing the Go etcdctl binary.

### Changed

- **README rewrite** for v0.4/v0.5 reality. Architecture diagram
  includes AuthInterceptor + per-key authz; quick-start uses
  both etcdctl and fastetcd-ctl; testing section cross-references
  the three-ring strategy doc; deployment section points at the
  Kubernetes / systemd / container paths.

### Remaining gap

- **openraft 0.10 upgrade** (waiting on its stable release) for
  real `MoveLeader.transfer_leader()`.

## [v0.4.0] — 2026-05-19

### 2026-05-19
- **feat(ctl):** `fastetcd-ctl` is now a real (small) client.
  Subcommands: `put`, `get [--prefix]`, `del [--prefix]`,
  `snapshot-save <path>` (streams `Maintenance.Snapshot` to a local
  file). Useful for smoke-testing fastetcd without the Go
  toolchain.
- **docs:** README rewrite for v0.4.0. Updated architecture
  diagram includes the AuthInterceptor + per-key authz; quick
  start uses both `etcdctl` and `fastetcd-ctl`; testing section
  cross-references `docs/02-testing.md`'s three rings; deployment
  section pointers at `docs/03-deploy.md`.
- **feat(server):** gRPC health service. Adds the standard
  `grpc.health.v1.Health` to the client port so service meshes
  and k8s gRPC probes work out of the box. All fastetcd services
  (KV / Cluster / Maintenance / Watch / Lease / Auth) are
  reported as `SERVING` at startup.
- **chore(deploy):** Helm chart at `deploy/charts/fastetcd`.
  StatefulSet of N replicas with stable peer DNS via a headless
  Service; auto-generates `--initial-cluster` from the replica
  set (override with `cluster.override`). Persistent
  volumeClaimTemplates per replica. Optional TLS via a referenced
  Secret. Optional `ServiceMonitor` for the Prometheus operator.
  k8s-native readiness/liveness via the new gRPC health service.

## [v0.4.0] — 2026-05-19

Production-grade hardening since v0.3.0: TLS, full Auth enforcement
(Phase 2 + Phase 3), Prometheus metrics, GitHub Actions CI,
distroless container, deployment guide.

### Added

- **TLS** for the client and peer gRPC listeners
  (`--cert-file` / `--key-file` / `--trusted-ca-file` /
  `--client-cert-auth`, matching etcd's flag shape).
- **Auth Phase 2** — per-request token enforcement.
  `AuthInterceptor` wraps every non-Auth gRPC service via
  `with_interceptor`; when auth is enabled, requests must carry a
  valid `token` metadata field. `AuthState` is now backed by
  `std::sync` primitives so the sync tonic interceptor can read
  live state. 4 enforcement tests.
- **Auth Phase 3** — per-key permission enforcement. New `authz`
  module checks every KV request against the authenticated user's
  roles + permissions. `Range` requires Read; `Put` /
  `DeleteRange` require Write. `root` user / `root` role bypass.
  3 enforcement tests.
- **Prometheus `/metrics`** endpoint on a side port
  (`--listen-metrics-url`, default `127.0.0.1:2381`). Exports
  etcd-compatible names: `etcd_server_has_leader`,
  `etcd_server_leader_changes_seen_total`,
  `etcd_mvcc_db_total_size_in_bytes`,
  `etcd_debugging_mvcc_current_revision`,
  `etcd_debugging_mvcc_compact_revision`. Lazy-refreshed on each
  scrape; no background task.
- **GitHub Actions CI** at `.github/workflows/ci.yml`. Matrix:
  Linux + macOS × default features, plus Linux-only `wal-engine`
  + `iouring` jobs. On tag push: builds release Linux binary and
  pushes a container image to `ghcr.io/glennswest/fastetcd`.
- **Container** images: `Dockerfile` (build-from-source) and
  `Dockerfile.ci` (consume pre-built artifact). Distroless
  runtime; runs as `nonroot`; exposes 2379 (client) + 2380 (peer)
  + 2381 (metrics).
- **`docs/03-deploy.md`** — production deployment guide covering
  container, systemd unit, TLS, Auth bootstrap, multi-node config,
  backups, and migration from upstream etcd.

### Remaining gap

- **openraft 0.10 upgrade** (waiting on its stable release) for
  real `MoveLeader.transfer_leader()`.

## [v0.3.0] — 2026-05-19

### 2026-05-19
- **feat(server):** Prometheus `/metrics` endpoint on a side port
  (`--listen-metrics-url`, default `127.0.0.1:2381`). Exports
  metric names that match etcd's where they map directly so
  existing dashboards and alerts work unchanged:
  `etcd_server_has_leader`,
  `etcd_server_leader_changes_seen_total`,
  `etcd_mvcc_db_total_size_in_bytes`,
  `etcd_debugging_mvcc_current_revision`,
  `etcd_debugging_mvcc_compact_revision`. Lazy-refreshed on every
  scrape — no background task — so the endpoint always reports
  current truth. Served via a minimal `hyper` HTTP/1 handler.
- **chore(ci):** GitHub Actions workflow at `.github/workflows/ci.yml`.
  Test matrix: Linux + macOS × default features, plus a
  Linux-only `wal-engine` job and a Linux-only `iouring` job
  (since `tokio-uring` is target-gated to Linux). On tag pushes
  (`vX.Y.Z`), builds a release Linux binary, pushes it to
  `ghcr.io/glennswest/fastetcd:tag` and `:latest` using
  `Dockerfile.ci` (which expects the binary as build context).
- **chore(container):** Two-Dockerfile setup. `Dockerfile` builds
  fastetcd from source in `rust:1.82-slim` then copies the binary
  into `gcr.io/distroless/cc-debian12`. `Dockerfile.ci` skips the
  build stage and copies a pre-built binary (the artifact from
  the CI job) for faster image production. Distroless gives us
  glibc without the full Debian userland; image runs as
  `nonroot`.
- **docs:** New `docs/03-deploy.md` covers container, systemd
  unit, TLS, Auth bootstrap, multi-node config, backups, and
  migration from upstream etcd.
- **feat(server):** Auth Phase 3 — per-key permission enforcement.
  New `authz` module checks every KV request against the
  authenticated user's roles. `Range` requires Read, `Put` and
  `DeleteRange` require Write. `root` user / `root` role bypass.
  Permission ranges follow etcd semantics (single key, prefix,
  `[key, range_end)`). The `AuthInterceptor` now inserts a typed
  `UserIdentity` into request extensions on every authenticated
  call; KV handlers read it and call `authz::authorize(...)`. 3
  new tests pass: out-of-range write is denied; in-range write is
  allowed; read-only role can't write.
- **feat(server):** TLS support for the client and peer gRPC
  listeners. New flags `--cert-file`, `--key-file`,
  `--trusted-ca-file`, `--client-cert-auth` matching etcd's
  shape. When `--cert-file` + `--key-file` are both set, the
  server listens over TLS (via tonic's `rustls` backend);
  `--client-cert-auth` additionally requires every client to
  present a TLS certificate signed by `--trusted-ca-file`. The
  same identity is used on both the client port and the peer
  port. Flag-parsing rejects the asymmetric case
  (`--cert-file` without `--key-file` etc.) with a clear error.
- **feat(server):** Auth Phase 2 — per-request token enforcement.
  `AuthState` refactored to use std::sync primitives (`AtomicBool`
  for the enabled flag, `std::sync::Mutex` for the token registry)
  so the sync tonic interceptor can read live state without an
  async runtime. `AuthInterceptor` now wraps every non-Auth gRPC
  service via `with_interceptor`; when auth is enabled it requires
  a valid `token` metadata field on every request and returns
  `Unauthenticated` otherwise. The Auth service itself stays
  unauthenticated so clients can still call `Authenticate`. 4 new
  enforcement tests pass: rejected-without-token,
  succeeds-with-valid-token, rejected-with-invalid-token,
  disable-restores-passthrough.

## [v0.3.0] — 2026-05-19

Closes Auth, Defragment, and the real iouring engine from the v0.2.0
"remaining known gaps" list. The only gap from v0.1.0 still open is
the openraft 0.10 upgrade (waiting on its stable release).

### Added

- **Auth gRPC service — Phase 1**: full surface of every Auth RPC
  (`AuthEnable` / `Disable` / `Status`, `Authenticate`, complete
  User / Role CRUD plus grant/revoke). Passwords hashed with
  `argon2`. `Authenticate` returns a 32-byte hex-encoded token
  tracked in an in-memory session registry. `AuthEnable` refuses
  unless a `root` user exists (matches etcd). `RoleDelete`
  cascade-drops the role from every user that referenced it.
  Persisted in dedicated `auth_state` / `auth_users` / `auth_roles`
  tables outside the MVCC revisioned space.
- **Real `Maintenance.Defragment`**: new `KvStore::defragment()`
  trait method (default no-op). redb wraps its `Database` in a
  `tokio::sync::RwLock` and runs `compact()` under the write
  lock; commits/reads share the read lock so defragmentation
  serializes against writers without starving readers.
  WAL-engine compacts by replaying its in-memory index to a tmp
  WAL and atomic-renaming. Tested end-to-end via the
  `Maintenance.Defragment` gRPC handler.
- **Real `IouringEngine`** (Linux-only, behind cargo feature
  `iouring`). A dedicated OS thread (`fastetcd-iouring`) hosts a
  `tokio_uring::start` runtime that owns the WAL file and the
  in-memory MVCC index. The public engine forwards every
  operation through a bounded `mpsc` channel and awaits results
  on per-call oneshots. Same on-disk format as `WalEngine` so a
  file written by one can be read by the other. The conformance
  suite ports verbatim (6 tests, gated `cfg(target_os = "linux")`).
  Non-Linux builds with `--features iouring` compile cleanly
  because `tokio-uring` is target-gated to Linux in `Cargo.toml`.

### Changed

- **`Maintenance.MoveLeader`** continues to return `Unimplemented`
  pending the openraft 0.10 upgrade (alpha-only, not safe to
  depend on yet). It now validates the target is a current voter
  before bailing.

### Known gaps remaining

- **openraft 0.10 upgrade** (waiting on stable release) — unlocks
  real `MoveLeader` via `Trigger::transfer_leader`.
- **iouring tuning**: `O_DIRECT` + aligned buffers (page-cache
  bypass) and group-commit windowing are follow-ups; the current
  iouring engine delivers the io_uring submission path but not
  yet the tail-latency wins those features unlock.
- **Auth Phase 2**: per-request token enforcement. tonic 0.12's
  sync interceptor signature can't `.await` the async token
  registry; the right shape is a `tower::Layer` wrapper, which
  is the next follow-up.

## [v0.2.0] — 2026-05-19

### 2026-05-19
- **feat(storage):** Real `IouringEngine` implementation
  (Linux-only, behind cargo feature `iouring`). A dedicated OS
  thread hosts a `tokio_uring::start` runtime that owns the WAL
  file and the in-memory MVCC index; the public engine forwards
  every operation through a bounded `mpsc::channel<Command>` and
  awaits results on per-call oneshots. Same on-disk format as
  `WalEngine` (length-prefixed bincode batches) so the two are
  binary-compatible. Conformance suite ports verbatim — six
  tests gated by `cfg(target_os = "linux")` exercise the engine
  on Linux CI. macOS/Windows builds with `--features iouring`
  compile cleanly because the `tokio-uring` dep is also
  target-gated to Linux. **Known gaps** vs a full production
  io_uring engine: `O_DIRECT` + aligned-buffer plumbing
  (page-cache bypass) and group-commit windowing (coalescing
  commits within ~1ms into one fsync) are both follow-ups; the
  current commit lands the io_uring submission path, not yet
  the tail-latency wins those features deliver.
- **feat(storage + server):** `Maintenance.Defragment` is real.
  New `KvStore::defragment()` trait method (default no-op for
  engines that don't compact). redb engine wraps its
  `Database` in a `tokio::sync::RwLock` and runs `compact()`
  under the write lock; commit/snapshot take the read lock so
  defrag serializes against writes without starving reads. WAL
  engine compacts by replaying its in-memory index into a fresh
  WAL file and atomically renaming. `Maintenance.Defragment`
  delegates to the active engine's implementation.
  Test: write+overwrite 50 keys, defragment, verify the latest
  value is still readable.
- **feat(server):** Auth gRPC service — Phase 1. Implements every
  Auth RPC (`AuthEnable` / `Disable` / `Status`, `Authenticate`,
  full User / Role CRUD, grant / revoke). Passwords hashed with
  `argon2` (default cost). Authenticate validates the stored hash
  and returns a 32-byte hex-encoded random token; tokens live in
  an in-memory registry on the local node. AuthEnable refuses
  unless a `root` user has been added (matches etcd's
  precondition). RoleDelete cascade-drops the role from every
  user that referenced it. New `mvcc/auth.rs` defines persisted
  `StoredUser` / `StoredRole` / `StoredPermission` types; auth
  data lives in dedicated tables outside the MVCC revisioned
  space (matching etcd's split). 6 new gRPC tests pass.
- **Phase 2 (not in this commit):** per-request token enforcement
  via a tower::Layer wrapper. The Phase 1 `AuthInterceptor`
  placeholder is wired but accepts every request — tonic 0.12's
  sync interceptor signature can't await the token registry
  cleanly; switching to a tower::Layer is the right shape.

## [v0.2.0] — 2026-05-19

Closes every known gap from v0.1.0 except the iouring kernel-bypass
backend (which remains genuinely multi-week follow-up work). 84
tests pass workspace-wide (+9 since v0.1.0). The `etcd-client`
third-party Rust client successfully drives a fastetcd server
through put / get / range / delete / txn / lease / watch /
member-list / status without modification — the strongest
wire-compat signal we can ship in CI.

### Added

- **Lease auto-expiry ticker** (`crates/server/src/lease_expiry.rs`).
  Background task runs on the leader only, once per second walks
  the persisted lease set, and proposes `LeaseRevoke` entries for
  any whose `deadline_unix_secs < now`. Cascade-delete fires through
  the same path explicit revokes use, so watchers see the delete
  events normally. Closes the v0.1.0 "no auto-expiry" gap.
- **Real cluster membership handlers** (`crates/server/src/cluster.rs`).
  `MemberAdd` derives a stable node id from the peer URL, registers
  the URL with the live `PeerEndpoints` map, calls openraft's
  `add_learner`, and (for non-learner) `change_membership` to
  promote. `MemberRemove` rebuilds the voter set minus the target
  and drops peer/directory entries (refuses self-removal).
  `MemberUpdate` rewrites the peer URL. `MemberPromote` lifts a
  learner to voter. `MemberList` returns the live directory with
  name + URLs + learner state. Replaces the v0.1.0 `Unimplemented`
  stubs.
- **Revision-preserving migration**
  (`MvccStore::bulk_load_records`, `MigrationMode::PreserveRevisions`).
  `fastetcd-migrate --preserve-revisions` writes `mvcc_kv` +
  `mvcc_idx` (+ `lease_keys`) directly so source records retain
  their original `create_revision`, `mod_revision`, `version`, and
  `lease`. After migration with the flag, `Range(rev)` and
  `Watch(start_rev)` behave identically to the source server's
  history.
- **Third-party Rust client compatibility tests**
  (`crates/server/tests/etcd_client_compat.rs`). Uses the
  `etcd-client` crate (etcdv3/etcd-client) — which shares no code
  with fastetcd — to drive fastetcd through put / get / range with
  prefix / delete-range with prev_kv / Txn compare-and-set / lease
  grant+attach+revoke with cascade-delete / Watch / MemberList /
  Status. 8 tests, ~0.5s, runs in normal CI without needing the
  Go toolchain.
- **`tests/etcdctl_smoke.sh`** — optional out-of-tree wire-compat
  smoke using the canonical `etcdctl` client. Boots a release-mode
  fastetcd, runs a representative command set, tears down. Skips
  with a clear message when etcdctl isn't installed.
- **`docs/02-testing.md`** — three-ring testing strategy: workspace
  tests, third-party client, upstream etcd suites (with concrete
  pointers at the robustness suite + Jepsen + K8s e2e for higher
  rings).

### Changed

- `Maintenance.MoveLeader` now validates the target is a current
  voter (`FailedPrecondition` if not — matches etcd) and returns a
  typed `Unimplemented` with a precise explanation of the
  openraft-0.9 limitation. Real leader transfer arrives with an
  openraft 0.10 upgrade.

### Remaining known gaps

- **iouring kernel-bypass file I/O.** `WalEngine` ships the
  architecture; swapping the file-I/O layer for glommio + `O_DIRECT`
  remains the right next step for sub-ms p99 on Linux.
- **openraft 0.10 upgrade** — needed for `MoveLeader`'s real
  implementation and potentially other primitives.
- **Auth gRPC service**.
- **`Defragment` / `Downgrade`**.

## [v0.1.0] — 2026-05-19

### 2026-05-19
- **test(server):** Third-party Rust client compatibility tests.
  New `tests/etcd_client_compat.rs` uses the widely-used
  `etcd-client` crate (etcdv3/etcd-client) — which shares no code
  with fastetcd — to validate the wire protocol end-to-end: put /
  get / range with prefix / delete-range with prev_kv / Txn
  compare-and-set / Lease grant+attach+revoke with cascade-delete /
  Watch (drop create-ack + progress notifies) / MemberList /
  Status. 8 tests, runs in ~0.5s. This is the strongest in-repo
  wire-compat signal.
- **test(harness):** `tests/etcdctl_smoke.sh` — optional shell
  harness that boots a release-build fastetcd and runs `etcdctl`
  through a representative command set (put/get/del/range/txn/
  lease/member/endpoint-status). Skipped when etcdctl isn't on
  PATH.
- **docs:** New `docs/02-testing.md` lays out the three-ring
  testing strategy (workspace tests → third-party Rust client →
  upstream etcd robustness / etcdctl / Jepsen / Kubernetes e2e)
  and points future correctness work at etcd's robustness suite
  as the highest-ROI investment.
- **feat(storage + migrate):** Revision-preserving migration. New
  `MvccStore::bulk_load_records(BulkKey[], next_rev)` writes
  `mvcc_kv` + `mvcc_idx` (and `lease_keys`) directly so source
  records retain their original `create_revision`, `mod_revision`,
  `version`, `lease`, and tombstone state. `fastetcd-migrate`
  gains `--preserve-revisions` (and a corresponding
  `MigrationMode::PreserveRevisions` library API). After migration
  with the new flag, `Range(rev)` and `Watch(start_rev)` behave
  identically to the source server's history. Multi-generation
  per-key is collapsed to "everything up to the most recent
  tombstone + its generation"; multi-generation history support
  is a follow-up.
- **feat(server):** `Maintenance.MoveLeader` now validates the
  target is a current voter and returns a typed
  `FailedPrecondition` for invalid targets; for valid ones it
  surfaces openraft 0.9's lack of an explicit `transfer_leader`
  primitive via `Unimplemented` with a precise message. Real
  transfer arrives with the openraft 0.10 upgrade.
- **feat(server):** Real cluster membership handlers. `MemberAdd`
  derives a stable node id from the peer URL (FNV-1a hash, masked
  to 63 bits), registers the URL with the live `PeerEndpoints` map
  used by `GrpcNetworkFactory` (so the leader can dial the new node
  immediately), and calls openraft's `add_learner`; voting members
  additionally call `change_membership` to promote. `MemberRemove`
  removes from voters via `change_membership(remaining, false)` and
  drops the peer/directory entries (refuses to remove self).
  `MemberUpdate` rewrites the peer URL entry; `MemberPromote` lifts
  a learner to voter. `MemberList` returns the live directory which
  also tracks `name`, `client_urls`, and `is_learner` per member.
  The directory is seeded from `--initial-cluster` on bootstrap.
  Replaces the previous `Unimplemented` stubs. New test verifies
  MemberAdd-as-learner appears in MemberList.
- **feat(server):** Lease auto-expiry ticker. Background task runs
  on the leader only (followers are no-ops) and once per second
  walks the persisted lease set; any lease with
  `deadline_unix_secs < now` is auto-revoked via the same
  `FastetcdLogEntry::LeaseRevoke` path explicit revokes use, so
  attached keys cascade-delete through Raft. Leadership transitions
  hand the work to the new leader on its next tick. Closes a known
  gap from v0.1.0. End-to-end test (`lease_expiry_grpc.rs`) grants
  a 1-second lease, attaches two keys, waits 2.5s, and verifies the
  lease is gone and both keys are deleted.

## [v0.1.0] — 2026-05-19

Initial usable release. fastetcd boots a single-node or multi-node
cluster, serves the full etcd v3 KV / Watch / Lease / Cluster /
Maintenance gRPC surface, and persists state through openraft.

### Added

- **Project scaffold and Cargo workspace** with six crates: `proto`,
  `storage`, `raft`, `server`, `migrate`, `ctl`. Apache 2.0 license.
- **Vendored etcd v3.6.11 `.proto` files** (`rpc.proto`, `kv.proto`,
  `auth.proto`) with a reproducible `vendor.sh` pipeline that strips
  gogoproto / grpc-gateway / versionpb annotations. `tonic-build`
  codegen emits stubs under `fastetcd_proto::{etcdserverpb, mvccpb,
  authpb}`.
- **Engine-agnostic `KvStore` trait** with concrete `WriteBatch`
  value, `Snapshot` trait, `WriteOptions`, and a conformance test
  suite reusable by any backend.
- **`redb` storage engine** (default; cross-platform). Single-file
  ACID B-tree. Passes the conformance suite (6 tests).
- **`WalEngine` storage engine** (opt-in via `wal-engine` feature).
  Append-only WAL + in-memory `BTreeMap` index, replay-on-open.
  Passes the conformance suite + persistence-across-reopen.
- **MVCC state machine** over `KvStore`: revisions
  (`Revision { main, sub }` packed 16-byte BE), per-key
  generation index, revision-keyed value table, atomic
  multi-mutation `apply`. `Range` supports current/historical
  reads, `limit` (with `more`), `keys_only`, `count_only`.
  `Compact(rev)` walks `KeyIndex` and prunes generations whose
  tombstone is `<= rev`; preserves the floor record per key for
  exact-rev historical reads. `Txn` with `Compare`
  (Version/CreateRev/ModRev/Value/Lease × Equal/NotEqual/Greater/
  Less, single-key or range) and atomic success/failure branches
  sharing one revision. `range_events()` backs Watch historical
  replay.
- **Lease layer** on `MvccStore`: lease records persisted in the
  `lease` table, `lease_keys` reverse index updated on every
  Put/DeleteRange with non-zero lease, `apply_lease_grant`,
  `apply_lease_revoke` cascade-deletes attached keys, `lease_ttl`
  computes remaining seconds, `lease_list`.
- **openraft integration**: `FastetcdLogEntry` (Apply / Txn /
  Compact / LeaseGrant / LeaseRevoke / LeaseKeepAlive / Noop),
  `FastetcdLogResponse`, `FastetcdStateMachine` wrapping
  `MvccStore`, `KvLogStore` (persistent `RaftLogStorage` over
  `KvStore`), `FastetcdSnapshotBuilder`.
- **Multi-node Raft peer transport**: internal proto
  `fastetcd.raft.RaftPeer` (AppendEntries / Vote / InstallSnapshot)
  carrying bincode-serialized openraft messages.
  `GrpcNetworkFactory` lazily dials peers over tonic;
  `RaftPeerService` handles inbound RPCs.
- **KV gRPC service**: Range, Put, DeleteRange, Txn, Compact.
  Routes mutations through Raft; reads served from `MvccStore`
  (serializable; on single-node this is linearizable since there
  are no replicas). `OutOfRange` on compacted-rev reads.
- **Watch gRPC service** (Phase 1 + 2): live event fan-out via a
  `broadcast::channel` on `MvccStore` plus historical replay from
  `start_revision`. Range filters, NOPUT/NODELETE filters,
  `prev_kv`, progress-notify timer, compacted-rev cancellation.
- **Lease gRPC service**: Grant, Revoke, KeepAlive (bidi stream),
  TimeToLive (with optional attached-keys list), Leases.
- **Cluster gRPC service**: `MemberList` returns self;
  Add/Remove/Update/Promote return `Unimplemented` (membership
  changes via openraft are tracked as follow-up work).
- **Maintenance gRPC service**: Status (real raft + MVCC values),
  Hash / HashKV (SHA-256 folded to u32), Snapshot (streaming),
  Alarm, Defragment (no-op), MoveLeader / Downgrade unimplemented.
- **`fastetcd-migrate` tool**: reads an etcd v3 BoltDB snapshot
  (via `bbolt-rs`), keeps the latest record per user-key, bulk-
  applies into a fresh `MvccStore`. Tombstones detected via the
  trailing `b't'` on bolt keys.
- **`fastetcd` server binary** with etcd-shaped flags:
  `--name`, `--node-id`, `--cluster-id`, `--data-dir`,
  `--listen-client-url`, `--listen-peer-url`, `--initial-cluster`,
  `--initial-cluster-state`. Multi-port serving: client services
  on one socket, RaftPeer service on the peer socket. Single
  `redb` file in `--data-dir` backs both MVCC state and Raft log.
- **Criterion benchmark harness** for the MVCC layer: put-single,
  range-single, batched put, delete-range, comparable across
  engines (`redb` vs `wal`).
- **75 tests pass workspace-wide.** All gRPC services have
  end-to-end tests through real tonic clients; a 3-node multinode
  test validates Raft replication over the gRPC peer transport.

### Known gaps

These are documented and tracked as follow-up work:

- **iouring kernel-bypass file I/O.** The `iouring` cargo feature
  exists and depends on `wal-engine`, but the glommio + `O_DIRECT`
  file-I/O layer that delivers the actual sub-ms p99 is not yet
  implemented. `WalEngine` provides the architectural shape that
  swap will live under.
- **Lease auto-expiry.** Deadlines are persisted and
  `LeaseTimeToLive` returns correct remaining seconds, but no
  leader-side ticker auto-revokes expired leases. Clients must
  explicitly revoke for cascade-delete to fire.
- **Cluster membership changes** (`MemberAdd` / `Remove` / `Update`
  / `Promote`). The peer transport is in; the gRPC handlers
  currently return `Unimplemented`. Wiring through openraft's
  `change_membership` API is straightforward follow-up.
- **Auth gRPC service** is deferred. Common deployments use TLS
  client-cert authentication outside the etcd Auth subsystem.
- **`MoveLeader` / `Defragment` / `Downgrade`** return
  `Unimplemented`.
- **Migration tool revision history**: currently imports only the
  latest record per user-key. Revision-preserving import is a
  follow-up (it requires direct mvcc_kv / mvcc_idx writes
  bypassing the public `apply` path).

### Storage architecture (recap)

```
┌──────────────────────────────────────────────────────────────────┐
│ tonic gRPC: KV / Watch / Lease / Cluster / Maintenance           │
└──────────────────────────────┬───────────────────────────────────┘
                               │ (writes proposed through Raft)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ openraft — consensus, log replication, membership, snapshots     │
│ KvLogStore over KvStore   ·   FastetcdStateMachine over MvccStore │
└──────────────────────────────┬───────────────────────────────────┘
                               │ (apply)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ MVCC state machine: revisions, generations, leases, events       │
└──────────────────────────────┬───────────────────────────────────┘
                               │ (KvStore trait, runtime-selectable)
                               ▼
                ┌──────────────┴──────────────┐
                │                             │
            redb engine                  wal-engine
        (default, B-tree)         (append-only WAL +
                                   in-memory BTreeMap;
                                   iouring backend below
                                   this is future work)
```

## [v0.0.0] — pre-release

See git history for individual commits prior to v0.1.0.

