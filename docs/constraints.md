# Constraints — moved out of CLAUDE.md

Kept here whole, newest notes as they were. Write new intentions here.

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

38. **Write-through RAM cache: resident key index + byte-bounded value cache (#82, P1) — done, shipped in v1.11.0.**
    A single-object GET walked redb for the `KeyIndex` and then the
    record; on a spinning disk (X9, ublk) a cold page is a seek, so
    apiserver GETs took 0.2–0.55 s. etcd keeps every key's index in RAM
    (`treeIndex`) and lets the page cache hold values. Plan:
    - **Resident key index** (`crates/storage/src/mvcc/cache.rs`):
      key → generations, a `BTreeMap` behind a std `RwLock`, built at
      open from `mvcc_idx` (chunked scan), updated only after the engine
      commit and only under the write-state lock, so it never runs ahead
      of the engine. Readers take the index guard while still holding
      the write-state lock (with the engine snapshot), so index,
      snapshot and revision agree; the walk is synchronous (etcd's
      treeIndex holds its RLock the same way). `mvcc_idx` stays on disk
      unchanged: no format change, any version can open the file.
    - Used by Range, Txn compares, apply (index load, DeleteRange key
      scan), and compaction (walks RAM, not the whole `mvcc_idx`).
      Watch backfill keeps reading the disk index.
    - **Latest-value cache**: sharded LRU by bytes,
      `--value-cache-bytes` (default min(128 MiB, 5% of RAM/cgroup
      limit); 0 = off), entries above `--value-cache-max-entry-bytes`
      (256 KiB) not cached. Apply inserts each written key's record
      after commit, deletes drop it; a read that misses at the latest
      revision fills it (only if newer than what is there). An entry is
      used only when its exact `Revision` is the one the index names
      for the read, so a stale entry can only miss, never answer.
    - Snapshot install goes through `MvccStore::install_tables` (commit
      + counters + index rebuild + cache clear under the write lock).
    - `--engine-cache-bytes` sets redb's cache (its default is 1 GiB);
      default 256 MiB. Memory = value cache + engine cache + index.
    - Metrics: value cache hits/misses/evictions/bytes/entries/budget,
      index keys/bytes, `fastetcd_mvcc_get_duration_seconds{cache}`.
    Work items:
    - [x] cache.rs (index + LRU) with unit tests.
    - [x] MvccStore wiring (open, range, txn, apply, compact, bulk load,
      install_tables); state machine uses install_tables.
    - [x] redb cache size; server flags; metrics.
    - [x] Tests: consistency under concurrent writes/compaction with the
      cache on, eviction bound, install clears; GET latency via bench.
    - [x] Docs (01, 03 § Memory, README), changelog; release.
    - Found while benchmarking: the leader's lease precheck evaluates
      every Txn's compares; holding the write lock for that queued every
      CAS Txn with the apply loop (writes -15%, 4-6 s stalls). Fixed: the
      lock covers only snapshot + index copy, and a Txn naming no lease
      skips the evaluation.
    - Verified: sc-build of 5b37fd2, whole workspace green (`--locked
      --no-fail-fast`, 299 tests). `ram_cache_slow_disk` (4 ms per engine
      read): hot GET 0 engine reads, p50 8 µs / p99 18 µs; cold or cache
      off 1 read (was 2), p50 5.4 ms. With the cache's revision match
      removed, the consistency test and the hot-GET test fail.
      `read_latency.sh v1.10.0` on dev (SSD, page cache warm): 1 member
      writes 368 → 496/s, max 1.7 s → 0.24 s; quiet runs linearizable
      p99 59 → 0.3 ms and 23 → 4.3 ms; under a concurrent build, noise.
    - Not verified here: the issue's acceptance on an X9 blade (apiserver
      GET p99 < 20 ms on a spinning disk, RSS over a long run) needs the
      golden on that hardware.

39. **Streaming writes: the raft log is a sequential WAL; redb is checkpointed in the background (#85, P1) — done, shipped in v1.12.0.**
    Today the raft log lives in redb: every append is a durable redb
    commit, and since #71 that commit also carries every apply made
    since the last one (applies are non-durable redb commits). So the
    fsync on the client's path writes scattered B-tree pages: seeks on a
    spinning disk. Plan:
    - **WAL** (`crates/storage/src/raft_wal.rs`, byte records, no
      openraft types): `<data-dir>/wal/<seq>.wal` segments, preallocated
      (`--wal-segment-bytes`, 16 MiB), records `len|crc32|kind|index|
      payload`, append-only. Entry, Truncate, Purge, Vote, Committed
      records; a new segment starts with the current vote/committed/
      purge. Open: replay in order; a bad record in the last segment is
      a torn tail (cut there, re-preallocate), elsewhere an error.
    - **Log store** (`crates/raft/src/wal_log_store.rs`): one writer
      thread groups whatever openraft appended meanwhile into one
      fdatasync (group commit) and calls `LogFlushed` after it. Entries
      read from a byte-bounded tail cache or by pread. Vote is synced
      before `save_vote` returns; committed rides the next write.
    - **Checkpointer**: every `--wal-checkpoint-interval-ms` (100) or
      `--wal-checkpoint-entries` (10000) applied entries: fdatasync the
      redb file outside redb's writer lock (so the commit has little left
      to flush), then one durable redb commit. Only then does a purge
      openraft asked for drop WAL segments: the WAL never loses an entry
      redb has not made durable.
    - Applies stay non-durable redb commits made before the client is
      answered (responses carry revisions); RAM index/cache unchanged.
      Back-pressure is the existing ack-after-apply + proposer cap.
    - **Upgrade**: no `wal/` → build it from redb's `raft_log`/`raft_meta`
      (temp dir, rename), then clear redb's `raft_log`. Vote and purge are
      mirrored into redb's `raft_meta` (non-durable) so a backup restores
      to a consistent log. Restore paths move `wal/` aside with the
      snapshots. One-way: no downgrade below this version.
    Work items:
    - [x] storage raft_wal + unit tests (append/replay/torn tail/purge).
    - [x] raft wal_log_store + checkpointer + migration; tests (openraft's
      log-store Suite, restart, upgrade, a node that drops segments).
    - [x] server: flags, main wiring, restore/corruption paths, fsck,
      metrics, sizing; multinode/fastpath/snapshot-transfer harnesses on
      the WAL; real-binary upgrade test (`wal_upgrade.rs`).
    - [x] Docs (00, 01, 03, 04, 05, README), changelog.
    - [x] Full workspace green on dev; bench vs v1.11.0; release.
    - Changed while doing it (bench-driven): applies first stayed
      non-durable redb commits, and the checkpoint's durable commit then
      held redb's writer through its fsync, so applies stalled behind it
      (write p99 2x worse than 1.11). Fixed with the write-behind layer
      (the issue's point 2): applies never touch redb. And `fallocate`d
      segments made every fdatasync an ext4 journal commit (~10 ms, up
      to 3 s); segments are now zero-filled by a background thread.
    - Verified: sc-build of 6074121, whole workspace green (337 tests,
      `--locked --no-fail-fast`), incl. openraft's log-store Suite over
      WAL + write-behind, a 3000-step model test of the write-behind,
      a real-binary upgrade from a redb-log data dir, SIGKILL replay.
      `read_latency.sh v1.11.0` (1 member, 2 rounds): writes 601 → 1662/s
      and 693 → 1513/s, p50 44 → 18 ms, p99 135 → 111 / 125 → 135 ms,
      linearizable p99 50 → 0.6 ms. 3 members: 683–924 writes/s vs
      199–314, p99 165–236 ms vs 543–2935; maxima still multi-second
      when the shared disk stalls (all members' WAL fsync max 5.15 s at
      once), as before: #83.
    - Not verified here: the X9 spinning-disk acceptance and the
      power-cut durability stage (need the golden on that hardware).

40. **Research: upstream etcd vs fastetcd v1.11 / v1.12, fast and slow disk (#90) — done (docs; no release).**
    Deliverable `docs/benchmarks/etcd-vs-fastetcd.md` + CSVs in
    `docs/benchmarks/data/`, reproducible by `tests/bench/compare.sh
    <etcd-version>`. Plan:
    - Contenders: etcd release binary (pinned version + sha256 from its
      SHA256SUMS), fastetcd v1.11.0 and v1.12.0 built from the tags.
    - etcd's own `benchmark` tool built from the etcd tag with a
      downloaded Go: put, range (linearizable / serializable), txn-put,
      watch, lease-keepalive; 1 member then 3 members.
    - Durability: `fastetcd-bench --mode durability` writes per-client
      sequential keys, logs the last acknowledged one, the server is
      SIGKILLed mid-write; after restart, count missing acknowledged keys
      and time to first linearizable read. (kill -9 only on dev: a power
      cut needs a VM.)
    - Resources: RSS idle / under load, CPU, `/proc/<pid>/io` write_bytes
      per 1000 writes.
    - Kubernetes-shaped: rustkube's `test/e2e/get-latency.sh` with
      `RK_FASTETCD` pointing at each contender (a flag-translating wrapper
      for etcd).
    - Slow disk: a throttled pve VM, requested on #90 (2026-10-03); no
      unprivileged way to throttle dev's disk.
    Work items:
    - [x] fastetcd-bench durability mode; compare.sh (download, build, run,
      CSV); sc-build runs on dev (fast disk).
    - [x] rustkube rig per contender.
    - [x] Doc with tables + conclusion; changelog.
    - [x] Slow-disk run on the VMs the master provided (pve benchfast /
      benchslow, user `bench`; the master removes them).
    - Result: v1.12 wins reads under load everywhere (apiserver GET p99
      1.0 vs 835 ms on the slow disk); etcd wins raw write throughput
      (1.3–6x) and lease keepalive. Filed #92 (keepalive via Raft), #93
      (write-behind back-pressure behind the checkpoint fsync), #94 (P1,
      proposer caps writes per fsync on a slow disk).

41. **Group commit on a slow disk (#95, P1; also #94) — done, shipped in v1.13.0.**
    Measured on benchslow (pve VM, fdatasync p50 25 ms that day), 1
    member, etcd's `benchmark put`, 20 clients: etcd 219/s (avg 90 ms,
    ~9 puts per WAL fsync of 35 ms); v1.12 34/s (avg 568 ms, ~4 puts per
    fsync of 119 ms, and 92 checkpoints using 11.2 s of disk in ~12 s).
    Lone writes (1 client, 3/s) are equal (p50 27 ms). With
    `--wal-checkpoint-interval-ms 1000` v1.12 did 97/s and fsyncs fell
    to ~40 ms: checkpoints running back to back slow every WAL fsync,
    and the proposer's 3 in-flight writes cap what one fsync carries.
    Plan:
    - **Proposer**: first tried 64 `client_write`s in flight, assuming
      (as #85's docs said) that RaftCore does not wait for an append's
      fsync. Wrong: openraft 0.9.24 `RaftCore::append_to_log` awaits
      the `LogFlushed` callback, so every fsync carries exactly one
      entry (measured: `entries_synced == fsyncs`) and 64 in flight was
      worse (1.6 per fsync in the test, 21/s on benchslow). Then tried
      freeing the slot when a batch is durable (d993c78): RaftCore,
      blocked on the next batch's fsync, then answers a batch an fsync
      late. Kept: **one batch in flight, slot freed when answered**
      (benchslow, interleaved: 1 member light load 70 vs 210 ms avg,
      etcd 120; 3 members lone p50 75 vs 119 ms; bursts equal or better).
    - **Checkpoint pacing**: after a checkpoint that took `d`, the next
      waits at least max(interval, 4·d) from its end, so checkpoints use
      at most ~20% of a slow disk's time; on a fast disk (d ≪ 25 ms)
      nothing changes. Write-behind bytes still bound RAM.
    - **Metrics**: `fastetcd_wal_entries_synced_total`,
      `fastetcd_wal_proposals_synced_total` (entries / proposals per
      fsync = their rate / the fsync rate).
    Work items:
    - [x] proposer in-flight; checkpoint pacing; metrics; tests.
    - [x] Docs (00, 01, 03 § Metrics), changelog.
    - [x] sc-build green; benchslow: 20 / 1000 clients and lone writes
      vs v1.12 and etcd; release; golden.
    - Verified: sc-build of cb8c285, whole workspace green (339 tests,
      `--locked --no-fail-fast`). `concurrent_writers_share_a_wal_fsync`
      (20 closed-loop writers, 200 ms fsyncs): 10–12 proposals per fsync
      in 5 repeats; with `IN_FLIGHT = 3` put back, 4.6 and it fails.
      `slow_checkpoints_leave_the_disk_to_the_wal`: paced to 4x a 40 ms
      checkpoint, last write still made durable. benchslow, release build
      of cb8c285 vs v1.12.0 vs etcd v3.7.2 (etcd `benchmark put`), 1
      member: lone p50 31–45 / 44 / 34 ms; 10 clients at 30/s 29.8 and
      27.4/s avg 100 / 135 ms vs 15.3/s 562 ms vs 29.3/s 121 ms; 20
      clients 133 / 186/s vs 26/s vs 89/s; 1000 clients 1 432 / 4 029/s
      vs 788/s vs 7 142/s. 3 members (one disk): lone p50 136 / 248 /
      42 ms; 30/s 24.5/s avg 308 ms vs 10.9/s 833 ms vs 26.5/s 275 ms;
      1000 clients 1 121 / 421 / 2 540/s.
    - Filed: #97 (3 members, a lone write pays the leader's and a
      follower's fsync in series: openraft 0.9 replicates only after the
      leader's own fsync). Still open: #94 (`MAX_BATCH` 256 caps writes
      per fsync at 1000 clients).
    - Not verifiable here: the 5-pod run on server3 (X9 blade).

42. **A presentation of its purpose and functionality (#27, P2) — done (docs; no release).**
    `docs/presentation.md`, a Marp deck of 8–15 slides like the other
    components' decks: purpose; place in stormcos (stormcentral's graph:
    rustkube and irondirectory depend on it, it depends on nothing;
    stormconsole and flowsdn's ClusterMesh read it); one architecture
    diagram; what works today, from the code; planned work on its own
    slide; interfaces; how it ships (special golden, stage mode) and runs
    (stormd); status and the open issues that matter. Every claim
    checked against the code or the doc it came from.
    Work items:
    - [x] Deck; README link; changelog.
    - [x] Verify: sc-build of the deck commit runs `npx
      @marp-team/marp-cli@4 docs/presentation.md` on dev: renders, 12
      slides. PNG export needs a browser dev lacks, so the layout was not
      looked at; dense slides use smaller type. Defaults, flags, metric
      and RPC names re-checked against main.rs / the protos; the golden
      against stormcos `deploy/build-goldens.sh` (stage mode, #81).

43. **A request-level error at apply never stops a member (#49, P0).**
    `MvccStore` returns an error for a request that cannot apply, and
    the state machine turns every apply error into an openraft
    `StorageError`, which stops RaftCore on every member (and again on
    replay). Triggers found reading the code, not only the issue's: a
    Put with `ignore_value`/`ignore_lease` on a missing key (alone or in
    a Txn), `Compact` at a future revision, below the compacted one or
    <= 0, `LeaseKeepAlive` of an unknown lease, `LeaseGrant` with TTL
    <= 0, a Txn `Range` at a compacted or future revision. Since #75 a
    batch carrying one of them fails the whole batch. Plan:
    - storage: `Refusal` (KeyNotFound, LeaseNotFound, Compacted,
      FutureRevision, InvalidArgument) and `MvccError::Refused`; every
      request-level error is raised before anything is written, so a
      refused entry applies nothing.
    - raft: `apply_data` turns a refusal into `FastetcdLogResponse::
      Refused { revision, refusal }` (new variant, appended); only
      storage/internal errors stay fatal. Same on every member (the
      state they check is replicated).
    - Leader precheck (`precheck.rs`, beside #19's lease check, with
      the read barrier on a hit): ignore_* put on a missing key with no
      earlier put of it in the same op list, Compact out of range,
      KeepAlive of a missing lease, TTL <= 0. Refused before proposing,
      so mixed-version members never see such an entry from a new leader.
    - server: refusal → etcd's gRPC status (`etcdserver: key not found`
      InvalidArgument, lease not found NotFound, `mvcc: required
      revision has been compacted` / `is a future revision` OutOfRange);
      KeepAlive of an unknown lease answers TTL 0, as etcd's does.
    Work items:
    - [x] storage Refusal + raise sites; raft Refused response, apply,
      precheck, ForwardWrite; server mapping, lease keepalive, compaction.
    - [x] Tests: apply-level (raft, refused entry + batch neighbours
      applied, replay), gRPC single node (put/txn/compact/keepalive),
      3-member (cluster keeps serving after each).
    - [x] Docs, changelog; release; close #49.
    - Changed while doing it: etcd grants a TTL <= 0 at its minimum
      (2 s) rather than refusing it, so the handler raises it before
      proposing; 0 < TTL < 2 is still granted as asked (#108, P3).
      Local Range errors now carry etcd's text (clientv3 matches
      ErrCompacted by message).
    - Verified: sc-build of 44c90f7, 348 tests pass; the only failure
      was a load-timing flake in `wal_log_store` (#109/#110, deadlines
      widened). New tests: `refused_apply.rs` (raft), `refused_requests.rs`
      (gRPC), `multinode_grpc::a_refused_request_stops_no_member`. With
      the state machine's refusal handling patched out on dev, both raft
      tests, the 3-member test and 2 of 5 gRPC tests fail, the 3-member
      one with the issue's `apply failed: etcdserver: key not found`
      (Unavailable); the other gRPC cases are caught by the precheck.

44. **3 members: multi-second write maxima under read_latency (#83, P2).**
    Worked while #49 is paused (its WIP is on branch
    `wip/2026-10-06-49-refusals`, not merged). Dev's shared disk stalls
    every member's fsync at once (#85), so first tell disk stalls from
    fastetcd stalls:
    - Bench prints when its slowest writes started (UTC, as the logs);
      the WAL warns `slow raft WAL fdatasync` at >= 1 s (etcd's
      threshold); `METRICS=1` adds leader-change / proposal metrics and
      the members' election + slow-fsync log lines.
    - `DATA_ROOT=/dev/shm`: the same bench on tmpfs, where no fsync
      stalls. Maxima there are fastetcd's own.
    Work items:
    - [x] Instrumentation (0698e2f).
    - [x] sc-build at 818e88a (v1.17.1): 3 members, disk and tmpfs
      interleaved, twice. Disk: write max 1.57–2.18 s, every member's
      longest WAL fsync 0.80–1.13 s at the same moment, the slowest
      writes all started within 1 ms of each other. tmpfs: max 32–81 ms
      (leader and follower clients), 4–7k writes/s. One leader change
      (bootstrap) in every run.
    - [x] Attributed: the disk. A write pays the leader's fsync then a
      follower's (#97), so a ~1 s stall shows as a ~2 s write. Nothing in
      fastetcd stalls (tmpfs max < 0.1 s); no elections.
    - [x] Changelog; close #83.

45. **Bool flags take etcd's values; the chart's pods start (#53, P1).**
    `--auto-defrag`/`--upgrade-backup` (default on) were clap `SetTrue`
    switches: `=false` and the chart's `--auto-defrag=true` were parse
    errors, so every chart pod exited at startup, and only the env var
    with exactly `false` could turn them off. `--client-cert-auth=true`
    and the other default-off flags fail the same way. Go's flag package
    (etcd) takes `--flag`, `--flag=true|false` (and 1/0, t/f), not
    `--flag false`. Plan, for every server bool flag (`--force`,
    `--repair` and other subcommand switches stay switches):
    - `num_args = 0..=1, default_missing_value = "true",
      BoolishValueParser`: bare = true, `=false` turns it off, env vars
      take true/false/1/0/yes/no/on/off. No `require_equals`: `--flag
      false` (accepted for `--enable-grpc-gateway` since 1.8) keeps
      working, so nothing that parsed before breaks (a PATCH, 1.14.1).
    Work items:
    - [x] main.rs flags; unit tests parsing each form.
    - [x] `tests/chart_args.sh`: render the chart with helm and start the
      rendered command for real (autoDefrag true and false), on sc-build.
    - [x] Docs (01-configuration), changelog; release; golden; close #53.
    - Verified: sc-build of 62d87ed ran `tests/chart_args.sh`: three
      rendered pods (defaults, autoDefrag=false, bool flags in extraArgs)
      started and served a put/get; the v1.14.0 binary refused
      `--auto-defrag=true`/`=false` with the issue's error.

46. **A live snapshot can be restored (#61, P1).** `Maintenance.Snapshot`
    (`etcdctl snapshot save`, `fastetcd-ctl snapshot-save`) streamed the
    raft snapshot body, which nothing reads back (`fastetcd restore`,
    `fastetcd-migrate` and etcdctl all refuse it), and which lacks the
    lease tables (#41), the raft membership and node metadata anyway.
    Plan (the issue's second option):
    - The RPC streams a `FEBACKUP` file, the format of `--backup-dir`
      backups: every table from one engine snapshot (through the
      write-behind layer, so RAM-only applies are in it), checksummed.
      `fastetcd restore <file>` already restores it. Encoded straight
      into the gRPC stream from a blocking thread: no temp file (a
      full copy could fill a bounded volume, #14), no second copy in
      RAM; `remaining_bytes` from bincode's serialized sizes.
    - `fastetcd-ctl snapshot-save` checks the magic and the checksum
      after writing and says how to restore it.
    - `fastetcd restore` on a pre-fix raft-snapshot file says what it is
      and that it cannot be restored; its error no longer points at
      snapshot-save as a hot backup if that is unrestorable (it now is).
    - etcdctl `snapshot status/restore` still need BoltDB: documented.
    Work items:
    - [x] backup.rs: dump + writer split; maintenance.rs stream.
    - [x] ctl verify; restore's messages.
    - [x] Tests: snapshot-save over gRPC → `fastetcd restore` → start →
      keys, leases and auth present; checksum; a write made just before
      the snapshot (write-behind) is in it; old-format file refused.
    - [x] Docs (03 § Backups, 05), changelog; release; golden; close #61.
    - Verified: sc-build of 2580a61, 352 tests pass (the 1 failure #45);
      `snapshot_restore.rs` 2/2. With v1.14.1's maintenance.rs put back
      on dev, the restore test fails (the stream is not a backup).

47. **Raft snapshots carry the lease tables (#41, P1).** A snapshot held
    only `mvcc_kv`/`mvcc_idx`/`mvcc_meta` (+ auth since #32), so a member
    caught up by one had no `lease`/`lease_keys` rows for leases granted
    before it, or stale ones. Its keys on those leases then never expire
    there, and a revoke deletes them on the other members but not on it
    (the revoke finds no `lease_keys` rows): MVCC divergence. Leases have
    no RAM state (expiry walks the `lease` table; `next_lease_id` is in
    `mvcc_meta`, already carried and reloaded). Plan:
    - `LeaseTables` (storage, like `AuthTables`): read both tables from
      the build's engine snapshot; replace both in the install batch.
    - A second trailer after the auth trailer (`FETCDTR2`, leases).
      bincode reads one value and ignores what follows, so a 1.5–1.15
      member decodes payload + auth trailer as before; a body with no
      second trailer (older leader) leaves the lease tables alone, as
      today.
    - Not done here: moving #19's lease check to apply. A member caught
      up by a pre-fix snapshot keeps wrong lease tables until its next
      snapshot install, which no version gate can see, so an apply-time
      check would make members disagree. Follow-up issue.
    Work items:
    - [x] storage LeaseTables; raft trailer + build + install; unit tests
      (both directions of compat).
    - [x] Tests: install on a learner with a stale lease (leader's lease
      + keys + deadline there, stale gone, revoke deletes the keys, ids
      allocate after the leader's); a real chunked InstallSnapshot
      carries a lease (`snapshot_transfer_grpc`); old bodies decode.
    - [x] Docs (03 upgrade note: members caught up before 1.16 may hold
      wrong lease tables; remedy), changelog; release; close #41.
    - Verified: sc-build of 686f580, whole workspace green (356 tests).

48. **Lease RPCs are authorized (#47, P1).** `lease.rs` never called
    authz: with auth on, any user could revoke any lease (deleting keys
    it cannot write), list any lease's keys, and attach keys to a lease
    whose other keys it cannot write. Rules read in etcd release-3.5's
    source (`apply_auth.go`, `v3_server.go`, `v3rpc/lease.go`), root
    exempt (`IsAdminPermitted`), each over the keys attached now:
    - `LeaseRevoke`: write on every key (`checkLeasePuts`).
    - `LeaseKeepAlive`: write on every key (`checkLeaseRenew`; not in
      the issue's list). Denied → the stream ends with the error.
    - `LeaseTimeToLive`: with `keys: true` only, read on every key.
    - `LeaseLeases`: read on every key of every lease (`checkLeaseLeases`).
    - `Put` with a lease, and every put in both branches of a Txn
      (`checkPutAuth`): write on every key already on that lease.
    - `LeaseGrant`: a login (the interceptor).
    Checked at the API layer against the serving member's applied auth
    state, as #22/#31; etcd checks put and revoke at apply. Work items:
    - [x] storage: keys attached to a lease, and to all leases.
    - [x] server: lease.rs checks; kv.rs put/txn lease accesses.
    - [x] Tests `authz_lease.rs`: each denied for a user lacking the
      permission, allowed with it and for root; negative check with the
      checks stubbed (all 5 fail).
    - [x] Docs (auth section), changelog; release; close #47.
    - Verified: sc-build of 45d8c4b, 360 pass, 1 = #45's flake.

49. **A batch takes what queued, bounded by bytes (#94, P1).** Since #95
    one batch is in RaftCore at a time and the next forms from all that
    queued, but `MAX_BATCH = 256` proposals (512 KiB) caps one fsync at
    1000 clients (etcd ~350–450 per fsync there). The cap was a guard for
    replication: a batch is one log entry, an entry cannot be split
    across AppendEntries, the peer port decodes at most tonic's default
    4 MiB per message, and nothing bounded an AppendEntries' size
    (openraft sends up to 300 entries: 8 full batches already exceed
    4 MiB, which a follower refuses and openraft resends forever, as a
    Network error). Plan:
    - WAL log reader: `limited_get_log_entries` (used only by openraft's
      replication) returns entries up to `REPLICATION_BYTES` (1 MiB of
      encoded entries, always at least one). openraft 0.9.24 gives each
      AppendEntries `heartbeat_interval` (250 ms) to be sent, appended
      and fsynced (`C::timeout(heartbeat_interval, ..)` in
      replication), and resends a slow one at the same size: 3 MiB did
      not make it on dev (first try), 1 MiB is two full batches.
    - Proposer: `MAX_BATCH` 256 → 4096; `MAX_BATCH_BYTES` stays 512 KiB
      (2 MiB first tried: entries that big could not replicate within
      a heartbeat). #94's case is the count: 1000 x ~250 B = 250 KiB.
    - Peer port decodes up to 16 MiB (a single client write near 4 MiB
      plus the RPC's framing could exceed 4 MiB).
    Work items:
    - [x] Code; tests: limited read bound (many entries; one entry over
      the bound); 1000 writers on a 200 ms fsync share fsyncs beyond 256;
      a learner added after ~24 MiB of batches catches up from the log.
    - [x] Docs (00, 03 group commit), changelog; release; close #94.
    - Verified: sc-build of fc11ea3, whole workspace green (364). 1000
      writers: 500 per fsync (cap 256 put back: 222, fails). Learner:
      caught up 2/2 (~47 s); replication bound disabled: never.
    - Not verified: the issue's slow-VM bench (benchslow is gone).

50. **Test containers per the stormcos test standard (#36, P1) — done.** One
    image from `test/Containerfile` (`FROM scratch`: the static
    `fastetcd-test` as `/test`, and the commit's static `fastetcd` as
    `/fastetcd`), built by `test/build.sh` on the build box (musl), run by
    stormcentral as `/test short|medium|long`; JSON lines, exit 0/1/2.
    The node's fastetcd is rustkube's live store, so nothing cluster-wide
    (compact, defrag, alarms, auth, membership) is ever done to it:
    - **short** (< 2 min): the node's fastetcd at `$STORM_NODE:2379`:
      `/health`, status + no alarms, put/get/txn/range/delete under
      `/storm-test/<run id>/`, a watch sees them, a lease with a key
      revoked; everything it made removed. A client port that needs a
      client certificate (stormcos#146; the runner gives none) → skip,
      saying so. Then the commit's own `/fastetcd`, started on loopback in
      the pod: up, a write and a read.
    - **medium** (~10 min): the commit's binary in the pod, data under
      `/results` (or `$TMPDIR`): history + watch + compaction, lease
      expiry and keep-alive, auth + RBAC (incl. lease authz, #47), the
      NOSPACE alarm on a small quota and recovery, live snapshot →
      `fastetcd restore` (#61), SIGKILL keeps every acknowledged write,
      3 members: follower forwarding, leader SIGKILLed → new leader,
      nothing acknowledged lost, old leader rejoins.
    - **long** (night): waves sized from the pod's own CPUs and memory
      limit (cgroup): ramp (keys), hold (mixed load, watches, lease
      churn), drain (delete, compact, defrag); per wave throughput, p99,
      RSS, fds, db size after defrag; a slower wave or growing residue
      fails.
    Work items:
    - [x] `test/` crate (workspace member, not a default member, so the
      golden's `cargo build` is unchanged), build.sh, Containerfile,
      requires.toml, README.
    - [x] sc-build: build.sh stages static binaries; run `medium` and a
      short-against-a-local-member in the job (`tests/test_container.sh`:
      short 7/7, medium 7/7, long 2 waves + trend, at c2ef8cb).
    - [x] `stormcentral test run fastetcd short` on a test machine: run
      3070765391 on pvetest2 (2026-10-08, a5d8834) passed through the
      whole pipeline (build VM, test/build.sh, the machine's registry, a
      Job): `commit-binary-serves` pass; the five node checks skip, the
      node's client port being mutual TLS with no client pair given to
      the Job (stormcentral#555).
    - [x] Docs (02-testing), changelog.
    - Found: online defragment broken after any non-durable commit
      (#119, fixed in 1.17.1).

51. **Election timeouts are settable; a slow fsync no longer has to cost
    the leader (#103, P2).** Read in openraft 0.9.24: heartbeats are sent
    only from RaftCore's `Notify::Tick` handler, and `append_to_log`
    awaits the append's fsync inside RaftCore, so a leader whose WAL
    fsync stalls sends no heartbeat until it returns; a stall longer than
    the followers' election timeout (1–2 s, fixed) elects a new leader.
    etcd has the same exposure and its remedy: `--heartbeat-interval` /
    `--election-timeout`. Plan:
    - Flags `--heartbeat-interval` (ms, 250) and `--election-timeout` (ms,
      1000), `ETCD_HEARTBEAT_INTERVAL` / `ETCD_ELECTION_TIMEOUT`; election
      range [t, 2t); etcd's checks (election >= 5x heartbeat, <= 50 s).
      Defaults unchanged. The heartbeat is also openraft's AppendEntries
      timeout (#94): documented.
    - WAL warns when an fsync outlasts the election timeout, naming it.
    - Test: 3 members, every fsync 2.5 s: the default timeout loses the
      leader (term rises); `--election-timeout` 10 s keeps it.
    Work items:
    - [x] Flags + validation (unit tests), WAL warning, harness test.
    - [x] Docs (01, 03), changelog; close #103 (released with #80).
    - Found while testing: with every member's disk stalled, the
      outcome depends on timing (followers stuck in their own fsync do
      not time out): no election, or 40 terms of churn with no leader.
      Asserted: a stall on the leader alone elects at 1 s (3/3), and a
      10 s timeout keeps the leader either way.

52. **`--snapshot-count` counts writes again, as etcd's does (#80, P2).**
    Since #75 a log entry can be a batch of hundreds of proposals, and
    openraft's `LogsSinceLast` and `max_in_snapshot_log_to_keep` count
    entries, so the log between snapshots (and kept after a purge) held
    up to 256x what sizing assumes. Plan (the issue's option 1, extended
    to the kept log):
    - The state machine counts proposals per applied entry (a batch's
      length, else 1; blank/membership 0) in a small (index, cumulative)
      log.
    - A task per member: once `--snapshot-count` proposals are applied
      since the last snapshot, `trigger().snapshot()`; after a snapshot
      at S, `trigger().purge_log(upto)` with upto the newest index leaving
      `--max-in-snapshot-log-to-keep` proposals after it. openraft's
      entry-counted policy stays as the backstop (unbatched, the two
      agree).
    - Test: proposals batched into few entries: snapshot after N
      proposals, log purged to about K proposals.
    Work items:
    - [x] SM proposal log; policy task; main wiring; test.
    - [x] Docs (01, 04, sizing text), changelog; release 1.18.0; close #80.

53. **fastetcd-migrate imports leases and auth (#60, P2).** It read only
    etcd's `key` bucket: keys kept etcd lease ids no lease backed (never
    expiring), and users, roles and `auth enable` were lost. From etcd
    release-3.5's source: `lease` bucket (key: BE lease id, value
    `leasepb.Lease {ID, TTL, RemainingTTL}`; etcd restores with
    RemainingTTL if set, else TTL, raised to its minimum), `authUsers` /
    `authRoles` (authpb User / Role), `auth` (`authEnabled` = {1}/{0});
    passwords are bcrypt hashes. Plan:
    - Grant every lease (id kept, TTL as etcd restores it, at least 2 s,
      fastetcd's minimum) before the keys; a key whose lease is not in the
      bucket gets no lease, counted and warned.
    - Auth: roles + permissions, users (their bcrypt hash as is, no_password,
      roles), then enable if etcd had it on — through `apply_auth`, so
      fastetcd's rules hold (enable needs root).
    - Server: `Authenticate` also verifies bcrypt hashes (crate `bcrypt`),
      so migrated users keep their passwords; new ones stay argon2.
    - Storage: `bulk_load_records` attaches lease_keys only for a live
      key's latest record (it attached every record, deleted ones too).
    - Tests: synthetic bolt round trip (leases, orphan, auth); bcrypt
      verify unit test; `tests/migrate_e2e.sh`: real etcd v3.5 → snapshot
      → migrate → fastetcd: keys, leases expiring, users logging in with
      their etcd passwords, RBAC enforced.
    Work items:
    - [x] migrate leases + auth; bulk-load fix; server bcrypt; tests.
    - [x] e2e with real etcd; docs (00, README migration), changelog;
      release; close #60.
    - Found by the e2e: the tool could not open any etcd file (bbolt-rs:
      magic, then no freelist): own reader (`bolt.rs`); etcd's trailing
      sha256; a denied call answers fastetcd's own text, not etcd's
      (#127).

54. **Pre-1.10 members are kept from batched logs, and reported (#77, P2).**
    A member older than 1.10 cannot decode `FastetcdLogEntry::Batch` and
    stalls on the first one; only prose (03-deploy) guarded against
    adding one or downgrading one once a cluster has batched, and nothing
    recorded that it had. Plan (the issue's 1–3; 4 is the rolling-upgrade
    test, #65):
    - Marker `batched` in `mvcc_meta`, staged into the first applied
      batch's commit; replicated with snapshots (mvcc_meta travels),
      reloaded at open and after an install. `MvccStore::has_batched()`.
      Gauge `fastetcd_log_has_batched`.
    - MemberAdd: with the marker set, ask the new member `ConfirmLeader`
      before adding it; an older one (`Unimplemented`) is refused,
      naming the version line; unreachable (not started yet, the usual
      `member add` order) goes ahead, with a warning. MemberPromote
      (and add as voter of a member that answers): refused unless the
      member answers as 1.10+.
    - Leader watch (every 30 s, marker set): every member asked
      `ConfirmLeader`; an older one (a downgrade) logs an error naming it
      and counts in `fastetcd_members_unable_to_read_batches`.
    Work items:
    - [x] storage marker; cluster guard; watch; metrics; tests.
    - [x] Docs (03 upgrade section), changelog; release; close #77.

55. **An abandoned snapshot transfer keeps the retained snapshot (#45, P2) — done, shipped in v1.20.1.**
    Flake in `snapshot_transfer_grpc` (learner installed a snapshot, its
    snapshot dir empty). Cause, from logs (repro 1 in ~10 on dev): the
    install took >1 s, the leader's InstallSnapshot timed out and resent;
    the resend's `begin_receiving_snapshot` rolled off the snapshot just
    installed, and openraft then dropped the duplicate ("No need to
    install snapshot; ... <= committed"), leaving nothing retained.
    Fix: `SnapshotStore::begin_incoming` no longer rolls off; `adopt`
    does it before its rename. Space: a receive that hits ENOSPC already
    discards every retained snapshot and retries (`SnapshotFile`), so a
    full disk is still handled; with room, a receive holds one more copy.
    Work items:
    - [x] Fix + unit test (abandoned transfer keeps the snapshot).
    - [x] Control: with the roll-off put back, 4 of 40 runs fail; with
      the fix 0 of 40.
    - [x] Full workspace green (sc-build of fdc26a2, 376 tests); docs
      (04), changelog; release 1.20.1; close #45.

56. **fastetcd-ctl talks to a TLS client port (#59, P2) — done, shipped in v1.21.0.** It took only
    `--endpoint` and `--user`, so `status`, `defrag`, `alarm` and
    `auth members/adopt` (no etcdctl equivalent) were unreachable on a
    member with `--cert-file`. Plan (etcdctl's options):
    - `--cacert`, `--cert`, `--key` (`ETCDCTL_CACERT`/`_CERT`/`_KEY`).
      `--cacert` is required for `https://`: the ctl has no system trust
      roots (tonic `tls` only; platform CAs are private anyway). `--cert`
      and `--key` go together. TLS options with an `http://` endpoint,
      or `https://` without `--cacert`, are errors naming the flag:
      never a silent plaintext dial.
    - An endpoint without a scheme (etcdctl's `127.0.0.1:2379`) is
      `https://` with TLS options, else `http://`.
    - One channel for every command; `--user`'s token goes on every
      command (it went only on `auth`), as etcdctl's `--user` does.
    Work items:
    - [x] ctl: options, one connect, token everywhere; unit tests.
    - [x] `tests/ctl_tls.sh` (real binaries, openssl CA): server TLS
      only and `--client-cert-auth`; every command over TLS; the
      refusals. 31/31 at d8885f8.
    - [x] Docs (01, 02, 03), changelog; release 1.21.0; close #59.

57. **A Txn inside a Txn runs, as in etcd (#56, P2) — done, shipped in v1.22.0.** `RequestOp.
    request_txn` was refused `Unimplemented`. etcd release-3.5
    (`apply.go`): `compareToPath` evaluates every compare, nested ones
    included (only in the branches taken), against the state *before*
    the txn; the ops then run in order in one write txn, so they share
    one revision (sub-revisions in order) and a Range sees earlier
    writes, nested or not. A nested `ResponseTxn` carries `succeeded` and
    its branch's responses. Plan:
    - storage: `TxnOp::Txn(NestedTxn { compares, success, failure })`
      and `TxnOpResult::Txn { succeeded, op_results }`, appended last.
      `MvccStore::txn` recurses: a nested txn's compares are evaluated
      against the snapshot and index the txn started from (nothing is
      committed until the end, under the write lock), as etcd's.
    - raft precheck (#19, #49): the puts that would run, through nested
      branches, are what is checked.
    - server: conv recursion; response recursion; authz and the space
      check already recurse.
    - **Version gate**: an old member cannot decode the new variant (its
      AppendEntries fails and it stops). A txn that nests is proposed
      only once every member answers `ConfirmLeader` with version >=
      1.22.0 (cached per membership); an older member → FailedPrecondition
      naming it, unreachable → Unavailable. Flat txns are unchanged.
    Work items:
    - [x] storage types + recursion; unit tests (branches, revision /
      sub-revision order, nested compares see pre-txn state, Range sees
      earlier nested writes, refusal inside a nested txn refuses all).
    - [x] raft precheck recursion; server conv/response/gate.
    - [x] Tests: gRPC (etcd-client) nested txn responses; 3 members
      (applied everywhere); gate with an older member.
    - [x] Docs (00/03 mixed versions), changelog; release 1.22.0; close #56.
    - Verified: sc-build of cce91ae, whole workspace green (387 tests);
      with the gate forced open the older-member test fails. Filed #134
      (MemberAdd does not refuse a member older than 1.22 afterwards).

58. **#95 measured on an X9 blade (#101, P2) — done (docs; no release).**
    server3 (one 7200 rpm disk, release 11.91 = fastetcd v1.18.0), leased;
    `tests/bench/blade_pods.py`, 6 runs x 5 pods 3 s apart, metrics off
    :2381. Report p50 39.5 ms (target < 30 not met; v1.12 236–1,040),
    p90 660 ms; slow reports = WAL fsyncs of 300–550 ms (disk stalls,
    checkpoints of 1.1–1.6 s in most of those windows, not all); 1.0–1.4
    proposals per fsync, so no queueing. `docs/benchmarks/blade-server3.md`.
    Follow-up #135 (checkpoint vs WAL on a spindle; a disk mode for the
    test container to measure it there).

59. **LeaseKeepAlive renews in the leader's RAM, as etcd's lessor (#92, P2) — done, shipped in v1.23.0.**
    Every keep-alive was a `FastetcdLogEntry::LeaseKeepAlive` proposal:
    a WAL fsync and an apply each (etcd 126k/s, v1.12 12k/s on dev).
    etcd (release-3.5 `lessor.go`, `v3_server.go`): the leader renews in
    memory after confirming leadership (`ensureLeadership`), followers
    forward to it, TimeToLive is answered by the leader, and a new leader
    `Promote`s every lease to a full TTL from then. Plan:
    - `crates/raft/src/lessor.rs`: per-lease RAM deadlines on the leader,
      reset to now + TTL for every lease the first time this member leads
      a term; effective deadline = max(RAM, persisted). Renew = read
      barrier (leadership confirmed), lease exists and is not expired,
      RAM deadline = now + TTL. No log entry.
    - Follower keep-alives keep going through `ForwardWrite` with the
      `LeaseKeepAlive` entry; a leader with the lessor renews it in RAM
      instead of proposing (an older leader proposes, as before).
    - TimeToLive: on the leader from the lessor; a follower asks the
      leader (new peer RPC `LeaseTimeToLive`), falling back to its own
      state on an older leader.
    - Expiry sweeper: the lessor's effective deadline.
    - **Mixed versions**: a member older than this, made leader, reads
      only persisted deadlines, which RAM renewals never update, and
      would expire every long-lived lease at once. So RAM renewal is used
      only while every member answers `ConfirmLeader` with >= 1.23 (the
      #56 version gate, moved to the raft crate and shared); otherwise a
      keep-alive is proposed through Raft as before.
    - Behaviour change, as etcd without lease checkpointing: a restart
      or leader change gives every lease a full TTL again.
    Work items:
    - [x] raft: shared version gate; lessor; ForwardWrite intercept;
      LeaseTimeToLive RPC + forwarder; server wiring (lease.rs,
      lease_expiry.rs, main, harnesses); metrics.
    - [x] Tests: renewals add no log entries and keep a lease past its
      persisted deadline; expiry once they stop; follower keep-alive;
      leader change keeps a renewed lease, then expires it; an older
      member → renewals through Raft.
    - [x] fastetcd-bench `--mode keepalive`; v1.22.0 vs this on dev.
    - [x] Docs (00, 01, 03 leases, mixed versions, metrics), changelog;
      release 1.23.0; close #92.
    - Verified: sc-build of f519c65, 390 tests pass, 1 failure
      (`membership_changes_forward_from_a_follower`, under a concurrent
      bench; 5/5 alone; #136). `tests/keepalive_bench.sh v1.22.0` (100
      clients, 50k): 1 member 4 526 → 239 358/s, p99 105 → 0.96 ms; 3
      members via a follower 1 694 → 27 256/s, via the leader 2 488 →
      99 820/s; WAL fsyncs in the run ~2 000 → 10–105.

60. **3 members: a lone write pays two fsyncs in series (#97, P2) — owner
    approved option 1 (2026-10-09): vendored openraft patch, offered
    upstream; see item 76.** Confirmed in openraft 0.9.24 and 0.9.25:
    `run_command(AppendInputEntries)` awaits the leader's own `LogFlushed`
    (`append_to_log`) before the `Replicate` commands that follow it run.
    Not fixable in fastetcd's code: reporting the flush early lets a
    non-durable leader count toward the commit quorum (an acknowledged
    write can be lost); followers take AppendEntries only from openraft's
    replication streams. Asked on #97 (2026-10-07): (1) patch a vendored
    openraft 0.9 (leader-only: submit, report the flush via Notify, ignore
    on a vote change), (2) adopt 0.10 alpha, (3) leave it until 0.10 is
    stable (recommended: safety first; the gap is one fsync per lone write
    on a slow disk, hidden under load and on SSD).

61. **Auth errors use etcd's codes and texts (#105, and #127) — done, shipped in v1.23.1.** clientv3
    turns a status into its typed errors by message (`rpctypes.Error`),
    and re-authenticates only on `ErrInvalidAuthToken` / `ErrUserEmpty` /
    `ErrAuthOldRevision` (`retry_interceptor.go` `shouldRefreshToken`).
    fastetcd answered its own texts, so a client whose token a member
    does not know (restarted, or not applied yet) failed for good, and
    `ErrPermissionDenied` checks never matched. From etcd release-3.5
    (`rpctypes/error.go`, `v3rpc/util.go`, `auth/store.go`):
    - token present, names no one: `Unauthenticated` `etcdserver: invalid
      auth token`; no token (and no client-cert CN): `InvalidArgument`
      `etcdserver: user name is empty` (interceptor, admin RPCs, authz);
    - permission missing, user gone, non-root on an admin RPC:
      `PermissionDenied` `etcdserver: permission denied`;
    - Authenticate: auth off `FailedPrecondition` `authentication is not
      enabled`; bad name or password `InvalidArgument` `authentication
      failed, invalid user ID or password`; a password for a no-password
      user `Unknown` (etcd's unmapped `ErrNoPasswordUser` text); the
      password changing mid-Authenticate `ErrAuthOldRevision` (clientv3
      retries);
    - user / role not found or already existing, root user missing on
      AuthEnable: etcd's `FailedPrecondition` texts; empty user / role
      name, permission not given: etcd's `InvalidArgument` texts.
    Work items:
    - [x] `etcd_errors.rs`; interceptor, admin, authz, Auth service,
      apply errors; tests updated to the codes.
    - [x] Tests: `tests/clientv3_reauth.sh` (clientv3 v3.5.17 through a
      member restart; v1.23.0 fails, this passes); each mapped error.
    - [x] Docs (03 auth errors), changelog; release 1.23.1; close #105,
      #127.

62. **Verify #85 on an X9 blade (#89, P2) — done (docs; no release): `docs/benchmarks/acceptance-85.md`.** What can
    be done from here: server3 is reachable (apiserver anonymous, fastetcd
    metrics on :2381); `tests/bench/blade_getlatency.py` runs rustkube's
    get-latency / lease-churn load against a node and reads the WAL /
    checkpoint / back-pressure metrics around it. Power cut: the owner put
    it on the VMs (2026-10-05, stormcentral README stage 12); pve installs
    of 11.88 / 11.91 (fastetcd >= 1.12) passed with that stage. Not doable:
    v1.11.0 on a blade (a node runs its release's golden; test runs error,
    stormcentral#526). Running it found #138: fastetcd on server3 wrote
    nothing from 19:09:47Z (70+ min), reads still served; asked stormblock
    (stormblock#337). Measure once #138 is understood and the node writes.
    2026-10-08: SSD vs v1.11.0 on a build VM (3 rounds, 2.9–4.4x writes,
    p99 lower, linearizable p99 ~15x) and pvetest1 (after only) done; the
    power cut is the pve install stage (`qm stop`, 301 of 300 survived on
    11.95 and 11.88): `docs/benchmarks/acceptance-85.md`. Left: server3's
    run (3 rounds of blade_getlatency.py) once it is on after 11:00Z.
    2026-10-09, server3 (1.18): GET p99 8–10 ms under load (met);
    renewals 278–465/s, p50 32–37 ms, p99 1.5–4 s, max 13.5 s; checkpoints
    up to 28 s of 60, WAL fsync up to 27.8 s: the write tail is #135.

63. **Verify #82 on an X9 blade (#84, P2) — done (docs; no release).** server3 (fastetcd 1.18):
    apiserver GET of the cilium-operator lease p99 2.9–6.4 ms idle, 9.4–
    12.1 ms at ~4 500 GETs/s (read-only: #138 stalled its writes); value
    cache 99.8% hits over the day, 99.5% of hit reads < 1 ms in the store.
    RSS was not exported: added etcd's `process_*` metrics (1.24.0).
    Left: RSS flat over a long run and cilium-operator lease renewals on
    time, once a blade runs 1.24 and writes (#138; goldens: stormcentral#362).
    2026-10-09: release 11.99 (fastetcd 73eb6ec, 1.29) installed on server3;
    `tests/bench/blade_rss.py` samples RSS and the cache budgets every 60 s
    and every kube-system Lease's renewals each second over hours, with
    `blade_getlatency.py` rounds (writes on) for load and for GET p99 with
    concurrent writes.
    2026-10-13: 11.99's run never happened (server3's 11.99 install
    failed; X9's last-known-good is 11.88, fastetcd < 1.24, no process
    metrics; 12.14 failed on X9; the master is installing 12.21, fastetcd
    1.31, on server1). Now: pvetest2 (12.21-flowsdn, fastetcd 1.31, a
    real node with live leases), leased 93b5dafc91 until 02:13Z:
    `blade_rss.py` 4 h + a `blade_getlatency.py` round (writes on) every
    30 min, `tmp/rss84-pvetest2/`. Then the same on an X9 blade with 12.21
    after the blades' power-on (11:00Z) — the issue's acceptance.
    Done on pvetest2 (`docs/benchmarks/blade-server3.md` § RSS …): RSS
    70–382 MiB, inside budget, no restart; hourly floor 70/78/88/102 MiB
    (redb page cache filling? redb reports no cache size); leases renewed
    in time (longest 9 s of 15), no holder change. Left: a longer run on
    an X9 blade with fastetcd >= 1.24, to see the floor level off.
    2026-10-14 04:35Z: no X9 blade can run it yet (blades off until
    ~11:00Z; 12.21 passed on server1 but fails on the Dell, stormcos#561,
    so X9 stays on 11.88). Queued after stormcos#561; start at power-on if
    server1 is still on 12.21 (lease it; blade_rss.py 8+ h + a
    blade_getlatency.py round every 30 min).
    2026-10-28 23:35Z: still no X9 run. Every blade is off (power window
    until ~11:00Z). server1 failed every install since 12.37 (rustkube#185,
    now closed; 12.55-flowsdn at 18:48Z), but its disk holds 12.55
    (fastetcd >= 1.35, process metrics). Parked on a probe of
    192.168.16.101:2381. When it answers: lease server1, blade_rss.py 8 h +
    a blade_getlatency.py round every 30 min.
    2026-10-29 13:10Z: server1 answers (fastetcd >= 1.35) but the release
    is installing 12.58 on it until 17:00Z. Lease 6930084ad4 queued behind
    it (420 min); `tmp/rss84-server1.sh` waits for it, then runs
    blade_rss.py until 23:40Z (power-off 00:00Z) with a
    blade_getlatency.py round every 30 min, into `tmp/rss84-server1/`.
    Done 14:47–20:47Z (`blade-server3.md` § X9 blade, six hours): RSS
    floor levels off at 93–98 MiB (max 230); GET p99 14.6–18.5 ms under
    writes; leases late in store-wide write stalls (#180, filed). Asked
    the owner on #84 whether to close it with the lease criterion on #180.
    2026-11-03: owner approved; closed. Numbers re-checked against
    `docs/benchmarks/data/rss-server1/`. The lease criterion is #180's.

64. **Write-behind back-pressure does not wait behind the checkpoint's
    fsync (#93, P2) — done, shipped in v1.24.1.** `WriteBehind::sync` (the checkpoint) held the flush
    lock through `base.sync()`, redb's durable commit and its fsync of
    everything flushed, so an over-budget commit's flush (an apply, and a
    client) waited for the whole fsync. redb 2.6.3's non-durable commit
    writes its pages to the file (`non_durable_commit` → `write_barrier`
    → `flush_write_buffer`), so an fdatasync of the file through another
    fd persists them. Plan:
    - `KvStore::presync` (default no-op; redb: `sync_data` on its file,
      no lock). `WriteBehind::sync`: flush under the lock; presync with no
      lock; flush what came meanwhile and commit durably under the lock
      (little left to write). The pre-fsync #85 had before write-behind.
    - Back-pressure also on the layer count (every commit clones the list
      and every read walks it; a slow checkpoint let thousands pile up).
    - `fastetcd_write_behind_backpressure_seconds_total`.
    Work items:
    - [x] storage: presync, sync split, layer cap, wait time; unit test
      (over-budget commits during a slow pre-fsync do not wait for it).
    - [x] bench: `put` on a build VM (fast disk), v1.24.0 vs this.
    - [x] Docs (00/03 write-behind, metrics), changelog; release 1.24.1;
      close #93.
    - Verified: `back_pressure_does_not_wait_for_the_checkpoints_fsync`
      (2 s modelled fsync: slowest over-budget commit < 0.5 s; the old
      sync put back: 1.96 s). Build VM, put 256 conns x 40k, 3 rounds:
      5 833 → 6 925, 8 407 → 9 593, 3 445 → 4 336/s; the old back-pressure
      never fired there (fsyncs ~10 ms), the layer cap fired 28–35 times
      (0.35–0.46 s in all). Not reproducible without a slow disk: the
      issue's 1–1.7 s fsyncs (dev, retired) and its put-vs-txn-put gap.

65. **/health, /livez, /readyz check what etcd's check (#69, P2) — done, shipped in v1.25.0.** They
    always answered healthy. etcd release-3.5 `etcdhttp/health.go`:
    - `/health[?serializable=true][&exclude=NOSPACE|CORRUPT]`: an alarm →
      503 `{"health":"false","reason":"ALARM NOSPACE"}`; unless
      serializable, no leader → `RAFT NO LEADER`; then a keys-only Range
      (linearizable unless serializable) within 5 s + 2x election timeout
      → `RANGE ERROR:…`. OK: 200 `{"health":"true","reason":""}`.
    - `/livez`: `serializable_read`. `/readyz`: `data_corruption`,
      `serializable_read`, `linearizable_read`, `non_learner`. Each also
      at `/readyz/<check>`; `?exclude=<check>`; `?verbose` lists them;
      503 with `[-]<check> failed: …` lines, else `ok`.
    - Alarms are this member's (fastetcd's are not replicated). CORRUPT
      (restored from a backup, #37) makes /health and /readyz false until
      disarmed, as etcd's; probes can `?exclude=` it.
    - `grpc.health.v1` follows /readyz (checked every second).
    - `etcd_server_health_success_total` / `_failures_total`.
    Work items:
    - [x] `health.rs`, routes, gRPC status task, metrics; tests of each
      state (no leader, NOSPACE, CORRUPT, excludes, verbose, per-check).
      Learner and timeout: by code, not tested.
    - [x] Chart probes: liveness /livez, readiness /readyz (gRPC health
      now follows /readyz and would restart members on quorum loss).
    - [x] Docs (README, 01, 03), changelog; release 1.25.0; close #69.

66. **A real rolling upgrade, process restarts included (#65, P2) — done, shipped in v1.25.1.** Every
    mixed-version rule (#32 AuthSync, #50 ForwardRead, #75/#77 batching,
    #85 WAL migration, #41 lease trailer, #56/#92 version gates) was
    tested in process with a simulated older peer. Plan:
    `tests/rolling_upgrade.sh <from-ref> <to-ref|tree>`: build both,
    3 members on `from` with auth (root; alice scoped to /app/), a 600 s
    lease with a key, 4 writers putting unique keys through all endpoints;
    replace members one at a time (SIGTERM, start `to` on the same data
    dir); check every acknowledged write is there (linearizable), the
    longest write outage, auth + RBAC, the lease and its key, each
    upgraded member's pre-version backup, batching 0 until the last
    member is >= 1.10 and on after, no member behind at the end.
    Scenarios: A v1.9.0 → tree, B v1.24.1 → tree, C tree → v1.24.1.
    Work items:
    - [x] Script; A, B, C on sc-build; fix what they find.
    - [x] Docs (02-testing, 03 upgrades), changelog; release 1.25.1;
      close #65.
    - Found and fixed: (1) an auth change through a second member right
      after one through the first refused as divergence while a member
      had not applied the first (`AuthGate::check` now re-surveys up to
      3 s; `a_member_behind_by_an_entry_is_not_divergence`, a member that
      answers two surveys late; the control without it is refused);
      (2) the pre-version safety backup ran after the raft log's move
      into the WAL (1.12+), so it lacked the data file's log; it now runs
      first, and the script checks the order. Old versions' own #105 bug
      (a token not yet applied on another member: own text, no
      re-authentication) means the script sets up through one member.

67. **Docs say how the golden really ships (#81, P3) — done (docs; no release).** 03-deploy, the
    README and the CLAUDE.md pillar said the golden is built from
    `/root/fastetcd` on dev at whatever `main` is. Truth (stormcos
    `deploy/build-goldens.sh` and `docs/goldens.md` at 27add14, and every
    stage run this session): a special golden staged by stormcentral,
    `stormcentral component stage fastetcd`, on a build VM of its own,
    from fetched trees at main's tip at that moment (no `/root`; the
    stage takes no commit); stormd config: mutual TLS on 2379, metrics
    on 0.0.0.0:2381, backups to /data/backup (FASTETCD_BACKUP_DIR), TCP
    liveness; blank goldens fastetcd-data 1G and fastetcd-backup 3G (stormcos#648, retain 2);
    started first in 30-kube on sno/master. Also: dev.g8.lo is retired
    (2026-10-07): sc-build runs on build VMs (README, 02, 03).
    Work items:
    - [x] 03, README, 02, CLAUDE.md pillar; changelog; close #81.
    - Verified: claims against stormcos 27add14's script and goldens.md;
      the stage's compile command on a build VM (all three static-pie,
      tree clean); `component build fastetcd` refused; stages this
      session recorded `fastetcd@<main's tip>`.

68. **Auth tokens expire: `--auth-token-ttl` (#46, P3) — done, shipped in v1.26.0.** Simple tokens
    lived in memory until the user was deleted or changed password, or the
    process restarted. etcd (`simpleTokenKeeper`, release-3.5): a token
    expires `--auth-token-ttl` (300 s) after its last use, each member on
    its own (the token is replicated, its timer is not); no raft entry.
    Plan: `AuthMemory` keeps each token's last use; a lookup past the TTL
    drops it and answers no one (so `etcdserver: invalid auth token`,
    #105, and clients re-authenticate); a use renews it; expired tokens are
    purged at most once a second. `--auth-token-ttl` (secs, 300,
    `ETCD_AUTH_TOKEN_TTL`); 0 rejected, as etcd.
    Work items:
    - [x] storage: last use, TTL, purge; unit test.
    - [x] server flag; gRPC test (a token used within the TTL keeps
      working, an idle one is refused with etcd's error, a new login works).
    - [x] Docs (01, 03 auth), changelog; release 1.26.0; close #46.

69. **A stall names the stuck operation (#138, P1) — done, shipped in v1.27.0; cause stormblock#334.** server3 wrote
    nothing for 70+ min; which operation hung could not be told. From the
    metrics captured during it: write-behind layers 0 and
    `checkpoint_durable_applied_index` 38495 vs applied 38500, so the
    checkpoint had flushed its layers into redb and was in its durable
    commit, an fsync of the data file that never returned; the WAL's
    fdatasync on the same filesystem stopped completing as well. Nothing
    was over the write-behind budget, so no apply waited on fastetcd's
    flush lock: the likely cause is below fastetcd (asked: stormblock#337).
    Plan (fastetcd's part):
    - `WalStats`: start time of the WAL fdatasync in flight and of the
      checkpoint in flight (0 = none); gauges
      `fastetcd_wal_fsync_inflight_seconds`,
      `fastetcd_checkpoint_inflight_seconds`.
    - A watchdog (with the checkpointer): while either has run >= 10 s,
      WARN every 10 s naming it, how long, and the index/target, so the
      next stall says which.
    Work items:
    - [x] stats, watchdog, metrics; test (a WAL fdatasync held by
      `set_sync_delay` and a slow checkpoint each show in flight and are
      named; `a_stuck_fsync_or_checkpoint_says_which_it_is`).
    - [x] Docs (03 disk stalls, metrics), changelog; release 1.27.0.
    - [x] The cause (stormblock#337, answered 2026-10-08): stormblock#334.
      server3 ran stormblock 11.91, whose stall watchdog dumped each ublk
      device's runtime under mint/clone load and lost a wake there, so
      that one device's requests (fastetcd's FLUSH) never completed while
      other volumes and health stayed fine. Fixed since
      golden-stormblock-65b6578787be; its health now lists `ublk_stuck`
      (golden-stormblock-5739be2a34ac). server3 was powered off and could
      not be read again. Docs (03 disk stalls) point at `ublk_stuck`.

70. **fastetcd-bench counts failed RPCs instead of panicking (#104, P3) — done, shipped in v1.27.1.**
    Every RPC was `unwrap()`ed, so a leader change in a 3-member run
    (`Unavailable` on a forwarded write) panicked the bench with no
    numbers (#88), and #93's 1000-connection runs died on "transport
    error". Plan: errors counted by gRPC code; transient ones
    (Unavailable, DeadlineExceeded, Unknown "transport error") retried
    with a growing backoff, up to 8 tries; connects too; an op that never
    succeeds is counted as failed; the summary prints both. Durability
    mode keeps stopping at the first failure (by design).
    `CHURN=1 tests/read_latency.sh` restarts the leader mid-run.
    Work items:
    - [x] bench retry + counts (put/get, read-under-load, keepalive);
      unit test; CHURN in read_latency.sh; run it on 3 members.
    - [x] Docs (01 bench, 02), changelog; release 1.27.1; close #104.

71. **A warning-free build (#78, P3) — done, shipped in v1.27.2.** The
    unused `RedbEngine` import in server main.rs (every build since 1.8)
    and a needless `mut` in fastetcd-bench (#104) removed; `cargo build
    --all-targets` gives 0 warnings and passes with `RUSTFLAGS="-D
    warnings"` (sc-build, build VM). The check is documented for releases
    (02-testing), not made the default: a lint a newer compiler adds must
    not stop the golden build.

72. **`--log-level` and `--max-request-bytes` do what etcd's do (#54, P3).**
    Both were accepted and ignored. etcd release-3.5: `--log-level`
    debug|info|warn|error|panic|fatal; `--max-request-bytes` refuses a
    proposal larger than it (`v3_server.go`: `InvalidArgument`
    `etcdserver: request is too large`) and caps gRPC receives at it +
    512 KiB (`grpcOverheadBytes`). Plan:
    - logging set up after parsing: `RUST_LOG` wins; else `--log-level`
      (panic/fatal → error); an unknown level refused.
    - `--max-request-bytes` set: client-port gRPC decode limit = value +
      512 KiB; `ServerState::propose` refuses an encoded entry larger
      than the value with etcd's error (gateway included).
    - Unset: today's limits (tonic's 4 MiB decode, no proposal check).
      Whether the default becomes etcd's 1.5 MiB (it could refuse writes
      that work today) is asked on #54.
    Work items:
    - [x] flags, logging, decode limit, proposal check; tests
      (`compat_flags.rs` on the binary; parse tests).
    - [x] Docs (01), changelog; release 1.28.0; ask the default.
    - Owner's decision (2026-10-09): keep the 4 MiB default, not set in
      the stormcos golden; the difference is documented in `--help`, 01
      and the README (2026-10-14). Closed.

73. **Watch fragments large responses when asked (#58, P3) — done, shipped in v1.29.0.** Every
    `WatchResponse` went out whole with `fragment: false`; a create's
    `fragment` was not read. etcd release-3.5 (`v3rpc/watch.go`
    `sendFragments`): for a watcher created with `fragment`, a response
    of `Size() >= maxRequestBytes` with 2+ events is split into
    responses of whole events, each under the limit (a single event over
    it goes alone), all but the last `fragment: true`; clientv3 joins
    them. Plan:
    - `Watcher.fragment`; one `send_events` used by live delivery and
      history replay/resync; limit = `--max-request-bytes` if set, else
      etcd's default 1.5 MiB (only splits; refuses nothing, so not the
      #54 default question).
    - Unfragmented watchers unchanged.
    Work items:
    - [x] watch.rs; unit test of the split (sizes, flags, order, one big
      event); gRPC test (fragments arrive 3/3/2, live and in replay; a
      watcher without fragment gets 8 whole); `tests/clientv3_fragment.sh`.
    - [x] Docs (00, 01, 02, README, deck), changelog; release 1.29.0;
      close #58.
    - Verified: sc-build of 6b63989, workspace 417 pass 0 fail.
      clientv3 v3.5.17 with a 2 MiB receive limit: with `WithFragment`
      the 3.2 MiB response arrives as one joined response of 8 events;
      without, `received message larger than max (3277020 vs. 2097152)`.

74. **Health routes on the metrics listener too (#133, P3).** stormcos
    wants HTTP `/livez` / `/readyz` probes (stormcos#192), but its client
    port 2379 is mutual TLS and stormd's probes hold no client
    certificate. etcd serves `/health` on `--listen-metrics-urls` for
    that. Plan: the metrics listener becomes an axum router: `/metrics`,
    `/`, and `health::router` merged (same handlers, same answers).
    Work items:
    - [x] metrics.rs `router`; tests in `health_checks.rs` (healthy and
      leaderless member over the metrics listener).
    - [x] Docs (01, 03, README, deck), changelog.
    - [x] sc-build green; release 1.30.0; close #133.
    - Verified: sc-build of 4733765: `tests/metrics_health.sh`
      15/15 (a certless probe is refused on the mTLS client port and
      answered on 2381, same bodies), workspace 419 pass 0 fail.

75. **Write throughput to etcd's level (#142, P1).** Owner (2026-10-12):
    the gap is a bug. Issue numbers are v1.12 (1000 clients: etcd 15–20k
    puts/s, fastetcd ~5k). Plan:
    - Measure main first (`FASTETCD_TAGS=tree WORKLOADS=... PERF=1
      tests/bench/compare.sh v3.7.2` on a build VM): puts/s, proposals
      per fsync, checkpoints, write-behind back-pressure, a perf profile.
    - Suspect (from reading the code): a batch of N proposals is applied
      as N `MvccStore::apply` calls, each with its own lock, engine
      snapshot and write-behind commit (a layer each; cap 1024), so a
      1000-put batch costs 1000 commits. Fix candidate: apply the simple
      sub-entries of a batch (one Put / DeleteRange each) in one context
      and one engine commit, each at its own revision, events per
      revision as now; batch progress then commits once with them.
    Found (build VM, 8 CPUs, 1000 clients, v1.30.0): etcd 22–34k puts/s,
    fastetcd 1.5–6.3k; txn-put etcd 19–35k, fastetcd 3–4k. perf: half the
    CPU in memmove + bincode. etcd's `benchmark put` writes ONE key
    (`--key-space-size` defaults to 1), and fastetcd rewrote the key's
    whole `KeyIndex` (every revision since the last compaction) into
    `mvcc_idx` on every put, and cloned it from the resident index:
    O(history) per write, O(n^2) for a hot key. Plus one write-behind
    commit (and layer) per sub-entry of a batch, so a batch of 1000 hit
    the 1024-layer back-pressure flush inline.
    Done:
    - `MvccStore::apply_many` (one snapshot, one context, one commit,
      events per revision); the state machine sends runs of a batch's
      sub-entries that cannot refuse after writing (`ApplyItem::mutations`
      / `::txn`: no ignore_* puts in multi-op, no Range at a revision, no
      nested txn) through it, batch progress staged for the run's end.
    - A key's index is serialized once per commit (`commit_ctx`), taken
      out of the context instead of cloned; compares read only the
      key's current revision (`compare_targets`), through the context.
    - put-1000c after the first two: 44.6k / 34.3k vs etcd 34.0k / 35.0k.
    Work items:
    - [x] Baseline measurement on a build VM (`compare.sh`: `tree`,
      `WORKLOADS`, `PERF=1` with frame pointers).
    - [x] apply_many + index once per commit; equivalence test
      (`apply_many_is_the_same_as_applying_one_by_one`).
    - [x] txns in the shared commit; compare targets.
    - [x] Bench again (put, txn-put, 1 and 3 members) vs etcd.
    - [x] Runs capped at 256 per commit, yielding between: an uncapped
      run of 4096 failed `multinode_fastpath` 1 in 4 (a leader change
      while a learner applied ~24 MiB of batches; the old code 0 in 4);
      capped, 5 of 5 pass.
    - [x] Full workspace (5416050: 421 pass, 0 fail); docs (benchmarks
      doc, 00), changelog; release 1.31.0; close #142.
    - Open: 3-member put p99 579 ms vs etcd's 183 ms; slow disk not
      measured (no bench VM; stormblock#409).

76. **The leader replicates without waiting for its own fsync (#97, P2).**
    Owner (2026-10-09, master's recommendation): vendor openraft 0.9.24
    (`vendor/openraft`, sha256 of the crate = Cargo.lock's checksum,
    `[patch.crates-io]`, excluded from the workspace), patch it, guard it
    with openraft's own tests plus fastetcd's 3-member and crash tests,
    offer the patch upstream, drop the copy once a release has it. Plan:
    - openraft `RaftCore::run_command(AppendInputEntries)`: when the
      command carries this node's own leader vote, submit the append
      (`log_store.append`, which the WAL returns from at once) and go on,
      so the `Replicate` commands after it run now. The flush comes back
      in order through one forwarder task as `Notify::LocalLogFlushed`;
      it updates the leader's own progress only if the leader's vote is
      unchanged; an error stops RaftCore as before. At most 16 leader
      appends unflushed (a semaphore): a stuck disk blocks RaftCore
      again, as before, instead of queueing without bound. Followers
      unchanged (they answer AppendEntries after their fsync).
    - Commit still needs a durable quorum (the leader counts only once
      flushed), so a write can commit on two followers before the
      leader's fsync returns. fastetcd keeps applied <= locally durable:
      the state machine waits for the log store's durable index before
      applying an entry (`LogProgress::last_durable`, a notify), so redb
      never gets ahead of the WAL. On the leader that wait overlaps the
      followers' fsync: about one fsync per lone write, not two.
    - Changed while doing it: with the leader no longer blocked on its
      fsync, a stalled leader disk kept the leadership (heartbeats went
      on) while writes hung. So a leader flush outstanding for a
      heartbeat interval holds replication and heartbeats until it
      completes, as every flush did before: #103's two stall tests pass
      unchanged (1 s timeout elects; 10 s keeps the leader).
    Work items:
    - [x] Vendor pristine 0.9.24 (f7473ba); patch; lock update.
    - [x] SM waits for local durability; wiring (main, harnesses).
    - [x] Tests: openraft's own suite in the vendor dir (184 pass,
      patched and not); workspace 422 pass; `a_lone_write_on_three_
      members_costs_about_one_fsync`; test container short/medium/long
      (leader SIGKILL, SIGKILL keeps acknowledged writes); rolling
      upgrade v1.31.0 -> tree and back.
    - [x] Measured (build VM, side by side, 3 rounds, every fsync 200 ms):
      lone write median 215-246 ms vs 433-438 ms unpatched.
    - [x] Docs (00, 03, deck, vendor/openraft/FASTETCD.md), changelog;
      release 1.32.0 (golden-fastetcd-d5852f07b743); upstream PR
      databendlabs/openraft#2163 (upstream's tests crate passes on it as
      on its base); close #97. When it ships in an openraft release,
      drop `vendor/openraft` (FASTETCD.md § Dropping it).

77. **A Range does not wait on the write path (#145, P2).** One /readyz
    took 57 ms (p50 0) on pvetest1 during an install (stormd's budget:
    p99 50 ms). Cause: every `MvccStore::range_with_revision` (readyz's
    serializable and linearizable reads, every client GET/LIST, the
    leader's Txn compare precheck) takes the `write_state` mutex, which
    an apply holds for its whole run (up to 256 writes, the engine
    commit, a write-behind back-pressure flush into redb) and compaction
    for its walk of every key. The read barrier is not it (a sole voter's
    read index is its term's first index). Plan:
    - Readers stop taking `write_state`. A commit, after its engine
      commit, publishes under the index's write guard: the index, the
      current revision, and a published-commit count; an engine-commit
      count is bumped just before. A reader reads the engine count, takes
      an engine snapshot, then the index read guard, and uses them only
      if published <= engine count read first (every published commit is
      in its snapshot); else retries, and after a few tries takes the
      lock as today. A snapshot newer than the index is fine: commits
      only add records, except compaction, which publishes its compact
      revision before deleting anything.
    - Wholesale replaces (snapshot install, reload, bulk load) hold a
      `replace` lock readers share while building their view.
    - `current_revision()` / `compact_revision()` read the published
      values; the Txn precheck and watch history use the same view.
    Work items:
    - [x] store: read view, publish, compaction order, replace lock.
    - [x] Tests: a read during a held commit / compaction answers at
      once; consistency under writes + compaction (the #82/#50 tests
      still pass); with the lock path forced, both held tests fail.
    - [x] Found by `tests/readyz_latency.sh` (1000 put clients, compaction
      every 2 s): the read still waited on the engine snapshot, behind the
      write-behind flush, which held the layers' write lock through redb's
      commit. The flush now commits first and drops its layers after
      (`a_snapshot_does_not_wait_for_a_flush`; the old order fails it).
    - [x] Docs (00, 02, 03), changelog; release 1.32.1; close #145, saying
      the budget need not move.
    - Verified: workspace 426 pass, 0 fail; vs v1.32.0 (7 rounds, p99
      42–262 ms, 2–9 answers over 50 ms a round), this (11 rounds) p99
      3.6–17 ms, 1 answer over 50 ms in all. Also fixed: the #138 watchdog test raced the
      leader's first fsync since #97; clippy in storage (#149; rest #152).

78. **MoveLeader and Downgrade answer Unimplemented (#57, P0).** MoveLeader: openraft 0.9 refuses votes
    while a voter's leader lease is live (`handle_vote_req`), so the
    target cannot simply elect. Options asked: (1) a second vendored
    openraft patch backporting 0.10's `transfer_leader` (lease expiry +
    target elects, after the target catches up; version-gated peer RPC),
    (2) pause the leader's sends in fastetcd's network until leases expire,
    (3) leave it until 0.10. Downgrade: fastetcd has no cluster version
    (per-feature version gates instead): (a) leave Unimplemented, (b)
    design one. Recommended: MoveLeader (1), Downgrade (a).
    Decided (master, for the owner, 2026-10-14): MoveLeader (1), Downgrade
    (a). Downgrade done: its answer and 03-deploy § Downgrades. MoveLeader
    design: openraft `ExternalCommand::AcceptLeaderTransfer {leader vote,
    to, last_log_id}` → `Engine::lease_waived_for` (handle_vote_req skips
    the lease check for that vote only; election timers unchanged) and the
    target elects if its log holds last_log_id; fastetcd: on the leader,
    pause the proposer, wait for the target's matched == last log, peer RPC
    `TransferLeader` to every voter (gated: all >= the new version), wait
    for current_leader == target (etcd's errors: not leader, bad leader
    transferee, leader transfer took too long). Not started: the session's
    auto-mode classifier refused the openraft edit as "security weaken"
    (the lease check); needs the owner's go-ahead to edit it.
    2026-10-14: go-ahead given on #57 (master, for the owner); the edit
    (engine `lease_waived` + `waive_lease`, the lease check in
    `handle_vote_req` skipped only for that vote until a deadline,
    `ExternalCommand::AcceptLeaderTransfer`, `Trigger::
    accept_leader_transfer`) was refused again by the auto-mode
    classifier. Needs the session's permission (a Bash/Edit allow rule
    for `vendor/openraft`, or a non-auto mode), not another decision.
    2026-10-14 (third try): refused again; tree reverted. Asked on #57
    (allow rule for `vendor/openraft/**` recommended) and `wait-owner`.
    2026-10-28 (fourth try): owner approved the edit (2026-10-14), the
    classifier still refused it. Waiting on a person's step (`wait-owner
    --action`): `Edit(vendor/openraft/**)` / `Write(vendor/openraft/**)` in
    `.claude/settings.local.json`. Patch shape on #57.
    2026-10-28: the owner added the allow rules; done as designed, one
    change: no all-members version gate. The leader asks every other voter
    `TransferLeader` and needs a quorum with the target to accept (a member
    older than 1.36 answers Unimplemented and keeps its lease), so a
    transfer works with a member down or older, as etcd's does. Work items:
    - [x] openraft `accept_leader_transfer` + engine tests (0d997a1).
    - [x] `leader_transfer.rs`, `Proposer::hold`, `TransferLeader` RPC,
      Maintenance handler, etcd's errors; 3-member tests (62f3c8d).
    - [x] sc-build: openraft suite 190 pass; workspace 464 pass 0 fail
      (c7a7243); move_leader tests ~155 ms per transfer; control with the
      waiver disabled (branch, deleted): `leader transfer took too long`.
    - [x] Docs (00, 03, README, deck, FASTETCD.md), changelog; release
      1.36.0; close #57.

79. **The wal and iouring engines cannot be selected (#55, P3) — waiting on
    the owner (asked 2026-10-14).** Only `benches/mvcc.rs` uses them; the
    server depends on redb's presync, corruption handling and defragment,
    and its raft log is its own WAL (#85). Asked: (1) drop them and
    `tokio-uring` (recommended), (2) keep as library-only, (3) make them
    selectable (`--storage-engine`, each engine brought up to the server's
    needs).
    Decided (master, for the owner, 2026-10-14): (1). Work: delete
    `wal_engine.rs`, `iouring_engine.rs`, features `wal-engine`/`iouring`,
    `tokio-uring`, their bench cases and docs mentions; lock updated;
    release 1.32.2 (PATCH). Done: workspace 426 pass, `--locked` build,
    clippy storage clean, bench builds.

80. **A lone write after idle (#102, P3) — done (test + docs; no release).**
    `fastetcd-bench --mode idle-put`, `tests/idle_write.sh`: one member,
    disk and tmpfs, WAL fsyncs and mean fsync per run. Every write is one
    fdatasync, idle or not; the after-idle penalty is the device's fsync
    (v1.12.0 lone 16.1 vs 6.2 ms, fsync 15.1 vs 5.7 ms); fastetcd's cold
    path ~0.8 ms on tmpfs, no timer. `docs/benchmarks/idle-write.md`.

81. **redb two-phase commit on a real disk (#67, P3) — done (docs; no
    release).** `two_phase_commit_cost` on a build VM disk, 3 rounds: p50
    10–10.7 → 16.4–20.3 ms per durable commit (~2x); tmpfs +3–5%. Since
    #85 it would cost ~one fsync per checkpoint, not per write. 05-backup.

82. **Range ignores sort and min/max revision filters (#154, P1).** Found
    working #64: `sort_order`, `sort_target`, `min/max_mod_revision`,
    `min/max_create_revision` are never read, so clientv3
    `concurrency.Mutex` (owner = first key by create revision) is not
    mutually exclusive on fastetcd (Cilium kvstore locks). Plan: etcd's
    `executeRange` post-processing in the server's Range and Txn-range
    responses (limit 0 down when sorting/filtering; count before filters;
    NONE + non-KEY target = ASCEND; KEY + ASCEND = no re-sort), no storage
    or wire change; tests with clientv3 (`Mutex` with lease IDs sorting
    against creation order) and `etcdctl get --sort-by`.
    Done: `range_options.rs` as planned, in serve_range (local and
    forwarded), conv (Txn Range read whole) and the Txn response.
    Verified: `tests/clientv3_concurrency.sh v1.33.0`: on 1.33.0 a second
    session's TryLock succeeded while the first held the lock; on the
    tree every Get option, the Mutex and the Election check out. Release
    1.33.1.

83. **etcd's Lock and Election services (#64, P3) — done (1.34.0).**
    `crates/server/src/concurrency.rs`, protos under
    `crates/proto/protos/etcd/server/`; gateway routes; tests
    `concurrency_grpc.rs`. Found: waitDeletes with a bound < 1 (a key at
    revision 1, fastetcd's first) waited on itself; fixed. Design
    (etcd v3.6.11 `v3lock/lock.go`, `v3election/election.go`, clientv3
    `concurrency` read): vendor `v3lock.proto` / `v3election.proto`
    (strip_annotations.py; vendor.sh); services behind the auth
    interceptor that run etcd's concurrency code server-side through
    `KvService` / `LeaseService` with the caller's metadata+extensions
    (so authorization is the caller's, as etcd's in-process client):
    - session: lease 0 → LeaseGrant TTL 60; given lease used as is; no
      keep-alive (etcd orphans the session at once).
    - Lock: key `name/<lease %x>`; Txn If create_rev(key)=0 Then
      put(key,"",lease)+get(owner) Else get(key)+get(owner), owner =
      prefix sorted CREATE ASCEND limit 1; myRev = header rev or key's
      create rev; if not owner: waitDeletes(pfx, myRev-1) = loop Range
      prefix CREATE DESCEND limit 1 max_create_revision, wait for that
      key's DELETE above the range's revision (MVCC event subscription
      taken before the Range; Lagged → re-Range); then Get(key): gone →
      "mutex: session is expired"; on wait error delete key. Unlock =
      Delete(key).
    - Election: Campaign (put if absent, else Proclaim if value differs,
      waitDeletes, Resign on cancel); Proclaim / Resign (Txn If
      create_rev(key)=rev; fail → "election: not leader"); Leader (first
      create, none → "election: no leader"); Observe (etcd's loop:
      leader, its PUTs, on DELETE the next); `"leader" field must be
      provided`.
    - Gateway routes `/v3/lock/lock|unlock`, `/v3/election/campaign|
      proclaim|leader|observe|resign`; pbjson for the two packages.
    - Tests: gRPC (two lockers exclude each other, lease expiry frees,
      unlock), election (campaign order, proclaim, leader, observe
      stream, resign), auth (denied without write on the prefix),
      `etcdctl lock`/`elect` against the services? (they use client-side
      concurrency; a Go client of v3lockpb in tests/).

84. **A replicated cluster_id (#38, P3) — waiting on the owner (asked
    2026-10-14).** Mechanism (technical): a log entry recording the id in
    `mvcc_meta` (travels in snapshots), version-gated like #56/#92.
    Asked: (1) new cluster default: random (rec.) / etcd's member hash /
    `1`; (2) mismatched member: adopt + warn + metric / refuse / refuse
    only on an explicit contradicting `--cluster-id` (rec.); (3) existing
    cluster: the leader's id (rec.) / the majority's.
    Decided (master, for the owner): 1(a), 2(c), 3(a). Done:
    `FastetcdLogEntry::ClusterId(u64)` (first applied wins,
    `MvccStore::apply_cluster_id`, `mvcc_meta` `cluster_id`, watch,
    reloaded on install); `cluster_id::reconcile` at startup, `follow`
    (leader proposes behind `MembersAtLeast((1,33,0))`; members adopt or,
    with an explicit contradicting flag, exit 78); random default; served
    id an `AtomicU64`; metrics. Release 1.33.0 (minor).

85. **Snapshot build and install stream their records (#62, P3).** Both
    ends held a decoded copy of the database (`range(.., 0)` into `Vec`s
    to build; `deserialize_from` the whole payload, then one `WriteBatch`
    of every record to install). Plan, **no format change**: the body is
    bincode 1 (struct = fields back to back; `Vec` = u64 length + items),
    so the same bytes can be written and read a record at a time; old and
    new members keep exchanging identical bodies (no gate, no upgrade).
    - Build: header fields, then per table a placeholder length, records
      from the one engine snapshot in chunks (`range(start, .., CHUNK)`),
      then the length patched (the file, or the memory fallback, is
      seekable); trailers (auth, leases: small) as now. ENOSPC retry
      re-reads from the same engine snapshot.
    - Install: decode header, then each table's records one by one into a
      bounded channel feeding **one** engine write transaction
      (`KvStore::commit_stream`, redb: begin_write, insert as they come,
      one durable commit; write-behind: flush, then the base's), with the
      delete_ranges first and the applied log id, membership, auth and
      lease replacement ops last: still atomic, still one fsync.
    - Tests: round trip byte-identical to the old encoder (both ways);
      install atomic on a decode error mid-stream (nothing changed);
      build never asks the engine for an unbounded range;
      `tests/snapshot_memory.sh`: a learner caught up by a ~300 MB
      snapshot, VmHWM of leader and learner, v1.32.2 vs tree.
    - Done: as planned (BODY_CHUNK 128 records; the stream ends with an
      Err on a decode error or panic; redb DeleteRange via retain_in).
      Verified: workspace 430 pass; `tests/snapshot_memory.sh v1.32.2`
      (~480 MiB data): learner install peak 666–679 → 83–91 MiB; leader
      build above base +426–622 → +39–271 MiB (redb page cache). Found: a
      restarted idle member's RSS ~480 MiB on both versions, ~100 MiB
      over the documented budgets (filed separately).

86. **WebSocket upgrades on the v3 gateway (#66, P3) — waiting on the
    owner (asked 2026-10-14).** Mechanism read in etcd v3.6.11 (wsproxy
    e5319fda7802): any `/v3/` upgrade → POST, body = each client message
    + `\n`, each response line → one text message, Origin/Referer
    forwarded, token from `Sec-WebSocket-Protocol: Bearer, <t>` or a
    `token` cookie; axum `ws` feature (tokio-tungstenite). Asked: origins
    (1) all, as etcd (browsers do not preflight a WebSocket: a page could
    read watches cross-origin), (2) same-origin by default +
    `--gateway-ws-allowed-origins`, no cookie token (recommended), (3) not
    yet (no client needs it).
    Decided (2). Built (6497472, 3e7fba7, 8cf93da) and reverted (8eca892):
    tonic 0.12.3's server serves connections without upgrades
    (`serve_connection`, mod.rs:694), so a 101 is followed by a reset.
    Asked again (2026-10-14): (1) serve the client port ourselves with
    `serve_connection_with_upgrades`, (2) upgrade tonic (+ axum 0.8,
    prost), (3) defer (recommended).
    Decided (3), defer: documented (03, README); closed. A browser client
    needing live watches gets its own issue (serving with upgrades or a
    tonic upgrade), the reverted code restored on top.

87. **Spawned-binary tests lose their ports (#63, P3) — done (tests).**
    `crates/server/tests/spawned`: random 127.x.y.z address + new ports
    on "in use" + the child's log in failures; health_http, wal_upgrade,
    apply_replay; metrics.rs's in-process server on a random loopback
    address.

89. **SIGTERM stops cleanly (#161) — done, shipped in v1.34.1.** Only SIGINT was handled, so a
    kubelet stop (SIGTERM, then SIGKILL) killed the process with the
    write-behind layers in RAM and redb's file marked dirty: every start
    re-walked the data file (redb's full repair: seconds on an HDD) and
    replayed the WAL tail. Read in redb 2.6.3: `Drop for Database` writes
    the allocator state and clears `recovery_required`, so a clean stop is:
    applies stopped, layers flushed, last `Database` handle dropped.
    Plan:
    - main: SIGTERM and SIGINT both start the shutdown: `raft.shutdown()`
      (RaftCore stops; no new applies), `sync()` (layers + durable commit),
      again if an apply landed meanwhile, then return (runtime drop drops
      every handle; redb's drop writes the clean header).
    - redb engine: a repair callback logs `data file was not closed
      cleanly` at open and `RedbEngine::repaired_at_open()` says so.
    - Test (`clean_shutdown.rs`, the real binary): SIGKILL → repaired at
      open (control); SIGTERM → exit 0, not repaired, every acknowledged
      revision in the data file (nothing to replay).
    Work items:
    - [x] main signal + shutdown order; engine repair report; test.
    - [x] Docs (03 deploy), changelog; release 1.34.1; close #161.
    - Verified: sc-build of cfec8b6, workspace 448 pass 0 fail;
      `clean_shutdown.rs` passes (3.2 s), its SIGKILL control shows the
      repair line. Clippy on the server crate fails on older lints (#152).

90. **Lease deadlines on monotonic time (#162) — done, shipped in v1.34.2.** The leader's lessor kept
    RAM deadlines in unix seconds and took the later of them and the
    persisted `deadline_unix_secs`, both against `SystemTime::now()`. An X9
    blade has no RTC battery and boots at 1970 until NTP: a forward step
    expires every lease at once (RAM deadlines all behind), a backward
    step (persisted deadlines written in 2026, clock at 1970) keeps dead
    leases alive for decades. etcd's lessor uses monotonic time only, and
    a new leader gives every lease a full TTL (promote). Plan:
    - Lessor RAM deadlines are `Instant`s: promote = now + TTL, renewal =
      now + TTL; a lease the leader has no deadline for yet gets now + TTL
      at first sight.
    - MvccStore stamps, in RAM, the `Instant` it applied each lease's
      grant or Raft-logged keep-alive (`lease_applied_at`); the leader's
      effective deadline = max(RAM deadline, applied + TTL). This covers
      what the persisted deadline covered (grants after promote, keep-
      alives proposed through Raft while the 1.23 gate is closed).
    - The persisted deadline is no longer compared with the clock on the
      leader (kept in the format: older members and the TimeToLive
      fallback with an older leader still read it).
    - Expiry compares `Instant`s.
    - Test: leases granted with a proposer clock of 1970 (forward step)
      and of year 3000 (backward step) expire exactly by their TTL.
    Work items:
    - [x] store stamp; lessor; tests.
    - [x] Docs (00, 03 leases), changelog; release 1.34.2; close #162.
    - Verified: sc-build of 3319d85, workspace 449 pass 0 fail;
      `lease_clock_step.rs` passes; with the old lessor (branch, deleted)
      it fails: a lease granted with a 1970 clock answers TimeToLive
      -1792631396 (expired at once).

91. **The upgrade safety backup fits or is skipped (#163) — done, shipped in v1.34.3.** On a version
    change, startup copies the whole data file to `<data-dir>/backups`
    (default on, retain 2), on the 1 GiB data volume: a file over ~330 MB
    fills it (ENOSPC, or a full volume the store must then run on). Chosen
    (no decision needed): cap by free space, keep the default on (safety
    first, #15's ordering; `--upgrade-backup=false` already turns it off).
    Plan:
    - Before pruning or copying: statvfs the backup directory's
      filesystem; used − (what the roll-off would free) + the file's size
      must stay at or below `--space-high-water-percent` (80) of it, or
      the backup is skipped with a WARN naming the numbers and the
      remedies (`--upgrade-backup-dir` on another volume; offline
      `fastetcd backup`); nothing is pruned then.
    - A copy that fails part-way is removed.
    - Tests: the fit rule; skipped when it would cross the mark (old
      backups kept, no file); written when it fits. (The removal of a
      part-written copy needs a filesystem that fills: not tested.)
    - Not here: periodic `--backup-dir` backups on their volume (#153).
    Work items:
    - [x] admin.rs + main wiring; tests.
    - [x] Docs (01, 04, --help), changelog; release 1.34.3; close #163.
    - Verified: sc-build of 540a32d, workspace 0 failures (both new tests
      pass), no clippy finding in admin.rs.

92. **An empty WAL opens without writing 16 MiB first (#164) — done, shipped in v1.34.4.** `Wal::open`
    on an empty `wal/` joined a zero-fill of the whole first segment plus
    its fsync before returning (`install_spare(spawn_spare(..))`): seconds
    on an SMR disk or ublk over an HDD, at every first boot. Plan:
    - Segment 0 is `create_segment` (fallocate + fsync, no data written);
      the spare is zero-filled in the background as for every later
      segment.
    - A preallocated segment's fdatasyncs are journal commits (#85: ~10 ms,
      up to 3 s on ext4), so the WAL rolls early: at the first append once
      the spare is ready, while the current segment is one that was only
      preallocated, it rolls to the zero-filled spare. A short segment
      replays as any other (zeros read as its end).
    - Tests: after opening an empty dir, segment 0 holds no written data
      (`lseek(SEEK_DATA)`, calibrated); the early roll happens once the
      spare is ready and everything replays. (A timing test was tried and
      dropped: on a shared build VM open and fill times were too noisy.)
    Work items:
    - [x] raft_wal.rs; tests.
    - [x] Docs (03), changelog; release 1.34.4; close #164.
    - Verified: workspace at 536b84c 0 failures; raft_wal 5/5 at f2bd72b;
      with the old open put back (branch, deleted) the content check fails
      ("opening an empty log wrote segment 0") and the roll test fails.

93. **Startup says where its time goes (#165) — done, shipped in v1.35.0.** No line timed startup, so
    the boot logs could not explain 4–5 s probe gaps (stormcos 12.39,
    12.41-flowsdn). Plan:
    - `startup.rs`: a timer; each phase logs `startup phase` (target
      `fastetcd::startup`) with `phase`, `took_ms`, `since_start_ms`; a
      summary line once both ports listen, and `raft_ready` (a leader is
      known, which `/readyz` waits for) from a task when it happens.
    - Phases: `data_file_open` (redb open, and its repair: `repaired`,
      #161), `index_load` (MvccStore: RAM index + cluster id),
      `state_machine_open` (retained snapshots), `upgrade_backup`,
      `wal_open` (replay, or the move from redb), `format_recovery`,
      `raft_start` (Raft::new + initialize), `listening` (both ports
      bound: the listeners are now bound before their servers are
      spawned, `TcpIncoming`, same nodelay/keepalive as `serve`).
    - Test: the real binary's log has every phase in order, the summary
      and `raft_ready`.
    Work items:
    - [x] startup.rs, main wiring; test.
    - [x] Docs (03), changelog; release 1.35.0; close #165.
    - Verified: sc-build of 6aece3b: `startup_phases.rs` (new store and
      restart), `tests/ctl_tls.sh` all checks (TLS on the self-bound
      listener), workspace 0 failures, no clippy finding in startup.rs.

94. **The live-bytes walk stays off tokio workers and is paced by its
    cost (#166) — done, shipped in v1.35.1.** `RedbEngine::usage` called redb's `begin_write` on the
    async thread (a tokio worker blocked while a checkpoint held redb's
    writer), then `WriteTransaction::stats()` walked every tree with the
    writer held (checkpoints and back-pressure flushes wait it out), every
    300 s whatever it cost. redb 2.6.3 has no other way to count in-use
    pages (`stats()` exists only on a write transaction). Plan:
    - `usage`: owned read guard; `begin_write` and the walk in
      `spawn_blocking`, as `commit` does.
    - `SpaceGuard::in_use_bytes`: a walk is due once max(max_age, 200 x
      the last walk's duration) has passed since it (at most ~0.5% of the
      time under redb's writer); one walk at a time (others get the cached
      number); monotonic time, not unix seconds; a walk of 1 s or more is
      logged with when the next is due. `Status` and the monitor both go
      through it. The NOSPACE alarm and reclaim never used this number
      (they use file/dir sizes + statvfs, and reclaim measures itself).
    - Tests: pacing rule; one walk at a time; usage still right.
    Work items:
    - [x] redb_engine, space.rs; tests.
    - [x] Docs (04), changelog; release 1.35.1; close #166.
    - Verified: sc-build of 3757766, workspace 0 failures, new tests pass;
      with begin_write put back on the async thread (branch, deleted) a
      50 ms sleep on the runtime took 1.0 s (the writer's hold).

95. **A follower whose fsync outlasts the heartbeat catches up (#167) — done, shipped in v1.35.2.**
    openraft 0.9's `ReplicationCore::send_log_entries` gives every
    AppendEntries `heartbeat_interval` (250 ms) to be sent, appended and
    fsynced by the follower. A follower whose fsync takes longer (X9
    blades: 0.3–0.8 s, max 84.7 s, `docs/benchmarks/blade-server3.md`)
    times out on every request carrying new entries; the leader resends,
    the follower fsyncs again, and under steady writes it never catches
    up. Plan:
    - Vendored openraft, a second patch: `Config::replication_timeout`
      (ms, 0 = `heartbeat_interval`, upstream's behaviour) and
      `Config::append_entries_timeout()`, used by `send_log_entries`.
      Recorded in `vendor/openraft/FASTETCD.md`.
    - fastetcd: `--replication-timeout` (ms, `ETCD_`-less, fastetcd's own;
      default `--election-timeout`, at least `--heartbeat-interval`). Safe
      to be long: a follower in its fsync cannot time out an election
      (its RaftCore waits for the flush), and the leader's timeout only
      resends.
    - Docs: 01 flag; 03 § Timeouts and slow disks: flags for HDD nodes.
    - Test (multinode_fastpath): 3 members, one follower's fsyncs 600 ms,
      continuous writes: with the default it keeps up (its matched index
      advances with the leader's); control with the heartbeat as timeout
      on a branch.
    Work items:
    - [x] openraft patch + FASTETCD.md; flag; test.
    - [x] Docs, changelog; release 1.35.2; close #167.
    - Verified: new test: the slow follower acknowledged 59-110 of 68-127
      entries in 8 s of writes (control, heartbeat as timeout: 0 of 141);
      openraft's own suite 182 pass; workspace 0 failures (7c72f08's
      rerun; da4cf44); whole-file runs 5/5 at da4cf44 vs 4/5 before #167
      (the #97 lone-write test's flake, filed #171).

96. **Peer connections time out and notice a dead peer (#168) — done, shipped in v1.35.3.**
    `dial_peer` set no connect timeout and no keepalive (tonic sets
    neither): a dial to a peer that never answers and a connection whose
    peer vanished without a reset held forwarded writes, reads, lease TTLs
    and membership changes for the OS's TCP timeout. Plan:
    - `DialOptions` (connect timeout, HTTP/2 keepalive interval and
      timeout, keepalive while idle) on `GrpcNetworkFactory` and
      `WriteForwarder` (`with_dial`); `dial_peer_with`.
    - main: etcd's `PeerDialTimeout`, 1 s + `--election-timeout`, for the
      connect and an unanswered ping; a ping every second. A closed
      connection fails its calls; the forwarder already drops a failed
      channel and dials again.
    - Test `peer_dial.rs`: a silent TCP peer, a TLS handshake that never
      completes, a peer that freezes on an open connection: each call
      fails in seconds; control on a branch without the options.
    Work items:
    - [x] network.rs, main; test.
    - [x] Docs (03 multi-node), changelog; release 1.35.3; close #168.
    - Found: tonic's `connect_timeout` bounds only the TCP connect (the
      TLS test still hung with it): the whole dial is bounded in
      `dial_peer_with` (3e127b4).
    - Verified: sc-build of 3e127b4, workspace 471 pass 0 fail, peer_dial
      4/4, peer_tls 3/3; control without the options (branch, deleted):
      all three peer tests still waiting after 30 s.

97. **A start repairing the data file is killed by its startup probe (#176,
    P1).** server1 (X9, SMR, 12.55): after a warm reset the first start did
    not answer `/livez` for 10 min (the metrics listener was bound only once
    serving), was killed at 600 probes, and the next start repaired redb
    again from scratch (176 s of 210 s in `data_file_open`); apiserver at
    1005 s. The first start's log is not in the boot log (replaced on the
    kill); the second's phases are (`pods.log`, `/api/v1/installs/
    server1-1793208481/bootlog/files/pods.log`).
    - [x] Metrics listener bound before the data file opens; `/livez` from
      `StartupStatus` while starting (progress: phases, redb repair
      reports, `/proc/self/io` rchar/wchar, CPU; 503 after 120 s without);
      open on a blocking thread. Tests; docs (03); release 1.36.1.
    - [x] `repair_cost` measurement: full repair grows with the file (0.4 s
      at 64 MiB of data to 2–8 s at 256–512 MiB on a build VM, a lower
      bound); quick-repair opens in ~10–80 ms at any size.
    - [x] Owner (2026-10-28): option 1, quick-repair on every durable
      commit (P0). `redb_engine.rs` `durability()` on every durable commit,
      one more after defragment's compaction and after a full repair at
      open; `unclean_at_open` (redb header's RECOVERY_REQUIRED bit) so a
      start still says it was killed. Tests: `quick_repair.rs` (child
      processes: no walk after durable/defragment; a walked file left
      quick; control: plain redb commit is walked), `clean_shutdown.rs`
      (SIGKILL: unclean, no walk). Docs 03, 05 (the trade-off); release
      1.37.0.

98. **Keep the data file small in the background (#177, P1).** Owner
    (2026-10-28): compaction, snapshot + purge and an online defrag that
    rewrites to a fresh file and switches over, running all the time,
    throttled for HDDs. What exists and what is new:
    - Compaction: rustkube's kube-apiserver compacts every 5 min
      (`--etcd-compaction-interval` 5m0s, upstream's claim, rustkube#139),
      and `--auto-compaction-retention` exists for a store without one.
      Nothing new; documented.
    - Snapshot + purge: by proposals since #80 (`--snapshot-count`). New:
      `--snapshot-interval-secs` (default 1800): a member with any
      proposal since its last snapshot snapshots once that old, so a quiet
      member's log and WAL are purged too.
    - **Background rewrite** (`RedbEngine::rewrite`, `KvStore::rewrite`):
      under a maintenance lock (not with an in-place defragment), start a
      capture of every engine commit atomically with a read transaction;
      copy every table in chunks into `<file>.rewrite` (non-durable
      commits, small cache), sleeping 4x each chunk's time (20% of the
      disk); apply the captured commits in passes until little is left;
      then under the engine's write lock (commits and new snapshots wait):
      the rest, a durable quick-repair commit, close it, rename over the
      data file, fsync the directory, reopen, swap the handle. Snapshots
      already open keep reading the old inode. A snapshot install during
      the copy (commit_stream) abandons the rewrite. A stale
      `<file>.rewrite` is removed at open.
    - Trigger (`defrag_task` in space.rs): every `--defrag-check-interval-secs`
      (600), with the paced live-bytes walk (#166): free share of the file
      >= `--defrag-free-percent` (50; 0 = off) and file >=
      `--defrag-min-file-bytes` (64 MiB), and the copy (live bytes) fits
      under `--space-high-water-percent`; else skipped, logged.
    - Metrics `fastetcd_defrag_rewrites_total{result}`,
      `fastetcd_defrag_rewrite_last_{seconds,pause_seconds,before_bytes,after_bytes}`;
      one INFO line per pass (file size, live bytes, free %, took).
    Work items:
    - [x] snapshot interval; engine rewrite + capture + tests
      (`rewrite.rs`: content equal, writes during the copy land, an old
      snapshot reads on, install abandons, leftover removed); write-behind
      delegate.
    - [x] server task, flags, metrics; `background_rewrite.rs` (a serving
      member: rewritten, smaller, no longer due, every write during it
      kept, metrics); docs (01, 04), changelog; release 1.37.0.
    - Found: the in-process harness keeps its raft log in the data file,
      and redb cuts its file only by whole regions, so a rewritten file
      can stay well above its live bytes (101 -> 68 MB for 45 MB of pages).
    - [ ] Measure on server1 (file size, open time) once it runs 1.37.
      2026-11-04 22:10Z: not yet. 12.61 (fastetcd bb06386, >= 1.37) is composed,
      on no machine: its install waits on pvetest1 behind 12.58.1; server1 does
      not answer (its last install, 12.58, failed). Before, 1.36 (12.58): server1
      file 152 MB (2026-10-29, `data/rss-server1/`), `data_file_open` 668 ms +
      `index_load` 1065 ms (707 keys); pvetest1 113.9 MB, 29.5 MB in use (74%
      free, so due at 1.37's first check). Then: read `data file rewritten and
      switched` and the `startup phase` lines in the install's pods.log
      (pvetest1 first, it is a look, not the acceptance), then server1 the same.
      Asked the master for server1 on 12.61; parked on a probe of server1:2381.
      2026-11-04 22:48Z, server1 on 12.61: rewrite 152.1 -> 67.9 MB, 5.46 s,
      switch pause 209 ms, in use 32.9 MB (78% free), 12454 records, 24
      commits caught up. Start before it: `data_file_open` 500 ms +
      `index_load` 781 ms (437 keys). Warm reset 23:17Z, file 34.2 MB:
      `data_file_open` 637 ms + `index_load` 580 ms (723 keys), no walk. So
      the file is 4.4x smaller and the open about the same: #176's
      quick-repair had already taken file size out of it. Closed.

99. **NOSPACE at under 320 MiB in use on the 1 GiB volume, and it never
    clears (#182, P0).** fastetcd's log on pvetest2 (12.61): file 807 MB,
    in use 475 MB, `used` 1243 MB of capacity 1282 MB on a 1 GiB volume, WAL
    never purged. Suspects: `used` counts apparent sizes (redb's file has
    holes); the WAL is bounded only in proposals (5000 to a snapshot, 1000
    kept: 900 MB at 900 KiB each); reclaim's snapshot does not fit and its
    purge keeps the whole log; defragment frees nothing (rewrite refused
    above the high-water mark); reclaim's retention keeps deleted keys.
    Plan: measure (`tests/large_values_space.sh`), then:
    - [x] `used` from allocated blocks.
    - [x] WAL bounded in bytes: snapshot + purge past a byte limit (an
      eighth of the volume), and reclaim purges all it can.
    - [x] Reclaim waits for its snapshot; rewrites when in-place frees
      little; compacts to the current revision under the alarm (a window
      before it, tried first, kept the deleted keys' values).
    - [x] Docs (04: what fits on a volume), changelog; release 1.38.0; answer
      the issue's three questions.
    - Verified: `tests/large_values_space.sh` against the fix (a953454) and
      the unfixed 7cbfc95: 351 MiB written, used 1.47 -> 0.77, WAL 382 -> 32
      MiB; alarm at 457 MiB; delete: cleared by itself, file 807 -> 7 MB
      (unfixed: raised still after 10 min).
