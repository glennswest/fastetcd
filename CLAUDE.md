# CLAUDE.md — fastetcd

Project-specific context. Cross-project rules live in `../CLAUDE.md`.

## Project summary

Rust implementation of the etcd v3 wire protocol. Wire-compatible gRPC.
Multi-node Raft from day one. Importer for existing etcd BoltDB data.
Focused on low resource overhead and predictable latency.

## Version

**`1.44.3`** — membership changes go to the leader, which waits for one
in progress, so a voter `member add` right after start-up no longer
fails "already undergoing a configuration change" (#234). 1.44.2:
MoveLeader waits for the proposals in flight and
re-checks the leader's log before asking the target, so a write cannot
leave the target behind under load (#237). 1.44.1: a watch at a compacted revision gets etcd's two responses
(created, then cancelled with `compact_revision` and no reason), so
clientv3 reports `ErrCompacted`; a watch from the compact revision itself
is served (#222). 1.44.0: a key's Version counts puts after a compaction,
as etcd's, from a replicated `PutVersions` entry; downgrade floor 1.44
(#220). 1.43.5: Lock, Campaign, Observe and Watch follow raft snapshot
installs (#216). 1.43.4: a lease's key range holds its own keys for every id
(#215). 1.43.3: a permission grant on the same key/range replaces its
type (#214). 1.43.2: a snapshot install clears whole tables (#213). 1.43.1:
a LeaseGrant of an existing id is refused (etcd's
`lease already exists`); a watch replay reads history and the compact
revision as one view (#212). 1.43.0: members are dialed at their address in the replicated
membership, and routes and MemberList follow it; MemberUpdate is
replicated (#211). 1.42: etcd's robustness suite runs against
fastetcd (#195). 1.41: new stores start at revision 1 (#160).

## Work plan and history

Moved to [docs/work-plan.md](docs/work-plan.md) (CLAUDE.md is loaded into every session; its size made the session's context thrash). Nothing was dropped. Not loaded by default: `grep -n '^##' docs/work-plan.md`, then read only the section you need. Write new intentions and progress there, not here.

## Architecture pillars

- **Storage**: trait-first (`KvStore`). The server always runs on
  `redb`: one file, `<data-dir>/fastetcd.redb`, holds MVCC, leases,
  auth and node metadata, behind a write-behind layer (applies in RAM,
  checkpointed in). The raft log and vote are a WAL in
  `<data-dir>/wal/` (#85); raft snapshots are files in
  `<data-dir>/snapshots/`. redb is the only engine: `wal` and
  `iouring` were removed in 1.32.2 (#55).
- **Consensus**: `openraft` 0.9 — core, not optional. Single-node is a
  cluster-of-one. Linearizable reads and writes avoid RaftCore's queue
  where they can: `read_index.rs` (sole voter, or a `ConfirmLeader`
  quorum; else `ensure_linearizable`) and `proposer.rs` (up to 3
  `client_write`s in flight, the rest batched). Applies commit without
  fsync; the raft log replays them (#71, #75).
- **gRPC**: `tonic` 0.12 + `prost`, generated from vendored etcd v3.6.11
  `.proto` files; pbjson serde for the v3 JSON gateway.
- **Client port**: gRPC + `/health` `/livez` `/readyz` + `/v3/...`
  gateway (tonic routes → axum). **Peer port**: `fastetcd.raft.RaftPeer`.
  **Metrics port**: axum, `/metrics` + the health routes (plain HTTP).
- **Async runtime**: `tokio`.
- **Logging/tracing**: `tracing` + `tracing-subscriber`, `RUST_LOG`.
- **Ships as**: a special golden, `stormcentral component stage
  fastetcd` (stormcos `build-goldens.sh` in stage mode, at main's tip;
  `docs/03-deploy.md`); the registry lists `fastetcd` (kind special), and
  `component build fastetcd` starts the same stage. Runs on the node as
  a kubelet static pod `etcd` (stormcos#559), mTLS on client and peer ports.
- **Docs**: `docs/01-configuration.md` is the flag reference, generated
  by hand from the binaries' `--help`. Keep it in step when adding a
  flag.

## Repo layout

```
crates/
  proto/      # etcd v3 + fastetcd protos: tonic stubs, pbjson serde
  storage/    # KvStore trait + engines; MVCC store (revisions, leases,
              # auth tables, events)
  raft/       # openraft glue: log store, state machine, snapshot files,
              # gRPC peer transport, forwarding, lease precheck,
              # read index (ConfirmLeader), proposer (group commit)
  server/     # binary `fastetcd`: services, auth, watch, gateway,
              # metrics, space monitor, backups, recovery, CLI
  migrate/    # binary `fastetcd-migrate`: etcd BoltDB snapshot → data dir
  ctl/        # binaries `fastetcd-ctl`, `fastetcd-bench`
docs/         # 00 design, 01 configuration, 02 testing, 03 deploy,
              # 04 disk space, 05 backup and recovery
deploy/       # Helm chart, systemd unit, packaging scripts
tests/        # etcdctl_smoke.sh, read_latency.sh (bench via sc-build)
```

## Constraints & rules

Moved to [docs/constraints.md](docs/constraints.md) (CLAUDE.md is loaded into every session; its size made the session's context thrash). Nothing was dropped. Not loaded by default: `grep -n '^##' docs/constraints.md`, then read only the section you need. Write new intentions and progress there, not here.

