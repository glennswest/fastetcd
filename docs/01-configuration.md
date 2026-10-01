# 01 — Configuration reference

Every setting of the `fastetcd` server, taken from the binary's own
`--help` (v1.8.0). Each flag also reads the environment variable shown.
A variable named `ETCD_*` in the last column is also read, as a drop-in
for an existing etcd configuration, when the `FASTETCD_*` one is unset.

Booleans marked *switch* take no value on the command line (see #53):
`--client-cert-auth` turns it on; the env var takes `true`/`false`.
`--auto-defrag` and `--upgrade-backup` are on by default and can only be
turned off with the env var set to `false` (`FASTETCD_AUTO_DEFRAG=false`).

## Ports and endpoints

| Port (default) | Flag | Serves |
|---|---|---|
| 2379, `127.0.0.1` | `--listen-client-urls` | etcd v3 gRPC (KV, Watch, Lease, Cluster, Maintenance, Auth), `grpc.health.v1`, fastetcd's `FastetcdAdmin` (`auth members`, `auth adopt`); HTTP `GET /health`, `/livez`, `/readyz`; the v3 JSON gateway `POST /v3/...` |
| 2380, `127.0.0.1` | `--listen-peer-urls` | raft between members (`fastetcd.raft.RaftPeer`: AppendEntries, Vote, InstallSnapshot, write/read/membership forwarding, AuthSync) |
| 2381, `127.0.0.1` | `--listen-metrics-url` | Prometheus `GET /metrics` (plain HTTP; empty disables) |

The defaults bind loopback. A member others must reach needs
`--listen-*-urls` on a routable address and `--advertise-client-urls` /
`--initial-advertise-peer-urls` naming it. Only the first URL of each
list is bound; the rest are accepted for compatibility.

## Identity and cluster

| Flag | Default | Env | etcd env | Meaning |
|---|---|---|---|---|
| `--name` | `default` | `FASTETCD_NAME` | `ETCD_NAME` | Member name; the key in `--initial-cluster`. |
| `--node-id` | from `--name` | `FASTETCD_NODE_ID` | | Numeric raft id; derived from the name when omitted. |
| `--data-dir` | `default.fastetcd` | `FASTETCD_DATA_DIR` | `ETCD_DATA_DIR` | The store: `fastetcd.redb`, `snapshots/`, `backups/`. |
| `--listen-client-urls` | `http://127.0.0.1:2379` | `FASTETCD_LISTEN_CLIENT_URLS` | `ETCD_LISTEN_CLIENT_URLS` | Client port. |
| `--listen-peer-urls` | `http://127.0.0.1:2380` | `FASTETCD_LISTEN_PEER_URLS` | `ETCD_LISTEN_PEER_URLS` | Peer port. |
| `--advertise-client-urls` | the listen URLs | `FASTETCD_ADVERTISE_CLIENT_URLS` | `ETCD_ADVERTISE_CLIENT_URLS` | This member's client URLs in `MemberList`. |
| `--initial-advertise-peer-urls` | the listen URLs | `FASTETCD_INITIAL_ADVERTISE_PEER_URLS` | `ETCD_INITIAL_ADVERTISE_PEER_URLS` | This member's raft address and its peer URLs in `MemberList`. Other members dial the URLs in *their* `--initial-cluster`. |
| `--initial-cluster` | empty | `FASTETCD_INITIAL_CLUSTER` | `ETCD_INITIAL_CLUSTER` | `name=URL,...`. Empty: a cluster of one. |
| `--initial-cluster-state` | `new` | `FASTETCD_INITIAL_CLUSTER_STATE` | `ETCD_INITIAL_CLUSTER_STATE` | `new` bootstraps; `existing` joins without initializing. |
| `--initial-cluster-token` | none | `FASTETCD_INITIAL_CLUSTER_TOKEN` | `ETCD_INITIAL_CLUSTER_TOKEN` | Derives the cluster id on a new store (FNV-1a-64, 63 bits). |
| `--cluster-id` | derived | `FASTETCD_CLUSTER_ID` | | Id in every response header; persisted on first start; never 0. Without it or a token: `1`, with a warning. |
| `--force-new-cluster` | off (*switch*) | `FASTETCD_FORCE_NEW_CLUSTER` | | Recovery: rebuild membership as a cluster of this member, keeping the data. One surviving member only. |

## TLS

| Flag | Env | etcd env | Meaning |
|---|---|---|---|
| `--cert-file`, `--key-file` | `FASTETCD_CERT_FILE`, `FASTETCD_KEY_FILE` | `ETCD_CERT_FILE`, `ETCD_KEY_FILE` | Client port identity (gRPC, `/health`, `/v3`). |
| `--trusted-ca-file` | `FASTETCD_TRUSTED_CA_FILE` | `ETCD_TRUSTED_CA_FILE` | CA client certificates are verified against. |
| `--client-cert-auth` (*switch*) | `FASTETCD_CLIENT_CERT_AUTH` | `ETCD_CLIENT_CERT_AUTH` | Require a client certificate; with auth on, its CN is the user (#20). |
| `--peer-cert-file`, `--peer-key-file` | `FASTETCD_PEER_CERT_FILE`, `FASTETCD_PEER_KEY_FILE` | `ETCD_PEER_CERT_FILE`, `ETCD_PEER_KEY_FILE` | Peer port identity, also presented when dialling members. Needs `--peer-trusted-ca-file` and `https://` peer URLs. |
| `--peer-trusted-ca-file` | `FASTETCD_PEER_TRUSTED_CA_FILE` | `ETCD_PEER_TRUSTED_CA_FILE` | CA members' peer certificates are verified against. |
| `--peer-client-cert-auth` (*switch*) | `FASTETCD_PEER_CLIENT_CERT_AUTH` | `ETCD_PEER_CLIENT_CERT_AUTH` | Refuse peer callers without a peer-CA certificate. |

Details: [03-deploy § TLS](03-deploy.md#tls).

## API

| Flag | Default | Env | etcd env | Meaning |
|---|---|---|---|---|
| `--enable-grpc-gateway` | `true` | `FASTETCD_ENABLE_GRPC_GATEWAY` | `ETCD_ENABLE_GRPC_GATEWAY` | The v3 JSON gateway on the client port ([03-deploy](03-deploy.md#v3-json-gateway)). Takes `=true`/`=false`. |
| `--listen-metrics-url` | `127.0.0.1:2381` | `FASTETCD_LISTEN_METRICS_URL` | `ETCD_LISTEN_METRICS_URLS` | `/metrics` address (host:port, no scheme); empty disables. |

## Raft log and snapshots

| Flag | Default | Env | etcd env | Meaning |
|---|---|---|---|---|
| `--snapshot-count` | `5000` | `FASTETCD_SNAPSHOT_COUNT` | `ETCD_SNAPSHOT_COUNT` | Snapshot (then purge the log) every N applied entries. |
| `--max-in-snapshot-log-to-keep` | `1000` | `FASTETCD_MAX_IN_SNAPSHOT_LOG_TO_KEEP` | `ETCD_MAX_IN_SNAPSHOT_LOG_TO_KEEP` | Snapshotted entries kept after a purge, for followers just behind. |
| `--max-snapshots` | `1` | `FASTETCD_MAX_SNAPSHOTS` | `ETCD_MAX_SNAPSHOTS` | Snapshots kept on disk (each a full copy), rolled off before a new one is written. |

## Compaction and disk space

| Flag | Default | Env | etcd env | Meaning |
|---|---|---|---|---|
| `--auto-compaction-retention` | `0` (off) | `FASTETCD_AUTO_COMPACTION_RETENTION` | `ETCD_AUTO_COMPACTION_RETENTION` | **Revisions** of history to keep, not hours: etcd's `revision` mode only. An etcd config's `ETCD_AUTO_COMPACTION_RETENTION=1` means one revision here (#21). |
| `--auto-compaction-interval-secs` | `300` | `FASTETCD_AUTO_COMPACTION_INTERVAL_SECS` | | How often the leader's compaction ticker runs. |
| `--quota-backend-bytes` | `0` | `FASTETCD_QUOTA_BACKEND_BYTES` | `ETCD_QUOTA_BACKEND_BYTES` | Footprint ceiling; `0` = the volume's real size. |
| `--space-high-water-percent` | `80` | `FASTETCD_SPACE_HIGH_WATER_PERCENT` | | Reclaim (compact → snapshot → purge → defragment) above this. |
| `--space-alarm-percent` | `95` | `FASTETCD_SPACE_ALARM_PERCENT` | | NOSPACE: writes refused; reads, deletes, compaction, defrag work. |
| `--space-clear-percent` | `70` | `FASTETCD_SPACE_CLEAR_PERCENT` | | NOSPACE clears itself below this. |
| `--space-check-interval-secs` | `30` | `FASTETCD_SPACE_CHECK_INTERVAL_SECS` | | Occupancy sampling; `0` disables the monitor. |
| `--space-reclaim-retention` | `1000` | `FASTETCD_SPACE_RECLAIM_RETENTION` | | Revisions kept when compacting under pressure (even with auto-compaction off). |
| `--auto-defrag` | on | `FASTETCD_AUTO_DEFRAG` | | Let reclaim defragment. Off only with `FASTETCD_AUTO_DEFRAG=false` (#53). |
| `--expected-nodes` | `0` (off) | `FASTETCD_EXPECTED_NODES` | | Check the volume against the sizing model at startup (warn only); enables auto-compaction if it was left at 0. |
| `--expected-pods-per-node` | `30` | `FASTETCD_EXPECTED_PODS_PER_NODE` | | Pod density for `--expected-nodes`. |

Details: [04-disk-space](04-disk-space.md).

## Backups and recovery

| Flag | Default | Env | Meaning |
|---|---|---|---|
| `--backup-dir` | unset (off) | `FASTETCD_BACKUP_DIR` | Periodic checksummed backups; put it on another volume. |
| `--backup-interval-secs` | `900` | `FASTETCD_BACKUP_INTERVAL_SECS` | At least this often, if anything changed. |
| `--backup-every-revisions` | `10000` | `FASTETCD_BACKUP_EVERY_REVISIONS` | Also after this many revisions; `0` disables the trigger. |
| `--backup-retain` | `4` | `FASTETCD_BACKUP_RETAIN` | Backups kept. |
| `--on-corruption` | `restore` | `FASTETCD_ON_CORRUPTION` | `restore` (lone member only) or `refuse`. |
| `--upgrade-backup` | on | `FASTETCD_UPGRADE_BACKUP` | Copy the data file before a different version opens it. Off only with `FASTETCD_UPGRADE_BACKUP=false` (#53). |
| `--upgrade-backup-dir` | `<data-dir>/backups` | `FASTETCD_UPGRADE_BACKUP_DIR` | Where those copies go. |
| `--upgrade-backup-retain` | `2` | `FASTETCD_UPGRADE_BACKUP_RETAIN` | How many are kept. |

Details: [05-backup-and-recovery](05-backup-and-recovery.md).

## Logging and accepted-but-ignored etcd flags

Logging goes to stderr and is set by **`RUST_LOG`** (tracing
`EnvFilter` syntax; default `info`), e.g. `RUST_LOG=debug` or
`RUST_LOG=info,openraft=warn`.

These are accepted so etcd command lines and config files start, and do
nothing: `--log-level` (`ETCD_LOG_LEVEL`) and `--max-request-bytes`
(`ETCD_MAX_REQUEST_BYTES`; gRPC keeps tonic's 4 MiB limit), see #54;
`--log-outputs`, `--logger`, `--metrics`, `--enable-pprof`.

## Offline subcommands

Run with the server stopped; each opens `<data-dir>/fastetcd.redb`
exclusively and refuses if it is in use.

| Command | Does |
|---|---|
| `fastetcd backup --out <file>` | Consistent single-file copy of the store. |
| `fastetcd restore <backup> [--force]` | Restore a backup (either format) over the data dir; refuses a newer dir without `--force`; keeps the old file as `fastetcd.redb.replaced-*`. |
| `fastetcd fsck [--repair]` | Check structure, format version, raft membership and MVCC counters; `--repair` fixes the raft/format metadata. |
| `fastetcd defrag` | Return freed pages to the filesystem; works on a full volume with no quorum. |
| `fastetcd sizing --nodes N [--pods-per-node P]` | Volume size for a cluster shape, with the arithmetic. Needs no data dir. |

## Other binaries

**`fastetcd-ctl`** — a small client. Options: `--endpoint` (default
`http://127.0.0.1:2379`), `--user name:password`. Plaintext only: it has
no TLS options (#59). Commands: `put <key> <value>`, `get <key>
[--prefix]`, `del <key> [--prefix]`, `snapshot-save <path>`, `status`,
`defrag`, `compact <revision>`, `alarm [--disarm]`, `auth members`,
`auth adopt <member>`.

**`fastetcd-migrate --from <etcd snapshot.db> --to <data-dir>
[--force] [--preserve-revisions]`** — import an etcd v3 BoltDB
snapshot. Without `--preserve-revisions` only the latest value of each
key is imported.

**`fastetcd-bench`** — load generator. `--endpoint`, `--mode put |
get-lin | get-ser | read-under-load` (default `put`), `--conns 64`,
`--total 50000`, `--val-bytes 256`, `--keys 10000`. `read-under-load`
(#71): `--conns` clients loop GET + CAS Txn on their own key (a prefix
Range every fifth loop) for `--duration-secs 20` while a probe makes
`--probes 200` sequential linearizable, then serializable, Ranges;
prints write rate and latency and both probes' percentiles.
