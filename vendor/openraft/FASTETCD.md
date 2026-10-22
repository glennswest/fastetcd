# openraft 0.9.24, patched for fastetcd

This directory is openraft 0.9.24 as published on crates.io
(`openraft-0.9.24.crate` from static.crates.io, sha256
`80d35e2f60cdf9bcfc39a020966091017c6dc2a4b43b355a22ca3e76106f4a0a`, the
checksum Cargo.lock recorded for it), with two changes. The workspace uses
it through `[patch.crates-io]` in the top-level `Cargo.toml`; it is not a
workspace member (`exclude`). Licence: MIT OR Apache-2.0, unchanged.

The unchanged copy is commit `f7473ba`; `git diff f7473ba -- vendor/openraft`
is the whole patch (`src/core/raft_core.rs`, `src/core/notify.rs`,
`src/raft/mod.rs` for #97; `src/config/config.rs`,
`src/replication/mod.rs` for #167; this file).

## What it changes (fastetcd#97)

In 0.9, `RaftCore::run_command(Command::AppendInputEntries)` awaits the
log store's flush (`append_to_log` waits for `LogFlushed`) before the
`Replicate` commands that follow it run. On a leader that puts the
leader's fsync and a follower's in series: a lone write on three members
costs two fsyncs, where etcd's costs one (its leader writes its WAL while
the entries are on the way to the followers).

With the patch, an `AppendInputEntries` that carries this node's own
leader vote (`engine.leader.vote == vote`):

- submits the append (`RaftLogStorage::append`) and does not wait for it,
  so the `Replicate` commands after it go out at once;
- hands the flush callback to one forwarder task, which waits for the
  flushes in submission order and sends `Notify::LocalLogFlushed`;
- counts the leader in its own quorum (`update_local_progress`) only on
  that notify, and only if the leader's vote is still the one the append
  was submitted under. A flush error stops RaftCore, as before.

Followers are unchanged: they append, wait for the flush, then answer
AppendEntries. A commit therefore still needs a quorum of durable copies;
what changes is that the leader's copy no longer has to be first.

Bounds, so a stuck disk behaves as before:

- at most 16 leader appends unflushed (`MAX_UNFLUSHED_LEADER_APPENDS`);
  the 17th waits;
- if the oldest unflushed append is a heartbeat interval old or more,
  RaftCore waits for every submitted flush before it sends anything to a
  follower (heartbeats included). Before the patch every append blocked
  RaftCore; a stalled leader went silent and its followers elected
  another. That still happens: fastetcd's #103 tests
  (`a_stall_on_the_leader_longer_than_the_election_timeout_elects_again`,
  `an_election_timeout_above_the_stall_keeps_the_leader`) pass unchanged.

The log store must make an appended entry readable as soon as `append`
returns (replication reads it before the flush). fastetcd's
`WalLogStore` does: entries go into its in-RAM tail before the write is
queued.

What fastetcd adds outside this crate: a leader can now see an entry
committed (on two followers) before its own fsync of it returns. The
state machine applies an entry only once this member's log holds it
durably (`LogProgress::wait_durable`, `FastetcdStateMachine::
follow_log_durability`), so the data file is never made durable ahead of
the log.

## Second change: the AppendEntries timeout (fastetcd#167)

0.9's `ReplicationCore::send_log_entries` gives every AppendEntries
`heartbeat_interval` to be sent, appended and flushed by the follower. A
follower whose flush takes longer times out on every request carrying new
entries; the leader resends them, the follower flushes again, and under
steady writes it never catches up (X9 blades' fsyncs: 0.3-0.8 s, the
heartbeat 250 ms).

`Config::replication_timeout` (milliseconds; `0`, the default, keeps
`heartbeat_interval`) and `Config::append_entries_timeout()`, which
`send_log_entries` now uses. Nothing else changes: heartbeats are still
sent every `heartbeat_interval`; a request that times out is resent as
before. fastetcd sets it from `--replication-timeout` (default: the
election timeout).

## Upstream

Offered to openraft's `release-0.9` as
https://github.com/databendlabs/openraft/pull/2163 (2026-10-13), the same
change with fastetcd's references taken out of its comments.

The #167 change is not offered upstream yet.

## Dropping it

When an openraft release does this itself (0.10's IO pipeline separates
submitting from flushing), remove `vendor/openraft`, the `exclude` and the
`[patch.crates-io]` entry, and restore openraft's `source` and `checksum`
lines in Cargo.lock (`cargo update -p openraft --precise <version>`).
