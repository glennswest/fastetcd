#!/usr/bin/env bash
# Write latency under a busy watch load, and the shared runtime's
# scheduling delay (fastetcd#194): should the raft apply loop get a
# thread of its own?
#
#   tests/watch_load.sh            # through sc-build: sc-build tests/watch_load.sh
#
# For 1 and 3 members (MEMBER_COUNTS), and 0, 100 and 1000 watch streams
# on the bench's prefix (WATCHERS), each draining every event, runs
# `fastetcd-bench --mode put` (CONNS clients, TOTAL puts) through
# tests/read_latency.sh with METRICS=1, and prints the put latency next to
# fastetcd_runtime_schedule_delay_seconds: how long a task woken on the
# runtime (the apply task among them) waited before it ran. A dedicated
# apply thread can only remove that wait, so it matters only where the
# delay's tail is a real part of the write p99.
#
# Exits non-zero if any run fails (#203): every run is still made, the
# failed ones are named at the end, and a failed run's whole output is
# printed, not only the filtered lines.
set -euo pipefail
cd "$(dirname "$0")/.."
out=$(mktemp "${TMPDIR:-/tmp}/watch_load.XXXXXX")
trap 'rm -f "$out"' EXIT
failed=()
for members in ${MEMBER_COUNTS:-1 3}; do
    for w in ${WATCHERS:-0 100 1000}; do
        echo "=================== $members member(s), $w watchers"
        rc=0
        MEMBERS=$members METRICS=1 \
            BENCH_ARGS="--mode put --conns ${CONNS:-64} --total ${TOTAL:-20000} --watchers $w" \
            tests/read_latency.sh >"$out" 2>&1 || rc=$?
        if [ "$rc" -ne 0 ]; then
            cat "$out"
            echo "FAILED: $members member(s), $w watchers: tests/read_latency.sh exited $rc"
            failed+=("$members member(s)/$w watchers (exit $rc)")
            continue
        fi
        # grep finding no line is not a failure; the run passed.
        grep -E '^(==|--|  |watchers|mode=|BENCH)|fastetcd_runtime_schedule|fastetcd_wal_fsync_max|fastetcd_watch|etcd_server_is_leader|etcd_server_leader_changes' "$out" |
            grep -v '^-- members' || [ "${PIPESTATUS[0]}" -le 1 ]
    done
done
if [ "${#failed[@]}" -ne 0 ]; then
    echo "watch_load: ${#failed[@]} run(s) failed: ${failed[*]}" >&2
    exit 1
fi
