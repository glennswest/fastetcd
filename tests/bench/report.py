#!/usr/bin/env python3
"""Side-by-side tables from compare.sh's CSVs (fastetcd#90).

    tests/bench/report.py docs/benchmarks/data/<run> [<run> ...]

Prints Markdown: per disk and cluster size, one table per kind of result,
contenders as columns. Several runs of the same disk and size are shown
as "a / b" (round 1 / round 2 ...), so the spread is visible.
"""
import csv
import os
import sys
from collections import OrderedDict, defaultdict

WORKLOADS = [
    ("put-1c", "put, 1 client"),
    ("put-1000c", "put, 1000 clients"),
    ("txn-put-1000c", "txn-put, 1000 clients"),
    ("range-lin-1000c", "range linearizable, 1000 clients"),
    ("range-ser-1000c", "range serializable, 1000 clients"),
    ("lease-keepalive-1000c", "lease keepalive, 1000 clients"),
    ("watch-create-1000w", "watch create, 1000 watches"),
    ("watch-events", "watch events delivered"),
]


def load(dirs, name):
    rows = []
    for d in dirs:
        p = os.path.join(d, name + ".csv")
        if os.path.exists(p):
            with open(p) as f:
                rows += list(csv.DictReader(f))
    return rows


def fmt(v, digits=0):
    if v in ("", None):
        return "–"
    try:
        x = float(v)
    except ValueError:
        return v
    if x != x:  # nan: the tool printed no such percentile
        return "–"
    if digits == 0:
        return f"{x:,.0f}".replace(",", " ")
    return f"{x:.{digits}f}"


def joined(vals, digits=0):
    vals = [fmt(v, digits) for v in vals]
    return " / ".join(vals) if vals else "–"


def contenders(rows):
    seen = OrderedDict()
    for r in rows:
        seen[r["contender"]] = None
    return list(seen)


def table(head, body):
    out = ["| " + " | ".join(head) + " |", "|" + "|".join(["---"] + ["---:"] * (len(head) - 1)) + "|"]
    out += ["| " + " | ".join(r) + " |" for r in body]
    return "\n".join(out)


def by(rows, *keys):
    g = defaultdict(list)
    for r in rows:
        g[tuple(r[k] for k in keys)].append(r)
    return g


def main(dirs):
    bench = load(dirs, "bench")
    res = load(dirs, "resources")
    dur = load(dirs, "durability")
    k8s = load(dirs, "k8s")
    groups = OrderedDict()
    for r in bench + res + dur:
        groups[(r["disk"], r["members"])] = None
    for disk, members in groups:
        sel = lambda rows: [r for r in rows if r["disk"] == disk and r["members"] == members]
        b, rs, d = sel(bench), sel(res), sel(dur)
        cs = contenders(b or rs or d)
        print(f"### {disk}, {members} member{'s' if members != '1' else ''}\n")
        if b:
            g = by(b, "workload", "contender")
            body = []
            for w, label in WORKLOADS:
                if not any((w, c) in g for c in cs):
                    continue
                cells = [label]
                for c in cs:
                    rr = g.get((w, c), [])
                    if w == "watch-events":
                        cells.append(joined([x["rps"] for x in rr]) + " ev/s")
                    else:
                        cells.append(joined([x["rps"] for x in rr]) + "/s, p99 " + joined([x["p99_ms"] for x in rr], 1) + " ms")
                body.append(cells)
            print("Throughput and p99 latency (etcd's `benchmark`):\n")
            print(table(["workload"] + cs, body) + "\n")
            body = []
            for w, label in WORKLOADS:
                if w == "watch-events" or not any((w, c) in g for c in cs):
                    continue
                cells = [label]
                for c in cs:
                    rr = g.get((w, c), [])
                    cells.append(" · ".join(f"{fmt(x['p50_ms'], 1)} / {fmt(x['p99_ms'], 1)} / {fmt(x['p999_ms'], 1)}" for x in rr) or "–")
                body.append(cells)
            print("Latency p50 / p99 / p99.9, ms (rounds separated by ·):\n")
            print(table(["workload"] + cs, body) + "\n")
        if rs:
            g = by(rs, "contender")
            body = []
            for col, label, digits in [
                ("rss_idle_mib", "RSS idle, MiB", 0),
                ("rss_load_max_mib", "RSS peak under the 1000-client put, MiB", 0),
                ("cpu_s_per_1000_puts", "CPU seconds per 1000 puts", 3),
                ("disk_write_kib_per_1000_puts", "bytes to storage per 1000 puts, KiB", 0),
                ("write_syscall_kib_per_1000_puts", "bytes written by syscalls per 1000 puts, KiB", 0),
            ]:
                body.append([label] + [joined([x[col] for x in g.get((c,), [])], digits) for c in cs])
            print("Resources (summed over members):\n")
            print(table(["", *cs], body) + "\n")
        if d:
            g = by(d, "contender")
            body = [
                ["acknowledged puts before SIGKILL"] + [joined([x["acked"] for x in g.get((c,), [])]) for c in cs],
                ["acknowledged puts missing after restart"] + [joined([x["lost"] for x in g.get((c,), [])]) for c in cs],
                ["restart to first linearizable read, ms"] + [joined([x["recovery_ms"] for x in g.get((c,), [])]) for c in cs],
            ]
            print("Durability (every member SIGKILLed mid-write, 16 writers):\n")
            print(table(["", *cs], body) + "\n")
    disks = OrderedDict((r["disk"], None) for r in k8s)
    for disk in disks:
        rows = [r for r in k8s if r["disk"] == disk]
        cs = contenders(rows)
        g = by(rows, "phase", "who", "what", "contender")
        keys = OrderedDict(((r["phase"], r["who"], r["what"]), None) for r in rows)
        body = []
        for ph, who, what in keys:
            cells = [f"{ph}: {who} {what}"]
            for c in cs:
                rr = g.get((ph, who, what, c), [])
                if what == "renew loops/s":
                    cells.append(joined([x["p50_ms"] for x in rr]))
                else:
                    cells.append(" · ".join(f"{fmt(x['p50_ms'], 1)} / {fmt(x['p99_ms'], 1)}" for x in rr) or "–")
            body.append(cells)
        print(f"### {disk}: Kubernetes (rustkube apiserver, 1 member)\n")
        print("p50 / p99 ms per request; the last row is the background load's rate:\n")
        print(table(["", *cs], body) + "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
