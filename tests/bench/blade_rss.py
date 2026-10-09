#!/usr/bin/env python3
"""fastetcd#84: RSS over a long run, and the cilium-operator lease renewals.

    OUT=tmp/blade84 python3 tests/bench/blade_rss.py <node address> [hours] [sample secs]

Every sample (60 s): fastetcd's `process_resident_memory_bytes` and what it
should stay near (`fastetcd_value_cache_bytes` / `_budget_bytes`,
`fastetcd_key_index_bytes`, write-behind and raft-log cache bytes), the
revision and put count. Every second: kube-system's Leases through the
apiserver (anonymous, as a test node allows); each Lease's renewTime steps
are recorded, so the longest gap between two renewals, and any change of
holder (`leaseTransitions`), is known per Lease.

Writes `samples.csv` and `renewals.csv` under OUT, and prints a summary
at the end (and on Ctrl-C). Run blade_getlatency.py beside it for load.
"""
import csv, http.client, json, os, ssl, sys, threading, time, urllib.request
from datetime import datetime

if len(sys.argv) < 2:
    sys.exit("usage: OUT=tmp/blade84 blade_rss.py <node address> [hours] [sample secs]")
NODE = sys.argv[1]
HOURS = float(sys.argv[2]) if len(sys.argv) > 2 else 4
EVERY = float(sys.argv[3]) if len(sys.argv) > 3 else 60
OUT = os.environ.get("OUT", "tmp/blade84")
CTX = ssl._create_unverified_context()
LEASES = "/apis/coordination.k8s.io/v1/namespaces/kube-system/leases"
WANT = ("process_resident_memory_bytes", "process_virtual_memory_bytes", "process_open_fds",
        "process_start_time_seconds", "process_cpu_seconds_total",
        "fastetcd_value_cache_bytes", "fastetcd_value_cache_budget_bytes", "fastetcd_value_cache_entries",
        "fastetcd_key_index_keys", "fastetcd_key_index_bytes",
        "fastetcd_write_behind_bytes", "fastetcd_wal_cached_bytes",
        "fastetcd_value_cache_hits_total", "fastetcd_value_cache_misses_total",
        "etcd_debugging_mvcc_current_revision", "etcd_debugging_mvcc_put_total",
        "etcd_mvcc_db_total_size_in_bytes")
stop = threading.Event()


def metrics():
    out = {}
    with urllib.request.urlopen(f"http://{NODE}:2381/metrics", timeout=10) as f:
        for line in f.read().decode().splitlines():
            parts = line.rsplit(" ", 1)
            if len(parts) == 2 and parts[0] in WANT:
                out[parts[0]] = float(parts[1])
    return out


def ts(s):
    # MicroTime: 2026-10-09T12:34:56.123456Z
    return datetime.strptime(s.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()


class Renewals:
    def __init__(self):
        self.last = {}     # name -> (renewTime, holder, transitions)
        self.gap = {}      # name -> longest seconds between two renewTimes seen
        self.n = {}        # name -> renewals seen
        self.dur = {}      # name -> leaseDurationSeconds
        self.changes = []  # (wall, name, old holder, new holder)
        self.errors = 0
        self.f = csv.writer(open(os.path.join(OUT, "renewals.csv"), "w", newline=""))
        self.f.writerow(["wall", "lease", "holder", "renew_time", "gap_s", "transitions"])

    def run(self):
        c = None
        while not stop.is_set():
            t0 = time.time()
            try:
                if c is None:
                    c = http.client.HTTPSConnection(NODE, 6443, context=CTX, timeout=30)
                c.request("GET", LEASES, headers={"Accept": "application/json"})
                r = c.getresponse()
                body = r.read()
                if r.status != 200:
                    raise OSError(f"HTTP {r.status}")
                for l in json.loads(body)["items"]:
                    self.see(t0, l)
            except (http.client.HTTPException, OSError, ValueError):
                self.errors += 1
                if c:
                    c.close()
                c = None
            stop.wait(max(0, 1 - (time.time() - t0)))

    def see(self, wall, l):
        name, s = l["metadata"]["name"], l.get("spec", {})
        if not s.get("renewTime"):
            return
        rt, holder, tr = ts(s["renewTime"]), s.get("holderIdentity", ""), s.get("leaseTransitions", 0)
        self.dur[name] = s.get("leaseDurationSeconds", 0)
        prev = self.last.get(name)
        if prev and rt == prev[0]:
            return
        gap = rt - prev[0] if prev else 0
        if prev:
            self.n[name] = self.n.get(name, 0) + 1
            self.gap[name] = max(self.gap.get(name, 0), gap)
            if holder != prev[1] or tr != prev[2]:
                self.changes.append((wall, name, prev[1], holder))
        self.last[name] = (rt, holder, tr)
        self.f.writerow([f"{wall:.1f}", name, holder, f"{rt:.3f}", f"{gap:.3f}", tr])


def mib(x):
    return f"{x / 2**20:8.1f}"


def main():
    os.makedirs(OUT, exist_ok=True)
    ren = Renewals()
    th = threading.Thread(target=ren.run, daemon=True)
    th.start()
    w = csv.writer(open(os.path.join(OUT, "samples.csv"), "w", newline=""))
    w.writerow(["wall"] + list(WANT))
    rows = []
    end = time.time() + HOURS * 3600
    try:
        while time.time() < end:
            t0 = time.time()
            try:
                m = metrics()
                rows.append((t0, m))
                w.writerow([f"{t0:.0f}"] + [m.get(k, "") for k in WANT])
                print(time.strftime("%H:%M:%SZ", time.gmtime(t0)),
                      "rss", mib(m.get("process_resident_memory_bytes", 0)), "MiB",
                      "vcache", mib(m.get("fastetcd_value_cache_bytes", 0)),
                      "index", mib(m.get("fastetcd_key_index_bytes", 0)),
                      "rev", int(m.get("etcd_debugging_mvcc_current_revision", 0)), flush=True)
            except OSError as e:
                print("metrics:", e, flush=True)
            time.sleep(max(0, EVERY - (time.time() - t0)))
    except KeyboardInterrupt:
        pass
    stop.set()
    summary(rows, ren)


def summary(rows, ren):
    print("\n== RSS ==")
    if rows:
        rss = [m.get("process_resident_memory_bytes", 0) for _, m in rows]
        starts = {m.get("process_start_time_seconds") for _, m in rows}
        hrs = (rows[-1][0] - rows[0][0]) / 3600
        print(f"samples {len(rows)} over {hrs:.2f} h, process starts seen {len(starts)}")
        print(f"rss first {mib(rss[0])} last {mib(rss[-1])} min {mib(min(rss))} max {mib(max(rss))} MiB")
        q = max(1, len(rss) // 4)
        print(f"rss mean of first quarter {mib(sum(rss[:q]) / q)}, last quarter {mib(sum(rss[-q:]) / q)} MiB")
        m = rows[-1][1]
        print(f"last: value cache {mib(m.get('fastetcd_value_cache_bytes', 0))} of "
              f"{mib(m.get('fastetcd_value_cache_budget_bytes', 0))} MiB, index "
              f"{mib(m.get('fastetcd_key_index_bytes', 0))} MiB ({int(m.get('fastetcd_key_index_keys', 0))} keys), "
              f"revisions {int(rows[0][1].get('etcd_debugging_mvcc_current_revision', 0))} -> "
              f"{int(m.get('etcd_debugging_mvcc_current_revision', 0))}")
    print("\n== Lease renewals (kube-system) ==")
    for name in sorted(ren.last):
        print(f"{name:45} renewals {ren.n.get(name, 0):6}  longest gap {ren.gap.get(name, 0):7.2f} s"
              f"  (leaseDuration {ren.dur.get(name)} s)")
    print(f"holder changes: {len(ren.changes)}; poll errors: {ren.errors}")
    for wall, name, a, b in ren.changes:
        print(f"  {time.strftime('%H:%M:%SZ', time.gmtime(wall))} {name}: {a} -> {b}")


if __name__ == "__main__":
    main()
