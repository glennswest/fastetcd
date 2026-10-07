#!/usr/bin/env python3
"""fastetcd#89: rustkube's get-latency / lease-churn load against a test
node's own apiserver and fastetcd, read off the node.

    OUT=tmp/blade python3 tests/bench/blade_getlatency.py <node address> [tag] [secs]

As rustkube's test/e2e/get-latency.sh: forty clients each renew their own
Lease (GET + PUT) in a loop and LIST the Leases every fifth loop, while a
probe makes 200 GETs of one Lease, idle and then under that load. Prints the
GET and renewal (PUT) latency percentiles, the renewal rate, and fastetcd's
WAL / checkpoint / write-behind metrics over the loaded window (:2381,
stormcos#242). Anonymous requests: a test node's apiserver allows them.
Lease the machine first (`stormcentral testhost lease`). Creates and
deletes namespace fastetcd-89.

READ_ONLY=1: no writes at all (for a member that cannot write, #138): the
probe GETs kube-system's cilium-operator-resource-lock Lease (fastetcd#84)
and the forty clients GET kube-system's Leases in turn and LIST them every
fifth loop.
"""
import http.client, json, os, ssl, sys, threading, time, urllib.request

if len(sys.argv) < 2:
    sys.exit("usage: OUT=tmp/blade blade_getlatency.py <node address> [tag] [load secs]")
NODE, TAG = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "g1")
SECS = float(sys.argv[3]) if len(sys.argv) > 3 else 60
OUT = os.environ.get("OUT", "tmp/blade")
READ_ONLY = os.environ.get("READ_ONLY") == "1"
NS = "kube-system" if READ_ONLY else "fastetcd-89"
CTX = ssl._create_unverified_context()
LEASES = f"/apis/coordination.k8s.io/v1/namespaces/{NS}/leases"


class Client:
    """One keep-alive connection, as client-go holds one."""

    def __init__(self):
        self.c = self.conn()

    @staticmethod
    def conn():
        return http.client.HTTPSConnection(NODE, 6443, context=CTX, timeout=30)

    def req(self, method, path, body=None):
        h = {"Accept": "application/json"}
        if body is not None:
            h["Content-Type"] = "application/json"
            body = json.dumps(body)
        for attempt in (0, 1):
            try:
                self.c.request(method, path, body=body, headers=h)
                r = self.c.getresponse()
                return r.status, r.read()
            except (http.client.HTTPException, OSError):
                if attempt:
                    raise
                self.c.close()
                self.c = self.conn()


WANT = ("fastetcd_value_cache_hits_total", "fastetcd_value_cache_misses_total",
        "fastetcd_wal_fsyncs_total", "fastetcd_wal_fsync_seconds_total", "fastetcd_wal_fsync_max_seconds",
        "fastetcd_wal_proposals_synced_total", "fastetcd_checkpoint_commit_max_seconds",
        "fastetcd_checkpoint_seconds_total", "fastetcd_checkpoints_total",
        "fastetcd_write_behind_backpressure_total", "etcd_debugging_mvcc_put_total")


def metrics():
    out = {"t": time.time()}
    with urllib.request.urlopen(f"http://{NODE}:2381/metrics", timeout=10) as f:
        for line in f.read().decode().splitlines():
            parts = line.rsplit(" ", 1)
            if len(parts) == 2 and parts[0] in WANT:
                out[parts[0]] = float(parts[1])
    return out


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def line(what, ms):
    return f"{what:22} n {len(ms):5}  p50 {pct(ms, .5):7.1f} ms  p99 {pct(ms, .99):7.1f} ms  max {max(ms):7.1f} ms"


def probe(c, path, n=200):
    ms = []
    for _ in range(n):
        t = time.perf_counter()
        s, _ = c.req("GET", path)
        ms.append((time.perf_counter() - t) * 1000)
        if s != 200:
            sys.exit(f"GET {path}: HTTP {s}")
    return ms


def main():
    os.makedirs(OUT, exist_ok=True)
    admin = Client()
    if READ_ONLY:
        probe_path = LEASES + "/cilium-operator-resource-lock"
        names = [l["metadata"]["name"] for l in json.loads(admin.req("GET", LEASES)[1])["items"]]
    else:
        probe_path = LEASES + "/probe"
        admin.req("POST", "/api/v1/namespaces", {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}})
        s, d = admin.req("POST", LEASES, {"apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
                                          "metadata": {"name": "probe"}, "spec": {"holderIdentity": "x"}})
        if s not in (200, 201, 409):
            sys.exit(f"create probe lease: {s} {d[:200]!r}")
    print(f"== {TAG} on {NODE} at {time.strftime('%H:%M:%SZ', time.gmtime())}, load {SECS:.0f} s"
          + (", read-only" if READ_ONLY else ""))
    idle = probe(Client(), probe_path)
    print("idle  " + line("GET lease", idle))

    stop = threading.Event()
    puts, renewals = [], [0]
    lock = threading.Lock()

    def reader(i):
        c = Client()
        n = 0
        while not stop.is_set():
            t = time.perf_counter()
            s, _ = c.req("GET", f"{LEASES}/{names[(i + n) % len(names)]}")
            if s == 200:
                with lock:
                    puts.append((time.perf_counter() - t) * 1000)
                    renewals[0] += 1
            if n % 5 == 0:
                c.req("GET", LEASES)
            n += 1

    def worker(i):
        c = Client()
        path = f"{LEASES}/l{i}"
        c.req("POST", LEASES, {"apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
                               "metadata": {"name": f"l{i}"}, "spec": {}})
        n = 0
        while not stop.is_set():
            s, d = c.req("GET", path)
            if s == 200:
                obj = json.loads(d)
                obj["spec"]["renewTime"] = f"2026-10-01T00:00:{n % 60:02d}.000000Z"
                t = time.perf_counter()
                s, _ = c.req("PUT", path, obj)
                if s == 200:
                    with lock:
                        puts.append((time.perf_counter() - t) * 1000)
                        renewals[0] += 1
            if n % 5 == 0:
                c.req("GET", LEASES)
            n += 1

    threads = [threading.Thread(target=reader if READ_ONLY else worker, args=(i,), daemon=True)
               for i in range(40)]
    for t in threads:
        t.start()
    time.sleep(3)
    with lock:
        puts.clear()
        renewals[0] = 0
    m0 = metrics()
    loaded = []
    c = Client()
    end = time.time() + SECS
    while time.time() < end:
        loaded += probe(c, probe_path, 50)
    m1 = metrics()
    stop.set()
    for t in threads:
        t.join(timeout=30)
    secs = m1["t"] - m0["t"]
    print("load  " + line("GET lease", loaded))
    what = "clients' GET" if READ_ONLY else "renewal (PUT lease)"
    print("load  " + line(what, puts))
    print(f"load  {'GETs' if READ_ONLY else 'renewals'} {renewals[0] / secs:.0f}/s by 40 clients")
    d = lambda k: m1[k] - m0[k]
    f = d("fastetcd_wal_fsyncs_total")
    fs = max(f, 1)
    print(f"fastetcd over {secs:.0f} s: {f:.0f} WAL fsyncs, mean "
          f"{1000 * d('fastetcd_wal_fsync_seconds_total') / fs:.1f} ms, "
          f"{d('fastetcd_wal_proposals_synced_total') / fs:.1f} proposals per fsync; "
          f"{d('fastetcd_checkpoints_total'):.0f} checkpoints using {d('fastetcd_checkpoint_seconds_total'):.1f} s; "
          f"write-behind back-pressure {d('fastetcd_write_behind_backpressure_total'):.0f}")
    print(f"fastetcd since start: max WAL fsync {m1['fastetcd_wal_fsync_max_seconds']:.3f} s, "
          f"max checkpoint commit {m1['fastetcd_checkpoint_commit_max_seconds']:.3f} s, "
          f"back-pressure {m1['fastetcd_write_behind_backpressure_total']:.0f}")
    hits, misses = d("fastetcd_value_cache_hits_total"), d("fastetcd_value_cache_misses_total")
    print(f"fastetcd value cache over the load: {hits:.0f} hits, {misses:.0f} misses "
          f"({100 * hits / max(hits + misses, 1):.2f}% hits)")
    with open(os.path.join(OUT, f"{TAG}.json"), "w") as fh:
        json.dump({"idle": idle, "loaded": loaded, "puts": puts, "m0": m0, "m1": m1}, fh)
    if not READ_ONLY:
        admin.req("DELETE", f"/api/v1/namespaces/{NS}")


if __name__ == "__main__":
    main()
