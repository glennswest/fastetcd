#!/usr/bin/env python3
"""fastetcd#101: the 5-pod run on a node (busybox, 3 s apart), reading each
pod's storm.io/start-timing and fastetcd's /metrics before, during, after.

    OUT=tmp/blade python3 tests/bench/blade_pods.py <node address> <run tag>

Needs the node's apiserver (:6443, anonymous on a test node) and fastetcd's
metrics port (:2381, stormcos#242) reachable; lease the machine first
(`stormcentral testhost lease`). Creates namespace fastetcd-101 and five
pods <tag>-t1..t5, prints each pod's start timing and a summary of the WAL
and checkpoint metrics, writes the samples to $OUT, deletes the pods.
Delete the namespace when done."""
import json, os, ssl, sys, time, urllib.request

NODE = sys.argv[1]
TAG = sys.argv[2] if len(sys.argv) > 2 else "r1"
API = f"https://{NODE}:6443"
MET = f"http://{NODE}:2381/metrics"
NS = "fastetcd-101"
OUT = os.environ.get("OUT", "tmp/blade")
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(API + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, context=CTX, timeout=20) as f:
            return f.status, json.loads(f.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:300]


WANT = ("fastetcd_wal_fsyncs_total", "fastetcd_wal_fsync_seconds_total",
        "fastetcd_wal_proposals_synced_total", "fastetcd_wal_entries_synced_total",
        "fastetcd_wal_fsync_max_seconds", "fastetcd_checkpoint_seconds_total",
        "fastetcd_checkpoints_total", "fastetcd_checkpoint_pace_seconds",
        "fastetcd_checkpoint_max_seconds", "etcd_server_proposals_applied_total",
        "etcd_debugging_mvcc_put_total", "etcd_debugging_mvcc_txn_total")


def metrics():
    with urllib.request.urlopen(MET, timeout=10) as f:
        out = {"t": time.time()}
        for line in f.read().decode().splitlines():
            if line.startswith("#"):
                continue
            parts = line.rsplit(" ", 1)
            if len(parts) == 2 and parts[0] in WANT:
                out[parts[0]] = float(parts[1])
        return out


def rate(a, b, num, den):
    d = b[den] - a[den]
    return (b[num] - a[num]) / d if d else float("nan")


def summary(label, a, b):
    secs = b["t"] - a["t"]
    f = b["fastetcd_wal_fsyncs_total"] - a["fastetcd_wal_fsyncs_total"]
    print(f"== {label}: {secs:.1f} s, {f:.0f} WAL fsyncs ({f / secs:.2f}/s), "
          f"avg fsync {1000 * rate(a, b, 'fastetcd_wal_fsync_seconds_total', 'fastetcd_wal_fsyncs_total'):.1f} ms, "
          f"proposals/fsync {rate(a, b, 'fastetcd_wal_proposals_synced_total', 'fastetcd_wal_fsyncs_total'):.2f}, "
          f"entries/fsync {rate(a, b, 'fastetcd_wal_entries_synced_total', 'fastetcd_wal_fsyncs_total'):.2f}, "
          f"puts {b['etcd_debugging_mvcc_put_total'] - a['etcd_debugging_mvcc_put_total']:.0f}, "
          f"txns {b['etcd_debugging_mvcc_txn_total'] - a['etcd_debugging_mvcc_txn_total']:.0f}, "
          f"checkpoints {b['fastetcd_checkpoints_total'] - a['fastetcd_checkpoints_total']:.0f} using "
          f"{b['fastetcd_checkpoint_seconds_total'] - a['fastetcd_checkpoint_seconds_total']:.2f} s of disk "
          f"({100 * (b['fastetcd_checkpoint_seconds_total'] - a['fastetcd_checkpoint_seconds_total']) / secs:.0f}%), "
          f"pace {b['fastetcd_checkpoint_pace_seconds'] * 1000:.0f} ms; "
          f"max fsync since start {b['fastetcd_wal_fsync_max_seconds']:.3f} s")


def main():
    os.makedirs(OUT, exist_ok=True)
    print(f"== {TAG} on {NODE} at {time.strftime('%H:%M:%SZ', time.gmtime())}")
    req("POST", "/api/v1/namespaces", {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}})
    idle0 = metrics()
    time.sleep(30)
    idle1 = metrics()
    summary("idle 30 s before", idle0, idle1)
    samples = [idle1]
    names = [f"{TAG}-t{i}" for i in range(1, 6)]
    for n in names:
        code, body = req("POST", f"/api/v1/namespaces/{NS}/pods", {
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": n},
            "spec": {"restartPolicy": "Never", "terminationGracePeriodSeconds": 0,
                     "containers": [{"name": "c", "image": "busybox", "command": ["sleep", "600"]}]}})
        print(f"create {n}: {code}" + ("" if code in (200, 201) else f" {body}"))
        samples.append(metrics())
        time.sleep(3)
    timing = {}
    deadline = time.time() + 120
    while time.time() < deadline and len(timing) < len(names):
        for n in names:
            if n in timing:
                continue
            code, p = req("GET", f"/api/v1/namespaces/{NS}/pods/{n}")
            if code == 200:
                t = (p["metadata"].get("annotations") or {}).get("storm.io/start-timing")
                if t and p["status"].get("phase") == "Running":
                    timing[n] = t
        samples.append(metrics())
        time.sleep(1)
    end = metrics()
    for n in names:
        print(f"{n} {timing.get(n, 'NO TIMING')}")
    summary("5-pod run", idle1, end)
    with open(os.path.join(OUT, f"{TAG}-samples.json"), "w") as f:
        json.dump(samples + [end], f)
    for n in names:
        req("DELETE", f"/api/v1/namespaces/{NS}/pods/{n}")


if __name__ == "__main__":
    main()
