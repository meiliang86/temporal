#!/usr/bin/env python3
"""Runs omes load against one or more namespaces of cluster-a at once and reports, per namespace:
throughput and end-to-end workflow latency (start -> close, from cluster-a's visibility DB), frontend
API latency and sync replication commit wait (from cluster-a's Prometheus metrics, before/after diff).

    bench.py --label t1-async --namespaces ns1 --rate 20 --duration 60s
    bench.py --label t1-max --namespaces ns1 --concurrency 64 --duration 60s   # closed loop
"""
import argparse
import json
import re
import subprocess
import sys
import time
import urllib.request

OMES = "/home/user/perf/bin/omes"
METRICS = "http://127.0.0.1:8000/metrics"
RESULTS = "/home/user/perf/results.jsonl"
LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)\{(.*)\} (\S+)$')
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')
OPS = ["StartWorkflowExecution", "RespondWorkflowTaskCompleted", "RespondActivityTaskCompleted"]


def scrape():
    out = {}
    for line in urllib.request.urlopen(METRICS).read().decode().splitlines():
        m = LINE.match(line)
        if not m:
            continue
        name, labels, value = m.groups()
        if not (name.startswith("service_latency") or name.startswith("sync_replication_commit")):
            continue
        out[(name, tuple(sorted(LABEL.findall(labels))))] = float(value)
    return out


def delta(before, after, name, **want):
    total = 0.0
    for (n, labels), v in after.items():
        if n != name:
            continue
        d = dict(labels)
        if all(d.get(k) == val for k, val in want.items()):
            total += v - before.get((n, labels), 0.0)
    return total


def avg_ms(before, after, base, **want):
    count = delta(before, after, base + "_count", **want)
    if count == 0:
        return None, 0
    return 1000 * delta(before, after, base + "_sum", **want) / count, int(count)


def e2e(run_id):
    sql = f"""
      select count(*),
             count(*) filter (where status = 2),
             extract(epoch from max(close_time) - min(start_time)),
             percentile_cont(0.5) within group (order by d), percentile_cont(0.9) within group (order by d),
             percentile_cont(0.99) within group (order by d), avg(d), max(d)
      from (select status, start_time, close_time,
                   extract(epoch from close_time - start_time) * 1000 as d
            from executions_visibility where workflow_id like 'w-{run_id}-%' and close_time is not null) x"""
    row = subprocess.check_output(
        ["psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "temporal_a_vis", "-At", "-F", ",", "-c", sql]
    ).decode().strip().split(",")
    closed, completed = int(row[0]), int(row[1])
    span = float(row[2] or 0)
    vals = [float(x) if x else None for x in row[3:]]
    return {
        "closed": closed, "completed": completed,
        "throughput_wf_per_s": round(completed / span, 1) if span else None,
        "e2e_p50_ms": vals[0], "e2e_p90_ms": vals[1], "e2e_p99_ms": vals[2], "e2e_avg_ms": vals[3], "e2e_max_ms": vals[4],
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--label", required=True)
    p.add_argument("--namespaces", required=True)
    p.add_argument("--rate", type=float, default=0, help="iterations/s per namespace; 0 = closed loop")
    p.add_argument("--concurrency", type=int, default=500)
    p.add_argument("--duration", default="60s")
    p.add_argument("--scenario", default="workflow_with_single_noop_activity")
    p.add_argument("--note", default="")
    a = p.parse_args()

    namespaces = a.namespaces.split(",")
    stamp = time.strftime("%H%M%S")
    before = scrape()
    procs = {}
    for ns in namespaces:
        run_id = f"{a.label}-{ns}-{stamp}"
        cmd = [OMES, "run-scenario-with-worker", "--scenario", a.scenario, "--language", "go",
               "--server-address", "127.0.0.1:7233", "--namespace", ns, "--run-id", run_id,
               "--duration", a.duration, "--max-concurrent", str(a.concurrency),
               "--continue-on-iteration-failure", "--worker-profile", "throughput-stress-baseline-x8"]
        if a.rate:
            cmd += ["--max-iterations-per-second", str(a.rate)]
        log = open(f"/home/user/perf/logs/omes-{run_id}.log", "w")
        procs[ns] = (run_id, subprocess.Popen(cmd, cwd="/home/user/perf/omes", stdout=log, stderr=subprocess.STDOUT))
    for ns, (run_id, proc) in procs.items():
        proc.wait()
    time.sleep(8)  # let visibility catch up
    after = scrape()

    for ns, (run_id, proc) in procs.items():
        r = {"label": a.label, "namespace": ns, "run_id": run_id, "rate": a.rate, "concurrency": a.concurrency,
             "duration": a.duration, "scenario": a.scenario, "note": a.note, "omes_exit": proc.returncode}
        r.update(e2e(run_id))
        label = ns.replace("-", "_")  # Prometheus label values have '-' replaced
        for op in OPS:
            v, n = avg_ms(before, after, "service_latency", namespace=label, operation=op, service_name="frontend")
            r[f"{op}_avg_ms"] = round(v, 1) if v is not None else None
        for op in ["RecordWorkflowTaskStarted", "RecordActivityTaskStarted"]:
            v, n = avg_ms(before, after, "service_latency", namespace=label, operation=op, service_name="history")
            r[f"{op}_avg_ms"] = round(v, 1) if v is not None else None
        v, n = avg_ms(before, after, "sync_replication_commit_latency", namespace=label)
        r["commit_wait_avg_ms"] = round(v, 1) if v is not None else None
        r["commit_waits"] = n
        r["commit_timeouts"] = int(delta(before, after, "sync_replication_commit_timeout", namespace=label))
        for k in list(r):
            if isinstance(r[k], float):
                r[k] = round(r[k], 1)
        print(json.dumps(r))
        with open(RESULTS, "a") as f:
            f.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    sys.exit(main())
