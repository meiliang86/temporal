#!/usr/bin/env python3
"""Prints markdown tables from results.jsonl."""
import json, sys
rows = [json.loads(l) for l in open("/home/user/perf/results.jsonl")]
prefix = sys.argv[1] if len(sys.argv) > 1 else ""
cols = ["label", "namespace", "note", "completed", "throughput_wf_per_s", "e2e_p50_ms", "e2e_p90_ms", "e2e_p99_ms",
        "e2e_avg_ms", "StartWorkflowExecution_avg_ms", "RespondWorkflowTaskCompleted_avg_ms",
        "RespondActivityTaskCompleted_avg_ms", "RecordWorkflowTaskStarted_avg_ms", "RecordActivityTaskStarted_avg_ms", "commit_wait_avg_ms", "commit_waits", "commit_timeouts"]
print("| " + " | ".join(cols) + " |")
print("|" + "---|" * len(cols))
for r in rows:
    if r["label"].startswith(prefix):
        print("| " + " | ".join("" if r.get(c) is None else str(r.get(c)) for c in cols) + " |")
