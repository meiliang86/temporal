# Sync replication: performance test results

Prototype: branch `sync-replication/v1-blocking` (commit `b33e181`; three-cluster runs on `35af3ba`, same server code). Date: 2026-11-22.

## Setup

- **Clusters:** two Temporal clusters, `cluster-a` (active) and `cluster-b` (standby). Each runs all services in one process, with 4 history shards and Postgres 16 persistence (one shared Postgres instance).
- **Network:** cross-cluster traffic goes through a TCP delay proxy (`tools/delayproxy`). The proxy adds a fixed one-way delay in each direction: 20 ms (≈40 ms RTT, nearby regions) or 0 ms (≈0 RTT).
- **Load:** omes `workflow_with_single_noop_activity` (one workflow with one no-op activity). Each workflow makes 7 transactions: start, workflow task started, workflow task completed, activity started, activity completed, workflow task started, workflow task completed. Go worker, profile `throughput-stress-baseline-x8`, one omes run per namespace, 60 s per run.
- **Sync config:** `history.syncReplicationEnabled` on cluster-a for the sync namespace(s). `history.ReplicationStreamEagerSyncStatusInterval=5ms` on both clusters, in every run (sync and async).
- **Measurements:**
  - End-to-end workflow latency is `close_time − start_time` from cluster-a's visibility database, so it is exact per workflow.
  - API latencies and commit waits are averages from cluster-a's Prometheus metrics, diffed before and after each run.
- **Machine:** one 4-CPU, 15 GB VM runs everything: both clusters, Postgres, omes and its workers. Async capacity is about 40 workflows/s, and closed-loop runs are CPU-bound. **Absolute numbers are not representative of production**; compare sync against async within the same table.

Raw results are in `results.jsonl`. To reproduce, run `start.sh`, then `tools/admin connect`, then `test1.sh` and `test2.sh`, then `summarize.py`.

## Test 1: one global namespace, sync vs async

### Fixed load: 10 workflows/s, ~25% of capacity, two trials each

| RTT | Mode | E2E p50 | E2E p90 | E2E p99 | Start | RespondWorkflowTaskCompleted | RespondActivityTaskCompleted | Commit wait (avg) |
|---|---|---|---|---|---|---|---|---|
| 40 ms | async | 40 / 41 ms | 49 / 49 ms | 70 / 75 ms | 8.8 ms | 5.7 ms | 5.7 ms | — |
| 40 ms | **sync** | **310 / 303 ms** | 342 / 332 ms | 368 / 350 ms | 66 ms | 57 ms | 57 ms | 46 ms |
| ~0 | async | 41 ms | 47 ms | 82 ms | 8.0 ms | 5.4 ms | 5.9 ms | — |
| ~0 | **sync** | **79 ms** | 94 ms | 1133 ms ⚠ | 35 ms | 13.5 ms | 13.3 ms | 9.6 ms |

- **Per commit:** a sync commit costs one RTT plus about 6 ms (apply on the standby, plus up to 5 ms of ack coalescing). At ~0 RTT it costs about 10 ms. Each user-facing API call gets that much slower.
- **Per workflow:** all 7 transactions wait, and 6 of the waits are on the critical path. End-to-end latency goes up by about 6 × (RTT + overhead): +265 ms at a 40 ms RTT, +38 ms at ~0.
- ⚠ **Reconnect stall:** the ~0 RTT p99 comes from 9 workflows in the last second of the run. They all completed together about 1.7 s late, because the replication streams reconnected at that moment (see finding 2).

### Closed loop: 16 workflows in flight (CPU-bound)

| RTT | Mode | Throughput | E2E p50 | E2E p99 | Commit wait (avg) |
|---|---|---|---|---|---|
| 40 ms | async | 37.1 wf/s | 328 ms | 786 ms | — |
| 40 ms | **sync** | **28.9 wf/s (−22%)** | 451 ms | 645 ms | 60 ms |
| ~0 | async | 39.7 wf/s | 307 ms | 737 ms | — |
| ~0 | **sync** | **35.3 wf/s (−11%)** | 365 ms | 659 ms | 28 ms |

## Test 2: four global namespaces on the same cluster pair, one sync and three async

All four namespaces run at the same time with a 40 ms RTT. The baseline is the same run with all four namespaces async.

### Fixed load: 5 workflows/s per namespace, 20/s total (~50% of capacity), two trials

| Run | Namespace | E2E p50 (trial 1 / 2) | E2E p99 (trial 1 / 2) | Start | Commit wait (avg) |
|---|---|---|---|---|---|
| all async | m-sync (async here) | 52 / 63 ms | 184 / 153 ms | 12 ms | — |
| all async | m-async-1..3 | 36–53 / 53–66 ms | 175–217 / 152–180 ms | 9–18 ms | — |
| mixed | **m-sync (sync)** | **318 / 335 ms** | 533 / 531 ms | 70–85 ms | 49–52 ms |
| mixed | m-async-1..3 | 41–59 / 64–74 ms | 141–159 / 151–159 ms | 10–16 ms | — |

- **The sync namespace** behaves as in test 1: +~270 ms end to end.
- **Async neighbors:** no clear effect. Their p50 averages 61 ms in mixed runs against 53 ms in all-async runs, which is inside the 36–66 ms trial-to-trial spread, and their p99 is no worse.

### Closed loop: 8 workflows in flight per namespace (32 total, saturating the machine)

| Run | Namespace | Throughput | E2E p50 | E2E p99 | Commit wait (avg) | Commit timeouts |
|---|---|---|---|---|---|---|
| all async | each of the 4 | 9.6–10.1 wf/s (39.6 total) | 602–640 ms | 1.75–1.79 s | — | — |
| mixed | **m-sync (sync)** | **3.3 wf/s** | 979 ms | **12.9 s** ⚠ | 174 ms | 12 |
| mixed | m-async-1..3 | 11.8–12.2 wf/s (39.5 total) | 454–499 ms | 1.44–1.46 s | — | — |

- **Throughput under saturation:** the sync namespace gets about a third of its async throughput. Total throughput stays the same, and the async namespaces absorb the spare capacity.
- **Commit wait grows to 174 ms:** that's 3× test 1 under similar load. The sync namespace's replication tasks share the per-shard replication lane with three times as much async traffic, so its commits wait behind that backlog (head-of-line blocking, as the design predicted).
- ⚠ **The tail is lost workflow tasks plus a stream reconnect** (findings 2 and 3). It is not a steady-state effect.

## Test 3: three clusters

- **Clusters:** `cluster-a` (active), `cluster-b` and `cluster-c`. cluster-c has **8 history shards** against 4 on the others, so each active shard streams to two cluster-c shards, and the commit must wait for both.
- **Namespaces:** `t3` is replicated to all three. `t2c` is replicated to a and b only and serves as the two-cluster comparison inside the same environment.
- **Network:** every pair of clusters is connected through delay proxies (`start3.sh`, `test3.sh`).
- **Load:** fixed at 10 workflows/s, 40 ms RTT to both standbys unless noted.

| Run | E2E p50 | E2E p99 | Commit wait (avg) | Notes |
|---|---|---|---|---|
| t3 async (trials 2, 3) | 44 / 41 ms | 251 / 109 ms | — | Trial 1 discarded as warm-up: async p50 was 4.2 s right after cluster-c started. |
| **t3 sync** (trials 2, 3) | **346 / 340 ms** | 3.2 / 1.4 s ⚠ | 72 / 55 ms | Each commit waits for the slower of two standbys. |
| t2c sync (a, b only, same environment) | 308 ms | 369 ms | 46 ms | Same as the two-cluster setup. cluster-c doesn't slow a namespace it isn't part of. |
| t3 sync, **uneven standbys** (b 40 ms RTT, c 80 ms RTT) | 529 ms | 701 ms | **88 ms** | The commit is set by the slowest standby: 80 ms + ~8 ms. |
| t3 closed loop, 16 in flight: async vs sync | 387 vs 556 ms | 981 vs 1175 ms | — / 80 ms | 29.7 vs 21.9 wf/s (−26%; −22% with two clusters). |

- **Consistency check:** all 8,471 `t3` workflows from these runs are present and completed in all three clusters' databases (visibility and executions rows match).
- **No 10 s commit timeouts.** The few counted "timeouts" (0–4 per run) are callers that gave up during reconnect stalls. Their context was cancelled, mostly `RecordWorkflowTaskStarted`, which then cost a 10 s workflow task timeout.
- ⚠ **Reconnect stalls are more frequent and longer.** A sync commit now waits through a stream reconnect on either standby's connection. These happen about every 5 minutes per connection, so the namespace sees one about every 2.5 minutes. With cluster-c's 8 streams per active cluster, a stall lasts about 2–3.5 s, against about 1.5 s with two clusters.

## Findings

1. **Deadlock found and fixed (`b33e181`).** Under load, the standby sometimes pulls state from the active to apply a task (`SyncWorkflowState`, `GetWorkflowExecutionRawHistory[V2]`). Those RPCs took the workflow lock as regular callers, so they waited behind a writer that was itself waiting for the standby, which caused 10 s stalls. Before the fix, sync closed-loop throughput at a 40 ms RTT was 18.2 wf/s with 249 timeouts; after it, 28.9 wf/s with 0 timeouts. All numbers above are from the fixed build.
2. **Every replication stream reconnect stalls all sync commits, here for about 1.5 s (2–3.5 s with three clusters, where a commit waits on either standby's reconnects).** The frontend sends a GOAWAY every 5 minutes (`frontend.keepAliveMaxConnectionAge`), so cross-cluster connections, and the streams on them, are recycled regularly. Async mode doesn't notice. Sync mode turns each reconnect into a p99 spike. Mitigation: open the new stream before closing the old one, or exempt replication connections from max-connection-age.
3. **Losing the poller loses the workflow task.** When the worker cancels a poll while `RecordWorkflowTaskStarted` waits for its commit, the workflow task is started on the server but never delivered, so the workflow stalls for the workflow task timeout (10 s). This can happen today too, but a ~50 ms wait instead of a ~5 ms write makes the window 10× wider. Mitigations: don't hold the poll response for the commit (the "durable-ack" relaxation), or have matching detect and re-dispatch the task.
4. **Sync namespaces need their own replication lane.** Under contention, sharing a lane with async traffic multiplies commit wait (finding from the closed-loop mixed run).
5. **Ack coalescing:** the 5 ms eager-ack interval is a visible share of commit latency when the RTT is low. Try 1 ms or event-driven acks.
