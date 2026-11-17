# Quorum (sync) replication for Temporal namespaces: design sketch

Status: draft for discussion. Based on `temporalio/temporal` at `23e4eab` (2026-11-16).

## TL;DR

**Yes, this is feasible**, and Temporal already has most of the Raft building blocks. The work splits in two:

- **The steady-state path is fairly easy.** "Hold every side effect until a majority acks" maps onto pieces that already exist. Every side effect already leaves through a transactional outbox (history task queues). Replication already acks per-shard watermarks back to the sender. Graceful failover already gates task executors.
- **Uncommitted mutable state is invisible too, not only side effects.** No RPC, task or other transaction may read a workflow's mutable state until its last write is committed. The gate is the workflow lock: whoever holds it waits for commit before using the state (§3.3). This serializes writes per workflow at quorum speed, and the replication sender must stop needing the workflow lock, or it deadlocks against a writer waiting for the very commit it has to ship.
- **Leader change is the hard part, and today's failover is not safe for it.** A majority-commit rule only means something if an unplanned failover never loses a committed write. Today that isn't guaranteed: with 3 clusters, the surviving clusters B and C can each hold committed writes the other lacks, on different shards. So you need four new pieces that Temporal doesn't have:
  - an election step that uses the failover version as the term,
  - fencing, so voters stop acking the old leader,
  - catch-up from the surviving peer before the new leader goes active,
  - rollback of the old leader's uncommitted tail when it comes back.

You also need at least **3 replicas** for a sync namespace. With 2, any single failure stops progress.

## 1. How replication works today, mapped to Raft

| Raft concept | Temporal today | Where |
|---|---|---|
| Log | Per-shard replication task queue. Task IDs are monotonic per shard, allocated with all other tasks in the same transaction. | `service/history/shard/task_key_manager.go:45`, `task_key_generator.go:52` |
| AppendEntries | `StreamWorkflowReplicationMessages`, a bi-directional stream per (source shard, target shard). The sender reads tasks from the DB (through a read buffer) and converts them when it sends them, from current mutable state or history. | `replication/stream_sender.go:498`, `ack_manager.go:522`, `raw_task_converter.go` |
| matchIndex | The receiver's `SyncReplicationState.inclusive_low_watermark`: the lowest task not yet applied (written to the target DB). The sender persists it per remote reader as `AckedReplicationTaskIDs[cluster][shard]`. | `stream_receiver.go:273`, `executable_task_tracker.go:111`, `stream_sender.go:257`, `shard/context_impl.go:434` |
| commitIndex | **Doesn't exist.** Acks are only used for queue GC and for checking handover catch-up. | |
| Apply to state machine | Writes and their side effects run without waiting for any peer. Side effects are transfer, timer, outbound, visibility and CHASM tasks, plus RPC responses. | `docs/architecture/history-service.md` (transactional outbox) |
| Term | Namespace **failover version**: monotonic and unique per cluster. It's already used for conflict resolution (higher version wins). | `common/namespace/namespace.go:214` |
| Leader election | Operator/automation calls `UpdateNamespace(activeCluster=X)`. Namespace metadata replicates asynchronously. | `service/frontend/namespace_handler.go` |
| Leadership transfer | Graceful failover (`REPLICATION_STATE_HANDOVER`): block writes and task execution, wait for the target to catch up, then switch. | `service/worker/migration/handover_workflow.go`, `transfer_queue_active_task_executor.go:145` |
| Log truncation on the old leader | NDC conflict resolution: the branch with the higher version wins, and events from the losing branch are *reapplied*. | `service/history/ndc/conflict_resolver.go`, `events_reapplier.go` |
| Per-tenant log isolation | Per-namespace isolated lanes. Each lane has its own watermark (built for throttling noisy namespaces). | `proto/.../replication/v1/message.proto:81,112`, `stream_receiver.go:49` |

So Temporal is already a leader-based replicated log. What's missing is the commit index, the rule that applies only committed state to the outside world, and a leader change that preserves committed entries.

## 2. Guarantees for a sync namespace

With N replica clusters and quorum Q = ⌊N/2⌋+1:

- **G1 (durability):** Before Temporal makes any effect visible outside the active cluster, the state that produced it is durably applied on at least Q clusters, counting the active one. Visible effects include RPC success, task dispatch to a worker, a callback, a child start, a parent notification and a read result.
- **G2 (RPO = 0 for any minority failure):** Losing up to N−Q clusters, including the active one, loses no write that anyone observed.
- **G3 (no split-brain visibility):** A partitioned old leader can keep writing locally, but it can't make any of those writes visible, because it can't reach Q.
- **G4 (no reads of uncommitted state):** Mutable state written by a transaction can't be observed until that transaction commits. That covers RPCs (reads and writes), queue tasks and later transactions on the same workflow. The only exception is the replication path itself, which has to read uncommitted state to ship it.
- **Non-goal:** Linearizable behavior for writes that nobody observed. Uncommitted writes on a failed leader are discarded, like an uncommitted Raft log tail. A client whose RPC timed out must retry, which is already the contract today.

Async and sync namespaces coexist on the same shards. The mode is a per-namespace replication-config field.

## 3. Steady-state protocol

### 3.1 Commit index

For a source shard `s`, a lane `ℓ` (the sync namespace's isolated lane) and a peer cluster `p`:

```
ack[p][s][ℓ]   = min over target shards t of p that s streams to: inclusive_low_watermark(s→t, ℓ)
commit[s][ℓ]   = the (Q-1)-th largest value of { ack[p][s][ℓ] : p ∈ peers }    // the active itself is always "acked"
```

With 3 clusters, `commit = max(ack[B], ack[C])`, so the faster peer sets the commit index. The `min over t` handles clusters with different shard counts: one source shard fans out to several target shards (`stream_sender.go:738`), and tasks filtered out for a target still advance its watermark.

`commit[s][ℓ]` is exclusive: every replication task in lane ℓ with ID < commit is durable on Q clusters.

Required changes to ack semantics:

1. **A DLQ entry must not count as an ack.** Today a NACKed task becomes a poison pill and is dropped from the tracker, which advances the watermark (`executable_task_tracker.go:125`). For sync lanes, a DLQ'd task has to hold back that peer's durable watermark. Track a second "durable" watermark alongside the progress watermark.
2. **Acks must be event-driven.** Today the receiver acks on a 1s ticker (`stream_receiver.go:235`, `history.ReplicationStreamSyncStatusDuration`), which puts about 1s on every committed write. Ack as soon as the low watermark advances, coalesced over a few milliseconds.
3. **Sender skip invariant.** The sender may skip a task, or advance `exclusive_high_watermark` past it, only if its content is already durable on the peer or is carried by a task with an ID at or below it. A task that is "covered by a later task" is **not** safe to skip on a sync lane: the watermark could move past it before the later task is applied. Every skip path (`stream_sender.go:546`, converter returning nil, and so on) needs an audit for this.
4. **Sync namespaces go on dedicated isolated lanes**, so a stuck async-namespace task or force-replication backlog can't block their watermark. This reuses the existing lane mechanism with a "pinned" lane type that never retires.

### 3.2 Commit barrier

Every transaction for a sync namespace records **`barrier = max replication-task ID created in this transaction`**. If the transaction created no replication task, the barrier is the previous one.

- The barrier is stamped onto every other history task created in the same transaction (transfer, timer, outbound, visibility, CHASM): a new field, `ReplicationBarrier`.
- Mutable state **persists** the barrier in `ExecutionInfo` (new field `last_replication_barrier`), written in the same transaction. That way a load after cache eviction, host restart or shard movement knows exactly which commit it has to wait for.
- The barrier is stamped explicitly instead of inferred from task ID order. IDs within a transaction come from one generator, but nothing guarantees replication tasks sort before the others, and the lane watermark only covers replication tasks.

**Commit test:** `committed(x) ⇔ commit[s][ℓ] > x.barrier`.

Each shard holds a `CommitTracker` with the current commit index per lane, plus a wait-list of waiters keyed by barrier. Each advance of `ack[p]` from `recvSyncReplicationState` wakes the waiters it satisfies.

### 3.3 Uncommitted mutable state is invisible

Today, once a transaction persists locally, the cached mutable state holds the new state, and the next lock holder sees it immediately. For sync namespaces, the **workflow lock becomes the visibility gate**. It works like a latch in a Raft-replicated database, held until the write is replicated.

**Rule:** any code that holds the workflow lock may use the mutable state only once `committed(ms.lastBarrier)` is true. If it isn't, the holder waits, keeping the lock, until it is.

How it works:

1. **Writer.** Acquire the lock, load the (committed) state, apply the request, persist locally. Then **keep holding the lock** and wait for `committed(newBarrier)` before releasing it and responding. Every write therefore starts from committed state; nothing is built on top of an uncommitted predecessor.
2. **Every other lock holder.** That means RPCs, queue tasks, timers, matching's `Record*TaskStarted` and CHASM. Enforce the check in one place: `workflow.ContextImpl.LoadMutableState` (`workflow/context.go:452`), and the equivalent for already-cached state. After the lock is acquired, it checks the barrier and waits if needed. This covers a writer whose deadline expired, which releases the lock with uncommitted state still in the cache, and a reload from the DB after eviction or shard movement (§3.2: the barrier is persisted).
3. **Deadline while waiting.** The waiter returns `Unavailable` and releases the lock. The state stays marked pending through its barrier, so the next holder waits too. Nothing uncommitted leaks out on a timeout.
4. **The replication sender is the only reader of uncommitted state, and it must never take the workflow lock.** Today the converter acquires the lock and calls `LoadMutableState` (`replication/raw_task_converter.go:434,620`). Under rule 1 that deadlocks: the writer holds the lock waiting for commit, while the sender waits for the lock to build the task that would produce the commit. The fix:
   - **Push-on-commit is mandatory, not just an optimization.** The writer builds the replication payload (the versioned transition and events) inside its transaction and hands it straight to the stream sender.
   - **The fallback path** (catch-up after a stream restart, cache miss) reads the persisted state from the DB without the lock. That's safe: whoever holds the lock is only waiting and doesn't mutate state until the commit arrives.
   - The same lock-free, uncommitted-read exemption applies to the source side of follower resend and backfill (`GetWorkflowExecutionRawHistoryV2`, `SyncWorkflowState`), since those serve replication too.
5. **Reads that bypass mutable state.** Each one must go through the gate before it answers:
   - **The current-execution record**: `GetCurrentWorkflowRunID` (`api/consistency_checker.go:97`) and the start-workflow conflict path, which builds "already started" and dedup responses straight from `CurrentWorkflowConditionFailedError`. These must load that run's mutable state through the gate before responding. The alternative is to also persist the barrier on the current record.
   - **History branch reads**: already bounded by `GetMutableState`'s next event ID (`api/getworkflowexecutionhistory/api.go:75`), so they're covered once that call is gated.
   - **Visibility**: covered by the task gate in §3.4.
   - Audit every other `executionManager.Get*` call in `service/history/api`.
6. **Followers.** Before an election (§4.5), passive clusters can hold an old leader's uncommitted state. For sync namespaces, send every read to the active cluster through namespace forwarding instead of serving it locally.

**Cost.** Writes to *one* workflow are fully serialized: each takes a local persist plus a quorum round trip. With a 30 ms round trip and about 10 ms of DB time, that caps a workflow at roughly 25 writes/s. This is an illustrative figure, not a measurement. Writes to different workflows still run in parallel, and shard-level ack waves batch them. Requests queued behind a pending write see longer lock waits, and so more `BusyWorkflow` / lock-timeout errors. A possible future fix is **group commit**: apply several queued requests for the same workflow in one transaction and pay the quorum round trip once.

### 3.4 Gating side effects

| Side effect | Path | Gate |
|---|---|---|
| Activity/WT dispatch to matching, child start, signal/cancel external, parent close notification, reset, delete | Transfer queue | **Yes.** In `queues.Executable.Execute` (`queues/executable.go:282`), before the task executor runs, the same way `HANDOVER` is checked today. Park the task on the commit tracker instead of busy-retrying. |
| Activity retry, which calls matching directly | Timer queue | **Yes** |
| User timers, WT/activity timeouts, which only write new state | Timer queue | Not strictly needed, since the new state is gated anyway. Gate them all for simplicity: timers almost always fire long after commit, so the cost is close to zero. |
| Nexus operations, completion callbacks | Outbound queue | **Yes** |
| Visibility writes, archival | Visibility/archival queues | Yes. Not needed for G2, but it prevents List/Count from showing state that can disappear. |
| CHASM component tasks | CHASM queues | **Yes** |
| **RPC responses**: Start, Signal, SignalWithStart, Update (accepted/completed), RespondWorkflowTaskCompleted (includes eager activities/WT), RespondActivityTask*, heartbeats, Reset, Terminate, Cancel, and dedup hits ("request ID already seen") | History handler | **Yes.** This is the most important gate: a success response *is* the side effect. |
| `RecordWorkflowTaskStarted` / `RecordActivityTaskStarted` responses (the poll result returned to a worker) | History handler | **Yes in strict mode.** Otherwise a worker can run local activities or user code against an uncommitted signal that is later discarded. |
| Reads: Describe, GetHistory (including the long poll used to fetch results), PollWorkflowExecutionUpdate, Query | History handler | **Yes.** Covered by §3.3: they can only ever load committed mutable state. |

The queue-task check on the task's own barrier is kept even though §3.3 would block the executor anyway. It parks the task *before* it takes the workflow lock, instead of holding the lock while waiting.

**RPC mechanism:** wrap the history handler. Each request context carries `maxObservedBarrier`. The workflow `Context` raises it on `LoadMutableState`, `CreateWorkflowExecution` and `UpdateWorkflowExecutionAsActive` (`workflow/context.go:452,569,758`). On success, the wrapper waits for `committed(maxObservedBarrier)` and then returns. That one hook covers every API, including dedup and read paths, without touching each handler. If the deadline expires, return `Unavailable`. The write is persisted locally, so a client retry with the same request ID hits the dedup path and waits again.

**Matching and workers need no change.** Matching only ever sees tasks that came out of gated transfer tasks. Speculative workflow tasks and Update "admitted" state are in memory and never promised to be durable, so they stay as they are.

### 3.5 No quorum

The active cluster keeps accepting and persisting writes, but every gated effect waits, so the namespace is effectively unavailable (CP). Two knobs:

- **Leader lease:** if there's been no quorum ack for `T`, reject new writes with `Unavailable` right away. This limits how much uncommitted tail builds up, which later has to be rolled back, and lets clients fail fast.
- **Degrade to async**, as an explicit operator or policy action (like MySQL semi-sync fallback). It's recorded in namespace metadata so the RPO change can be audited. Never do this silently.

## 4. Leader change (failover)

### 4.1 Planned failover

Keep the current handover workflow: block writes, wait for the target to catch up, switch. With sync replication, the wait is short as long as the target is in the commit majority. If it isn't, catch it up first.

### 4.2 Unplanned failover: why today's mechanism isn't enough

Clusters A (active), B and C. A commits shard 1's writes with acks from **B** and shard 2's writes with acks from **C**. Then A dies. Neither B nor C has every committed write. If B simply becomes active, its first write to a shard-2 workflow forks from older state. Conflict resolution then discards C's committed branch (higher version wins), and a write that a client saw succeed is lost. A per-shard watermark can't fix this on its own, because the leader is chosen per namespace, not per shard.

### 4.3 Election with fencing

The term is the failover version. This is a Paxos-style prepare round:

1. **Prepare.** The candidate B (picked by operator or automation, preferably the most up-to-date) picks `vB = next failover version for B`, greater than any version it has seen. It sends `PromoteNamespace{ns, vB}` to every replica.
2. **Promise.** A voter V accepts if `vB > promised[ns]`. It then:
   - persists `promised[ns] = vB` (local, not replicated namespace state),
   - **fences** the old term: from now on, the receiver refuses replication tasks for `ns` with version < vB. It doesn't apply or ack them, and returns `Fenced{vB}` so A learns it has been demoted,
   - replies with its persisted watermarks per (source shard, lane) for the old leader.
3. Once B has promises from Q replicas (counting itself), it is leader-elect for term vB. A can't commit anything else, because no quorum member will ack it.

Safety argument (same as Raft/Paxos): any write that was committed was acked by a commit quorum before those voters promised. The commit quorum and the election quorum intersect, so at least one promising voter has every committed write.

### 4.4 Catch-up before activation

B must collect the **union** of what the promising voters hold before it accepts writes. Followers currently keep no index of "what I applied from A". To fix that:

- **Inbound relay log:** in the same DB transaction where a follower applies a replication task from the active, it writes a small *relay* task into its **own** replication queue: `{source cluster, source shard, source task ID, execution key}`. These tasks are a new type that the sender normally never streams. They're trimmed once every voter's persisted watermark from that source is past them (voters gossip watermarks).
- **Catch-up stream:** B opens a stream to each promising voter V and asks for relay tasks with `source task ID ≥ B's persisted watermark` for each source shard. V converts them from its own mutable state into `SyncVersionedTransition` tasks; this reuses the stream sender and the state-based converter. Applying them is idempotent, so a conservative (lower) persisted watermark on B is fine.
- When every shard is caught up, B writes namespace metadata `active=B, version=vB` and starts accepting writes. Its first transactions establish the new term, just like a Raft leader committing an entry in its own term.

**Prerequisite:** state-based replication (transition history and versioned transitions) has to be enabled for sync namespaces. That's what makes "send the full current state from any replica" possible and idempotent.

The catch-up gap is bounded by the replication lag between the two followers, normally milliseconds of traffic, so it adds little to RTO.

### 4.5 When the old leader comes back

1. A gets `Fenced{vB}` from voters, or sees vB in namespace metadata, and becomes passive.
2. **Truncate the uncommitted tail.** B returns the per-shard commit point it reached during catch-up. Every A replication task above that point is an uncommitted write. For each affected execution, A resyncs from B (`SyncWorkflowState`, `historyservice/v1/service.proto:429`). If the execution doesn't exist on B (an un-acked `StartWorkflow`), A deletes it locally (`ForceDeleteWorkflowExecution`).
3. A's gated transfer, timer and outbound tasks for those transactions never committed, so they never ran. Discard them.
4. **Turn off NDC event reapply for sync namespaces**, or make it a policy. Reapplying signals from the discarded branch would bring back writes that were never acknowledged. Raft semantics say they disappear and the client retries.

### 4.6 Namespace metadata

Namespace metadata stays asynchronously replicated, and that's fine. Safety comes from voters acking only the highest term they've promised, not from every cluster agreeing on `activeCluster` at the same moment. A cluster that wrongly thinks it's active simply can't commit.

## 5. Performance

Added latency per gated effect ≈ **one round trip to the nearest peer, plus the target DB commit, plus streaming overhead**. To get close to that floor:

- Event-driven acks (§3.1). This is mandatory.
- **Push on commit** (required by §3.3, not optional): after the local transaction commits, send the replication task straight from memory, including the converted payload. Fall back to a lock-free DB read for catch-up only. Today's sender reads from the DB after a notification and converts on send, under the workflow lock.
- **Per-workflow write rate** is bounded by local persist + quorum round trip (§3.3). Group commit is the main lever if that's too low.
- Keep sync lanes separate from async lanes and from low-priority force-replication.
- The fastest peer sets the commit index. Put one replica close to the active (same region, different AZ or cell, or a nearby region) for low latency, and one far away for disaster recovery.

**Cost per workflow step (strict mode).** One activity step has about 5 quorum waits on its critical path: the WT completion response, the activity dispatch, the activity-started response, the completion response and the WT dispatch. With a 30 ms round trip, that's roughly +150 ms per step, compared with roughly 0 today. Figures are illustrative. If that's too much, offer a **"durable-ack" mode** that still gates client-acknowledged writes, cross-workflow effects, callbacks and reads, but doesn't gate `Record*TaskStarted` responses or dispatch. The cost of that relaxation is that a worker may run code against state that is later discarded, which is within at-least-once semantics.

Throughput: commits are per shard and per lane prefix, so they batch naturally. A busy shard pays one round trip per ack wave, not one per write.

## 6. Membership changes

- A cluster added to a sync namespace joins as a **learner**: it receives replication but isn't counted in Q. It becomes a voter once force replication finishes and it's within lag. The gradual-connect ramp already exists.
- Change membership one cluster at a time, or use joint consensus. Q and the voter set are versioned in namespace metadata, and every prepare and promise carries that version.
- A cheaper third replica could be a **witness** that stores relay and replication payloads but runs no workers. Catch-up could stream from it. This is future work.

## 7. Risks and open questions

1. **Head-of-line blocking:** one stuck or poison task on a sync lane stalls commit for every sync workflow on that shard, for that peer. With 3 replicas the other peer covers it. DLQ policy for sync lanes needs a decision: surface it as an availability incident, never drop it silently.
2. **Completeness of the mutable-state gate (§3.3):** any path that reads execution state without `LoadMutableState` leaks uncommitted state. Examples: direct persistence reads, the current-execution record, in-memory caches outside the workflow context, CHASM component reads. This needs a full inventory and a test hook that fails if uncommitted state is ever returned. In-memory-only structures (speculative workflow tasks, the Update registry's admitted state, the query registry) were never durable and stay out of scope.
3. **Lock hold time:** workflow locks are now held across a cross-region round trip. Lock-wait timeouts, `BusyWorkflow` thresholds and the queue processors' lock-acquire behavior need retuning for sync namespaces.
4. **Per-workflow active cluster** (`ActiveClusterName(routingKey)`, `namespace.go:193`): the term and the promise are really per (namespace, routing group). The design generalizes, but the promise state has to be keyed that way.
5. **Cross-namespace effects** (a child or a Nexus call into an async namespace) are gated on the parent's commit only. The target namespace keeps its own guarantees.
6. **Shard movement** inside a cluster: the commit tracker is rebuilt on shard load from persisted remote-reader acks (`AckedReplicationTaskIDs`). That's conservative and therefore safe.
7. **Clients can see extra latency or timeouts while a peer is degraded.** That needs SLOs, plus metrics for commit lag per shard and lane.

## 8. Suggested phases

1. **Observability only:** compute `commit[s][ℓ]`, event-driven acks, a durable-vs-progress watermark, and metrics for "would-be gating delay". No behavior change, but it measures the latency cost on real traffic.
2. **Gating:** persisted barrier, `CommitTracker`, the mutable-state gate in `LoadMutableState`, a lock-free push-on-commit replication sender, gates in queue executables and the history handler. Behind a per-namespace flag, with failover limited to the graceful handover path. This alone gives G1, G3 and G4, plus G2 for planned failovers.
3. **Safe unplanned failover:** relay log, promote/fence RPCs, catch-up stream, rollback of the old leader, reapply off. This completes G2.
4. **Latency work:** group commit per workflow, the durable-ack mode, witness replicas.
