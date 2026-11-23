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

Two more options sit on top of this:

- **Sync can be chosen per event type (§3.6).** A namespace lists which events trigger the sync path, for example only `WorkflowExecutionStarted`. A transaction containing one of those events waits for quorum. All other transactions take today's async path. Because the replication log is ordered, a committed sync transaction also makes every earlier async transaction in its lane durable.
- **Two clusters with a degraded mode (§5).** Majority quorum needs 3 clusters, so with 2 any single failure stops progress. For 2 clusters, commit uses an *in-sync replica set* (ISR, the Kafka model) instead. A failed peer is removed from the set, the survivor keeps running in a degraded (effectively async) state, and the peer is added back once it has caught up. A dead peer and a network partition look the same from inside 2 clusters, so changes to the set are made through the **control plane**, which acts as the arbiter: a CAS record of epoch, leader and in-sync set, plus a lease that the active must hold while degraded.

## v1 decisions

Agreed scope for the first implementation. Later sections describe the full design. Where they differ, this list and the "v1 failover" section below win for v1.

- **Two clusters, full quorum.** Sync mode is refused for namespaces replicated to more than two clusters. With three clusters, full quorum adds latency without riding out a single failure; three clusters will come later with majority quorum (§4.3–4.4). The v1 design keeps the extension points listed in "v1 failover".

- **Everything is sync.** For a sync namespace, every transaction waits for quorum: a fixed sync policy of "all". Per-event-type triggers (§3.6) are deferred. The config shape and the per-transaction classification hook are kept so they can be added later without a migration. Effects for v1:
  - every transaction stamps and persists the barrier,
  - no async transactions in a sync namespace,
  - no mixed per-transaction classification.
- **Namespace-level only.** Sync mode is refused for namespaces whose active cluster is resolved per business ID or routing key. Epoch, in-sync set, lease and fencing are all per namespace.
- **Strong consistency on the workflow.** A pending sync write blocks all access to that workflow (§3.3), reads included.
- **A quorum-wait timeout returns `Unavailable`.** A retry must return the original result. That requires:
  - the duplicate-request check waits on the same commit through the gate,
  - every duplicate-detection key lives in replicated mutable state (audit required),
  - a retry that lands on the new active after a failover is deduplicated there if the original write reached it.
- **Uncommitted writes of an old active are kept, not discarded** (superseded the earlier "discard" decision). A write that never committed was never reported as successful (the client got `Unavailable`, a timeout, or nothing), so its outcome is unknown and it may take effect late. When the old active returns, its leftover replication tasks apply as today, and NDC event reapply stays on. Discarding them is a possible later refinement (see "v1 failover").
- **One ack watermark (no durable-watermark work).** v1 counts the existing progress watermark as the commit ack. A task that goes to the replication DLQ still advances it, so a write can be treated as committed while that standby is missing it. If the standby is then promoted, that write is lost.
  - **Mitigation:** any replication DLQ entry for a sync namespace is a page-level alert. The DLQ'd workflows are re-replicated before that standby is eligible for a clean promote.
  - The proper fix is logged as future work (§10).
- **The control plane is push-only.** It calls the data plane: existing failover APIs, `UpdateSyncReplicationState` for the in-sync set, and `GetSyncReplicationStatus` for health, lag and drain state. The data plane never calls the control plane.
- **Failure detection is external.** The failure detector and the control plane make all membership and failover decisions, including human overrides, through the control plane's serialized record (§5.3–5.4). The data plane keeps only local safety reflexes: demote when fenced, return `Unavailable` on a quorum-wait timeout.

## v1 failover (two clusters)

Builds on Temporal's existing failover. It supersedes §4–5 where they differ for v1.

### Model

- **Leader and term are the namespace's existing active cluster and failover version.** Failover itself uses the existing APIs: the `namespace-handover` workflow for planned failover, and `UpdateNamespace(ActiveClusterName)` for forced failover.
- **The only new per-namespace state is the in-sync set**, pushed by the control plane and stored as a local, never-replicated field of the namespace metadata:
  - `in_sync_clusters`,
  - `required_acks` (1 for two clusters; also 1 for three-cluster majority later),
  - `version` (stale or duplicate pushes are ignored).
- **Commit rule:** a write commits when `required_acks` members of the in-sync set other than the active have acknowledged it. With in-sync = {active} (degraded), it commits locally.
- **Failover only to an in-sync cluster.** Automatic failover (planned or forced) targets only a member of the in-sync set. A double failure (the standby was dropped, then the active fails) leaves the namespace unavailable until the active returns. The plain forced failover remains a manual escape hatch: an operator guarantees the old active is down and accepts losing its degraded-period writes. No lease is needed, because a degraded active is never replaced automatically.
- **Fencing notice:** once a cluster is active for namespace N at failover version v, every replication ack it sends to another cluster carries "N is at v". The notice travels in the same message as the watermark it covers. The receiving cluster:
  - counts an ack toward a write only if the notice is **not newer than the failover version the write was made under**. The check is per write, not against the receiver's current version; the model checker found the latter unsafe;
  - fails pending commits of writes made under older versions immediately;
  - rejects new writes with `NamespaceNotActive` naming the new active.
- **Control-plane failover rules** (found necessary by the model checker; each part is needed):
  1. A forced failover drops the old active from the in-sync set. It is presumed dead, so the new active runs degraded until the old one rejoins.
  2. A cluster rejoins the in-sync set, and so can be failed back to, only when all of these hold:
     - the active already waits for it on every shard (two-step rejoin, step 1);
     - it has acked every task of the active (caught up);
     - it knows the current failover: its namespace metadata shows the current failover version;
     - its own leftover tasks are drained: the active has processed every task it wrote.

  Without these rules, after failovers A → B → A, a stale write from B at an intermediate version can replace a write A had already acknowledged (Raft's "Figure 8"). The alternative fix is Raft's term rule: commit and expose only writes made under the current failover version, with a version-bump write before exposing older state. It is rejected for v1 because it needs a no-op transaction in the data plane and a write on the first read after every failover. The control plane, which keeps a record of every operation and can query the clusters, enforces the rules above instead.

### Flows

| Flow | How | New pieces |
|---|---|---|
| Planned failover | Existing handover workflow, unchanged | None expected (needs a test) |
| Standby fails | Control plane pushes in-sync = {A}; A commits alone | Degraded mode |
| Forced failover (A down or partitioned, B in sync) | `UpdateNamespace(active = B)` on B; control plane drops A: in-sync = {B} | Fencing notice |
| Old active returns | A learns the failover and stops writing (fenced); A → B drains to A's fixed end; B → A catches up; two-step rejoin checks all of it | Status: known failover version, both drain directions, applied in-sync version per shard |
| Failback | Planned failover once A is in sync | None |
| Active fails while degraded | Not supported automatically: the namespace waits for A. Manual escape hatch: forced failover by an operator who guarantees A is down, accepting loss of degraded-period writes | None |

**Uncommitted writes of the old active:** kept. They apply on B when A's queue drains, and conflict resolution and event reapply handle workflows B already wrote, as today. "A is in sync again" means both directions are drained (A → B to A's fixed end, B → A caught up) and the rejoin step is complete. The alternative, discarding them, needs corrective tasks from B, a rollback mode in snapshot apply, and reapply off. It is deferred.

### API

- **`UpdateSyncReplicationState`** (admin, new): sets the in-sync set and `required_acks` for one or more namespaces.
- **`GetSyncReplicationStatus`** (admin, new), per namespace:
  - stored state, and the lowest version applied across shards (for rejoin step 1);
  - role (active, standby, fenced) and the failover version this cluster knows;
  - per peer and per shard: acked task ID against this cluster's highest replication task ID. Queried on both clusters, this gives both drain directions;
  - `caught_up(peer)` and `drained_to(peer)` precomputed from the above;
  - as follower, the applied position per source shard (for three-cluster candidate choice later).
- **Reused:** `UpdateNamespace`, the handover workflow, `GetReplicationStatus`.
- **Internal:** a fencing-notice field on the replication ack message.

### Extension points for three clusters and majority quorum

| Piece | v1 (two clusters) | Majority later |
|---|---|---|
| Commit rule | `required_acks` of the in-sync set, value 1 | Same code, in-sync = {A, B, C} |
| Fencing | Notice keyed by failover version | Same; every follower sends it |
| Status API | Includes follower applied positions | Used to pick the most up-to-date candidate |
| Election | Implicit (one follower) | Prepare/promise step before `UpdateNamespace(active)` |
| Catch-up | Not needed | Follower record of applied writes; pull from the other follower before activating |

### Verification

`sync-replication-model/` holds a TLA+ spec (checked with TLC) and an equivalent Python model checker, with results and counterexamples. All invariants hold within the checked bounds. Every deliberately broken variant is caught, including each part of the control-plane rules and the per-write fencing check.

### Build order

1. Test planned failover on a sync namespace.
2. Fencing notice and its handling (per-write check); test a forced failover with A partitioned.
3. In-sync state storage and the two admin RPCs (proto changes).
4. Commit by in-sync set; degraded mode.
5. Rejoin status (known failover version, both drain directions, applied in-sync version); full A → B → A test.

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

With N replica clusters and quorum Q = ⌊N/2⌋+1 (majority mode; §5 covers ISR mode). G1–G4 apply to *sync transactions*: every transaction when the namespace is fully sync, or only those containing a configured event type (§3.6).

- **G1 (durability):** Before Temporal makes any effect visible outside the active cluster, the state that produced it is durably applied on at least Q clusters, counting the active one. Visible effects include RPC success, task dispatch to a worker, a callback, a child start, a parent notification and a read result.
- **G2 (RPO = 0 for any minority failure):** Losing up to N−Q clusters, including the active one, loses no write that anyone observed.
- **G3 (no split-brain visibility):** A partitioned old leader can keep writing locally, but it can't make any of those writes visible, because it can't reach Q.
- **G4 (no reads of uncommitted state):** Mutable state written by a transaction can't be observed until that transaction commits. That covers RPCs (reads and writes), queue tasks and later transactions on the same workflow. The only exception is the replication path itself, which has to read uncommitted state to ship it.
- **Non-goal:** Linearizable behavior for writes that nobody observed. Uncommitted writes on a failed leader are discarded, like an uncommitted Raft log tail. A client whose RPC timed out must retry, which is already the contract today.

Async and sync namespaces coexist on the same shards. The mode, and the list of event types that trigger sync, are per-namespace replication-config fields.

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

1. **A DLQ entry should not count as an ack** (*deferred to future work, §10*). Today a NACKed task becomes a poison pill and is dropped from the tracker, which advances the watermark (`executable_task_tracker.go:125`). The full fix is a second "durable" watermark that a DLQ'd task holds back. v1 keeps today's single watermark and accepts the gap (see the v1 decisions).
2. **Acks must be event-driven.** Today the receiver acks on a 1s ticker (`stream_receiver.go:235`, `history.ReplicationStreamSyncStatusDuration`), which puts about 1s on every committed write. Ack as soon as the low watermark advances, coalesced over a few milliseconds.
3. **Sender skip invariant.** The sender may skip a task, or advance `exclusive_high_watermark` past it, only if its content is already durable on the peer or is carried by a task with an ID at or below it. A task that is "covered by a later task" is **not** safe to skip on a sync lane: the watermark could move past it before the later task is applied. Every skip path (`stream_sender.go:546`, converter returning nil, and so on) needs an audit for this.
4. **Sync namespaces go on dedicated isolated lanes**, so a stuck async-namespace task or force-replication backlog can't block their watermark. This reuses the existing lane mechanism with a "pinned" lane type that never retires.

### 3.2 Commit barrier

Every **sync transaction** (§3.6) records **`barrier = max replication-task ID created in this transaction`**. An async transaction, or one that created no replication task, keeps the previous barrier. Mutable state therefore tracks `lastSyncBarrier`, and the rest of this section calls it simply "the barrier".

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

These gates apply to effects of **sync transactions** (§3.6). Effects of async transactions go out immediately, as they do today.

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
- **Degrade to async**: in ISR mode (§5), drop the failed peer from the in-sync set through the control plane and keep running in a degraded state. In majority mode with N≥3 this is an explicit operator action. Either way it's recorded so the RPO change can be audited, and it never happens silently.

### 3.6 Selective sync per event type

**Configuration.** The namespace replication config gets a sync policy: a set of **sync triggers**.

- **History event types**, for example `WORKFLOW_EXECUTION_STARTED`, `WORKFLOW_EXECUTION_SIGNALED`, `WORKFLOW_EXECUTION_UPDATE_ACCEPTED`, `WORKFLOW_EXECUTION_COMPLETED`.
- **Non-event transitions** that have no history event at the time they happen: activity started, activity heartbeat, CHASM component transitions. These are named as operation types.
- An empty set means a fully async namespace. "All" means every transaction is sync, as in §3.1–3.4.

**Classification is per transaction, not per event.** Temporal persists events in batches, so one transaction can hold several events. A transaction is sync if it contains *any* trigger. The decision belongs in close-transaction (`CloseTransactionAsMutation` / `AsSnapshot`), which sees every event and task in the transaction.

- **Example:** the StartWorkflow transaction holds `WorkflowExecutionStarted` and `WorkflowTaskScheduled`. It's sync, so the client's run ID *and* the first workflow-task dispatch both wait for commit.
- A `RespondWorkflowTaskCompleted` that only schedules activities is async.

**Rules:**

- **Sync transaction:** §3.2–3.4 apply in full. The barrier is stamped and persisted, and the response, the transaction's tasks and its mutable state all wait for commit.
- **Async transaction:** no wait. The response and tasks go out immediately. Its tasks carry the workflow's current `lastSyncBarrier`, which was already committed when the state was loaded through the gate, so they never park.
- **The mutable-state gate (§3.3) checks `lastSyncBarrier`**, and only sync transactions advance it. That has two consequences:
  - State written by async transactions is visible immediately, as it is today.
  - A *pending* sync write blocks everything on that workflow, async writes included, until it commits. Otherwise an async write's response or tasks would expose the sync write's uncommitted state.
- **Free extra durability from ordering.** The replication log is ordered within each shard lane. When a sync transaction commits, every earlier transaction in that lane is durable too, including async ones and ones from other workflows.

**Lane sharing.** Sync and async transactions of a namespace must stay in **one lane**, because per-workflow ordering requires it: event-based replication needs earlier events first. So a sync commit waits behind any async backlog ahead of it in the lane, and a burst of async traffic shows up as sync commit latency. Measure this in phase 1 (§9).

**What failover guarantees per class.** Committed sync transactions have RPO = 0, and unplanned failover still needs §4 (election, fencing, catch-up) to protect them. Async transactions keep today's RPO, which equals the replication lag. When the old leader returns (§4.5), its uncommitted tail splits by class:

- **Uncommitted transactions containing a sync trigger** are discarded. Their client never got an ack.
- **Async-only transactions beyond the commit point** keep today's behavior: their events are reapplied to the new leader's branch. These may already have been visible, because async means visible immediately.

So the reapply policy follows the trigger set.

**Example policy: sync = {`WorkflowExecutionStarted`}.**

- A Start is acknowledged only after quorum, so an acknowledged workflow can't disappear in a failover.
- A retry with the same request ID returns only after that commit. It goes through the gate on `lastSyncBarrier`.
- Everything later in the workflow is async. After an unplanned failover, the workflow may resume from an earlier point, but it exists.
- Steady-state cost: one quorum wait per workflow start, and nothing per step.

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
4. **No NDC event reapply for uncommitted sync transactions.** Reapplying signals from the discarded branch would bring back writes that were never acknowledged. Raft semantics say they disappear and the client retries. Async-only transactions keep today's reapply behavior (§3.6).

### 4.6 Namespace metadata

Namespace metadata stays asynchronously replicated, and that's fine. Safety comes from voters acking only the highest term they've promised, not from every cluster agreeing on `activeCluster` at the same moment. A cluster that wrongly thinks it's active simply can't commit.

## 5. Two clusters: in-sync replica set with degraded mode

### 5.1 Why majority doesn't work with 2

With N = 2, Q = 2: every commit needs both clusters, so either failure blocks all sync transactions (§3.5).

### 5.2 In-sync replica (ISR) set

In ISR mode the commit rule is **"acked by every member of the ISR"** instead of "acked by a majority of all replicas". The leader is always in the ISR.

```
commit[s][ℓ] = min over p ∈ ISR \ {leader} of ack[p][s][ℓ]      // +∞ if the ISR is just the leader
```

- **Healthy, ISR = {A, B}:** the same as majority mode with 2 clusters. Every sync transaction waits for B.
- **B fails, ISR = {A}:** commit happens on local persist. The namespace is now **degraded**: sync transactions stop waiting, and the RPO for everything becomes async-level until B rejoins.
- **Who can become leader:** only an ISR member can become leader without losing data. It has every committed write, because commit required its ack. That makes §4.4's catch-up unnecessary in ISR mode. Fencing (§4.3) still applies.

This is Kafka's ISR model with `min.insync.replicas` as a knob, and close to MySQL semi-sync with fallback.

### 5.3 The control plane as arbiter

From inside 2 clusters, "B is dead" and "A and B are partitioned" look the same. Suppose A drops B on its own while B is only partitioned off, and failover automation promotes B. Now both clusters are active. Writes A committed while degraded are lost when the two reconcile, which is exactly what sync replication was supposed to prevent.

So ISR membership and the epoch must live outside both clusters. **The control plane plays that role.** It is both the record and the decision-maker. The clusters report health and replication lag to it and carry out its decisions.

**What the control plane has to provide:**

1. **A linearizable compare-and-set (CAS) record** per replication group: `{epoch (failover version), leader, isr[], degraded_since, per-shard watermarks at shrink time, degraded lease}`. Key it per cluster pair, with the list of sync namespaces it covers. Then one CAS degrades every affected namespace when a cluster fails, instead of one per namespace. Per-namespace or per-routing-group keys are possible if a finer grain is needed.
2. **A failure domain independent of both data clusters.** It must not live only in A's or B's region. It has to stay up when either region is down.
3. **Push-only interaction: the control plane calls the data plane, never the reverse.** The control plane must reach each cluster's admin endpoint. The data plane exposes:
   - `ApplyReplicationGroupState` (§5.4), which also carries lease renewals,
   - read APIs for health, per-shard ack lag, stream state and applied record versions, used by the failure detector and by the rejoin check.
4. **No part in the hot path.** It's used only for shrink, rejoin, promote and lease renewals. If the control plane is down, steady-state commits continue. Only membership changes and failovers wait.
5. **A durable audit log** of degraded windows and unclean promotes, so the RPO exposure is always known.

**The rules, all serialized by CAS on the control plane record:**

| Action | Decided by | Condition | Effect |
|---|---|---|---|
| **Shrink** | Control plane, from the lag and stream state it reads from A, plus its own probes | B's acks haven't moved on any sync lane with pending waiters for `D` (e.g. 10–30 s), or its stream has been down for `D` | CAS `isr {A,B} → {A}` at epoch e, and grant A a **degraded lease**. Only after A sees the CAS succeed may it commit without B. Waiters held at that moment are released. |
| **Lease renewal** | Control plane | A is degraded, and the epoch in its DB is unchanged | The control plane pushes a new `lease_expiry` to A periodically, for example every lease/3. If no renewal arrives before the lease expires (expiry minus a skew margin), A stops accepting writes for the group's namespaces. Behavior is the same as `HANDOVER` today. |
| **Promote** | Control plane | B ∈ isr at the current epoch, and A is unhealthy | CAS `epoch e → e', leader = B, isr = {B}`. B fences A (§4.3) as soon as it learns of the promotion, then starts degraded with its own lease. |
| **Rejoin** | Control plane, when B's lag read from A is under the threshold | B is caught up (§5.5) | Two steps (§5.4): A first confirms that it waits for B again, then the CAS `isr {A} → {A,B}` at the same epoch. The degraded lease ends. |
| **Unclean promote** | Operator through the control plane, never automatic | B ∉ isr (A died while degraded) | Allowed only **after A's degraded lease has expired**. Promotes B and accepts loss of A's writes since `degraded_since`. The control plane reports the exact per-shard window. |

**Why this is safe:**

- **Healthy mode (ISR = {A,B}):** every commit needs B's ack. Once B is promoted it stops acking A, so A can't commit, and A's shrink CAS fails on the stale epoch. That's how A learns it has been demoted.
- **Degraded mode (ISR = {A}):** A commits alone, but only while it holds the lease. An unclean promote of B waits for that lease to expire. So even when A is alive but cut off from the control plane, A has stopped writing before B starts.
- **Racing changes:** shrink and promote go through the same CAS record, so exactly one wins.

**Cost of a peer failure:** one pause of about `D` plus CAS latency on sync transactions. After that the survivor runs normally in the degraded state.

**Majority mode (N ≥ 3)** can also be driven by the control plane: it picks the candidate and runs §4.3's prepare round. There, safety comes from the voters' promises, not from the control plane record.

### 5.4 Making a decision take effect in both clusters

The control plane's central DB serializes every decision. But a decision changes nothing until the clusters apply it, so there's always a window between "committed in the control plane DB" and "applied in A and B". The design has to be safe throughout that window. It also has to be safe when one cluster can't be reached at all, which is the normal case during a failure.

**Two views, read for different purposes:**

- **Eligibility for promotion** is read *only* from the control plane DB. Only the control plane promotes, so a cluster's local, possibly stale copy never decides whether it may become leader.
- **The commit rule** (wait for B or not) is read *only* from the leader cluster's applied copy. The control plane never commits writes.

Every operation is ordered so that **both views are conservative at every moment**:

- **Loosening the leader's commit rule** (shrink: stop waiting for B) is committed in the control plane DB **first** and applied at A **second**. While A hasn't applied it, A still waits for B: stricter than needed, never unsafe.
- **Tightening the commit rule** (rejoin: wait for B again) is applied and confirmed at A **first**. Only then does the control plane DB make B eligible. If it ran the other way, the control plane could cleanly promote B while A was still committing alone.
- **A promotion** depends on fencing, not on notifying the old leader. B applies the new epoch, which fences A's stream, before it accepts any write. A may be unreachable and never hear about it. Its sync commits are blocked by B's fence (healthy mode) or stopped by its own lease (degraded mode).

| Operation | Order |
|---|---|
| **Shrink** | 1. CAS in the control plane DB: `isr={A}`, grant lease. 2. Push to A. 3. Each A shard commits without B only after it sees the new record. B doesn't need to be told. |
| **Lease renewal** | The control plane extends `lease_expiry` in its DB, but only while the epoch is unchanged, then pushes it to A. Once the epoch moves on, renewals stop, and A's lease runs out. A push carrying a newer epoch demotes A right away. |
| **Clean promote** (B ∈ isr) | 1. CAS: `epoch e→e', leader=B`. 2. Push to B. B's hosts apply fencing and activation from the *same* record, so any host that accepts writes as leader is already fencing A. 3. Push to A when reachable, which demotes it (§4.5). |
| **Unclean promote** (B ∉ isr) | 1. The control plane waits until its clock is past A's `lease_expiry` in its DB, plus a skew margin. 2. CAS. 3. Push to B as above. |
| **Rejoin** | 1. CAS: `joining=B` (B still not eligible). 2. Push to A. 3. **Every** A shard confirms it now waits for B. 4. CAS: `isr={A,B}`, lease cleared. |

**Applying to a cluster:**

- **A new admin API**, for example `ApplyReplicationGroupState{group, version, epoch, leader, isr, joining, lease_expiry}`. The cluster stores it with a CAS on `version` in its own DB, so stale or duplicate pushes are ignored, and returns the applied version.
- **The control plane runs a reconcile loop:** desired version in its DB, observed version per cluster, retry until they match. Each operation's status shows "committed", "applied at A" and "applied at B".
- **Keep this record out of namespace replication.** It must not flow through the namespace replication queue: one cluster could overwrite the other with a stale copy. It belongs in a separate per-cluster record that only the control plane writes.
- **Getting it to history hosts:** they pick it up like namespace metadata. The registry refresh interval is 2 s today (`system.namespaceCacheRefreshInterval`). A push notification after the admin API write cuts that delay. Delay only slows operations down; it never breaks safety, thanks to the ordering above.
- **Per-shard confirmation** (needed for rejoin step 3): each shard persists the record version it has applied in its shard info. The cluster reports the minimum over all shards. A shard that loads onto a new host refuses a registry snapshot older than its persisted version and waits for a refresh. Without that, shard movement could take a shard back to "don't wait for B" after the control plane has made B eligible.

**Lease timing across machines.** A's history hosts check the lease against wall-clock time:

- A host's deadline is `lease_expiry − skew_margin`, where `lease_expiry` comes from the latest renewal pushed by the control plane. Renewals advance only `lease_expiry`, which is monotonic, not the record version. A stale or reordered renewal is ignored.
- The control plane starts an unclean promote only after `lease_expiry + skew_margin` by its own clock.
- With clock skew bounded by NTP, A has stopped committing alone before B starts.
- A host that can't get a fresh record stops at its last known deadline.

### 5.5 Degraded state and recovery

1. **While degraded:**
   - Namespace metadata and the control plane show `DEGRADED` and `degraded_since`, with metrics and an alert.
   - Sync transactions commit on local persist.
   - Async transactions behave as they always do.
2. **B comes back** and first runs §4.5 rollback if it was the demoted leader. It then catches up as a *learner* through normal streaming, which can take a while after a long outage. If the backlog has passed retention, use force replication.
3. **Rejoin** when B's ack lag is below a threshold on *every* shard of the namespace's sync lanes, for example under 1,000 tasks or under 500 ms. The control plane then runs the two-step rejoin (§5.4). From then on, new sync transactions wait for B again. The remaining small lag shows up as a one-time latency bump. There's no correctness risk, because B must ack everything up to each barrier anyway. An optional handover-style pause can drain the lag to zero first.
4. Until the rejoin CAS succeeds, B is not eligible for promotion.

### 5.6 Configuration and N ≥ 3

- `quorum_mode: MAJORITY | ISR`, and `min_isr` (2 = never degrade, i.e. strict; 1 = degrade down to the leader alone).
- ISR mode also works for N ≥ 3, but commit then waits for the *slowest* ISR member. Majority waits for the fastest Q−1. Recommended default: MAJORITY for N ≥ 3 (the replica set is its own arbiter), ISR for N = 2.
- Degradation only changes behavior for sync transactions. Async transactions never waited.

## 6. Performance

Added latency per gated effect ≈ **one round trip to the nearest peer, plus the target DB commit, plus streaming overhead**. To get close to that floor:

- Event-driven acks (§3.1). This is mandatory.
- **Push on commit** (required by §3.3, not optional): after the local transaction commits, send the replication task straight from memory, including the converted payload. Fall back to a lock-free DB read for catch-up only. Today's sender reads from the DB after a notification and converts on send, under the workflow lock.
- **Per-workflow write rate** is bounded by local persist + quorum round trip (§3.3). Group commit is the main lever if that's too low.
- Keep sync lanes separate from async lanes and from low-priority force-replication.
- The fastest peer sets the commit index. Put one replica close to the active (same region, different AZ or cell, or a nearby region) for low latency, and one far away for disaster recovery.

**Cost per workflow step (strict mode).** One activity step has about 5 quorum waits on its critical path: the WT completion response, the activity dispatch, the activity-started response, the completion response and the WT dispatch. With a 30 ms round trip, that's roughly +150 ms per step, compared with roughly 0 today. Figures are illustrative. If that's too much, offer a **"durable-ack" mode** that still gates client-acknowledged writes, cross-workflow effects, callbacks and reads, but doesn't gate `Record*TaskStarted` responses or dispatch. The cost of that relaxation is that a worker may run code against state that is later discarded, which is within at-least-once semantics.

Throughput: commits are per shard and per lane prefix, so they batch naturally. A busy shard pays one round trip per ack wave, not one per write.

## 7. Membership changes

- A cluster added to a sync namespace joins as a **learner**: it receives replication but isn't counted in Q. It becomes a voter once force replication finishes and it's within lag. The gradual-connect ramp already exists.
- Change membership one cluster at a time, or use joint consensus. Q and the voter set are versioned in namespace metadata, and every prepare and promise carries that version.
- A cheaper third replica could be a **witness** that stores relay and replication payloads but runs no workers. Catch-up could stream from it. This is future work.

## 8. Risks and open questions

1. **Head-of-line blocking:** one stuck or poison task on a sync lane stalls commit for every sync workflow on that shard, for that peer. With 3 replicas the other peer covers it. DLQ policy for sync lanes needs a decision: surface it as an availability incident, never drop it silently.
2. **Completeness of the mutable-state gate (§3.3):** any path that reads execution state without `LoadMutableState` leaks uncommitted state. Examples: direct persistence reads, the current-execution record, in-memory caches outside the workflow context, CHASM component reads. This needs a full inventory and a test hook that fails if uncommitted state is ever returned. In-memory-only structures (speculative workflow tasks, the Update registry's admitted state, the query registry) were never durable and stay out of scope.
3. **Lock hold time:** workflow locks are now held across a cross-region round trip. Lock-wait timeouts, `BusyWorkflow` thresholds and the queue processors' lock-acquire behavior need retuning for sync namespaces.
4. **Per-workflow active cluster** (`ActiveClusterName(routingKey)`, `namespace.go:193`): the term and the promise are really per (namespace, routing group). The design generalizes, but the promise state has to be keyed that way.
5. **Cross-namespace effects** (a child or a Nexus call into an async namespace) are gated on the parent's commit only. The target namespace keeps its own guarantees.
6. **Shard movement** inside a cluster: the commit tracker is rebuilt on shard load from persisted remote-reader acks (`AckedReplicationTaskIDs`). That's conservative and therefore safe.
7. **Clients can see extra latency or timeouts while a peer is degraded.** That needs SLOs, plus metrics for commit lag per shard and lane.
8. **Trigger choice is a product decision:** which event types are worth a quorum round trip. Start, Update accepted and completion are natural candidates. Activity started and heartbeats are expensive and rarely worth it.
9. **Control-plane requirements** (§5.3): confirm it gives linearizable CAS and survives the loss of either data region. Then pick the lease duration and `D` so that `D` + CAS latency is an acceptable pause, and the lease is short enough for a quick unclean promote.

## 9. Suggested phases

1. **Observability only:** compute `commit[s][ℓ]`, event-driven acks, and metrics for "would-be gating delay", plus a DLQ alert for sync namespaces. No behavior change, but it measures the latency cost on real traffic.
2. **Gating:** the sync-trigger policy and per-transaction classification (§3.6), persisted barrier, `CommitTracker`, the mutable-state gate in `LoadMutableState`, a lock-free push-on-commit replication sender, gates in queue executables and the history handler. Behind a per-namespace flag, with failover limited to the graceful handover path. This alone gives G1, G3 and G4, plus G2 for planned failovers.
3. **Safe unplanned failover:** relay log, promote/fence RPCs, catch-up stream, rollback of the old leader, reapply off. This completes G2.
   - **2-cluster track:** ISR mode, the control-plane record, its CAS operations and the degraded lease, the `ApplyReplicationGroupState` admin API with per-shard applied versions and the reconcile loop, shrink/rejoin automation, the `DEGRADED` state and unclean promote (§5). This can ship before the majority-mode election, since ISR promotion needs no catch-up.
4. **Latency work:** group commit per workflow, the durable-ack mode, witness replicas.

## 10. Future work

- **Durable ack watermark.** Track a "durable" watermark per (stream, lane) next to today's progress watermark. A task NACKed into the replication DLQ advances the progress watermark but holds back the durable one, and commit uses the durable watermark. That closes the v1 gap where a DLQ'd write counts as committed on a standby that doesn't have it. It also needs:
  - a resolution path that releases the hold once the DLQ'd task is reprocessed or the workflow is re-replicated,
  - a rule that a standby with an unresolved DLQ entry isn't counted, which the failure detector can apply by dropping it from the in-sync set.
- **Selective sync per event type** (§3.6).
- **Majority mode for N ≥ 3:** election, relay log and catch-up from a peer (§4.3–4.4), so commit waits for the fastest standby instead of the slowest.
- **Per-business-ID active cluster** (active-active namespaces).
- **Group commit per workflow**, the durable-ack relaxation, and witness replicas.
