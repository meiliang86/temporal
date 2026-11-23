# Model check of the v1 two-cluster failover protocol

`model.py` is an exhaustive explicit-state model checker for the protocol in
`../sync-replication-design.md` ("v1 failover"). Within the bounds, it explores every interleaving of
writes, commits, reads, replication, acks (including lost and late ones), namespace propagation, planned
and forced failovers and control-plane membership changes, and checks the safety invariants in every
reachable state. This is what TLC does for a TLA+ spec on a bounded model. It is not a proof for unbounded
parameters.

Run: `python3 model.py --max-writes 3 --max-failovers 2 [--fingerprint] [--mutation NAME] [--trace]`

## What is modeled

- **Two clusters, one workflow history.** One history is enough to create divergent branches.
- **Writes and replication tasks.** A cluster that believes it is active appends `(write, failover version)` and enqueues a replication task carrying the resulting history (state-based replication).
- **Replication.** The other cluster processes tasks in order with NDC-style merge:
  - an extension is adopted (also by an active cluster: the "keep" model),
  - divergent branches resolve to the higher last failover version,
  - duplicates are no-ops.
- **Acks.** Acks are task-queue watermarks, as in Temporal, so an ignored task still counts as acked. Each ack carries the fencing notice: the sender's failover version if it believes it is active.
- **Ack channel.** Bounded FIFO; messages can be lost or arrive late.
- **Commit and read.**
  - Commit needs an ack from every other member of the active's applied in-sync set, from a peer whose fencing notice is not newer than the write's failover version.
  - Term rule: only the active commits, and only writes made under its current failover version.
  - A read on the active exposes its whole history once the last write is committed under the current version. Exposed writes count as acknowledged.
- **Failover.**
  - Planned handover: block writes, wait until the target acked every task, switch.
  - Forced failover: only to a cluster in the control plane's in-sync set.
- **Control plane.** Drop a non-active member (record first, push later); push the record to clusters at any time; two-step rejoin (the active waits for the member first; the member becomes promotable once caught up).
- **Namespace metadata** propagates asynchronously.
- **Failures.**
  - A crashed or partitioned cluster is one that takes no steps.
  - A demoted cluster may keep writing and trying to commit: the worst case.

## Invariants

- **Durable:** every acknowledged or exposed write is in the history of every in-sync (promotable) cluster.
- **OnActive:** every acknowledged or exposed write is in the history of the current active cluster.
- **Consistent:** acknowledged writes appear in the same relative order in both histories.

## Results

| Bound | Result |
|---|---|
| 2 writes, 1 failover | OK: 560,248 states, 4.6M transitions |
| 2 writes, 2 failovers | OK: 4,849,488 states, 39.8M transitions |
| 3 writes, 1 failover | OK: 4,754,592 states, 39.9M transitions |
| 3 writes, 2 failovers | see below |

Each mutation breaks one rule. The checker finds a violation for every one (2 writes, 2 failovers):

| Mutation | Counterexample |
|---|---|
| `no-fencing` | A writes w1; forced failover to B; B writes w2 and ignores A's w1 task (B's branch is newer); B's ack still advances A's watermark; A acknowledges w1, which B doesn't have. |
| `no-term-rule` | A writes w1; failover to B, then back to A; a stale B writes w2 at version 2; B is dropped; A commits the old w1 alone; B's w2 branch (version 2 > 1) replaces A's branch and w1 is lost. Raft's "Figure 8". |
| `one-step-rejoin` | A commits w1 alone while degraded; B becomes promotable at once, without having w1. |
| `rejoin-without-catch-up` | Same, through the two-step rejoin without the catch-up check. |
| `active-shrinks-first` | A stops waiting for B before the control plane records it; B is still promotable without A's new write. |
| `force-to-any` | Forced failover to B after it was dropped; the new active lacks w1. |
| `handover-without-drain` | Handover switches before the target has acked everything; an old write is then acknowledged that the new active ignored. |

## Protocol changes found by the checker

1. **The fencing check is per write.** An ack can confirm a write only if the acking peer was not active at a newer failover version than the one the write was made under. Comparing against the writer's *current* version is wrong: once the old active has learned the new version, a fenced ack would count again.
2. **Term rule (Raft's "commit only current-term entries").** The active never commits, or exposes through reads, a write made under an older failover version by counting acks or by committing alone. Such writes become durable when a later write under the current version commits. In Temporal terms:
   - waiters for writes from an older failover version fail instead of committing;
   - before a new active exposes a workflow whose last write is older than its current failover version, it commits a version-bump transition, like Raft's no-op entry.
