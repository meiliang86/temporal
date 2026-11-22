package workflow

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

// Sync replication (experimental, see docs/architecture/sync-replication-design.md on the design branch).
//
// For a namespace with history.syncReplicationEnabled, the active cluster persists each workflow
// transaction locally and then waits until every other cluster of the namespace has acknowledged
// applying the transaction's replication tasks. Until then nothing may observe the transaction:
//
//   - The writer keeps owning the workflow lock until the transaction is acknowledged, so the caller's
//     response, any reader of the mutable state, and any later transaction wait for it.
//   - While the writer waits, the lock is handed off: only the replication stream sender (a "sync
//     replication reader") may take it, so it can build the very replication task being waited on.
//     Every other caller backs off until the writer is done.
//   - History event notifications (long polls) are sent only after the acknowledgement.
//   - Mutable state loaded from the database may contain a transaction that was never acknowledged
//     (for example after a wait timed out and the cached state was cleared), so a load waits for
//     everything the shard has persisted so far to be acknowledged.

type (
	syncReplicationReaderKey      struct{}
	syncReplicationLockHandoffKey struct{}

	// syncReplicationLockHandoff lets a transaction hand the workflow lock to replication readers
	// while it waits for acknowledgement. Implemented by ContextImpl.
	syncReplicationLockHandoff interface {
		// beginSyncReplicationWait hands the lock off if the caller holds it, and returns a function
		// that takes it back. The returned function must be called exactly once.
		beginSyncReplicationWait() (end func())
	}
)

// WithSyncReplicationReader marks ctx as the replication stream sender reading the source workflow to
// build a replication task. Such a caller may take the workflow lock while a transaction is waiting
// for sync replication acknowledgement, and reads mutable state without starting a transaction.
func WithSyncReplicationReader(ctx context.Context) context.Context {
	return context.WithValue(ctx, syncReplicationReaderKey{}, true)
}

// WithSyncReplicationReaderIfEnabled marks ctx as a sync replication reader if the namespace uses sync
// replication. For RPCs that serve replication to another cluster (resend, backfill): the other cluster
// may need them to apply, and acknowledge, the very transaction a writer of this workflow waits on.
func WithSyncReplicationReaderIfEnabled(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
) context.Context {
	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil || !shardContext.GetConfig().SyncReplicationEnabled(namespaceEntry.Name().String()) {
		return ctx
	}
	return WithSyncReplicationReader(ctx)
}

func isSyncReplicationReader(ctx context.Context) bool {
	v, ok := ctx.Value(syncReplicationReaderKey{}).(bool)
	return ok && v
}

func withSyncReplicationLockHandoff(ctx context.Context, handoff syncReplicationLockHandoff) context.Context {
	return context.WithValue(ctx, syncReplicationLockHandoffKey{}, handoff)
}

// SyncReplicationClusters returns the remote clusters that must acknowledge a transaction of the given
// workflow before it takes effect, or nil if sync replication doesn't apply: the namespace doesn't
// enable it, isn't replicated, or the workflow isn't active in this cluster.
func SyncReplicationClusters(
	shardContext historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	workflowID string,
) []string {
	if namespaceEntry == nil ||
		namespaceEntry.ReplicationPolicy() != namespace.ReplicationPolicyMultiCluster ||
		!shardContext.GetConfig().SyncReplicationEnabled(namespaceEntry.Name().String()) {
		return nil
	}
	currentCluster := shardContext.GetClusterMetadata().GetCurrentClusterName()
	if namespaceEntry.ActiveClusterName(namespace.RoutingKey{ID: workflowID}) != currentCluster {
		return nil
	}
	var remoteClusters []string
	for _, cluster := range namespaceEntry.ClusterNames(workflowID) {
		if cluster != currentCluster {
			remoteClusters = append(remoteClusters, cluster)
		}
	}
	return remoteClusters
}

// maxReplicationTaskID returns the largest replication task ID in the given task maps, or 0 if there
// is none. Task IDs are assigned by the shard when the transaction is persisted.
func maxReplicationTaskID(taskMaps ...map[tasks.Category][]tasks.Task) int64 {
	var maxID int64
	for _, taskMap := range taskMaps {
		for _, task := range taskMap[tasks.CategoryReplication] {
			maxID = max(maxID, task.GetTaskID())
		}
	}
	return maxID
}

// waitForSyncReplication blocks, after a transaction of the given workflow is persisted, until the
// remote clusters have acknowledged its replication tasks (barrier = the largest of their IDs). If ctx
// carries a lock handoff, the workflow lock is handed to replication readers while waiting.
//
// Callers notify the queues about the transaction's tasks only after this returns, so tasks and long
// polls don't see an unacknowledged transaction. The replication stream sender is the exception: it
// is notified here, before waiting, or it would not send the tasks being waited on.
func waitForSyncReplication(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID string,
	workflowID string,
	taskMaps ...map[tasks.Category][]tasks.Task,
) error {
	barrier := maxReplicationTaskID(taskMaps...)
	if barrier <= 0 {
		return nil
	}
	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		// The transaction is already persisted, and without the namespace we can't tell whether it uses
		// sync replication. Don't turn a successful write into a failure (e.g. for a namespace that is
		// being deleted).
		return nil
	}
	clusters := SyncReplicationClusters(shardContext, namespaceEntry, workflowID)
	if len(clusters) == 0 {
		return nil
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	var replicationTasks []tasks.Task
	for _, taskMap := range taskMaps {
		replicationTasks = append(replicationTasks, taskMap[tasks.CategoryReplication]...)
	}
	shardContext.RecordSyncReplicationBarrier(barrier)
	engine.NotifyNewTasks(map[tasks.Category][]tasks.Task{tasks.CategoryReplication: replicationTasks})

	var handoff syncReplicationLockHandoff
	if h, ok := ctx.Value(syncReplicationLockHandoffKey{}).(syncReplicationLockHandoff); ok {
		handoff = h
	}
	return waitForSyncReplicationCommit(ctx, shardContext, namespaceEntry, clusters, barrier, handoff)
}

func waitForSyncReplicationCommit(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	clusters []string,
	barrier int64,
	handoff syncReplicationLockHandoff,
) error {
	nsName := namespaceEntry.Name().String()
	waitCtx, cancel := context.WithTimeout(ctx, shardContext.GetConfig().SyncReplicationCommitTimeout(nsName))
	defer cancel()

	if handoff != nil {
		end := handoff.beginSyncReplicationWait()
		defer end()
	}

	start := time.Now()
	err := shardContext.WaitForReplicationCommit(waitCtx, clusters, barrier)
	handler := shardContext.GetMetricsHandler().WithTags(metrics.NamespaceTag(nsName))
	metrics.SyncReplicationCommitLatency.With(handler).Record(time.Since(start))
	if err != nil {
		metrics.SyncReplicationCommitTimeout.With(handler).Record(1)
		return serviceerror.NewUnavailablef(
			"sync replication: transaction not acknowledged by clusters %v: %v", clusters, err,
		)
	}
	return nil
}
