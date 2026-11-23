package workflow

import (
	"context"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
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

// isSyncReplicationNamespace reports whether the namespace uses sync replication: it enables it and
// is replicated to more than one cluster.
func isSyncReplicationNamespace(shardContext historyi.ShardContext, namespaceEntry *namespace.Namespace) bool {
	return namespaceEntry != nil &&
		namespaceEntry.ReplicationPolicy() == namespace.ReplicationPolicyMultiCluster &&
		shardContext.GetConfig().SyncReplicationEnabled(namespaceEntry.Name().String())
}

// SyncReplicationClusters returns the remote clusters that must acknowledge a transaction of the given
// workflow before it takes effect, or nil if sync replication doesn't apply: the namespace doesn't
// enable it, isn't replicated, the workflow isn't active in this cluster, or the control plane put the
// namespace in degraded mode (no other member).
func SyncReplicationClusters(
	shardContext historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	workflowID string,
) []string {
	if !isSyncReplicationNamespace(shardContext, namespaceEntry) {
		return nil
	}
	currentCluster := shardContext.GetClusterMetadata().GetCurrentClusterName()
	if namespaceEntry.ActiveClusterName(namespace.RoutingKey{ID: workflowID}) != currentCluster {
		return nil
	}
	return syncReplicationCommitClusters(currentCluster, namespaceEntry, workflowID)
}

// syncReplicationCommitClusters returns the clusters other than currentCluster that a commit waits
// for. With a membership record from the control plane, those are its in-sync and joining clusters;
// none means degraded mode, where the active cluster commits alone. Without a record, every cluster
// of the namespace.
func syncReplicationCommitClusters(
	currentCluster string,
	namespaceEntry *namespace.Namespace,
	workflowID string,
) []string {
	var members []string
	if record := namespaceEntry.SyncReplication(); record != nil {
		members = append(slices.Clone(record.GetInSyncClusters()), record.GetJoiningClusters()...)
	} else {
		members = namespaceEntry.ClusterNames(workflowID)
	}
	var remoteClusters []string
	for _, cluster := range members {
		if cluster != currentCluster && !slices.Contains(remoteClusters, cluster) {
			remoteClusters = append(remoteClusters, cluster)
		}
	}
	return remoteClusters
}

// checkSyncReplicationAcquiredVersion refuses to act as active for a sync replication namespace under
// failoverVersion if the shard read a newer failover version of the namespace from the database at
// its acquisition. The host's namespace cache is then behind: for example the shard just moved to a
// host that missed a handover and a forced failover back. A write made under the old version could be
// acknowledged (this cluster may be alone in the membership record by then) and later lost to a
// branch made under a version in between.
func checkSyncReplicationAcquiredVersion(
	shardContext historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	failoverVersion int64,
) error {
	acquired, err := shardContext.SyncReplicationAcquiredNamespace(namespaceEntry.ID())
	if err != nil {
		return serviceerror.NewUnavailablef("sync replication: can't read namespace %s: %v", namespaceEntry.Name(), err)
	}
	if acquired == nil || acquired.FailoverVersion(namespace.EmptyBusinessID) <= failoverVersion {
		return nil
	}
	currentCluster := shardContext.GetClusterMetadata().GetCurrentClusterName()
	if activeCluster := acquired.ActiveClusterName(namespace.RoutingKey{}); activeCluster != currentCluster {
		return serviceerror.NewNamespaceNotActive(namespaceEntry.Name().String(), currentCluster, activeCluster)
	}
	return serviceerror.NewUnavailablef(
		"sync replication: namespace %s cache is at failover version %d, behind %d; retry",
		namespaceEntry.Name(), failoverVersion, acquired.FailoverVersion(namespace.EmptyBusinessID),
	)
}

// checkSyncReplicationWrite runs before a transaction is persisted. An active write of a sync
// replication namespace must not be made under a failover version older than the one the shard read
// at its acquisition (see checkSyncReplicationAcquiredVersion). Checking after persisting is too late:
// a later load would expose the write.
func checkSyncReplicationWrite(
	shardContext historyi.ShardContext,
	namespaceID string,
	failoverVersion int64,
) error {
	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil || !isSyncReplicationNamespace(shardContext, namespaceEntry) {
		return nil
	}
	clusterMetadata := shardContext.GetClusterMetadata()
	if clusterMetadata.ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), failoverVersion) != clusterMetadata.GetCurrentClusterName() {
		return nil // applying another cluster's writes
	}
	return checkSyncReplicationAcquiredVersion(shardContext, namespaceEntry, failoverVersion)
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

// lastWriteFailoverVersion returns the failover version of the last write recorded in a persisted
// workflow snapshot.
func lastWriteFailoverVersion(executionInfo *persistencespb.WorkflowExecutionInfo) int64 {
	if transitions := executionInfo.GetTransitionHistory(); len(transitions) > 0 {
		return transitions[len(transitions)-1].GetNamespaceFailoverVersion()
	}
	if executionInfo.GetVersionHistories() == nil {
		return common.EmptyVersion
	}
	currentHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.GetVersionHistories())
	if err != nil {
		return common.EmptyVersion
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(currentHistory)
	if err != nil {
		return common.EmptyVersion
	}
	return lastItem.GetVersion()
}

// waitForSyncReplication blocks, after a transaction of the given workflow is persisted, until the
// remote clusters have acknowledged its replication tasks (barrier = the largest of their IDs). If ctx
// carries a lock handoff, the workflow lock is handed to replication readers while waiting.
//
// failoverVersion is the namespace failover version the transaction was made under. Whether to wait
// is decided by it, not by what the namespace registry says now: a transaction made while this
// cluster was active must not take effect unacknowledged just because this host has since learned of
// a failover. Transactions made under another cluster's version apply replication and don't wait.
//
// Callers notify the queues about the transaction's tasks only after this returns, so tasks and long
// polls don't see an unacknowledged transaction. The replication stream sender is the exception: it
// is notified here, before waiting, or it would not send the tasks being waited on.
func waitForSyncReplication(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID string,
	workflowID string,
	failoverVersion int64,
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
	if !isSyncReplicationNamespace(shardContext, namespaceEntry) {
		return nil
	}
	clusterMetadata := shardContext.GetClusterMetadata()
	currentCluster := clusterMetadata.GetCurrentClusterName()
	if clusterMetadata.ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), failoverVersion) != currentCluster {
		return nil
	}
	// Recorded even when nothing is waited for (degraded mode): the control plane checks that every
	// write up to the barrier reached a peer before letting it rejoin or fail over.
	shardContext.RecordSyncReplicationBarrier(barrier)
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	var replicationTasks []tasks.Task
	for _, taskMap := range taskMaps {
		replicationTasks = append(replicationTasks, taskMap[tasks.CategoryReplication]...)
	}
	engine.NotifyNewTasks(map[tasks.Category][]tasks.Task{tasks.CategoryReplication: replicationTasks})

	var handoff syncReplicationLockHandoff
	if h, ok := ctx.Value(syncReplicationLockHandoffKey{}).(syncReplicationLockHandoff); ok {
		handoff = h
	}
	return waitForSyncReplicationCommit(ctx, shardContext, namespaceEntry, workflowID, failoverVersion, barrier, handoff)
}

// waitForSyncReplicationCommit waits until the clusters a commit needs (re-read from the namespace
// registry while waiting) have acknowledged every replication task up to barrier, for a write of the
// workflow made under failoverVersion.
func waitForSyncReplicationCommit(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	failoverVersion int64,
	barrier int64,
	handoff syncReplicationLockHandoff,
) error {
	currentCluster := shardContext.GetClusterMetadata().GetCurrentClusterName()
	namespaceID := namespaceEntry.ID()
	commitClusters := func() []string {
		if latest, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID); err == nil {
			namespaceEntry = latest
		}
		return syncReplicationCommitClusters(currentCluster, namespaceEntry, workflowID)
	}
	if len(commitClusters()) == 0 {
		return nil // degraded mode: this cluster commits alone
	}

	nsName := namespaceEntry.Name().String()
	waitCtx, cancel := context.WithTimeout(ctx, shardContext.GetConfig().SyncReplicationCommitTimeout(nsName))
	defer cancel()

	if handoff != nil {
		end := handoff.beginSyncReplicationWait()
		defer end()
	}

	start := time.Now()
	fencedBy, err := shardContext.WaitForReplicationCommit(waitCtx, namespaceID.String(), failoverVersion, barrier, commitClusters)
	handler := shardContext.GetMetricsHandler().WithTags(metrics.NamespaceTag(nsName))
	metrics.SyncReplicationCommitLatency.With(handler).Record(time.Since(start))
	if fencedBy != "" {
		// The cluster is active at a newer failover version and may have discarded this write. It will
		// never be acknowledged here; the caller should go to the new active cluster.
		metrics.SyncReplicationCommitFenced.With(handler).Record(1)
		return serviceerror.NewNamespaceNotActive(nsName, currentCluster, fencedBy)
	}
	if err != nil {
		metrics.SyncReplicationCommitTimeout.With(handler).Record(1)
		return serviceerror.NewUnavailablef(
			"sync replication: transaction not acknowledged by clusters %v: %v", commitClusters(), err,
		)
	}
	return nil
}
