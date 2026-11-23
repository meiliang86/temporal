package workflow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/testing/testhooks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func newSyncReplicationTestContext() *ContextImpl {
	return NewContext(
		tests.NewDynamicConfig(),
		definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID),
		chasm.WorkflowArchetypeID,
		log.NewNoopLogger(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		nil,
		testhooks.TestHooks{},
	)
}

// Failover versions in the tests: localVersion belongs to the current cluster, remoteVersion to the
// alternative cluster.
var (
	localVersion  = tests.Version
	remoteVersion = tests.Version + 1
)

func newSyncReplicationTestShard(t *testing.T, enabled bool) *historyi.MockShardContext {
	return newSyncReplicationTestShardWithNamespace(t, enabled, func() *namespace.Namespace { return tests.GlobalNamespaceEntry })
}

// newSyncReplicationTestShardWithNamespace returns a shard whose namespace registry returns the
// namespace from entry on every lookup, so a test can change it while a write waits.
func newSyncReplicationTestShardWithNamespace(t *testing.T, enabled bool, entry func() *namespace.Namespace) *historyi.MockShardContext {
	controller := gomock.NewController(t)
	config := tests.NewDynamicConfig()
	config.SyncReplicationEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(enabled)
	config.SyncReplicationCommitTimeout = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Second)

	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ bool, version int64) string {
			if version == remoteVersion {
				return cluster.TestAlternativeClusterName
			}
			return cluster.TestCurrentClusterName
		},
	).AnyTimes()
	namespaceRegistry := namespace.NewMockRegistry(controller)
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).DoAndReturn(
		func(namespace.ID) (*namespace.Namespace, error) { return entry(), nil },
	).AnyTimes()

	shardContext := historyi.NewMockShardContext(controller)
	shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
	shardContext.EXPECT().GetClusterMetadata().Return(clusterMetadata).AnyTimes()
	shardContext.EXPECT().GetNamespaceRegistry().Return(namespaceRegistry).AnyTimes()
	shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	// By default the database read at the shard's acquisition matches the cache.
	shardContext.EXPECT().SyncReplicationAcquiredNamespace(tests.NamespaceID).DoAndReturn(
		func(namespace.ID) (*namespace.Namespace, error) { return entry(), nil },
	).AnyTimes()
	return shardContext
}

// namespaceWithRecord returns the test global namespace (active in the current cluster unless
// activeCluster says otherwise) with the given sync replication membership record.
func namespaceWithRecord(activeCluster string, record *persistencespb.NamespaceSyncReplication, clusters ...string) *namespace.Namespace {
	if len(clusters) == 0 {
		clusters = []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName}
	}
	return namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters:          clusters,
			SyncReplication:   record,
		},
		localVersion,
	)
}

// replicationTasks returns a task map with one replication task with the given ID.
func replicationTasks(id int64) map[tasks.Category][]tasks.Task {
	task := &tasks.SyncVersionedTransitionTask{}
	task.SetTaskID(id)
	return map[tasks.Category][]tasks.Task{tasks.CategoryReplication: {task}}
}

// expectReplicationNotification expects the replication sender to be woken before the wait.
func expectReplicationNotification(t *testing.T, shardContext *historyi.MockShardContext) {
	engine := historyi.NewMockEngine(gomock.NewController(t))
	engine.EXPECT().NotifyNewTasks(gomock.Any()).Do(func(taskMap map[tasks.Category][]tasks.Task) {
		require.Len(t, taskMap[tasks.CategoryReplication], 1)
	})
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
	shardContext.EXPECT().RecordSyncReplicationBarrier(gomock.Any())
}

func TestMaxReplicationTaskID(t *testing.T) {
	replicationTask := func(id int64) tasks.Task {
		task := &tasks.SyncVersionedTransitionTask{}
		task.SetTaskID(id)
		return task
	}
	transferTask := &tasks.ActivityTask{}
	transferTask.SetTaskID(1000)

	require.Equal(t, int64(0), maxReplicationTaskID())
	require.Equal(t, int64(0), maxReplicationTaskID(nil, map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer: {transferTask},
	}))
	require.Equal(t, int64(42), maxReplicationTaskID(
		map[tasks.Category][]tasks.Task{
			tasks.CategoryTransfer:    {transferTask},
			tasks.CategoryReplication: {replicationTask(7), replicationTask(42)},
		},
		map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {replicationTask(13)},
		},
	))
}

func TestSyncReplicationClusters(t *testing.T) {
	enabled := newSyncReplicationTestShard(t, true)
	require.Equal(t,
		[]string{cluster.TestAlternativeClusterName},
		SyncReplicationClusters(enabled, tests.GlobalNamespaceEntry, tests.WorkflowID),
	)
	// Not replicated to any other cluster.
	require.Nil(t, SyncReplicationClusters(enabled, tests.LocalNamespaceEntry, tests.WorkflowID))
	// Not active in this cluster.
	require.Nil(t, SyncReplicationClusters(enabled, tests.GlobalStandbyNamespaceEntry, tests.WorkflowID))

	disabled := newSyncReplicationTestShard(t, false)
	require.Nil(t, SyncReplicationClusters(disabled, tests.GlobalNamespaceEntry, tests.WorkflowID))
}

func TestSyncReplicationCommitClusters(t *testing.T) {
	current, alternative, third := cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName, "third"
	commitClusters := func(record *persistencespb.NamespaceSyncReplication) []string {
		return syncReplicationCommitClusters(current, namespaceWithRecord(current, record, current, alternative, third), tests.WorkflowID)
	}

	// No record from the control plane: every cluster of the namespace.
	require.Equal(t, []string{alternative, third}, commitClusters(nil))
	// Full quorum.
	require.Equal(t, []string{alternative}, commitClusters(&persistencespb.NamespaceSyncReplication{
		Version: 1, InSyncClusters: []string{current, alternative},
	}))
	// Degraded mode: the active cluster is the only member and commits alone.
	require.Empty(t, commitClusters(&persistencespb.NamespaceSyncReplication{
		Version: 2, InSyncClusters: []string{current},
	}))
	// Rejoin, step 1: commits wait for the joining cluster too.
	require.Equal(t, []string{alternative}, commitClusters(&persistencespb.NamespaceSyncReplication{
		Version: 3, InSyncClusters: []string{current}, JoiningClusters: []string{alternative},
	}))
	// After a forced failover the old active is dropped from the record, but while it still believes
	// it is active it keeps waiting for the in-sync members (and is fenced by them).
	require.Equal(t, []string{alternative}, commitClusters(&persistencespb.NamespaceSyncReplication{
		Version: 4, InSyncClusters: []string{alternative},
	}))
	// Duplicates are ignored.
	require.Equal(t, []string{alternative, third}, commitClusters(&persistencespb.NamespaceSyncReplication{
		Version: 5, InSyncClusters: []string{current, alternative, third}, JoiningClusters: []string{alternative},
	}))
}

func TestLastWriteFailoverVersion(t *testing.T) {
	require.Equal(t, int64(11), lastWriteFailoverVersion(&persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 7, TransitionCount: 3},
			{NamespaceFailoverVersion: 11, TransitionCount: 4},
		},
	}))
	// Event history alone isn't used: sync replication requires state-based replication.
	require.Equal(t, common.EmptyVersion, lastWriteFailoverVersion(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionhistory.NewVersionHistories(versionhistory.NewVersionHistory(nil, []*historyspb.VersionHistoryItem{
			versionhistory.NewVersionHistoryItem(9, 7),
		})),
	}))
	require.Equal(t, common.EmptyVersion, lastWriteFailoverVersion(&persistencespb.WorkflowExecutionInfo{}))
}

func TestWaitForSyncReplication(t *testing.T) {
	ctx := context.Background()
	nsID := tests.NamespaceID.String()

	t.Run("no replication task", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		require.NoError(t, waitForSyncReplication(ctx, shardContext, nsID, tests.WorkflowID, localVersion, map[tasks.Category][]tasks.Task{}))
	})

	t.Run("disabled", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, false)
		require.NoError(t, waitForSyncReplication(ctx, shardContext, nsID, tests.WorkflowID, localVersion, replicationTasks(10)))
	})

	t.Run("replication applied from another cluster doesn't wait", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		require.NoError(t, waitForSyncReplication(ctx, shardContext, nsID, tests.WorkflowID, remoteVersion, replicationTasks(10)))
	})

	t.Run("acknowledged", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		expectReplicationNotification(t, shardContext)
		shardContext.EXPECT().WaitForReplicationCommit(gomock.Any(), nsID, localVersion, int64(10), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ string, _ int64, _ int64, clusters func() []string) (string, error) {
				require.Equal(t, []string{cluster.TestAlternativeClusterName}, clusters())
				return "", nil
			},
		)
		require.NoError(t, waitForSyncReplication(ctx, shardContext, nsID, tests.WorkflowID, localVersion, replicationTasks(10)))
	})

	t.Run("a write made while active waits even if this host has since learned of a failover", func(t *testing.T) {
		// The zombie case: without this, the write would take effect without any acknowledgement.
		shardContext := newSyncReplicationTestShardWithNamespace(t, true, func() *namespace.Namespace {
			return namespaceWithRecord(cluster.TestAlternativeClusterName, nil)
		})
		expectReplicationNotification(t, shardContext)
		shardContext.EXPECT().WaitForReplicationCommit(gomock.Any(), nsID, localVersion, int64(10), gomock.Any()).
			Return(cluster.TestAlternativeClusterName, nil)
		err := waitForSyncReplication(ctx, shardContext, nsID, tests.WorkflowID, localVersion, replicationTasks(10))
		var notActive *serviceerror.NamespaceNotActive
		require.ErrorAs(t, err, &notActive)
		require.Equal(t, cluster.TestAlternativeClusterName, notActive.ActiveCluster)
	})

	t.Run("not acknowledged", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		expectReplicationNotification(t, shardContext)
		shardContext.EXPECT().WaitForReplicationCommit(gomock.Any(), gomock.Any(), gomock.Any(), int64(10), gomock.Any()).Return("", context.DeadlineExceeded)
		err := waitForSyncReplication(ctx, shardContext, nsID, tests.WorkflowID, localVersion, replicationTasks(10))
		var unavailable *serviceerror.Unavailable
		require.ErrorAs(t, err, &unavailable)
	})

	t.Run("fenced", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		expectReplicationNotification(t, shardContext)
		shardContext.EXPECT().WaitForReplicationCommit(gomock.Any(), gomock.Any(), gomock.Any(), int64(10), gomock.Any()).
			Return(cluster.TestAlternativeClusterName, nil)
		err := waitForSyncReplication(ctx, shardContext, nsID, tests.WorkflowID, localVersion, replicationTasks(10))
		var notActive *serviceerror.NamespaceNotActive
		require.ErrorAs(t, err, &notActive)
		require.Equal(t, tests.Namespace.String(), notActive.Namespace)
		require.Equal(t, cluster.TestCurrentClusterName, notActive.CurrentCluster)
		require.Equal(t, cluster.TestAlternativeClusterName, notActive.ActiveCluster)
	})

	t.Run("degraded mode records the barrier and doesn't wait", func(t *testing.T) {
		shardContext := newSyncReplicationTestShardWithNamespace(t, true, func() *namespace.Namespace {
			return namespaceWithRecord(cluster.TestCurrentClusterName, &persistencespb.NamespaceSyncReplication{
				Version: 1, InSyncClusters: []string{cluster.TestCurrentClusterName},
			})
		})
		engine := historyi.NewMockEngine(gomock.NewController(t))
		engine.EXPECT().NotifyNewTasks(gomock.Any())
		shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
		shardContext.EXPECT().RecordSyncReplicationBarrier(int64(10))
		// No WaitForReplicationCommit call expected.
		require.NoError(t, waitForSyncReplication(ctx, shardContext, nsID, tests.WorkflowID, localVersion, replicationTasks(10)))
	})

	t.Run("membership is re-read while waiting", func(t *testing.T) {
		var entry atomic.Pointer[namespace.Namespace]
		entry.Store(namespaceWithRecord(cluster.TestCurrentClusterName, &persistencespb.NamespaceSyncReplication{
			Version: 1, InSyncClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		}))
		shardContext := newSyncReplicationTestShardWithNamespace(t, true, entry.Load)
		expectReplicationNotification(t, shardContext)
		shardContext.EXPECT().WaitForReplicationCommit(gomock.Any(), gomock.Any(), gomock.Any(), int64(10), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ string, _ int64, _ int64, clusters func() []string) (string, error) {
				require.Equal(t, []string{cluster.TestAlternativeClusterName}, clusters())
				entry.Store(namespaceWithRecord(cluster.TestCurrentClusterName, &persistencespb.NamespaceSyncReplication{
					Version: 2, InSyncClusters: []string{cluster.TestCurrentClusterName},
				}))
				require.Empty(t, clusters())
				return "", nil
			},
		)
		require.NoError(t, waitForSyncReplication(ctx, shardContext, nsID, tests.WorkflowID, localVersion, replicationTasks(10)))
	})

	t.Run("hands the lock off while waiting", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		wfContext := newSyncReplicationTestContext()
		require.NoError(t, wfContext.Lock(ctx, locks.PriorityHigh))

		expectReplicationNotification(t, shardContext)
		shardContext.EXPECT().WaitForReplicationCommit(gomock.Any(), gomock.Any(), gomock.Any(), int64(10), gomock.Any()).DoAndReturn(
			func(context.Context, string, int64, int64, func() []string) (string, error) {
				// The replication sender can take the lock while the writer waits...
				readerCtx := WithSyncReplicationReader(context.Background())
				require.NoError(t, wfContext.Lock(readerCtx, locks.PriorityLow))
				wfContext.Unlock()
				// ...but nobody else can.
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				require.ErrorIs(t, wfContext.Lock(ctx, locks.PriorityHigh), context.DeadlineExceeded)
				return "", nil
			},
		)
		handoffCtx := withSyncReplicationLockHandoff(ctx, wfContext)
		require.NoError(t, waitForSyncReplication(handoffCtx, shardContext, nsID, tests.WorkflowID, localVersion, replicationTasks(10)))

		// The writer owns the lock again.
		require.True(t, wfContext.locked.Load())
		require.Nil(t, wfContext.syncReplicationHold)
		require.False(t, wfContext.TryLock(locks.PriorityHigh))
		wfContext.Unlock()
	})
}

func TestSyncReplicationLockHandoff(t *testing.T) {
	t.Run("unlocked context hands nothing off", func(t *testing.T) {
		wfContext := newSyncReplicationTestContext()
		end := wfContext.beginSyncReplicationWait()
		require.Nil(t, wfContext.syncReplicationHold)
		end()
		require.True(t, wfContext.TryLock(locks.PriorityHigh))
		wfContext.Unlock()
	})

	t.Run("others wait for the writer, readers don't", func(t *testing.T) {
		controller := gomock.NewController(t)
		wfContext := newSyncReplicationTestContext()
		require.NoError(t, wfContext.Lock(context.Background(), locks.PriorityHigh))
		mutableState := historyi.NewMockMutableState(controller) // no calls expected
		wfContext.MutableState = mutableState

		end := wfContext.beginSyncReplicationWait()
		require.NotNil(t, wfContext.syncReplicationHold)
		require.False(t, wfContext.TryLock(locks.PriorityHigh))

		acquired := make(chan error, 1)
		go func() {
			acquired <- wfContext.Lock(context.Background(), locks.PriorityHigh)
		}()

		// A replication reader gets in and may even release with an error; the state stays.
		readerCtx := WithSyncReplicationReader(context.Background())
		require.NoError(t, wfContext.Lock(readerCtx, locks.PriorityLow))
		wfContext.Clear()
		require.Equal(t, historyi.MutableState(mutableState), wfContext.MutableState)
		wfContext.Unlock()

		select {
		case err := <-acquired:
			t.Fatalf("lock acquired during sync replication wait: %v", err)
		case <-time.After(20 * time.Millisecond):
		}

		end()
		// The writer has the lock back; the waiter gets it only after the writer releases it.
		select {
		case err := <-acquired:
			t.Fatalf("lock acquired while the writer still holds it: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		wfContext.Unlock()
		select {
		case err := <-acquired:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("waiter not woken after the writer released the lock")
		}
		wfContext.Unlock()
	})

	t.Run("waiter gives up on its own deadline", func(t *testing.T) {
		wfContext := newSyncReplicationTestContext()
		require.NoError(t, wfContext.Lock(context.Background(), locks.PriorityHigh))
		end := wfContext.beginSyncReplicationWait()
		defer func() {
			end()
			wfContext.Unlock()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, wfContext.Lock(ctx, locks.PriorityHigh), context.DeadlineExceeded)
	})
}

func TestWithSyncReplicationReaderIfEnabled(t *testing.T) {
	enabled := newSyncReplicationTestShard(t, true)
	require.True(t, isSyncReplicationReader(WithSyncReplicationReaderIfEnabled(context.Background(), enabled, tests.NamespaceID)))

	disabled := newSyncReplicationTestShard(t, false)
	require.False(t, isSyncReplicationReader(WithSyncReplicationReaderIfEnabled(context.Background(), disabled, tests.NamespaceID)))
}

// A shard whose host's namespace cache is behind the database read at the shard's acquisition must not
// act as active under the cache's version.
func TestSyncReplicationAcquisitionGate(t *testing.T) {
	nsID := tests.NamespaceID.String()
	stateBased, streams := true, true
	newShard := func(t *testing.T, acquired *namespace.Namespace, readErr error) *historyi.MockShardContext {
		controller := gomock.NewController(t)
		config := tests.NewDynamicConfig()
		config.SyncReplicationEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
		config.EnableTransitionHistory = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(stateBased)
		config.EnableReplicationStream = dynamicconfig.GetBoolPropertyFn(streams)
		clusterMetadata := cluster.NewMockMetadata(controller)
		clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
		clusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ bool, version int64) string {
				if version == remoteVersion {
					return cluster.TestAlternativeClusterName
				}
				return cluster.TestCurrentClusterName
			},
		).AnyTimes()
		namespaceRegistry := namespace.NewMockRegistry(controller)
		namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
		shardContext := historyi.NewMockShardContext(controller)
		shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
		shardContext.EXPECT().GetClusterMetadata().Return(clusterMetadata).AnyTimes()
		shardContext.EXPECT().GetNamespaceRegistry().Return(namespaceRegistry).AnyTimes()
		shardContext.EXPECT().SyncReplicationAcquiredNamespace(tests.NamespaceID).Return(acquired, readErr).AnyTimes()
		return shardContext
	}
	withVersion := func(activeCluster string, version int64) *namespace.Namespace {
		return namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: nsID, Name: tests.Namespace.String()},
			&persistencespb.NamespaceConfig{},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: activeCluster,
				Clusters:          []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
			},
			version,
		)
	}

	// The database agrees with the cache (or is older): the write goes ahead.
	require.NoError(t, checkSyncReplicationWrite(newShard(t, withVersion(cluster.TestCurrentClusterName, localVersion), nil), nsID, localVersion))
	require.NoError(t, checkSyncReplicationWrite(newShard(t, nil, nil), nsID, localVersion))

	// The host missed a failover to the other cluster: not active.
	err := checkSyncReplicationWrite(newShard(t, withVersion(cluster.TestAlternativeClusterName, localVersion+11), nil), nsID, localVersion)
	var notActive *serviceerror.NamespaceNotActive
	require.ErrorAs(t, err, &notActive)
	require.Equal(t, cluster.TestAlternativeClusterName, notActive.ActiveCluster)

	// The host missed a failover away and back: still active here, but not under the old version.
	err = checkSyncReplicationWrite(newShard(t, withVersion(cluster.TestCurrentClusterName, localVersion+20), nil), nsID, localVersion)
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)

	// The database can't be read: refuse rather than risk it.
	err = checkSyncReplicationWrite(newShard(t, nil, errors.New("database unavailable")), nsID, localVersion)
	require.ErrorAs(t, err, &unavailable)

	// Applying the other cluster's writes is never gated.
	require.NoError(t, checkSyncReplicationWrite(newShard(t, withVersion(cluster.TestCurrentClusterName, localVersion+20), nil), nsID, remoteVersion))

	// Nor are namespaces without sync replication.
	disabled := newSyncReplicationTestShard(t, false)
	require.NoError(t, checkSyncReplicationWrite(disabled, nsID, localVersion))

	// Sync replication without state-based replication or without streams: active writes are
	// refused, not made async.
	current := withVersion(cluster.TestCurrentClusterName, localVersion)
	var failedPrecondition *serviceerror.FailedPrecondition
	stateBased = false
	require.ErrorAs(t, checkSyncReplicationWrite(newShard(t, current, nil), nsID, localVersion), &failedPrecondition)
	require.NoError(t, checkSyncReplicationWrite(newShard(t, current, nil), nsID, remoteVersion))
	stateBased, streams = true, false
	require.ErrorAs(t, checkSyncReplicationWrite(newShard(t, current, nil), nsID, localVersion), &failedPrecondition)
	streams = true

	// Loads apply the same check against the cache's failover version.
	require.ErrorAs(t,
		checkSyncReplicationAcquiredVersion(newShard(t, withVersion(cluster.TestCurrentClusterName, localVersion+20), nil), tests.GlobalNamespaceEntry, localVersion),
		&unavailable,
	)
}
