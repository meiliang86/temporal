package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
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

func newSyncReplicationTestShard(t *testing.T, enabled bool) *historyi.MockShardContext {
	controller := gomock.NewController(t)
	config := tests.NewDynamicConfig()
	config.SyncReplicationEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(enabled)
	config.SyncReplicationCommitTimeout = dynamicconfig.GetDurationPropertyFnFilteredByNamespace(time.Second)

	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	namespaceRegistry := namespace.NewMockRegistry(controller)
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	shardContext := historyi.NewMockShardContext(controller)
	shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
	shardContext.EXPECT().GetClusterMetadata().Return(clusterMetadata).AnyTimes()
	shardContext.EXPECT().GetNamespaceRegistry().Return(namespaceRegistry).AnyTimes()
	shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	return shardContext
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

func TestWaitForSyncReplication(t *testing.T) {
	t.Run("no replication task", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		require.NoError(t, waitForSyncReplication(context.Background(), shardContext, tests.NamespaceID.String(), tests.WorkflowID, map[tasks.Category][]tasks.Task{}))
	})

	t.Run("disabled", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, false)
		require.NoError(t, waitForSyncReplication(context.Background(), shardContext, tests.NamespaceID.String(), tests.WorkflowID, replicationTasks(10)))
	})

	t.Run("acknowledged", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		expectReplicationNotification(t, shardContext)
		shardContext.EXPECT().WaitForReplicationCommit(gomock.Any(), []string{cluster.TestAlternativeClusterName}, int64(10)).Return(nil)
		require.NoError(t, waitForSyncReplication(context.Background(), shardContext, tests.NamespaceID.String(), tests.WorkflowID, replicationTasks(10)))
	})

	t.Run("not acknowledged", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		expectReplicationNotification(t, shardContext)
		shardContext.EXPECT().WaitForReplicationCommit(gomock.Any(), gomock.Any(), int64(10)).Return(context.DeadlineExceeded)
		err := waitForSyncReplication(context.Background(), shardContext, tests.NamespaceID.String(), tests.WorkflowID, replicationTasks(10))
		var unavailable *serviceerror.Unavailable
		require.ErrorAs(t, err, &unavailable)
	})

	t.Run("hands the lock off while waiting", func(t *testing.T) {
		shardContext := newSyncReplicationTestShard(t, true)
		wfContext := newSyncReplicationTestContext()
		require.NoError(t, wfContext.Lock(context.Background(), locks.PriorityHigh))

		expectReplicationNotification(t, shardContext)
		shardContext.EXPECT().WaitForReplicationCommit(gomock.Any(), gomock.Any(), int64(10)).DoAndReturn(
			func(context.Context, []string, int64) error {
				// The replication sender can take the lock while the writer waits...
				readerCtx := WithSyncReplicationReader(context.Background())
				require.NoError(t, wfContext.Lock(readerCtx, locks.PriorityLow))
				wfContext.Unlock()
				// ...but nobody else can.
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				require.ErrorIs(t, wfContext.Lock(ctx, locks.PriorityHigh), context.DeadlineExceeded)
				return nil
			},
		)
		ctx := withSyncReplicationLockHandoff(context.Background(), wfContext)
		require.NoError(t, waitForSyncReplication(ctx, shardContext, tests.NamespaceID.String(), tests.WorkflowID, replicationTasks(10)))

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
