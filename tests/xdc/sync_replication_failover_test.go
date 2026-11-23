package xdc

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/testhooks"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker/migration"
	"go.temporal.io/server/tests/testcore"
)

// Failover of sync replication namespaces with two clusters (v1). The control plane is played by the
// test: it pushes membership records with UpdateSyncReplicationState and checks progress with
// GetSyncReplicationStatus, following the rules of the design (see
// docs/architecture/sync-replication-design.md on the design branch):
//
//   - Commits wait for the record's in-sync and joining clusters; an active cluster alone in the
//     record commits alone (degraded mode).
//   - A forced failover drops the old active from the record; acks carry a fencing notice so the old
//     active can't acknowledge writes the new active may have discarded.
//   - A cluster rejoins in two steps: joining (commits wait for it), then in sync (promotable) once it
//     knows the current failover, the old active's writes are drained to the active, and it has
//     applied everything of the active's.

func (s *syncReplicationTestSuite) requireTwoClusters() {
	if len(s.clusters) != 2 {
		s.T().Skip("the v1 failover protocol covers two clusters")
	}
}

// pushRecord plays the control plane: it stores the membership record on the given clusters.
func (s *syncReplicationTestSuite) pushRecord(
	ctx context.Context,
	ns string,
	record *persistencespb.NamespaceSyncReplication,
	clusters ...*testcore.TestCluster,
) {
	for _, c := range clusters {
		resp, err := c.AdminClient().UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{
			Namespaces: []string{ns},
			State:      record,
		})
		s.Require().NoError(err)
		s.Require().Len(resp.GetResults(), 1)
		s.Require().True(resp.GetResults()[0].GetApplied(), "cluster %s", c.ClusterName())
	}
	// Every history host must apply the record before the control plane relies on it.
	for _, c := range clusters {
		s.EventuallyWithT(func(t *assert.CollectT) {
			require.GreaterOrEqual(t, s.syncStatus(ctx, t, c, ns).GetMinAppliedVersion(), record.GetVersion())
		}, replicationWaitTime, replicationCheckInterval)
	}
}

func (s *syncReplicationTestSuite) syncStatus(
	ctx context.Context,
	t require.TestingT,
	c *testcore.TestCluster,
	ns string,
) *adminservice.GetSyncReplicationStatusResponse {
	resp, err := c.AdminClient().GetSyncReplicationStatus(ctx, &adminservice.GetSyncReplicationStatusRequest{Namespace: ns})
	require.NoError(t, err)
	return resp
}

func (s *syncReplicationTestSuite) peerStatus(
	ctx context.Context,
	t require.TestingT,
	c *testcore.TestCluster,
	ns string,
	peer *testcore.TestCluster,
) *adminservice.GetSyncReplicationStatusResponse_PeerStatus {
	for _, p := range s.syncStatus(ctx, t, c, ns).GetPeers() {
		if p.GetCluster() == peer.ClusterName() {
			return p
		}
	}
	require.Fail(t, "peer not reported", peer.ClusterName())
	return nil
}

func (s *syncReplicationTestSuite) failoverVersion(ctx context.Context, c *testcore.TestCluster, ns string) int64 {
	resp, err := c.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	s.Require().NoError(err)
	return resp.GetFailoverVersion()
}

// replicationGate blocks, while closed, a cluster's apply of the namespace's replication tasks.
type replicationGate struct {
	mu     sync.Mutex
	closed bool
	open   chan struct{}
}

func (s *syncReplicationTestSuite) newReplicationGate(c *testcore.TestCluster, namespaceID string) *replicationGate {
	g := &replicationGate{open: make(chan struct{})}
	close(g.open)
	s.T().Cleanup(c.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.HistoryReplicationTaskInterceptor, func(task *replicationspb.ReplicationTask, execute func() error) error {
			if task.GetRawTaskInfo().GetNamespaceId() == namespaceID {
				g.mu.Lock()
				open := g.open
				g.mu.Unlock()
				<-open
			}
			return execute()
		}),
		testhooks.GlobalScope,
	))
	s.T().Cleanup(g.Open)
	return g
}

func (g *replicationGate) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.closed = true
		g.open = make(chan struct{})
	}
}

func (g *replicationGate) Open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		g.closed = false
		close(g.open)
	}
}

// holdOutboundReplication keeps a cluster from sending the namespace's replication tasks until the
// returned function is called. A cluster that receives a task under a newer failover version
// refreshes the namespace from the sender, so a zombie must not receive the new active's tasks.
func (s *syncReplicationTestSuite) holdOutboundReplication(c *testcore.TestCluster, namespaceID string) (release func()) {
	released := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(released) }) }
	s.T().Cleanup(c.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.HistoryReplicationTaskConversionInterceptor, func(
			_ historytasks.Task,
			convert func() (*replicationspb.ReplicationTask, error),
		) (*replicationspb.ReplicationTask, error) {
			<-released
			return convert()
		}),
		namespace.ID(namespaceID),
	))
	s.T().Cleanup(release)
	return release
}

// holdNamespaceReplication keeps a cluster from applying the namespace's metadata changes (so it
// doesn't learn of a failover) until the returned function is called.
func (s *syncReplicationTestSuite) holdNamespaceReplication(c *testcore.TestCluster, ns string) (release func()) {
	var mu sync.Mutex
	holding := true
	var held []func() error
	s.T().Cleanup(c.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.NamespaceReplicationTaskInterceptor, func(_ context.Context, _ *replicationspb.NamespaceTaskAttributes, execute func() error) error {
			mu.Lock()
			defer mu.Unlock()
			if holding {
				held = append(held, execute)
				return nil
			}
			return execute()
		}),
		namespace.Name(ns),
	))
	return func() {
		mu.Lock()
		defer mu.Unlock()
		holding = false
		for _, execute := range held {
			s.Require().NoError(execute())
		}
		held = nil
	}
}

func (s *syncReplicationTestSuite) startWorkflowOn(ctx context.Context, c *testcore.TestCluster, ns, workflowID string) (string, error) {
	resp, err := c.FrontendClient().StartWorkflowExecution(ctx, s.startRequest(ns, workflowID, "sync-repl-tq-"+workflowID, "req-"+workflowID))
	return resp.GetRunId(), err
}

func (s *syncReplicationTestSuite) requireStartTimesOut(ctx context.Context, c *testcore.TestCluster, ns, workflowID string) {
	startCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := s.startWorkflowOn(startCtx, c, ns, workflowID)
	s.Require().Error(err, "start of %s must not be acknowledged", workflowID)
}

func (s *syncReplicationTestSuite) requireOnCluster(ctx context.Context, c *testcore.TestCluster, ns, workflowID string) {
	resp, err := c.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	s.Require().NoError(err, "%s on cluster %s", workflowID, c.ClusterName())
	s.Require().NotNil(resp.GetDatabaseMutableState())
}

func (s *syncReplicationTestSuite) runHandover(ctx context.Context, from, to *testcore.TestCluster, ns string) {
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  from.Host().FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.Require().NoError(err)
	defer sysClient.Close()
	run, err := sysClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 "sync-repl-handover-" + ns + "-" + to.ClusterName(),
		TaskQueue:          primitives.DefaultWorkerTaskQueue,
		WorkflowRunTimeout: time.Minute,
	}, "namespace-handover", migration.NamespaceHandoverParams{
		Namespace:              ns,
		RemoteCluster:          to.ClusterName(),
		AllowedLaggingSeconds:  10,
		HandoverTimeoutSeconds: 30,
	})
	s.Require().NoError(err)
	s.Require().NoError(run.Get(ctx, nil))
	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, c := range s.clusters {
			resp := s.describeNamespace(t, c, ns, true)
			require.Equal(t, to.ClusterName(), resp.GetReplicationConfig().GetActiveClusterName())
		}
	}, replicationWaitTime, replicationCheckInterval)
	s.waitForNamespaceCacheRefresh()
}

// The record is local to each cluster, versioned, and survives namespace updates and their
// replication.
func (s *syncReplicationTestSuite) TestFailover_MembershipRecordIsLocalAndVersioned() {
	s.requireTwoClusters()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	ns := s.createNamespace(true, s.clusters)
	a, b := s.clusters[0], s.clusters[1]

	// No record yet.
	status := s.syncStatus(ctx, s.T(), a, ns)
	s.Nil(status.GetState())
	s.Equal(a.ClusterName(), status.GetActiveCluster())

	// Different records on the two clusters: neither is copied to the other.
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 2, InSyncClusters: []string{a.ClusterName(), b.ClusterName()}}, a)
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 1, InSyncClusters: []string{a.ClusterName()}, JoiningClusters: []string{b.ClusterName()}}, b)

	// A stale or duplicate push changes nothing.
	for _, version := range []int64{1, 2} {
		resp, err := a.AdminClient().UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{
			Namespaces: []string{ns},
			State:      &persistencespb.NamespaceSyncReplication{Version: version, InSyncClusters: []string{a.ClusterName()}},
		})
		s.Require().NoError(err)
		s.False(resp.GetResults()[0].GetApplied())
		s.Equal(int64(2), resp.GetResults()[0].GetStoredVersion())
	}
	// Invalid records are rejected.
	for _, record := range []*persistencespb.NamespaceSyncReplication{
		{Version: 3},
		{Version: 3, InSyncClusters: []string{"not-a-cluster"}},
		{Version: 3, InSyncClusters: []string{a.ClusterName()}, JoiningClusters: []string{a.ClusterName()}},
	} {
		_, err := a.AdminClient().UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{
			Namespaces: []string{ns},
			State:      record,
		})
		var invalidArgument *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgument, "%v", record)
	}

	// A namespace update on the active cluster, replicated to the other, keeps both records.
	_, err := a.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace:  ns,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{Description: "updated"},
	})
	s.Require().NoError(err)
	s.EventuallyWithT(func(t *assert.CollectT) {
		require.Equal(t, "updated", s.describeNamespace(t, b, ns, true).GetNamespaceInfo().GetDescription())
	}, replicationWaitTime, replicationCheckInterval)
	s.Equal(int64(2), s.syncStatus(ctx, s.T(), a, ns).GetState().GetVersion())
	s.Equal(int64(1), s.syncStatus(ctx, s.T(), b, ns).GetState().GetVersion())
	s.Equal([]string{b.ClusterName()}, s.syncStatus(ctx, s.T(), b, ns).GetState().GetJoiningClusters())

	// So does a failover.
	s.failover(ns, 0, b.ClusterName(), s.failoverVersion(ctx, a, ns)+1)
	s.Equal(int64(2), s.syncStatus(ctx, s.T(), a, ns).GetState().GetVersion())
	s.Equal(int64(1), s.syncStatus(ctx, s.T(), b, ns).GetState().GetVersion())
}

// Degraded mode: once the control plane drops the standby, the active commits alone, including a
// write that was already waiting. The standby then rejoins in two steps.
func (s *syncReplicationTestSuite) TestFailover_DegradedModeAndRejoin() {
	s.requireTwoClusters()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	ns := s.createNamespace(true, s.clusters)
	a, b := s.clusters[0], s.clusters[1]
	namespaceID := s.describeNamespace(s.T(), a, ns, true).GetNamespaceInfo().GetId()
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 1, InSyncClusters: []string{a.ClusterName(), b.ClusterName()}}, a, b)

	// The standby stops applying: writes block.
	gate := s.newReplicationGate(b, namespaceID)
	gate.Close()
	s.requireStartTimesOut(ctx, a, ns, "degraded-before")

	// A write that is waiting when the control plane drops the standby commits without it.
	a.OverrideDynamicConfig(s.T(), dynamicconfig.SyncReplicationCommitTimeout, 20*time.Second)
	pending := make(chan error, 1)
	go func() {
		_, err := s.startWorkflowOn(ctx, a, ns, "degraded-pending")
		pending <- err
	}()
	select {
	case err := <-pending:
		s.Require().Failf("write acknowledged without the standby", "err: %v", err)
	case <-time.After(time.Second):
	}
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 2, InSyncClusters: []string{a.ClusterName()}}, a, b)
	select {
	case err := <-pending:
		s.Require().NoError(err)
	case <-time.After(15 * time.Second):
		s.Require().Fail("pending write not committed in degraded mode")
	}

	// New writes commit alone, and the earlier unacknowledged one is visible now: its retry succeeds.
	_, err := s.startWorkflowOn(ctx, a, ns, "degraded-new")
	s.Require().NoError(err)
	_, err = s.startWorkflowOn(ctx, a, ns, "degraded-before")
	s.Require().NoError(err)
	// The standby doesn't have them and is reported as behind.
	s.False(s.peerStatus(ctx, s.T(), a, ns, b).GetDrained())

	// Rejoin, step 1: commits wait for the joining standby again.
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{
		Version: 3, InSyncClusters: []string{a.ClusterName()}, JoiningClusters: []string{b.ClusterName()},
	}, a, b)
	a.OverrideDynamicConfig(s.T(), dynamicconfig.SyncReplicationCommitTimeout, 2*time.Second)
	s.requireStartTimesOut(ctx, a, ns, "joining-blocked")

	// The standby catches up: the control plane sees it drained, and writes commit with it.
	gate.Open()
	s.EventuallyWithT(func(t *assert.CollectT) {
		require.True(t, s.peerStatus(ctx, t, a, ns, b).GetDrained())
	}, replicationWaitTime, replicationCheckInterval)
	for _, workflowID := range []string{"degraded-before", "degraded-pending", "degraded-new", "joining-blocked"} {
		s.requireOnCluster(ctx, b, ns, workflowID)
	}

	// Step 2: in sync again. Every acknowledged write is on the standby.
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 4, InSyncClusters: []string{a.ClusterName(), b.ClusterName()}}, a, b)
	runID, err := s.startWorkflowOn(ctx, a, ns, "rejoined")
	s.Require().NoError(err)
	s.requireStatusOnStandbys(ctx, []*testcore.TestCluster{b}, ns, "rejoined", runID, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
}

// Planned failover (the handover workflow) and failback: after each, the new active waits for the
// other cluster, and the fencing notice left over from the first failover doesn't block writes after
// the failback.
func (s *syncReplicationTestSuite) TestFailover_PlannedFailoverAndFailback() {
	s.requireTwoClusters()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	ns := s.createNamespace(true, s.clusters)
	a, b := s.clusters[0], s.clusters[1]
	namespaceID := s.describeNamespace(s.T(), a, ns, true).GetNamespaceInfo().GetId()
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 1, InSyncClusters: []string{a.ClusterName(), b.ClusterName()}}, a, b)

	runID, err := s.startWorkflowOn(ctx, a, ns, "before-failover")
	s.Require().NoError(err)

	s.runHandover(ctx, a, b, ns)

	// B is active and waits for A.
	gateA := s.newReplicationGate(a, namespaceID)
	gateA.Close()
	s.requireStartTimesOut(ctx, b, ns, "on-b-blocked")
	gateA.Open()
	runIDB, err := s.startWorkflowOn(ctx, b, ns, "on-b")
	s.Require().NoError(err)
	s.requireStatusOnStandbys(ctx, []*testcore.TestCluster{a}, ns, "on-b", runIDB, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	// A workflow created before the failover is writable on B.
	_, err = b.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "before-failover", RunId: runID},
		SignalName:        "on-b",
		RequestId:         "signal-on-b",
	})
	s.Require().NoError(err)

	// While B was active, A received B's fencing notice for the namespace.
	versionB := s.failoverVersion(ctx, b, ns)
	s.EventuallyWithT(func(t *assert.CollectT) {
		require.Equal(t, versionB, s.peerStatus(ctx, t, a, ns, b).GetFencedAtFailoverVersion())
	}, replicationWaitTime, replicationCheckInterval)

	// Failback. A's writes are made under a newer version than B's notice, so they commit, and a
	// workflow whose last write was made on B loads on A.
	s.runHandover(ctx, b, a, ns)
	s.Greater(s.failoverVersion(ctx, a, ns), versionB)
	runIDA, err := s.startWorkflowOn(ctx, a, ns, "after-failback")
	s.Require().NoError(err)
	s.requireStatusOnStandbys(ctx, []*testcore.TestCluster{b}, ns, "after-failback", runIDA, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	_, err = a.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "before-failover", RunId: runID},
		SignalName:        "on-a",
		RequestId:         "signal-on-a",
	})
	s.Require().NoError(err)
	describeResp, err := a.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: "on-b"},
	})
	s.Require().NoError(err)
	s.Equal(runIDB, describeResp.GetWorkflowExecutionInfo().GetExecution().GetRunId())
}

// Forced failover while the old active is cut off from the namespace change: the old active keeps
// believing it is active (a zombie). The new active's acks fence it, so it acknowledges nothing and
// exposes nothing it wrote after the failover. When it returns, it rejoins and the namespace fails
// back to it.
func (s *syncReplicationTestSuite) TestFailover_ForcedFailoverFencesOldActive() {
	s.requireTwoClusters()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	ns := s.createNamespace(true, s.clusters)
	a, b := s.clusters[0], s.clusters[1]
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 1, InSyncClusters: []string{a.ClusterName(), b.ClusterName()}}, a, b)
	runID0, err := s.startWorkflowOn(ctx, a, ns, "before-failover")
	s.Require().NoError(err)
	versionA := s.failoverVersion(ctx, a, ns)

	// Forced failover to B; A doesn't hear about it, neither through namespace replication nor from
	// B's replication tasks. The control plane drops A (B commits alone).
	namespaceID := s.describeNamespace(s.T(), a, ns, true).GetNamespaceInfo().GetId()
	releaseA := s.holdNamespaceReplication(a, ns)
	releaseB := s.holdOutboundReplication(b, namespaceID)
	_, err = b.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace:         ns,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{ActiveClusterName: b.ClusterName()},
	})
	s.Require().NoError(err)
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 2, InSyncClusters: []string{b.ClusterName()}}, b)
	s.waitForNamespaceCacheRefresh()
	versionB := s.failoverVersion(ctx, b, ns)
	s.Greater(versionB, versionA)
	s.Equal(a.ClusterName(), s.describeNamespace(s.T(), a, ns, true).GetReplicationConfig().GetActiveClusterName())

	// B commits alone.
	runIDB, err := s.startWorkflowOn(ctx, b, ns, "on-b")
	s.Require().NoError(err)

	// The zombie A: a new write is persisted and replicated, but B's ack is fenced, so A doesn't
	// acknowledge it...
	s.EventuallyWithT(func(t *assert.CollectT) {
		require.Equal(t, versionB, s.peerStatus(ctx, t, a, ns, b).GetFencedAtFailoverVersion())
	}, replicationWaitTime, replicationCheckInterval)
	s.Equal(a.ClusterName(), s.describeNamespace(s.T(), a, ns, true).GetReplicationConfig().GetActiveClusterName())
	_, err = s.startWorkflowOn(ctx, a, ns, "zombie-write")
	var notActive *serviceerror.NamespaceNotActive
	s.Require().ErrorAs(err, &notActive)
	// ...nor does it acknowledge writes to existing workflows...
	_, err = a.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "before-failover", RunId: runID0},
		SignalName:        "zombie",
		RequestId:         "signal-zombie",
	})
	s.Require().ErrorAs(err, &notActive)
	// ...nor expose them: loading the zombie write's state is fenced too.
	_, err = a.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: "zombie-write"},
	})
	s.Require().Error(err)

	// A returns: it learns of the failover. The control plane checks it before letting it rejoin:
	// A knows the current failover, and A's writes (including the zombie one) are drained to B.
	releaseA()
	releaseB()
	s.EventuallyWithT(func(t *assert.CollectT) {
		status := s.syncStatus(ctx, t, a, ns)
		require.Equal(t, b.ClusterName(), status.GetActiveCluster())
		require.GreaterOrEqual(t, status.GetMinKnownFailoverVersion(), versionB)
		require.True(t, s.peerStatus(ctx, t, a, ns, b).GetDrained())
	}, replicationWaitTime, replicationCheckInterval)
	s.waitForNamespaceCacheRefresh()

	// Rejoin, step 1 (joining), then wait until A has everything of B's.
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{
		Version: 3, InSyncClusters: []string{b.ClusterName()}, JoiningClusters: []string{a.ClusterName()},
	}, b, a)
	s.EventuallyWithT(func(t *assert.CollectT) {
		require.True(t, s.peerStatus(ctx, t, b, ns, a).GetDrained())
	}, replicationWaitTime, replicationCheckInterval)
	s.requireOnCluster(ctx, a, ns, "on-b")
	// Step 2: A is in sync and promotable again. B's writes now wait for A.
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 4, InSyncClusters: []string{a.ClusterName(), b.ClusterName()}}, b, a)
	runIDB2, err := s.startWorkflowOn(ctx, b, ns, "on-b-in-sync")
	s.Require().NoError(err)
	s.requireStatusOnStandbys(ctx, []*testcore.TestCluster{a}, ns, "on-b-in-sync", runIDB2, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)

	// Failback to A. Everything B acknowledged is there, and A's earlier workflows are writable.
	s.runHandover(ctx, b, a, ns)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, s.standbyStatus(ctx, a, ns, "on-b", runIDB))
	_, err = a.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "before-failover", RunId: runID0},
		SignalName:        "after-failback",
		RequestId:         "signal-after-failback",
	})
	s.Require().NoError(err)
	runIDA, err := s.startWorkflowOn(ctx, a, ns, "after-failback")
	s.Require().NoError(err)
	s.requireStatusOnStandbys(ctx, []*testcore.TestCluster{b}, ns, "after-failback", runIDA, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	// The records survived the failovers' namespace replication.
	s.Equal(int64(4), s.syncStatus(ctx, s.T(), a, ns).GetState().GetVersion())
	s.Equal(int64(4), s.syncStatus(ctx, s.T(), b, ns).GetState().GetVersion())
}

// A shard of the new active moves to a history host whose namespace cache hasn't seen the forced
// failover yet. The new active had already written the workflow under the new version, so that host
// ignores the zombie old active's write to it as a stale branch. Its ack must still fence the old
// active: otherwise the old active acknowledges a write the in-sync cluster doesn't have.
//
// The onebox runs one history host per cluster, so the move is played by closing the shard (it is
// re-acquired) while the SyncReplicationStaleNamespaceView hook gives the acks the pre-failover view
// of the namespace cache. The old active's shard is closed too: fencing notices are kept in memory,
// and a reloaded shard must get them again from the new active.
func (s *syncReplicationTestSuite) TestFailover_NewShardOwnerWithStaleNamespaceCacheFences() {
	s.requireTwoClusters()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	ns := s.createNamespace(true, s.clusters)
	a, b := s.clusters[0], s.clusters[1]
	namespaceID := s.describeNamespace(s.T(), a, ns, true).GetNamespaceInfo().GetId()
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 1, InSyncClusters: []string{a.ClusterName(), b.ClusterName()}}, a, b)
	a.OverrideDynamicConfig(s.T(), dynamicconfig.SyncReplicationCommitTimeout, 10*time.Second)

	const workflowID = "stale-cache"
	runID, err := s.startWorkflowOn(ctx, a, ns, workflowID)
	s.Require().NoError(err)

	// Forced failover to B. A doesn't hear about it, nor receive B's writes.
	releaseNamespace := s.holdNamespaceReplication(a, ns)
	releaseOutbound := s.holdOutboundReplication(b, namespaceID)
	// Leave the clusters converged for the next test: A learns of the failover before it receives
	// B's tasks (as a zombie it would refuse to apply them, stalling its acks for every namespace).
	defer func() {
		releaseNamespace()
		s.EventuallyWithT(func(t *assert.CollectT) {
			require.Equal(t, b.ClusterName(), s.describeNamespace(t, a, ns, true).GetReplicationConfig().GetActiveClusterName())
		}, replicationWaitTime, replicationCheckInterval)
		s.waitForNamespaceCacheRefresh()
		releaseOutbound()
		s.EventuallyWithT(func(t *assert.CollectT) {
			require.True(t, s.peerStatus(ctx, t, b, ns, a).GetDrained())
		}, replicationWaitTime, replicationCheckInterval)
	}()
	_, err = b.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace:         ns,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{ActiveClusterName: b.ClusterName()},
	})
	s.Require().NoError(err)
	s.pushRecord(ctx, ns, &persistencespb.NamespaceSyncReplication{Version: 2, InSyncClusters: []string{b.ClusterName()}}, b)
	s.waitForNamespaceCacheRefresh()
	versionB := s.failoverVersion(ctx, b, ns)

	// B writes the workflow under the new failover version.
	_, err = b.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		SignalName:        "on-b",
		RequestId:         "signal-on-b",
	})
	s.Require().NoError(err)

	// The workflow's shard on B moves to a host with a stale namespace cache; A's shard reloads.
	s.T().Cleanup(b.InjectHook(s.T(), testhooks.NewHook(testhooks.SyncReplicationStaleNamespaceView, true), namespace.ID(namespaceID)))
	// (A refuses to load the workflow already: it has B's fencing notice, in memory. Both clusters
	// have the same number of shards, so B's shard ID is A's too.)
	resp, err := b.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	s.Require().NoError(err)
	shardID, err := strconv.ParseInt(resp.GetShardId(), 10, 32)
	s.Require().NoError(err)
	for _, c := range []*testcore.TestCluster{b, a} {
		_, err = c.AdminClient().CloseShard(ctx, &adminservice.CloseShardRequest{ShardId: int32(shardID)})
		s.Require().NoError(err)
	}

	s.Equal(a.ClusterName(), s.describeNamespace(s.T(), a, ns, true).GetReplicationConfig().GetActiveClusterName())

	// The zombie A hands out the workflow task and completes the workflow, under its old version.
	// B ignores both as a stale branch. A must not acknowledge either.
	pollCtx, pollCancel := context.WithTimeout(ctx, 15*time.Second)
	defer pollCancel()
	pollResp, err := a.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: ns,
		TaskQueue: &taskqueuepb.TaskQueue{Name: "sync-repl-tq-" + workflowID, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "zombie-worker",
	})
	if err != nil || len(pollResp.GetTaskToken()) == 0 {
		// Not acknowledged. Check it is because B fenced A, not for some other reason: A's reloaded
		// shard has B's fencing notice for the new version.
		s.EventuallyWithT(func(t *assert.CollectT) {
			require.Equal(t, versionB, s.peerStatus(ctx, t, a, ns, b).GetFencedAtFailoverVersion())
		}, replicationWaitTime, replicationCheckInterval)
		return
	}
	_, err = a.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: ns,
		TaskToken: pollResp.GetTaskToken(),
		Identity:  "zombie-worker",
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.Failf("old active acknowledged a write the in-sync cluster doesn't have",
		"A handed out the workflow task (started event %d) and completing the workflow returned %v; on B the workflow is %v",
		pollResp.GetStartedEventId(), err, s.standbyStatus(ctx, b, ns, workflowID, runID))
}
