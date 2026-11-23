package xdc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// syncReplicationTestSuite covers history.syncReplicationEnabled: the active cluster (clusters[0])
// acknowledges a workflow transaction only after every other cluster of the namespace has applied it,
// and hides it until then.
type syncReplicationTestSuite struct {
	xdcBaseSuite
}

func TestSyncReplicationTestSuite(t *testing.T) {
	t.Parallel()
	// Sync replication supports state-based replication only (transition history on).
	for _, clusterCount := range []int{2, 3} {
		t.Run(fmt.Sprintf("%dClusters", clusterCount), func(t *testing.T) {
			s := &syncReplicationTestSuite{}
			s.enableTransitionHistory = true
			s.clusterNames = []string{"active", "standby1", "standby2"}[:clusterCount]
			suite.Run(t, s)
		})
	}
}

func (s *syncReplicationTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.SyncReplicationEnabled.Key():                   true,
		dynamicconfig.SyncReplicationCommitTimeout.Key():             2 * time.Second,
		dynamicconfig.ReplicationStreamEagerSyncStatusInterval.Key(): 5 * time.Millisecond,
	}
	s.setupSuite()
}

func (s *syncReplicationTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *syncReplicationTestSuite) SetupTest() {
	s.setupTest()
}

// Every acknowledged write must already be in every standby's database: no polling.
func (s *syncReplicationTestSuite) TestAcknowledgedWritesAreOnStandby() {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	ns := s.createNamespace(true, s.clusters)
	active := s.clusters[0]
	workflowID := "sync-repl-" + uuid.NewString()
	taskQueue := "sync-repl-tq-" + uuid.NewString()

	startResp, err := active.FrontendClient().StartWorkflowExecution(ctx, s.startRequest(ns, workflowID, taskQueue, uuid.NewString()))
	s.Require().NoError(err)
	s.requireStatusOnStandbys(ctx, s.clusters[1:], ns, workflowID, startResp.GetRunId(), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)

	//nolint:staticcheck // TODO: replace with taskpoller.TaskPoller
	poller := &testcore.TaskPoller{
		Client:    active.FrontendClient(),
		Namespace: ns,
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:  "worker",
		WorkflowTaskHandler: func(*workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("done"),
					},
				},
			}}, nil
		},
		Logger: s.logger,
		T:      s.T(),
	}
	_, err = poller.PollAndProcessWorkflowTask()
	s.Require().NoError(err)
	s.requireStatusOnStandbys(ctx, s.clusters[1:], ns, workflowID, startResp.GetRunId(), enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
}

// While one standby doesn't apply replication, a write blocks and times out (even if the other
// standbys acknowledge it), stays invisible on the active, and a retry returns the original result
// once that standby catches up.
func (s *syncReplicationTestSuite) TestUnacknowledgedWriteIsHidden() {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	ns := s.createNamespace(true, s.clusters)
	namespaceID := s.describeNamespace(s.T(), s.clusters[0], ns, true).GetNamespaceInfo().GetId()
	active := s.clusters[0]
	standby := s.clusters[len(s.clusters)-1] // the paused one; any others keep applying

	release := make(chan struct{})
	released := false
	releaseReplication := func() {
		if !released {
			released = true
			close(release)
		}
	}
	defer releaseReplication()
	s.T().Cleanup(standby.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.HistoryReplicationTaskInterceptor, func(task *replicationspb.ReplicationTask, execute func() error) error {
			if task.GetRawTaskInfo().GetNamespaceId() == namespaceID {
				<-release
			}
			return execute()
		}),
		testhooks.GlobalScope,
	))

	workflowID := "sync-repl-blocked-" + uuid.NewString()
	request := s.startRequest(ns, workflowID, "sync-repl-tq-"+uuid.NewString(), uuid.NewString())

	startCtx, startCancel := context.WithTimeout(ctx, 5*time.Second)
	_, err := active.FrontendClient().StartWorkflowExecution(startCtx, request)
	startCancel()
	s.Require().Error(err, "start must not be acknowledged while the standby doesn't apply it")

	// The run is persisted on the active cluster but must not be visible yet.
	describeCtx, describeCancel := context.WithTimeout(ctx, 5*time.Second)
	_, err = active.FrontendClient().DescribeWorkflowExecution(describeCtx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	describeCancel()
	s.Require().Error(err, "unacknowledged workflow must not be visible on the active cluster")

	releaseReplication()

	// A retry with the same request ID returns the original run once the standby has applied it.
	startResp, err := active.FrontendClient().StartWorkflowExecution(ctx, request)
	s.Require().NoError(err)
	s.requireStatusOnStandbys(ctx, s.clusters[1:], ns, workflowID, startResp.GetRunId(), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)

	describeResp, err := active.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	s.Require().NoError(err)
	s.Equal(startResp.GetRunId(), describeResp.GetWorkflowExecutionInfo().GetExecution().GetRunId())
}

// Only the namespace's own clusters must acknowledge: a cluster the namespace isn't replicated to
// doesn't block it, even while that cluster applies nothing at all.
func (s *syncReplicationTestSuite) TestOnlyNamespaceClustersMustAcknowledge() {
	if len(s.clusters) < 3 {
		s.T().Skip("needs a cluster outside the namespace")
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	ns := s.createNamespace(true, s.clusters[:2])
	active := s.clusters[0]
	outsider := s.clusters[2]

	release := make(chan struct{})
	defer close(release)
	s.T().Cleanup(outsider.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.HistoryReplicationTaskInterceptor, func(_ *replicationspb.ReplicationTask, execute func() error) error {
			<-release
			return execute()
		}),
		testhooks.GlobalScope,
	))

	workflowID := "sync-repl-subset-" + uuid.NewString()
	startResp, err := active.FrontendClient().StartWorkflowExecution(ctx, s.startRequest(ns, workflowID, "sync-repl-tq-"+uuid.NewString(), uuid.NewString()))
	s.Require().NoError(err)
	s.requireStatusOnStandbys(ctx, s.clusters[1:2], ns, workflowID, startResp.GetRunId(), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
}

func (s *syncReplicationTestSuite) startRequest(ns, workflowID, taskQueue, requestID string) *workflowservice.StartWorkflowExecutionRequest {
	return &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           requestID,
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "sync-replication-test-workflow"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	}
}

// requireStatusOnStandbys checks the workflow straight in each standby cluster's database, once, with
// no polling: an acknowledged write must already be there.
func (s *syncReplicationTestSuite) requireStatusOnStandbys(
	ctx context.Context,
	standbys []*testcore.TestCluster,
	ns, workflowID, runID string,
	expected enumspb.WorkflowExecutionStatus,
) {
	for _, standby := range standbys {
		s.Equal(expected, s.standbyStatus(ctx, standby, ns, workflowID, runID), "cluster %s", standby.ClusterName())
	}
}

// standbyStatus reads the workflow straight from a standby cluster's database.
func (s *syncReplicationTestSuite) standbyStatus(ctx context.Context, standby *testcore.TestCluster, ns, workflowID, runID string) enumspb.WorkflowExecutionStatus {
	resp, err := standby.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	if err != nil {
		s.T().Logf("standby DescribeMutableState: %v", err)
		return enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
	}
	return resp.GetDatabaseMutableState().GetExecutionState().GetStatus()
}
