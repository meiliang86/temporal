package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func syncReplicationTestNamespace(clusters []string, record *persistencespb.NamespaceSyncReplication) *persistence.GetNamespaceResponse {
	return &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: "ns-id", Name: "ns"},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "a",
				Clusters:          clusters,
				SyncReplication:   record,
				FailoverHistory:   []*persistencespb.FailoverStatus{{FailoverVersion: 1}},
			},
			FailoverVersion: 11,
		},
		IsGlobalNamespace: true,
	}
}

func TestValidateSyncReplicationState(t *testing.T) {
	ns := syncReplicationTestNamespace([]string{"a", "b"}, nil)
	valid := []*persistencespb.NamespaceSyncReplication{
		{Version: 1, InSyncClusters: []string{"a", "b"}},
		{Version: 2, InSyncClusters: []string{"a"}},                                // degraded
		{Version: 3, InSyncClusters: []string{"a"}, JoiningClusters: []string{"b"}}, // rejoin, step 1
		{Version: 4, InSyncClusters: []string{"b"}},                                // after forced failover to b
	}
	for _, state := range valid {
		require.NoError(t, validateSyncReplicationState(ns, state), "%v", state)
	}

	invalid := []*persistencespb.NamespaceSyncReplication{
		{Version: 0, InSyncClusters: []string{"a"}},
		{Version: -1, InSyncClusters: []string{"a"}},
		{Version: 1},
		{Version: 1, JoiningClusters: []string{"b"}},
		{Version: 1, InSyncClusters: []string{"a", "c"}},
		{Version: 1, InSyncClusters: []string{"a"}, JoiningClusters: []string{"c"}},
		{Version: 1, InSyncClusters: []string{"a", "b"}, JoiningClusters: []string{"b"}},
		{Version: 1, InSyncClusters: []string{"a", "a"}},
	}
	for _, state := range invalid {
		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, validateSyncReplicationState(ns, state), &invalidArgument, "%v", state)
	}

	local := syncReplicationTestNamespace([]string{"a"}, nil)
	require.Error(t, validateSyncReplicationState(local, valid[0]))
	notGlobal := syncReplicationTestNamespace([]string{"a", "b"}, nil)
	notGlobal.IsGlobalNamespace = false
	require.Error(t, validateSyncReplicationState(notGlobal, valid[0]))
}

func TestBuildSyncReplicationStatus(t *testing.T) {
	record := &persistencespb.NamespaceSyncReplication{Version: 5, InSyncClusters: []string{"a", "b"}}
	ns := syncReplicationTestNamespace([]string{"a", "b"}, record)
	shard := func(id int32, barrier, acked, applied, known, fence int64) *historyservice.ShardReplicationStatus {
		status := &historyservice.ShardReplicationStatus{
			ShardId:                        id,
			SyncReplicationBarrier:         barrier,
			SyncReplicationAppliedVersions: map[string]int64{"ns-id": applied},
			NamespaceFailoverVersions:      map[string]int64{"ns-id": known},
			RemoteClusters: map[string]*historyservice.ShardReplicationStatusPerCluster{
				"b": {AckedTaskId: acked},
			},
		}
		if fence > 0 {
			status.RemoteClusters["b"].FencedNamespaceFailoverVersions = map[string]int64{"ns-id": fence, "other": 99}
		}
		return status
	}

	// Shard 2 hasn't applied the record nor learned the latest failover, and the peer is behind on it.
	status := buildSyncReplicationStatus(ns, []string{"b"}, []*historyservice.ShardReplicationStatus{
		shard(2, 300, 299, 4, 1, 0),
		shard(1, 100, 150, 5, 11, 12),
	})
	require.Equal(t, "ns", status.GetNamespace())
	require.Equal(t, "ns-id", status.GetNamespaceId())
	require.Equal(t, "a", status.GetActiveCluster())
	require.Equal(t, int64(11), status.GetFailoverVersion())
	require.True(t, proto.Equal(record, status.GetState()))
	require.Equal(t, int64(4), status.GetMinAppliedVersion())
	require.Equal(t, int64(1), status.GetMinKnownFailoverVersion())
	require.Len(t, status.GetPeers(), 1)
	peer := status.GetPeers()[0]
	require.Equal(t, "b", peer.GetCluster())
	require.False(t, peer.GetDrained())
	require.Equal(t, int64(12), peer.GetFencedAtFailoverVersion())
	require.Len(t, peer.GetShards(), 2)
	require.Equal(t, int32(1), peer.GetShards()[0].GetShardId()) // sorted by shard
	require.Equal(t, int64(100), peer.GetShards()[0].GetSyncReplicationBarrier())
	require.Equal(t, int64(150), peer.GetShards()[0].GetAckedTaskId())

	// Every shard acked up to its barrier: drained.
	status = buildSyncReplicationStatus(ns, []string{"b"}, []*historyservice.ShardReplicationStatus{
		shard(1, 100, 100, 5, 11, 0),
		shard(2, 300, 301, 5, 11, 0),
	})
	require.True(t, status.GetPeers()[0].GetDrained())
	require.Equal(t, int64(5), status.GetMinAppliedVersion())
	require.Equal(t, int64(11), status.GetMinKnownFailoverVersion())
	require.Zero(t, status.GetPeers()[0].GetFencedAtFailoverVersion())

	// A peer with no progress reported on some shard is not drained.
	missing := shard(2, 0, 0, 5, 11, 0)
	delete(missing.RemoteClusters, "b")
	status = buildSyncReplicationStatus(ns, []string{"b"}, []*historyservice.ShardReplicationStatus{
		shard(1, 100, 100, 5, 11, 0),
		missing,
	})
	require.False(t, status.GetPeers()[0].GetDrained())
}

func (s *adminHandlerSuite) TestUpdateSyncReplicationState() {
	ctx := context.Background()
	metadataMgr := s.mockResource.MetadataMgr
	state := &persistencespb.NamespaceSyncReplication{Version: 5, InSyncClusters: []string{"a"}, JoiningClusters: []string{"b"}}

	// Invalid requests.
	_, err := s.handler.UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{Namespaces: []string{"ns"}})
	s.Error(err)
	_, err = s.handler.UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{State: state})
	s.Error(err)

	// Stored: the record is written into the namespace's replication config and nothing else changes.
	stored := syncReplicationTestNamespace([]string{"a", "b"}, &persistencespb.NamespaceSyncReplication{Version: 4, InSyncClusters: []string{"a"}})
	metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "ns"}).Return(stored, nil)
	metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 77}, nil)
	metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(stored, nil)
	metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
			s.Equal(int64(77), request.NotificationVersion)
			s.True(request.IsGlobalNamespace)
			expected := proto.Clone(stored.Namespace).(*persistencespb.NamespaceDetail)
			expected.ReplicationConfig.SyncReplication = state
			s.True(proto.Equal(expected, request.Namespace), "%v", request.Namespace)
			return nil
		},
	)
	resp, err := s.handler.UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{Namespaces: []string{"ns"}, State: state})
	s.NoError(err)
	s.Len(resp.GetResults(), 1)
	s.Equal("ns", resp.GetResults()[0].GetNamespace())
	s.True(resp.GetResults()[0].GetApplied())
	s.Equal(int64(5), resp.GetResults()[0].GetStoredVersion())
	// The caller's request isn't aliased into the stored record.
	s.Equal(int64(4), stored.Namespace.ReplicationConfig.SyncReplication.GetVersion())

	// Stale or duplicate push: nothing written.
	current := syncReplicationTestNamespace([]string{"a", "b"}, &persistencespb.NamespaceSyncReplication{Version: 5, InSyncClusters: []string{"a"}})
	metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "ns"}).Return(current, nil)
	resp, err = s.handler.UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{Namespaces: []string{"ns"}, State: state})
	s.NoError(err)
	s.False(resp.GetResults()[0].GetApplied())
	s.Equal(int64(5), resp.GetResults()[0].GetStoredVersion())

	// A newer record stored concurrently (between the two reads) wins.
	stale := syncReplicationTestNamespace([]string{"a", "b"}, nil)
	newer := syncReplicationTestNamespace([]string{"a", "b"}, &persistencespb.NamespaceSyncReplication{Version: 9, InSyncClusters: []string{"a"}})
	metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "ns"}).Return(stale, nil)
	metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 78}, nil)
	metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(newer, nil)
	resp, err = s.handler.UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{Namespaces: []string{"ns"}, State: state})
	s.NoError(err)
	s.False(resp.GetResults()[0].GetApplied())
	s.Equal(int64(9), resp.GetResults()[0].GetStoredVersion())

	// A concurrent namespace write fails the conditional update; the error goes back to the caller,
	// which retries.
	metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "ns"}).Return(stale, nil)
	metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 79}, nil)
	metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(stale, nil)
	metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.ConditionFailedError{Msg: "notification version changed"})
	_, err = s.handler.UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{Namespaces: []string{"ns"}, State: state})
	var conditionFailed *persistence.ConditionFailedError
	s.True(errors.As(err, &conditionFailed))

	// One invalid namespace in the request: nothing is written for any of them.
	other := syncReplicationTestNamespace([]string{"a", "c"}, nil)
	metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "ns"}).Return(stale, nil)
	metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "other"}).Return(other, nil)
	_, err = s.handler.UpdateSyncReplicationState(ctx, &adminservice.UpdateSyncReplicationStateRequest{Namespaces: []string{"ns", "other"}, State: state})
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
}

func (s *adminHandlerSuite) TestGetSyncReplicationStatus() {
	ctx := context.Background()
	record := &persistencespb.NamespaceSyncReplication{Version: 5, InSyncClusters: []string{s.currentClusterName, "b"}}
	ns := syncReplicationTestNamespace([]string{s.currentClusterName, "b"}, record)
	s.mockResource.MetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "ns"}).Return(ns, nil).Times(2)
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		s.currentClusterName: {ShardCount: 2},
		"b":                  {ShardCount: 2},
	}).AnyTimes()
	shards := []*historyservice.ShardReplicationStatus{
		{ShardId: 1, SyncReplicationBarrier: 10, RemoteClusters: map[string]*historyservice.ShardReplicationStatusPerCluster{"b": {AckedTaskId: 10}}},
		{ShardId: 2, SyncReplicationBarrier: 20, RemoteClusters: map[string]*historyservice.ShardReplicationStatusPerCluster{"b": {AckedTaskId: 25}}},
	}
	s.mockHistoryClient.EXPECT().GetReplicationStatus(gomock.Any(), &historyservice.GetReplicationStatusRequest{
		RemoteClusters:              []string{"b"},
		SyncReplicationNamespaceIds: []string{"ns-id"},
	}).Return(&historyservice.GetReplicationStatusResponse{Shards: shards}, nil)

	resp, err := s.handler.GetSyncReplicationStatus(ctx, &adminservice.GetSyncReplicationStatusRequest{Namespace: "ns"})
	s.NoError(err)
	s.Len(resp.GetPeers(), 1)
	s.True(resp.GetPeers()[0].GetDrained())

	// A shard that didn't report could hold an undrained write: no answer rather than a wrong one.
	s.mockHistoryClient.EXPECT().GetReplicationStatus(gomock.Any(), gomock.Any()).
		Return(&historyservice.GetReplicationStatusResponse{Shards: shards[:1]}, nil)
	_, err = s.handler.GetSyncReplicationStatus(ctx, &adminservice.GetSyncReplicationStatusRequest{Namespace: "ns"})
	var unavailable *serviceerror.Unavailable
	s.ErrorAs(err, &unavailable)
}
