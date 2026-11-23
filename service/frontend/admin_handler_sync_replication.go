package frontend

import (
	"context"
	"slices"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

// Sync replication failover (v1, two clusters): the control plane decides which clusters a sync
// replication namespace's commits wait for (the membership record) and pushes that record to every
// cluster of the namespace with UpdateSyncReplicationState. It reads each cluster's progress with
// GetSyncReplicationStatus to decide when a cluster may rejoin and when a failover may happen.
// See docs/architecture/sync-replication-design.md on the design branch.

// UpdateSyncReplicationState stores the membership record in each listed namespace, if its version
// is higher than the stored one. The record is local to this cluster: it is written straight to the
// metadata store, so namespace replication neither sends nor overwrites it. History hosts pick it up
// on their next namespace registry refresh; GetSyncReplicationStatus reports when they all have.
func (adh *AdminHandler) UpdateSyncReplicationState(
	ctx context.Context,
	request *adminservice.UpdateSyncReplicationStateRequest,
) (_ *adminservice.UpdateSyncReplicationStateResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)
	if request == nil || request.GetState() == nil || len(request.GetNamespaces()) == 0 {
		return nil, errRequestNotSet
	}

	// Validate every namespace before writing any, so a bad request changes nothing.
	namespaces := make([]*persistence.GetNamespaceResponse, 0, len(request.GetNamespaces()))
	for _, name := range request.GetNamespaces() {
		resp, err := adh.persistenceMetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: name})
		if err != nil {
			return nil, err
		}
		if err := validateSyncReplicationState(resp, request.GetState()); err != nil {
			return nil, err
		}
		namespaces = append(namespaces, resp)
	}

	response := &adminservice.UpdateSyncReplicationStateResponse{}
	for _, resp := range namespaces {
		result, err := adh.updateSyncReplicationState(ctx, resp, request.GetState())
		if err != nil {
			return nil, err
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func (adh *AdminHandler) updateSyncReplicationState(
	ctx context.Context,
	resp *persistence.GetNamespaceResponse,
	state *persistencespb.NamespaceSyncReplication,
) (*adminservice.UpdateSyncReplicationStateResponse_Result, error) {
	name := resp.Namespace.GetInfo().GetName()
	stored := resp.Namespace.GetReplicationConfig().GetSyncReplication()
	if stored.GetVersion() >= state.GetVersion() {
		return &adminservice.UpdateSyncReplicationStateResponse_Result{
			Namespace:     name,
			StoredVersion: stored.GetVersion(),
		}, nil
	}

	metadata, err := adh.persistenceMetadataManager.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	// The namespace was read before the metadata: the update below is conditioned on the metadata's
	// notification version, which any namespace write since then has changed, so a concurrent write
	// makes this one fail rather than be lost. Read the namespace again under that version.
	current, err := adh.persistenceMetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: resp.Namespace.GetInfo().GetId()})
	if err != nil {
		return nil, err
	}
	if err := validateSyncReplicationState(current, state); err != nil {
		return nil, err
	}
	stored = current.Namespace.GetReplicationConfig().GetSyncReplication()
	if stored.GetVersion() >= state.GetVersion() {
		return &adminservice.UpdateSyncReplicationStateResponse_Result{
			Namespace:     name,
			StoredVersion: stored.GetVersion(),
		}, nil
	}

	detail := common.CloneProto(current.Namespace)
	detail.ReplicationConfig.SyncReplication = common.CloneProto(state)
	if err := adh.persistenceMetadataManager.UpdateNamespace(ctx, &persistence.UpdateNamespaceRequest{
		Namespace:           detail,
		IsGlobalNamespace:   current.IsGlobalNamespace,
		NotificationVersion: metadata.NotificationVersion,
	}); err != nil {
		return nil, err
	}
	return &adminservice.UpdateSyncReplicationStateResponse_Result{
		Namespace:     name,
		StoredVersion: state.GetVersion(),
		Applied:       true,
	}, nil
}

// validateSyncReplicationState checks a membership record against the namespace it is for. It does
// not require the active cluster to be in sync: after a forced failover the control plane drops the
// old active, and the old active must accept that record while it still believes it is active.
func validateSyncReplicationState(
	resp *persistence.GetNamespaceResponse,
	state *persistencespb.NamespaceSyncReplication,
) error {
	name := resp.Namespace.GetInfo().GetName()
	if !resp.IsGlobalNamespace || len(resp.Namespace.GetReplicationConfig().GetClusters()) < 2 {
		return serviceerror.NewInvalidArgumentf("namespace %s is not replicated to more than one cluster", name)
	}
	if state.GetVersion() <= 0 {
		return serviceerror.NewInvalidArgument("sync replication state version must be positive")
	}
	if len(state.GetInSyncClusters()) == 0 {
		return serviceerror.NewInvalidArgument("sync replication state must have at least one in-sync cluster")
	}
	namespaceClusters := resp.Namespace.GetReplicationConfig().GetClusters()
	seen := make(map[string]struct{}, len(state.GetInSyncClusters())+len(state.GetJoiningClusters()))
	for _, cluster := range slices.Concat(state.GetInSyncClusters(), state.GetJoiningClusters()) {
		if !slices.Contains(namespaceClusters, cluster) {
			return serviceerror.NewInvalidArgumentf("cluster %q is not a cluster of namespace %s", cluster, name)
		}
		if _, ok := seen[cluster]; ok {
			return serviceerror.NewInvalidArgumentf("cluster %q is listed more than once (a joining cluster is not in sync yet)", cluster)
		}
		seen[cluster] = struct{}{}
	}
	return nil
}

// GetSyncReplicationStatus reports what this cluster knows about a sync replication namespace and how
// far replication has progressed between this cluster and each other cluster of the namespace.
func (adh *AdminHandler) GetSyncReplicationStatus(
	ctx context.Context,
	request *adminservice.GetSyncReplicationStatusRequest,
) (_ *adminservice.GetSyncReplicationStatusResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)
	if request == nil || request.GetNamespace() == "" {
		return nil, errRequestNotSet
	}
	resp, err := adh.persistenceMetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: request.GetNamespace()})
	if err != nil {
		return nil, err
	}
	namespaceID := resp.Namespace.GetInfo().GetId()
	currentCluster := adh.clusterMetadata.GetCurrentClusterName()
	var peers []string
	for _, cluster := range resp.Namespace.GetReplicationConfig().GetClusters() {
		if cluster != currentCluster {
			peers = append(peers, cluster)
		}
	}

	status, err := adh.historyClient.GetReplicationStatus(ctx, &historyservice.GetReplicationStatusRequest{
		RemoteClusters:              peers,
		SyncReplicationNamespaceIds: []string{namespaceID},
	})
	if err != nil {
		return nil, err
	}
	shardCount := adh.clusterMetadata.GetAllClusterInfo()[currentCluster].ShardCount
	if int32(len(status.GetShards())) < shardCount {
		// A shard that didn't report could hold a write a peer doesn't have yet.
		return nil, serviceerror.NewUnavailablef(
			"replication status covers %d of %d history shards", len(status.GetShards()), shardCount,
		)
	}
	return buildSyncReplicationStatus(resp, peers, status.GetShards()), nil
}

func buildSyncReplicationStatus(
	resp *persistence.GetNamespaceResponse,
	peers []string,
	shards []*historyservice.ShardReplicationStatus,
) *adminservice.GetSyncReplicationStatusResponse {
	namespaceID := resp.Namespace.GetInfo().GetId()
	out := &adminservice.GetSyncReplicationStatusResponse{
		Namespace:       resp.Namespace.GetInfo().GetName(),
		NamespaceId:     namespaceID,
		ActiveCluster:   resp.Namespace.GetReplicationConfig().GetActiveClusterName(),
		FailoverVersion: resp.Namespace.GetFailoverVersion(),
		State:           resp.Namespace.GetReplicationConfig().GetSyncReplication(),
	}
	shards = slices.Clone(shards)
	slices.SortFunc(shards, func(a, b *historyservice.ShardReplicationStatus) int {
		return int(a.GetShardId() - b.GetShardId())
	})
	for i, shard := range shards {
		applied := shard.GetSyncReplicationAppliedVersions()[namespaceID]
		known := shard.GetNamespaceFailoverVersions()[namespaceID]
		if i == 0 {
			out.MinAppliedVersion, out.MinKnownFailoverVersion = applied, known
		} else {
			out.MinAppliedVersion = min(out.MinAppliedVersion, applied)
			out.MinKnownFailoverVersion = min(out.MinKnownFailoverVersion, known)
		}
	}
	for _, peer := range peers {
		peerStatus := &adminservice.GetSyncReplicationStatusResponse_PeerStatus{
			Cluster: peer,
			Drained: len(shards) > 0,
		}
		for _, shard := range shards {
			remote, ok := shard.GetRemoteClusters()[peer]
			acked := remote.GetAckedTaskId()
			if !ok || acked < shard.GetSyncReplicationBarrier() {
				peerStatus.Drained = false
			}
			peerStatus.FencedAtFailoverVersion = max(
				peerStatus.FencedAtFailoverVersion,
				remote.GetFencedNamespaceFailoverVersions()[namespaceID],
			)
			peerStatus.Shards = append(peerStatus.Shards, &adminservice.GetSyncReplicationStatusResponse_ShardProgress{
				ShardId:                shard.GetShardId(),
				SyncReplicationBarrier: shard.GetSyncReplicationBarrier(),
				AckedTaskId:            acked,
			})
		}
		out.Peers = append(out.Peers, peerStatus)
	}
	return out
}
