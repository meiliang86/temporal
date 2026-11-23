package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestSyncReplicationFencer(t *testing.T) {
	controller := gomock.NewController(t)

	newNamespace := func(id, name, activeCluster string, version int64, clusters ...string) *namespace.Namespace {
		return namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: id, Name: name},
			&persistencespb.NamespaceConfig{},
			&persistencespb.NamespaceReplicationConfig{ActiveClusterName: activeCluster, Clusters: clusters},
			version,
		)
	}
	current, alternative := cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName
	entries := map[namespace.ID]*namespace.Namespace{
		"active-sync":  newNamespace("active-sync", "active-sync", current, 11, current, alternative),
		"standby-sync": newNamespace("standby-sync", "standby-sync", alternative, 12, current, alternative),
		"active-async": newNamespace("active-async", "active-async", current, 13, current, alternative),
		"single":       newNamespace("single", "single", current, 14, current),
	}
	registry := namespace.NewMockRegistry(controller)
	listed := 0
	registry.EXPECT().GetAllNamespaces().DoAndReturn(func() []*namespace.Namespace {
		listed++
		var all []*namespace.Namespace
		for _, entry := range entries {
			all = append(all, entry)
		}
		return all
	}).AnyTimes()
	registry.EXPECT().GetNamespaceByID(gomock.Any()).DoAndReturn(func(id namespace.ID) (*namespace.Namespace, error) {
		return entries[id], nil
	}).AnyTimes()

	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(current).AnyTimes()

	config := tests.NewDynamicConfig()
	config.SyncReplicationEnabled = func(name string) bool { return name != "active-async" }

	now := time.Unix(1000, 0)
	fencer := newSyncReplicationFencer(config, clusterMetadata, registry)
	fencer.now = func() time.Time { return now }

	// Only sync replication namespaces replicated to several clusters and active here.
	require.Equal(t, map[string]int64{"active-sync": 11}, fencer.activeNamespaceFailoverVersions())
	require.Equal(t, 1, listed)

	// Failover: the standby namespace becomes active here. Active state and versions are read from
	// the registry on every call, without waiting for the namespace list to be refreshed.
	entries["standby-sync"] = newNamespace("standby-sync", "standby-sync", current, 22, current, alternative)
	entries["active-sync"] = newNamespace("active-sync", "active-sync", alternative, 21, current, alternative)
	require.Equal(t, map[string]int64{"standby-sync": 22}, fencer.activeNamespaceFailoverVersions())
	require.Equal(t, 1, listed)

	// A namespace that turns sync is picked up when the list is refreshed.
	config.SyncReplicationEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	require.NotContains(t, fencer.activeNamespaceFailoverVersions(), "active-async")
	now = now.Add(syncReplicationNamespaceListTTL)
	require.Equal(t, map[string]int64{"standby-sync": 22, "active-async": 13}, fencer.activeNamespaceFailoverVersions())
	require.Equal(t, 2, listed)

	// A nil fencer (no registry in the tool box) sends no notice.
	require.Nil(t, newSyncReplicationFencer(config, clusterMetadata, nil).activeNamespaceFailoverVersions())
}
