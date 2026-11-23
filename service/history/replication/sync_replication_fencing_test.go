package replication

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type fencerTestEnv struct {
	fencer  *syncReplicationFencer
	cached  map[namespace.ID]*namespace.Namespace // the host's namespace cache
	stored  map[namespace.ID]*namespace.Namespace // the database
	dbReads map[namespace.ID]int
	dbErr   error
	listed  int
	now     time.Time
	enabled func(string) bool
}

func newFencerNamespace(id, activeCluster string, version int64, clusters ...string) *namespace.Namespace {
	return namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: id, Name: id},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{ActiveClusterName: activeCluster, Clusters: clusters},
		version,
	)
}

func newFencerTestEnv(t *testing.T, hooks testhooks.TestHooks) *fencerTestEnv {
	controller := gomock.NewController(t)
	env := &fencerTestEnv{
		cached:  map[namespace.ID]*namespace.Namespace{},
		stored:  map[namespace.ID]*namespace.Namespace{},
		dbReads: map[namespace.ID]int{},
		now:     time.Unix(1000, 0),
		enabled: func(name string) bool { return name != "active-async" },
	}
	registry := namespace.NewMockRegistry(controller)
	registry.EXPECT().GetAllNamespaces().DoAndReturn(func() []*namespace.Namespace {
		env.listed++
		var all []*namespace.Namespace
		for _, entry := range env.cached {
			all = append(all, entry)
		}
		return all
	}).AnyTimes()
	registry.EXPECT().GetNamespaceByID(gomock.Any()).DoAndReturn(func(id namespace.ID) (*namespace.Namespace, error) {
		if entry, ok := env.cached[id]; ok {
			return entry, nil
		}
		return nil, serviceerror.NewNamespaceNotFound(id.String())
	}).AnyTimes()

	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	// The shard's acquisition read (cached per acquisition by the shard, see its tests).
	shardContext := historyi.NewMockShardContext(controller)
	shardContext.EXPECT().SyncReplicationAcquiredNamespace(gomock.Any()).DoAndReturn(func(id namespace.ID) (*namespace.Namespace, error) {
		env.dbReads[id]++
		if env.dbErr != nil {
			return nil, env.dbErr
		}
		return env.stored[id], nil
	}).AnyTimes()
	shardController := shard.NewMockController(controller)
	shardController.EXPECT().GetShardByID(int32(7)).Return(shardContext, nil).AnyTimes()

	config := tests.NewDynamicConfig()
	config.SyncReplicationEnabled = func(name string) bool { return env.enabled(name) }
	env.fencer = newSyncReplicationFencer(config, clusterMetadata, registry, shardController, 7, hooks)
	env.fencer.now = func() time.Time { return env.now }
	return env
}

// set puts the namespace both in the cache and in the database.
func (env *fencerTestEnv) set(entry *namespace.Namespace) {
	env.cached[entry.ID()] = entry
	env.stored[entry.ID()] = entry
}

func TestSyncReplicationFencer(t *testing.T) {
	current, alternative := cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName
	env := newFencerTestEnv(t, testhooks.TestHooks{})
	env.set(newFencerNamespace("active-sync", current, 11, current, alternative))
	env.set(newFencerNamespace("standby-sync", alternative, 12, current, alternative))
	env.set(newFencerNamespace("active-async", current, 13, current, alternative))
	env.set(newFencerNamespace("single", current, 14, current))

	// Only sync replication namespaces replicated to several clusters and active here.
	versions, err := env.fencer.activeNamespaceFailoverVersions()
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"active-sync": 11}, versions)
	require.Equal(t, 1, env.listed)

	// Failover, seen by the cache and the database. Active state and versions are read for every ack,
	// without waiting for the namespace list to be refreshed.
	env.set(newFencerNamespace("standby-sync", current, 22, current, alternative))
	env.set(newFencerNamespace("active-sync", alternative, 21, current, alternative))
	versions, err = env.fencer.activeNamespaceFailoverVersions()
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"standby-sync": 22}, versions)
	require.Equal(t, 1, env.listed)

	// A namespace that turns sync is picked up when the list is refreshed, and read from the database.
	env.enabled = func(string) bool { return true }
	versions, err = env.fencer.activeNamespaceFailoverVersions()
	require.NoError(t, err)
	require.NotContains(t, versions, "active-async")
	env.now = env.now.Add(syncReplicationNamespaceListTTL)
	versions, err = env.fencer.activeNamespaceFailoverVersions()
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"standby-sync": 22, "active-async": 13}, versions)
	require.Equal(t, 2, env.listed)

	// A fencer without a registry or shard controller sends no notice.
	versions, err = (*syncReplicationFencer)(nil).activeNamespaceFailoverVersions()
	require.NoError(t, err)
	require.Nil(t, versions)
}

// The shard was acquired by a host whose namespace cache hasn't seen the failover yet. The notice
// comes from the database read made after the acquisition.
func TestSyncReplicationFencer_StaleCacheAfterShardAcquisition(t *testing.T) {
	current, alternative := cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName
	env := newFencerTestEnv(t, testhooks.TestHooks{})
	env.cached["ns"] = newFencerNamespace("ns", alternative, 11, current, alternative)
	env.stored["ns"] = newFencerNamespace("ns", current, 22, current, alternative)

	versions, err := env.fencer.activeNamespaceFailoverVersions()
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"ns": 22}, versions)

	// The cache catches up, then sees a failback: the newer of the two views wins.
	env.cached["ns"] = newFencerNamespace("ns", alternative, 31, current, alternative)
	versions, err = env.fencer.activeNamespaceFailoverVersions()
	require.NoError(t, err)
	require.Empty(t, versions)
}

func TestSyncReplicationFencer_DatabaseErrors(t *testing.T) {
	current, alternative := cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName
	env := newFencerTestEnv(t, testhooks.TestHooks{})
	env.cached["ns"] = newFencerNamespace("ns", alternative, 11, current, alternative)
	env.stored["ns"] = newFencerNamespace("ns", current, 22, current, alternative)

	// No notice can be built: the caller must not ack.
	env.dbErr = errors.New("database unavailable")
	_, err := env.fencer.activeNamespaceFailoverVersions()
	require.Error(t, err)
	// Retried on the next ack.
	env.dbErr = nil
	versions, err := env.fencer.activeNamespaceFailoverVersions()
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"ns": 22}, versions)

	// A namespace missing from the database: the cache alone decides.
	delete(env.stored, "ns")
	versions, err = env.fencer.activeNamespaceFailoverVersions()
	require.NoError(t, err)
	require.Empty(t, versions)
}
