package replication

import (
	"sync"
	"time"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

// syncReplicationNamespaceListTTL is how long the list of sync replication namespaces is reused. Only
// membership in the list is cached: whether this cluster is active, and at which failover version, is
// read for every ack.
const syncReplicationNamespaceListTTL = time.Second

// syncReplicationFencer builds the fencing notice a stream receiver attaches to its acks: the
// failover version of every sync replication namespace this cluster believes it is active for.
//
// The sender (the other cluster, when it was active before) must not count an ack toward a write it
// made under an older failover version: having become active, this cluster may have ignored that
// write's replication task as a stale branch while the watermark still moved past it. The notice is
// built after the watermark it travels with.
//
// The notice must reflect every failover under which this cluster wrote to the shard. The host's
// namespace cache isn't enough: a shard can move to a host whose cache hasn't seen the failover yet,
// right after the previous owner wrote under the new version. That host would ignore the old
// active's tasks without fencing them. So the notice uses whichever is newer of the cache and the
// namespace as the shard read it from the database after its acquisition
// (ShardContext.SyncReplicationAcquiredNamespace), which sees every version the shard was written
// under.
type syncReplicationFencer struct {
	config          *configs.Config
	clusterMetadata cluster.Metadata
	registry        namespace.Registry
	shardController shard.Controller
	shardID         int32
	testHooks       testhooks.TestHooks
	now             func() time.Time

	mu           sync.Mutex
	namespaceIDs []namespace.ID
	listedAt     time.Time
}

func newSyncReplicationFencer(
	config *configs.Config,
	clusterMetadata cluster.Metadata,
	registry namespace.Registry,
	shardController shard.Controller,
	shardID int32,
	testHooks testhooks.TestHooks,
) *syncReplicationFencer {
	if config == nil || clusterMetadata == nil || registry == nil || shardController == nil {
		return nil
	}
	return &syncReplicationFencer{
		config:          config,
		clusterMetadata: clusterMetadata,
		registry:        registry,
		shardController: shardController,
		shardID:         shardID,
		testHooks:       testHooks,
		now:             time.Now,
	}
}

// activeNamespaceFailoverVersions returns namespace ID -> failover version for every sync replication
// namespace active in this cluster, or nil if there is none. An error means the notice can't be
// built; the ack must not be sent without it.
func (f *syncReplicationFencer) activeNamespaceFailoverVersions() (map[string]int64, error) {
	if f == nil {
		return nil, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	namespaceIDs := f.syncReplicationNamespaceIDsLocked()
	if len(namespaceIDs) == 0 {
		return nil, nil
	}
	shardContext, err := f.shardController.GetShardByID(f.shardID)
	if err != nil {
		return nil, err
	}

	currentCluster := f.clusterMetadata.GetCurrentClusterName()
	var versions map[string]int64
	for _, namespaceID := range namespaceIDs {
		entry, err := shardContext.SyncReplicationAcquiredNamespace(namespaceID)
		if err != nil {
			return nil, err
		}
		if cached := f.cachedNamespace(namespaceID); cached != nil &&
			(entry == nil || cached.FailoverVersion(namespace.EmptyBusinessID) >= entry.FailoverVersion(namespace.EmptyBusinessID)) {
			entry = cached
		}
		if entry == nil || !entry.ActiveInCluster(currentCluster) {
			continue
		}
		if versions == nil {
			versions = make(map[string]int64)
		}
		versions[namespaceID.String()] = entry.FailoverVersion(namespace.EmptyBusinessID)
	}
	return versions, nil
}

// cachedNamespace returns the host's cached namespace entry, or nil.
func (f *syncReplicationFencer) cachedNamespace(namespaceID namespace.ID) *namespace.Namespace {
	if stale, ok := testhooks.Get(f.testHooks, testhooks.SyncReplicationStaleNamespaceView, namespaceID); ok && stale {
		return nil // test only: the cache hasn't seen the latest failover
	}
	entry, err := f.registry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil
	}
	return entry
}

func (f *syncReplicationFencer) syncReplicationNamespaceIDsLocked() []namespace.ID {
	if now := f.now(); f.namespaceIDs == nil || now.Sub(f.listedAt) >= syncReplicationNamespaceListTTL {
		ids := make([]namespace.ID, 0, len(f.namespaceIDs))
		for _, entry := range f.registry.GetAllNamespaces() {
			if entry.ReplicationPolicy() == namespace.ReplicationPolicyMultiCluster &&
				f.config.SyncReplicationEnabled(entry.Name().String()) {
				ids = append(ids, entry.ID())
			}
		}
		f.namespaceIDs = ids
		f.listedAt = now
	}
	return f.namespaceIDs
}
