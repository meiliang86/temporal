package replication

import (
	"sync"
	"time"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
)

// syncReplicationNamespaceListTTL is how long the list of sync replication namespaces is reused. Only
// membership in the list is cached: whether this cluster is active, and at which failover version, is
// read from the namespace registry for every ack.
const syncReplicationNamespaceListTTL = time.Second

// syncReplicationFencer builds the fencing notice a stream receiver attaches to its acks: the
// failover version of every sync replication namespace this cluster believes it is active for.
//
// The sender (the other cluster, when it was active before) must not count an ack toward a write it
// made under an older failover version: having become active, this cluster may have ignored that
// write's replication task as a stale branch while the watermark still moved past it. The notice is
// built after the watermark it travels with, so it reflects at least what this host knew when it
// applied the acknowledged tasks.
type syncReplicationFencer struct {
	config          *configs.Config
	clusterMetadata cluster.Metadata
	registry        namespace.Registry
	now             func() time.Time

	mu           sync.Mutex
	namespaceIDs []namespace.ID
	listedAt     time.Time
}

func newSyncReplicationFencer(
	config *configs.Config,
	clusterMetadata cluster.Metadata,
	registry namespace.Registry,
) *syncReplicationFencer {
	if config == nil || clusterMetadata == nil || registry == nil {
		return nil
	}
	return &syncReplicationFencer{
		config:          config,
		clusterMetadata: clusterMetadata,
		registry:        registry,
		now:             time.Now,
	}
}

// activeNamespaceFailoverVersions returns namespace ID -> failover version for every sync replication
// namespace active in this cluster, or nil if there is none.
func (f *syncReplicationFencer) activeNamespaceFailoverVersions() map[string]int64 {
	if f == nil {
		return nil
	}
	currentCluster := f.clusterMetadata.GetCurrentClusterName()
	var versions map[string]int64
	for _, namespaceID := range f.syncReplicationNamespaceIDs() {
		entry, err := f.registry.GetNamespaceByID(namespaceID)
		if err != nil || !entry.ActiveInCluster(currentCluster) {
			continue
		}
		if versions == nil {
			versions = make(map[string]int64)
		}
		versions[namespaceID.String()] = entry.FailoverVersion(namespace.EmptyBusinessID)
	}
	return versions
}

func (f *syncReplicationFencer) syncReplicationNamespaceIDs() []namespace.ID {
	f.mu.Lock()
	defer f.mu.Unlock()
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
