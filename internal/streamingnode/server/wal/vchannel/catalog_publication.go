package vchannel

import (
	"slices"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ApplyCatalogPublication performs no catalog I/O. RW and RO install identical
// durability facts; RO retires local metadata only after its own view references
// have drained, independently of the primary's readiness and release timing.
func (m *PChannelRecoveryManager) ApplyCatalogPublication(publication *streamingpb.WALReplicaCatalogPublication) error {
	for _, metas := range [][]*streamingpb.VChannelMeta{publication.Vchannels, publication.VchannelBases} {
		for _, meta := range metas {
			module, err := m.catalogModule(meta.GetVchannel())
			if err != nil {
				return err
			}
			if module.vchannelView.catalogOwnerIsNewer(meta) {
				continue
			}
			if module.readOnly {
				module.vchannelView.applyPublishedLifecycle(meta)
			}
			module.vchannelView.MarkSnapshotPersisted(meta)
			if m.config.SummaryManager != nil && meta.GetTransformMaterializedTimeTick() > 0 {
				m.config.SummaryManager.AdvanceGCTimeTick(meta.GetVchannel(), meta.GetTransformMaterializedTimeTick())
			}
		}
	}
	for _, meta := range publication.Segments {
		module, err := m.catalogModule(meta.GetVchannel())
		if err != nil {
			return err
		}
		module.mu.Lock()
		view := module.segments[meta.GetSegmentId()]
		module.mu.Unlock()
		if view == nil {
			return merr.WrapErrServiceInternalMsg("catalog completion has no observed segment %d", meta.GetSegmentId())
		}
		module.markSegmentSnapshotPersisted(meta.GetSegmentId(), view, meta)
	}
	for _, meta := range publication.RemovedSchemas {
		module, err := m.catalogModule(meta.GetVchannel())
		if err != nil {
			return err
		}
		module.vchannelView.MarkSchemaCleanupPersisted(meta)
		if module.runtime.Notifier != nil {
			module.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameVChannel)
		}
	}
	for _, meta := range publication.RemovedSegments {
		module, err := m.catalogModule(meta.GetVchannel())
		if err != nil {
			return err
		}
		module.mu.Lock()
		view := module.segments[meta.GetSegmentId()]
		if module.readOnly && view != nil {
			if module.pendingCleanup == nil {
				module.pendingCleanup = make(map[int64]*segment.SegmentView)
			}
			module.pendingCleanup[meta.GetSegmentId()] = view
		}
		module.mu.Unlock()
		if module.readOnly {
			module.reclaimReadOnlyCatalog()
		} else {
			module.completeSegmentCleanup(meta.GetSegmentId(), view)
		}
	}
	for _, meta := range publication.RemovedVchannels {
		module, err := m.catalogModule(meta.GetVchannel())
		if err != nil {
			return err
		}
		if module.vchannelView.catalogOwnerIsNewer(meta) {
			continue
		}
		if module.readOnly {
			module.mu.Lock()
			module.catalogRemovedAt = meta.GetCheckpointTimeTick()
			module.mu.Unlock()
			module.reclaimReadOnlyCatalog()
		} else {
			module.completeVChannelCleanup(meta.GetCheckpointTimeTick())
		}
	}
	return nil
}

// Persistence may complete after a later CreateCollection replaces the owner.
// The old callback used to retain the captured view pointer; wire completions
// instead use the collection's WAL creation boundary to preserve that isolation.
func (info *VChannelView) catalogOwnerIsNewer(snapshot *streamingpb.VChannelMeta) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	return info.meta.GetCreateCollectionTimeTick() > snapshot.GetCreateCollectionTimeTick()
}

func (m *PChannelRecoveryManager) catalogModule(vchannel string) (*VChannelRecoveryModule, error) {
	module, ok := m.modules.Get(vchannel)
	if !ok || module.vchannelView == nil {
		return nil, merr.WrapErrServiceInternalMsg("catalog completion has no observed vchannel %s", vchannel)
	}
	return module, nil
}

// applyPublishedLifecycle merges completion of already observed DDL. It cannot
// replace newer WAL state with the older snapshot that finished persistence.
func (info *VChannelView) applyPublishedLifecycle(snapshot *streamingpb.VChannelMeta) {
	info.mu.Lock()
	defer info.mu.Unlock()
	partitions := make(map[int64]*streamingpb.PartitionInfoOfVChannel)
	for _, partition := range snapshot.GetCollectionInfo().GetPartitions() {
		partitions[partition.GetPartitionId()] = partition
	}
	for len(info.pendingDrops) > 0 {
		drop := info.pendingDrops[0]
		if drop.timeTick > snapshot.GetCheckpointTimeTick() {
			break
		}
		if drop.partitionID == 0 {
			if snapshot.GetState() != streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED {
				break
			}
		} else if partition := partitions[drop.partitionID]; partition == nil || partition.GetState() != streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED || partition.GetCheckpointTimeTick() != drop.timeTick {
			break
		}
		info.completeDropLocked(drop.timeTick)
	}
	if version := qviews.FromProtoDataVersion(snapshot.GetSegmentDataVersionSummary()); version.GT(qviews.FromProtoDataVersion(info.meta.GetSegmentDataVersionSummary())) {
		info.meta.SegmentDataVersionSummary = version.IntoProto()
	}
	if info.meta.CollectionInfo == nil {
		return
	}
	// Schema tombstones do not change schema objects retained by query runtimes.
	for _, published := range snapshot.GetCollectionInfo().GetSchemas() {
		if published.GetState() != streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED {
			continue
		}
		for _, schema := range info.meta.CollectionInfo.Schemas {
			if schema.GetCheckpointTimeTick() == published.GetCheckpointTimeTick() {
				schema.State = published.State
			}
		}
	}
	info.meta.CollectionInfo.Partitions = slices.DeleteFunc(info.meta.CollectionInfo.Partitions, func(partition *streamingpb.PartitionInfoOfVChannel) bool {
		return partition.GetState() == streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED &&
			partition.GetCheckpointTimeTick() <= snapshot.GetCheckpointTimeTick() && partitions[partition.GetPartitionId()] == nil
	})
}

// Caller must not hold the module lock. Delaying metadata retirement never
// delays the primary checkpoint, nor does it authorize any object deletion.
func (m *VChannelRecoveryModule) reclaimReadOnlyCatalog() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.readOnly || m.removed {
		return
	}
	if _, referenced := m.queryResources.OldestDataVersion(); referenced {
		return
	}
	for id := range m.pendingCleanup {
		delete(m.segments, id)
		delete(m.querySealed, id)
		delete(m.cleanupSegments, id)
		m.dirtyMu.Lock()
		delete(m.dirtySegments, id)
		m.dirtyMu.Unlock()
	}
	clear(m.pendingCleanup)
	m.refreshQueryRetentionLocked()
	if m.catalogRemovedAt > 0 && len(m.segments) == 0 {
		m.removed = true
		if m.onCleanup != nil {
			m.onCleanup(m)
		}
	}
}
