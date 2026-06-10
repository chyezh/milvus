package growing

import (
	"context"
	"math"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type Snapshot struct {
	VChannels          map[string]*streamingpb.VChannelMeta
	SegmentAssignments map[int64]*streamingpb.SegmentAssignmentMeta
}

func (m *Manager) Name() string {
	return "growing"
}

func (m *Manager) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	if funcutil.IsControlChannel(msg.VChannel()) && !msg.IsPChannelLevel() {
		// CChannel messages only provide global ordering here. They should not mutate
		// vchannel/segment state, but checkpoints can still advance over them.
		return moduleapi.ObserveResult{}
	}
	return m.observeMessage(ctx, msg)
}

func (m *Manager) SwitchIntoMetaAndData() moduleapi.Snapshot {
	m.metaAndData = true
	for _, vchannel := range m.vchannelViews {
		vchannel.SwitchIntoMetaAndData()
	}
	for _, segment := range m.segmentViews {
		segment.SwitchIntoMetaAndData()
	}
	return m.Snapshot()
}

func (m *Manager) RequirePersist() {
	task := m.newPersistTask(
		m.channelName,
		m.catalog,
		m.logger,
		scheduler.After(m.lastPersistTask),
		func() {
			if m.runtime.Notifier != nil {
				m.runtime.Notifier.NotifyBarrierUpdated()
			}
		},
	)
	if task == nil {
		return
	}
	m.lastPersistTask = m.runtime.Scheduler.Submit(task)
}

func (m *Manager) finalizeTombstones() bool {
	finalized := false
	for _, segment := range m.segmentViews {
		finalized = segment.TryFinalizeTombstone() || finalized
	}
	for _, vchannel := range m.vchannelViews {
		finalized = vchannel.TryFinalizeTombstone() || finalized
	}
	return finalized
}

func (m *Manager) PartitionDurableFrontier(collectionID int64, partitionID int64) walcheckpoint.Barrier {
	return m.durableFrontier(
		func(vchannel *vChannelView) bool {
			return vchannelCollectionID(vchannel) == collectionID
		},
		func(segment *segmentView) bool {
			meta := segment.AssignmentMeta()
			return meta.GetCollectionId() == collectionID && meta.GetPartitionId() == partitionID
		},
	)
}

func (m *Manager) VChannelDurableFrontier(vchannel string) walcheckpoint.Barrier {
	return m.durableFrontier(
		func(info *vChannelView) bool {
			return info.AssignmentMeta().GetVchannel() == vchannel
		},
		func(segment *segmentView) bool {
			return segment.AssignmentMeta().GetVchannel() == vchannel
		},
	)
}

func (m *Manager) AllDurableFrontier() walcheckpoint.Barrier {
	return m.durableFrontier(
		func(*vChannelView) bool { return true },
		func(*segmentView) bool { return true },
	)
}

func (m *Manager) durableFrontier(
	matchVChannel func(*vChannelView) bool,
	matchSegment func(*segmentView) bool,
) walcheckpoint.Barrier {
	owners := make(durableFrontierOwners, 0)
	for _, vchannel := range m.vchannelViews {
		if vchannel != nil && matchVChannel(vchannel) {
			owners = append(owners, vchannel)
			if transformLog := m.transformLog(vchannel.Name()); transformLog != nil {
				owners = append(owners, transformLogDurableFrontierOwner{
					vchannel:     vchannel,
					transformLog: transformLog,
				})
			}
		}
	}
	for _, segment := range m.segmentViews {
		if segment != nil && matchSegment(segment) {
			owners = append(owners, segment)
		}
	}
	return owners
}

type transformLogDurableFrontierOwner struct {
	vchannel     *vChannelView
	transformLog *transformLogView
}

func (o transformLogDurableFrontierOwner) DurableFrontierTimeTick() uint64 {
	o.vchannel.mu.Lock()
	persistedDataTimeTick := o.vchannel.persistedDataTimeTick
	o.vchannel.mu.Unlock()
	if o.transformLog.log.HasDirty() ||
		o.transformLog.log.HasPendingWork(persistedDataTimeTick) ||
		o.transformLog.hasPendingTask() {
		return persistedDataTimeTick
	}
	return math.MaxUint64
}

func vchannelCollectionID(vchannel *vChannelView) int64 {
	if vchannel == nil || vchannel.AssignmentMeta() == nil || vchannel.AssignmentMeta().GetCollectionInfo() == nil {
		return 0
	}
	return vchannel.AssignmentMeta().GetCollectionInfo().GetCollectionId()
}

type durableFrontierOwner interface {
	DurableFrontierTimeTick() uint64
}

type durableFrontierOwners []durableFrontierOwner

func (owners durableFrontierOwners) TimeTick() uint64 {
	if len(owners) == 0 {
		return math.MaxUint64
	}
	frontier := uint64(math.MaxUint64)
	for _, owner := range owners {
		ownerFrontier := owner.DurableFrontierTimeTick()
		if ownerFrontier < frontier {
			frontier = ownerFrontier
		}
	}
	return frontier
}

func (m *Manager) DataCheckpointTimeTick() uint64 {
	dataTimeTick := uint64(math.MaxUint64)
	for _, vchannel := range m.vchannelViews {
		if vchannel == nil {
			continue
		}
		if timetick := vchannel.DataCheckpointTimeTick(); timetick < dataTimeTick {
			dataTimeTick = timetick
		}
	}
	for _, segment := range m.segmentViews {
		if segment == nil {
			continue
		}
		if timetick := segment.DataCheckpointTimeTick(); timetick < dataTimeTick {
			dataTimeTick = timetick
		}
	}
	return dataTimeTick
}

func frontierBefore(timetick uint64) uint64 {
	if timetick == 0 {
		return 0
	}
	return timetick - 1
}

func (m *Manager) Snapshot() *Snapshot {
	currentSegments := m.segmentViews
	currentVChannels := m.vchannelViews
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(currentSegments))
	vchannels := make(map[string]*streamingpb.VChannelMeta, len(currentVChannels))
	activeCollections := make(map[string]int64)
	activePartitions := make(map[string]map[int64]struct{})
	for channelName, vchannel := range currentVChannels {
		if vchannel.IsActive() {
			vchannelMeta := vchannel.AssignmentMeta()
			vchannelSnapshot := proto.Clone(vchannelMeta).(*streamingpb.VChannelMeta)
			vchannels[channelName] = vchannelSnapshot
			if collectionInfo := vchannelSnapshot.GetCollectionInfo(); collectionInfo != nil {
				activeCollections[channelName] = collectionInfo.GetCollectionId()
				partitions := make(map[int64]struct{}, len(collectionInfo.GetPartitions()))
				activePartitionInfos := make([]*streamingpb.PartitionInfoOfVChannel, 0, len(collectionInfo.GetPartitions()))
				for _, p := range collectionInfo.GetPartitions() {
					if !isPartitionNormal(p.GetState()) {
						continue
					}
					partitions[p.PartitionId] = struct{}{}
					activePartitionInfos = append(activePartitionInfos, p)
				}
				collectionInfo.Partitions = activePartitionInfos
				activePartitions[channelName] = partitions
			}
		}
	}
	for segmentID, segment := range currentSegments {
		if !segment.IsGrowing() {
			continue
		}
		segmentMeta := segment.AssignmentMeta()
		if _, ok := vchannels[segmentMeta.Vchannel]; !ok {
			if m.logger != nil {
				m.logger.Warn("getSnapshot: skipping orphaned growing segment with non-active vchannel",
					zap.Int64("segmentID", segmentID),
					zap.String("vchannel", segmentMeta.Vchannel),
					zap.Int64("collectionID", segmentMeta.CollectionId),
				)
			}
			continue
		}
		if collectionID, ok := activeCollections[segmentMeta.Vchannel]; ok && collectionID != segmentMeta.CollectionId {
			if m.logger != nil {
				m.logger.Warn("getSnapshot: skipping orphaned growing segment with collection mismatch",
					zap.Int64("segmentID", segmentID),
					zap.String("vchannel", segmentMeta.Vchannel),
					zap.Int64("collectionID", segmentMeta.CollectionId),
				)
			}
			continue
		}
		partitions, hasActivePartitionInfo := activePartitions[segmentMeta.Vchannel]
		if _, ok := partitions[segmentMeta.PartitionId]; hasActivePartitionInfo && !ok {
			if m.logger != nil {
				m.logger.Warn("getSnapshot: skipping orphaned growing segment with dropped partition",
					zap.Int64("segmentID", segmentID),
					zap.String("vchannel", segmentMeta.Vchannel),
					zap.Int64("collectionID", segmentMeta.CollectionId),
					zap.Int64("partitionID", segmentMeta.PartitionId),
				)
			}
			continue
		}
		segments[segmentID] = proto.Clone(segmentMeta).(*streamingpb.SegmentAssignmentMeta)
	}
	return &Snapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
	}
}

func (m *Manager) RecoverTransformLogs(ctx context.Context) error {
	for _, transformLog := range m.transformLogs {
		if err := transformLog.Recover(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (v *transformLogView) Recover(ctx context.Context) error {
	result, err := v.log.Recover(ctx, nil)
	if err != nil {
		return err
	}
	if !result.Recovered {
		return nil
	}
	return nil
}

func (m *Manager) NotifyCheckpointPersisted(metaTimeTick uint64, dataTimeTick uint64) {
	task := m.newCleanupTask(
		metaTimeTick,
		dataTimeTick,
		scheduler.All(scheduler.After(m.lastPersistTask), scheduler.After(m.lastCleanupTask)),
	)
	if task == nil {
		return
	}
	m.lastCleanupTask = m.runtime.Scheduler.Submit(task)
}

var (
	_ moduleapi.Module              = (*Manager)(nil)
	_ moduleapi.DurableFrontierView = (*Manager)(nil)
	_ moduleapi.DataCheckpointView  = (*Manager)(nil)
)
