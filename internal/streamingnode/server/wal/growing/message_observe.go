package growing

import (
	"context"

	"go.uber.org/zap"

	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

func (m *Manager) observeMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	switch msg.MessageType() {
	case message.MessageTypeTimeTick:
		return emptyObserveResult()
	case message.MessageTypeCreateCollection:
		return m.observeCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(msg))
	case message.MessageTypeCreatePartition:
		return m.observeCreatePartitionMessage(message.MustAsImmutableCreatePartitionMessageV1(msg))
	case message.MessageTypeSchemaChange:
		return m.observeSchemaChangeMessage(ctx, message.MustAsImmutableSchemaChangeMessageV2(msg))
	case message.MessageTypeAlterCollection:
		return m.observeAlterCollectionMessage(ctx, message.MustAsImmutableAlterCollectionMessageV2(msg))
	case message.MessageTypeCreateSegment:
		return m.observeCreateSegmentMessage(ctx, message.MustAsImmutableCreateSegmentMessageV2(msg))
	case message.MessageTypeInsert:
		return m.observeInsertMessage(ctx, message.MustAsImmutableInsertMessageV1(msg))
	case message.MessageTypeDelete:
		return m.observeDeleteMessage(ctx, message.MustAsImmutableDeleteMessageV1(msg))
	case message.MessageTypeFlush:
		return m.observeFlushMessage(ctx, message.MustAsImmutableFlushMessageV2(msg))
	case message.MessageTypeManualFlush:
		return m.observeManualFlushMessage(ctx, message.MustAsImmutableManualFlushMessageV2(msg))
	case message.MessageTypeFlushAll:
		return m.observeFlushAllMessage(ctx, message.MustAsImmutableFlushAllMessageV2(msg))
	case message.MessageTypeDropCollection:
		return m.observeDropCollectionMessage(ctx, message.MustAsImmutableDropCollectionMessageV1(msg))
	case message.MessageTypeDropPartition:
		return m.observeDropPartitionMessage(ctx, message.MustAsImmutableDropPartitionMessageV1(msg))
	case message.MessageTypeTruncateCollection:
		return m.observeTruncateCollectionMessage(ctx, message.MustAsImmutableTruncateCollectionMessageV2(msg))
	case message.MessageTypeAlterWAL:
		return m.observeAlterWALMessage(ctx, message.MustAsImmutableAlterWALMessageV2(msg))
	case message.MessageTypeTxn:
		return m.observeTxnMessage(ctx, message.AsImmutableTxnMessage(msg))
	default:
		return emptyObserveResult()
	}
}

func (m *Manager) observeCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) moduleapi.ObserveResult {
	if vchannel := m.vchannelViews[msg.VChannel()]; vchannel != nil {
		if vchannel.ShouldSkipReplay(msg.TimeTick()) {
			return emptyObserveResult()
		}
		if vchannel.CanStartNewCollectionAt(msg.TimeTick()) {
			vchannel := m.addVChannel(newVChannelMetaFromCreateCollectionMessage(msg))
			return moduleapi.ObserveResult{Meta: vchannel.MetaBarrier()}
		}
		if !vchannelMatchesCollection(vchannel, msg.Header().GetCollectionId()) {
			return emptyObserveResult()
		}
		if !vchannel.CanReplayAt(msg.TimeTick()) {
			return emptyObserveResult()
		}
		m.logInconsistency(msg, "create collection vchannel already exists", zap.String("vchannel", msg.VChannel()))
		return moduleapi.ObserveResult{Meta: vchannel.MetaBarrier()}
	}
	vchannel := m.addVChannel(newVChannelMetaFromCreateCollectionMessage(msg))
	return moduleapi.ObserveResult{Meta: vchannel.MetaBarrier()}
}

func (m *Manager) observeCreatePartitionMessage(msg message.ImmutableCreatePartitionMessageV1) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return emptyObserveResult()
	}
	if !vchannelMatchesCollection(vchannel, msg.Header().GetCollectionId()) {
		return emptyObserveResult()
	}
	if vchannel.ShouldSkipReplay(msg.TimeTick()) ||
		vchannel.ShouldSkipPartitionReplay(msg.Header().GetPartitionId(), msg.TimeTick()) {
		return emptyObserveResult()
	}
	if !vchannel.CanReplayAt(msg.TimeTick()) {
		return emptyObserveResult()
	}
	return vchannel.ObserveCreatePartitionMessageV1(msg)
}

func (m *Manager) observeSchemaChangeMessage(ctx context.Context, msg message.ImmutableSchemaChangeMessageV2) moduleapi.ObserveResult {
	vchannel, result, canMutate := m.prepareActiveVChannelObserve(ctx, msg.TimeTick(), msg.VChannel(), msg.Header().GetCollectionId(), true)
	if !canMutate {
		return result
	}
	result = composeObserveResults(result, m.flushVChannelTransformLogBuffer(msg.TimeTick(), msg.VChannel()))
	observeResult := vchannel.ObserveSchemaChangeMessageV2(msg)
	m.refreshRetainedSegmentSchemas(vchannel)
	return composeObserveResults(result, observeResult)
}

func (m *Manager) observeAlterCollectionMessage(ctx context.Context, msg message.ImmutableAlterCollectionMessageV2) moduleapi.ObserveResult {
	schemaChange := messageutil.IsSchemaChange(msg.Header())
	vchannel, result, canMutate := m.prepareActiveVChannelObserve(ctx, msg.TimeTick(), msg.VChannel(), msg.Header().GetCollectionId(), schemaChange)
	if !canMutate {
		return result
	}
	observeResult := vchannel.ObserveAlterCollectionMessageV2(msg)
	if schemaChange {
		result = composeObserveResults(result, m.flushVChannelTransformLogBuffer(msg.TimeTick(), msg.VChannel()))
		m.refreshRetainedSegmentSchemas(vchannel)
	}
	return composeObserveResults(result, observeResult)
}

func (m *Manager) prepareActiveVChannelObserve(
	ctx context.Context,
	timetick uint64,
	vchannelName string,
	collectionID int64,
	flushSegments bool,
) (*vChannelView, moduleapi.ObserveResult, bool) {
	vchannel := m.retainedVChannel(vchannelName)
	if vchannel == nil {
		return nil, emptyObserveResult(), false
	}
	result := emptyObserveResult()
	if flushSegments {
		result = m.flushRetainedVChannelSegmentsCreatedBefore(ctx, timetick, vchannelName, collectionID)
	}
	if !vchannelMatchesCollection(vchannel, collectionID) ||
		vchannel.ShouldSkipReplay(timetick) ||
		!vchannel.CanReplayAt(timetick) {
		return vchannel, result, false
	}
	if !vchannel.IsActive() {
		return vchannel, composeObserveResults(result, moduleapi.ObserveResult{Meta: vchannel.MetaBarrier()}), false
	}
	return vchannel, result, true
}

func (m *Manager) observeCreateSegmentMessage(ctx context.Context, msg message.ImmutableCreateSegmentMessageV2) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		m.logInconsistency(msg, "create segment vchannel not found", zap.String("vchannel", msg.VChannel()), zap.Int64("segmentID", msg.Header().GetSegmentId()))
		return emptyObserveResult()
	}
	if !vchannelMatchesCollection(vchannel, msg.Header().GetCollectionId()) {
		return emptyObserveResult()
	}
	if !vchannel.CanReplayAt(msg.TimeTick()) ||
		!vchannel.CanReplayPartitionAt(msg.Header().GetPartitionId(), msg.TimeTick()) {
		return emptyObserveResult()
	}
	segment := m.segmentViews[msg.Header().GetSegmentId()]
	if segment != nil && shouldSkipTombstonedSegmentMeta(segment.AssignmentMeta(), msg.TimeTick()) {
		return emptyObserveResult()
	}
	if segment == nil {
		if !vchannel.HasPartitionMeta(msg.Header().GetPartitionId()) {
			m.logInconsistency(msg, "create segment partition not found", zap.String("vchannel", msg.VChannel()), zap.Int64("partitionID", msg.Header().GetPartitionId()), zap.Int64("segmentID", msg.Header().GetSegmentId()))
			return emptyObserveResult()
		}
		_, schema := vchannel.GetSchema(msg.TimeTick())
		if schema == nil {
			m.logInconsistency(msg, "create segment schema not found", zap.String("vchannel", msg.VChannel()), zap.Int64("segmentID", msg.Header().GetSegmentId()), zap.Uint64("timeTick", msg.TimeTick()))
			return emptyObserveResult()
		}
		segment = newSegmentViewFromCreateSegmentMessage(msg, schema, m.runtimeConfig())
		m.addSegmentView(segment)
	}
	return segment.ObserveCreateSegmentMessageV2(ctx, msg)
}

func (m *Manager) observeFlushMessage(ctx context.Context, msg message.ImmutableFlushMessageV2) moduleapi.ObserveResult {
	segment := m.segmentViews[msg.Header().GetSegmentId()]
	if segment == nil {
		m.logInconsistency(msg, "flush segment not found", zap.Int64("segmentID", msg.Header().GetSegmentId()))
		return emptyObserveResult()
	}
	if shouldSkipTombstonedSegmentMeta(segment.AssignmentMeta(), msg.TimeTick()) {
		return emptyObserveResult()
	}
	if !m.segmentOwnerCanReplayAt(segment, msg.TimeTick()) {
		return emptyObserveResult()
	}
	return segment.Flush(ctx, msg.TimeTick())
}

func (m *Manager) observeManualFlushMessage(ctx context.Context, msg message.ImmutableManualFlushMessageV2) moduleapi.ObserveResult {
	return m.flushVChannelSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel(), msg.Header().GetCollectionId())
}

func (m *Manager) observeFlushAllMessage(ctx context.Context, msg message.ImmutableFlushAllMessageV2) moduleapi.ObserveResult {
	result := m.observeSegments(ctx, msg.TimeTick(), func(segment *segmentView) bool {
		return segment.CreateTimeTick() < msg.TimeTick()
	}, func(segment *segmentView) moduleapi.ObserveResult {
		return segment.Flush(ctx, msg.TimeTick())
	})
	return composeObserveResults(result, m.flushAllTransformLogBuffers(msg.TimeTick()))
}

func (m *Manager) observeDropCollectionMessage(ctx context.Context, msg message.ImmutableDropCollectionMessageV1) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return emptyObserveResult()
	}
	result := m.flushVChannelSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel(), msg.Header().GetCollectionId())
	if !vchannelMatchesCollection(vchannel, msg.Header().GetCollectionId()) {
		return result
	}
	if vchannel.ShouldSkipReplay(msg.TimeTick()) {
		return result
	}
	if !vchannel.CanReplayAt(msg.TimeTick()) {
		return result
	}
	return composeObserveResults(result, vchannel.ObserveDropCollectionMessageV1(msg))
}

func (m *Manager) observeDropPartitionMessage(ctx context.Context, msg message.ImmutableDropPartitionMessageV1) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return emptyObserveResult()
	}
	result := m.flushPartitionSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel(), msg.Header().GetCollectionId(), msg.Header().GetPartitionId())
	if !vchannelMatchesCollection(vchannel, msg.Header().GetCollectionId()) {
		return result
	}
	if vchannel.ShouldSkipReplay(msg.TimeTick()) ||
		!vchannel.CanReplayAt(msg.TimeTick()) ||
		!vchannel.CanReplayPartitionAt(msg.Header().GetPartitionId(), msg.TimeTick()) {
		return result
	}
	if !vchannel.HasPartitionMeta(msg.Header().GetPartitionId()) {
		m.logInconsistency(msg, "drop partition partition not found", zap.String("vchannel", msg.VChannel()), zap.Int64("partitionID", msg.Header().GetPartitionId()))
		return result
	}
	return composeObserveResults(result, vchannel.ObserveDropPartitionMessageV1(msg))
}

func (m *Manager) observeTruncateCollectionMessage(ctx context.Context, msg message.ImmutableTruncateCollectionMessageV2) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return emptyObserveResult()
	}
	result := m.flushVChannelSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel(), msg.Header().GetCollectionId())
	if !vchannelMatchesCollection(vchannel, msg.Header().GetCollectionId()) {
		return result
	}
	if vchannel.ShouldSkipReplay(msg.TimeTick()) || !vchannel.CanReplayAt(msg.TimeTick()) {
		return result
	}
	return composeObserveResults(result, vchannel.ObserveTruncateCollectionMessageV2(msg))
}

func (m *Manager) observeAlterWALMessage(ctx context.Context, msg message.ImmutableAlterWALMessageV2) moduleapi.ObserveResult {
	result := m.observeSegments(ctx, msg.TimeTick(), func(segment *segmentView) bool {
		return segment.CreateTimeTick() < msg.TimeTick()
	}, func(segment *segmentView) moduleapi.ObserveResult {
		return segment.Flush(ctx, msg.TimeTick())
	})
	return composeObserveResults(result, m.flushAllTransformLogBuffers(msg.TimeTick()))
}

func (m *Manager) flushVChannelSegmentsCreatedBefore(
	ctx context.Context,
	timetick uint64,
	vchannel string,
	collectionID int64,
) moduleapi.ObserveResult {
	info := m.retainedVChannel(vchannel)
	if info == nil {
		return emptyObserveResult()
	}
	result := m.flushRetainedVChannelSegmentsCreatedBefore(ctx, timetick, vchannel, collectionID)
	if !vchannelMatchesCollection(info, collectionID) {
		return result
	}
	if info.ShouldSkipReplay(timetick) || !info.CanReplayAt(timetick) {
		return result
	}
	return composeObserveResults(result, m.flushVChannelTransformLogBuffer(timetick, vchannel))
}

func (m *Manager) flushRetainedVChannelSegmentsCreatedBefore(
	ctx context.Context,
	timetick uint64,
	vchannel string,
	collectionID int64,
) moduleapi.ObserveResult {
	return m.observeSegments(ctx, timetick, func(segment *segmentView) bool {
		meta := segment.AssignmentMeta()
		return meta.GetVchannel() == vchannel &&
			meta.GetCollectionId() == collectionID &&
			segment.CreateTimeTick() < timetick
	}, func(segment *segmentView) moduleapi.ObserveResult {
		return segment.Flush(ctx, timetick)
	})
}

func (m *Manager) flushPartitionSegmentsCreatedBefore(
	ctx context.Context,
	timetick uint64,
	vchannel string,
	collectionID int64,
	partitionID int64,
) moduleapi.ObserveResult {
	info := m.retainedVChannel(vchannel)
	if info == nil {
		return emptyObserveResult()
	}
	result := m.flushRetainedPartitionSegmentsCreatedBefore(ctx, timetick, vchannel, collectionID, partitionID)
	if !vchannelMatchesCollection(info, collectionID) {
		return result
	}
	if info.ShouldSkipReplay(timetick) ||
		!info.CanReplayAt(timetick) ||
		!info.CanReplayPartitionAt(partitionID, timetick) ||
		!info.HasPartitionMeta(partitionID) {
		return result
	}
	return composeObserveResults(result, m.flushVChannelTransformLogBuffer(timetick, vchannel))
}

func (m *Manager) flushRetainedPartitionSegmentsCreatedBefore(
	ctx context.Context,
	timetick uint64,
	vchannel string,
	collectionID int64,
	partitionID int64,
) moduleapi.ObserveResult {
	return m.observeSegments(ctx, timetick, func(segment *segmentView) bool {
		meta := segment.AssignmentMeta()
		return meta.GetVchannel() == vchannel &&
			meta.GetCollectionId() == collectionID &&
			meta.GetPartitionId() == partitionID &&
			segment.CreateTimeTick() < timetick
	}, func(segment *segmentView) moduleapi.ObserveResult {
		return segment.Flush(ctx, timetick)
	})
}

func (m *Manager) flushVChannelTransformLogBuffer(timetick uint64, vchannel string) moduleapi.ObserveResult {
	info := m.retainedVChannel(vchannel)
	if info == nil || info.ShouldSkipReplay(timetick) {
		return emptyObserveResult()
	}
	return info.FlushTransformLogBuffer(timetick)
}

func (m *Manager) flushAllTransformLogBuffers(timetick uint64) moduleapi.ObserveResult {
	result := emptyObserveResult()
	for _, info := range m.vchannelViews {
		if !info.CanReplayAt(timetick) {
			continue
		}
		result = composeObserveResults(result, info.FlushTransformLogBuffer(timetick))
	}
	return result
}

func (m *Manager) observeSegments(
	_ context.Context,
	timetick uint64,
	matches func(*segmentView) bool,
	observe func(*segmentView) moduleapi.ObserveResult,
) moduleapi.ObserveResult {
	result := emptyObserveResult()
	for _, segment := range m.segmentViews {
		if !matches(segment) {
			continue
		}
		if shouldSkipTombstonedSegmentMeta(segment.AssignmentMeta(), timetick) {
			continue
		}
		if !m.segmentOwnerCanReplayAt(segment, timetick) {
			continue
		}
		segmentResult := observe(segment)
		result = composeObserveResults(result, segmentResult)
	}
	return result
}

func (m *Manager) segmentOwnerCanReplayAt(segment *segmentView, timetick uint64) bool {
	segmentMeta := segment.AssignmentMeta()
	vchannel := m.retainedVChannel(segmentMeta.GetVchannel())
	if vchannel == nil || segmentMeta.GetCollectionId() != vchannelCollectionID(vchannel) {
		return true
	}
	return vchannel.CanReplayAt(timetick) && vchannel.CanReplayPartitionAt(segmentMeta.GetPartitionId(), timetick)
}

func (m *Manager) observeInsertMessage(ctx context.Context, msg message.ImmutableInsertMessageV1) moduleapi.ObserveResult {
	vchannelManager := m.retainedVChannel(msg.VChannel())
	if vchannelManager == nil {
		m.logInconsistency(msg, "insert vchannel not found", zap.String("vchannel", msg.VChannel()))
		return emptyObserveResult()
	}
	if !vchannelMatchesCollection(vchannelManager, msg.Header().GetCollectionId()) {
		return emptyObserveResult()
	}
	if !vchannelManager.CanReplayAt(msg.TimeTick()) {
		return emptyObserveResult()
	}
	result := emptyObserveResult()
	for _, partition := range msg.Header().GetPartitions() {
		if !vchannelManager.CanReplayExistingPartitionAt(partition.GetPartitionId(), msg.TimeTick()) {
			continue
		}
		segmentID := partition.GetSegmentAssignment().GetSegmentId()
		segment := vchannelManager.Segment(segmentID)
		if segment == nil {
			m.logInconsistency(msg, "insert segment not found", zap.String("vchannel", msg.VChannel()), zap.Int64("segmentID", segmentID))
			continue
		}
		if !segment.CanReplayInsert(msg.TimeTick()) {
			m.logInconsistency(msg, "insert segment cannot replay insert", zap.String("vchannel", msg.VChannel()), zap.Int64("segmentID", segmentID))
			continue
		}
		segmentResult := segment.ObserveInsertMessageV1(ctx, msg, partition)
		result = composeObserveResults(result, segmentResult)
	}
	return result
}

func (m *Manager) observeDeleteMessage(ctx context.Context, msg message.ImmutableDeleteMessageV1) moduleapi.ObserveResult {
	if !m.metaAndData {
		return emptyObserveResult()
	}
	vchannelManager := m.retainedVChannel(msg.VChannel())
	if vchannelManager == nil {
		m.logInconsistency(msg, "delete vchannel not found", zap.String("vchannel", msg.VChannel()))
		return emptyObserveResult()
	}
	if !vchannelMatchesCollection(vchannelManager, msg.Header().GetCollectionId()) {
		return emptyObserveResult()
	}
	partitionID := msg.MustBody().GetPartitionID()
	if !vchannelManager.CanReplayAt(msg.TimeTick()) ||
		!vchannelManager.CanReplayExistingPartitionAt(partitionID, msg.TimeTick()) {
		return emptyObserveResult()
	}
	return vchannelManager.ObserveDeleteMessageV1(ctx, msg)
}

func (m *Manager) observeTxnMessage(ctx context.Context, msg message.ImmutableTxnMessage) moduleapi.ObserveResult {
	result := emptyObserveResult()
	observedSegments := make(map[int64]struct{})
	deletes := make(map[string][]message.ImmutableDeleteMessageV1)
	timetick := msg.TimeTick()
	msg.RangeOver(func(im message.ImmutableMessage) error {
		var subResult moduleapi.ObserveResult
		switch im.MessageType() {
		case message.MessageTypeInsert:
			insert := message.MustAsImmutableInsertMessageV1(im)
			vchannelView := m.retainedVChannel(insert.VChannel())
			if vchannelView == nil {
				m.logInconsistency(insert, "txn insert vchannel not found", zap.String("vchannel", insert.VChannel()))
				return nil
			}
			if !vchannelMatchesCollection(vchannelView, insert.Header().GetCollectionId()) {
				return nil
			}
			if !vchannelView.CanReplayAt(timetick) {
				return nil
			}
			for _, partition := range insert.Header().GetPartitions() {
				if !vchannelView.CanReplayExistingPartitionAt(partition.GetPartitionId(), timetick) {
					continue
				}
				segmentID := partition.GetSegmentAssignment().GetSegmentId()
				if _, observed := observedSegments[segmentID]; observed {
					continue
				}
				segment := vchannelView.Segment(segmentID)
				if segment == nil {
					m.logInconsistency(insert, "txn insert segment not found", zap.String("vchannel", insert.VChannel()), zap.Int64("segmentID", segmentID))
					continue
				}
				if !segment.CanReplayInsert(timetick) {
					m.logInconsistency(insert, "txn insert segment cannot replay insert", zap.String("vchannel", insert.VChannel()), zap.Int64("segmentID", segmentID))
					continue
				}
				observedSegments[segmentID] = struct{}{}
				subResult = composeObserveResults(subResult, segment.ObserveTxnMessage(ctx, msg))
			}
		case message.MessageTypeDelete:
			deleted := message.MustAsImmutableDeleteMessageV1(im)
			deletes[deleted.VChannel()] = append(deletes[deleted.VChannel()], deleted)
		default:
			m.logInconsistency(im, "unexpected message type in txn message", zap.String("messageType", im.MessageType().String()))
			return nil
		}
		result = composeObserveResults(result, subResult)
		return nil
	})
	for _, vchannelDeletes := range deletes {
		result = composeObserveResults(result, m.observeTxnDeleteMessages(vchannelDeletes, timetick))
	}
	return result
}

func (m *Manager) observeTxnDeleteMessages(deletes []message.ImmutableDeleteMessageV1, timetick uint64) moduleapi.ObserveResult {
	if !m.metaAndData || len(deletes) == 0 {
		return emptyObserveResult()
	}
	vchannelName := deletes[0].VChannel()
	vchannelManager := m.retainedVChannel(vchannelName)
	if vchannelManager == nil {
		m.logInconsistency(deletes[0], "txn delete vchannel not found", zap.String("vchannel", vchannelName))
		return emptyObserveResult()
	}
	if !vchannelMatchesCollection(vchannelManager, deletes[0].Header().GetCollectionId()) {
		return emptyObserveResult()
	}
	if !vchannelManager.CanReplayAt(timetick) {
		return emptyObserveResult()
	}
	filteredDeletes := deletes[:0]
	for _, deleted := range deletes {
		partitionID := deleted.MustBody().GetPartitionID()
		if !vchannelManager.CanReplayExistingPartitionAt(partitionID, timetick) {
			continue
		}
		filteredDeletes = append(filteredDeletes, deleteMessageWithTimeTick(deleted, timetick))
	}
	if len(filteredDeletes) == 0 {
		return emptyObserveResult()
	}
	return vchannelManager.ObserveDeleteMessagesV1(filteredDeletes)
}

func deleteMessageWithTimeTick(deleted message.ImmutableDeleteMessageV1, timetick uint64) message.ImmutableDeleteMessageV1 {
	if deleted.TimeTick() == timetick {
		return deleted
	}
	msg := message.NewDeleteMessageBuilderV1().
		WithVChannel(deleted.VChannel()).
		WithHeader(deleted.Header()).
		WithBody(deleted.MustBody()).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(deleted.LastConfirmedMessageID()).
		IntoImmutableMessage(deleted.MessageID())
	return message.MustAsImmutableDeleteMessageV1(msg)
}

func emptyObserveResult() moduleapi.ObserveResult {
	return moduleapi.ObserveResult{}
}

func vchannelMatchesCollection(vchannel *vChannelView, collectionID int64) bool {
	return vchannelCollectionID(vchannel) == collectionID
}

func dataBarrierResult(barrier walcheckpoint.Barrier) moduleapi.ObserveResult {
	return moduleapi.ObserveResult{Data: barrier}
}

func composeBarrier(left walcheckpoint.Barrier, right walcheckpoint.Barrier) walcheckpoint.Barrier {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return walcheckpoint.NewCompositeBarrier(left, right)
}

func composeObserveResults(results ...moduleapi.ObserveResult) moduleapi.ObserveResult {
	composed := moduleapi.ObserveResult{}
	for _, result := range results {
		composed.Meta = composeBarrier(composed.Meta, result.Meta)
		composed.Data = composeBarrier(composed.Data, result.Data)
	}
	return composed
}

func (m *Manager) logInconsistency(msg message.ImmutableMessage, reason string, fields ...zap.Field) {
	if m.logger == nil {
		return
	}
	fields = append(fields, log.FieldMessage(msg))
	m.logger.Warn("inconsistent growing observe state", append([]zap.Field{zap.String("reason", reason)}, fields...)...)
}
