package recovery

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// recoverFromStream recovers the recovery storage from the recovery stream.
func (r *recoveryStorageImpl) recoverFromStream(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	lastTimeTickMessage message.ImmutableMessage,
) (snapshot *RecoverySnapshot, err error) {
	r.metrics.ObserveStateChange(recoveryStorageStateStreamRecovering)
	r.metrics.ObServePersistedMetrics(r.checkpoint.TimeTick)
	r.SetLogger(resource.Resource().Logger().With(
		log.FieldComponent(componentRecoveryStorage),
		log.String("channel", recoveryStreamBuilder.Channel().String()),
		log.String("startMessageID", r.checkpoint.MessageID.String()),
		log.Uint64("fromTimeTick", r.checkpoint.TimeTick),
		log.Uint64("toTimeTick", lastTimeTickMessage.TimeTick()),
		log.String("state", recoveryStorageStateStreamRecovering),
	))

	r.Logger().Info(ctx, "recover from wal stream...")
	rs := recoveryStreamBuilder.Build(BuildRecoveryStreamParam{
		StartCheckpoint: r.checkpoint.MessageID,
		EndTimeTick:     lastTimeTickMessage.TimeTick(),
	})
	defer func() {
		rs.Close()
		if err != nil {
			r.Logger().Warn(ctx, "recovery from wal stream failed", log.Err(err))
			return
		}
	}()
L:
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "failed to recover from wal")
		case msg, ok := <-rs.Chan():
			if !ok {
				// The recovery stream is reach the end, we can stop the recovery.
				break L
			}
			r.ObserveMessage(ctx, msg)
		}
	}
	if rs.Error() != nil {
		return nil, errors.Wrap(rs.Error(), "failed to read the recovery info from wal")
	}
	snapshot = r.getSnapshot()
	snapshot.TxnBuffer = rs.TxnBuffer()
	logFields := []zap.Field{
		log.String("channel", recoveryStreamBuilder.Channel().String()),
		log.Int("vchannels", len(snapshot.VChannels)),
		log.Int("segments", len(snapshot.SegmentAssignments)),
		log.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		log.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
	}
	if snapshot.AlterWALInfo != nil {
		logFields = append(logFields,
			log.Bool("foundAlterWALMsg", snapshot.AlterWALInfo.FoundAlterWALMsg),
			log.Stringer("targetWALName", snapshot.AlterWALInfo.TargetWALName),
		)
	}
	r.Logger().Info(ctx, "recovery from wal stream done", logFields...)
	return snapshot, nil
}

// getSnapshot returns the snapshot of the recovery storage.
// Use this function to get the snapshot after recovery is finished,
// and use the snapshot to recover all write ahead components.
func (r *recoveryStorageImpl) getSnapshot() *RecoverySnapshot {
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(r.segments))
	vchannels := make(map[string]*streamingpb.VChannelMeta, len(r.vchannels))
	// Collect active vchannels and build a set of active partition IDs (globally unique).
	activePartitions := make(map[int64]struct{})
	for channelName, vchannel := range r.vchannels {
		if vchannel.IsActive() {
			vchannels[channelName] = proto.Clone(vchannel.meta).(*streamingpb.VChannelMeta)
			for _, p := range vchannel.meta.CollectionInfo.Partitions {
				activePartitions[p.PartitionId] = struct{}{}
			}
		}
	}
	for segmentID, segment := range r.segments {
		if !segment.IsGrowing() {
			continue
		}
		// Defensive filtering: skip GROWING segments whose parent vchannel does not exist
		// or is not active, or whose partition has been dropped. This can happen due to
		// non-atomic etcd persistence or Kafka offset compaction replaying CreateSegment
		// for dropped collections/partitions.
		if _, ok := vchannels[segment.meta.Vchannel]; !ok {
			r.Logger().Warn(context.TODO(), "getSnapshot: skipping orphaned growing segment with non-active vchannel",
				log.Int64("segmentID", segmentID),
				log.String("vchannel", segment.meta.Vchannel),
				log.Int64("collectionID", segment.meta.CollectionId),
			)
			continue
		}
		if _, ok := activePartitions[segment.meta.PartitionId]; !ok {
			r.Logger().Warn(context.TODO(), "getSnapshot: skipping orphaned growing segment with dropped partition",
				log.Int64("segmentID", segmentID),
				log.String("vchannel", segment.meta.Vchannel),
				log.Int64("collectionID", segment.meta.CollectionId),
				log.Int64("partitionID", segment.meta.PartitionId),
			)
			continue
		}
		segments[segmentID] = proto.Clone(segment.meta).(*streamingpb.SegmentAssignmentMeta)
	}
	snapshot := &RecoverySnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		Checkpoint:         r.checkpoint.Clone(),
	}
	if r.alterWALInfo != nil {
		alterWALInfoCopy := *r.alterWALInfo
		snapshot.AlterWALInfo = &alterWALInfoCopy
	}
	return snapshot
}
