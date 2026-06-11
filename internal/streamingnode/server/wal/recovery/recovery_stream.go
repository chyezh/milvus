package recovery

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// runBoundedMetaScannerAndSwitchModules recovers fast module metadata from a bounded WAL scan,
// then switches modules into MetaAndData mode and returns their open snapshot.
func (r *recoveryStorageImpl) runBoundedMetaScannerAndSwitchModules(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	lastTimeTickMessage message.ImmutableMessage,
) (snapshot *RecoverySnapshot, err error) {
	r.metrics.ObserveStateChange(recoveryStorageStateStreamRecovering)
	r.metrics.ObServePersistedMetrics(r.checkpoint.TimeTick)
	r.SetLogger(resource.Resource().Logger().With(
		log.FieldComponent(componentRecoveryStorage),
		zap.String("channel", recoveryStreamBuilder.Channel().String()),
		zap.String("startMessageID", r.checkpoint.MessageID.String()),
		zap.Uint64("fromTimeTick", r.checkpoint.TimeTick),
		zap.Uint64("toTimeTick", lastTimeTickMessage.TimeTick()),
		zap.String("state", recoveryStorageStateStreamRecovering),
	))

	r.Logger().Info("recover from wal stream...")
	rs := recoveryStreamBuilder.Build(BuildRecoveryStreamParam{
		StartCheckpoint: r.checkpoint.MessageID,
		EndTimeTick:     lastTimeTickMessage.TimeTick(),
	})
	defer func() {
		rs.Close()
		if err != nil {
			r.Logger().Warn("recovery from wal stream failed", zap.Error(err))
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
			r.observeMetaScannerMessage(ctx, msg)
		}
	}
	if rs.Error() != nil {
		return nil, errors.Wrap(rs.Error(), "failed to read the recovery info from wal")
	}
	snapshot = r.switchModulesIntoMetaAndData()
	snapshot.TxnBuffer = rs.TxnBuffer()
	logFields := []zap.Field{
		zap.String("channel", recoveryStreamBuilder.Channel().String()),
		zap.Int("vchannels", len(snapshot.VChannels)),
		zap.Int("segments", len(snapshot.SegmentAssignments)),
		zap.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		zap.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
	}
	if snapshot.AlterWALInfo != nil {
		logFields = append(logFields,
			zap.Bool("foundAlterWALMsg", snapshot.AlterWALInfo.FoundAlterWALMsg),
			zap.Stringer("targetWALName", snapshot.AlterWALInfo.TargetWALName),
		)
	}
	r.Logger().Info("recovery from wal stream done", logFields...)
	return snapshot, nil
}

func (r *recoveryStorageImpl) switchModulesIntoMetaAndData() *RecoverySnapshot {
	snapshot := &RecoverySnapshot{
		Checkpoint: r.checkpointManager.Snapshot(),
	}
	for _, module := range r.modules {
		moduleSnapshot := module.SwitchIntoMetaAndData()
		for _, s := range moduleapi.FlattenModuleSnapshot(moduleSnapshot) {
			switch typed := s.(type) {
			case *moduleapi.VChannelModuleSnapshot:
				snapshot.VChannels = typed.VChannels
			case *moduleapi.SegmentModuleSnapshot:
				snapshot.SegmentAssignments = typed.Segments
			}
		}
	}
	if r.alterWALInfo != nil {
		alterWALInfoCopy := *r.alterWALInfo
		snapshot.AlterWALInfo = &alterWALInfoCopy
	}
	return snapshot
}
