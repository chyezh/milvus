package recovery

import (
	"context"
	"math"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/log"
	msgadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
)

// isDirty checks if the recovery storage mem state is not consistent with the persisted recovery storage.
func (rs *recoveryStorageImpl) isDirty() bool {
	if rs.pendingPersistSnapshot != nil {
		return true
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()
	checkpointDirty := rs.checkpointManager != nil && rs.checkpointManager.HasDirty()
	return rs.dirtyCounter > 0 || rs.pendingSalvageCheckpoint != nil || checkpointDirty
}

// TODO: !!! all recovery persist operation should be a compare-and-swap operation to
// promise there's only one consumer of wal.
// But currently, we don't implement the CAS operation of meta interface.
// Should be fixed in future.
func (rs *recoveryStorageImpl) backgroundTask() {
	ticker := time.NewTicker(rs.cfg.persistInterval)
	defer func() {
		ticker.Stop()
		rs.Logger().Info("recovery storage background task, perform a graceful exit...")
		if err := rs.persistDritySnapshotWhenClosing(); err != nil {
			rs.Logger().Warn("failed to persist dirty snapshot when closing", zap.Error(err))
		}
		rs.backgroundTaskNotifier.Finish(struct{}{})
		rs.Logger().Info("recovery storage background task exit")
	}()

	for {
		select {
		case <-rs.backgroundTaskNotifier.Context().Done():
			return
		case <-rs.persistNotifier:
		case <-ticker.C:
		}
		if err := rs.persistDirtySnapshot(rs.backgroundTaskNotifier.Context(), zap.DebugLevel); err != nil {
			return
		}
	}
}

// persistDritySnapshotWhenClosing persists the dirty snapshot when closing the recovery storage.
func (rs *recoveryStorageImpl) persistDritySnapshotWhenClosing() error {
	ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.gracefulTimeout)
	defer cancel()

	for {
		if rs.taskScheduler != nil {
			if err := rs.taskScheduler.WaitIdle(ctx); err != nil {
				return err
			}
		}
		for rs.isDirty() {
			if err := rs.persistDirtySnapshot(ctx, zap.InfoLevel); err != nil {
				return err
			}
		}
		if rs.taskScheduler != nil {
			if err := rs.taskScheduler.WaitIdle(ctx); err != nil {
				return err
			}
		}
		if !rs.isDirty() {
			break
		}
	}
	rs.gracefulClosed = true
	return nil
}

// persistDirtySnapshot persists the dirty snapshot to the catalog.
func (rs *recoveryStorageImpl) persistDirtySnapshot(ctx context.Context, lvl zapcore.Level) (err error) {
	if rs.pendingPersistSnapshot == nil {
		// if there's no dirty snapshot, generate a new one.
		rs.pendingPersistSnapshot = rs.consumeDirtySnapshot()
	}
	if rs.pendingPersistSnapshot == nil {
		return nil
	}

	snapshot := rs.pendingPersistSnapshot
	rs.metrics.ObserveIsOnPersisting(true)
	logger := rs.Logger().With(
		zap.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		zap.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
	)
	defer func() {
		if err != nil {
			logger.Warn("failed to persist dirty snapshot", zap.Error(err))
			return
		}
		rs.pendingPersistSnapshot = nil
		logger.Log(lvl, "persist dirty snapshot")
		rs.metrics.ObserveIsOnPersisting(false)
	}()

	rs.refreshSnapshotCheckpoint(snapshot)

	// Salvage checkpoint must be persisted before the consume checkpoint to guarantee ordering:
	// if the node crashes between these two writes, the next snapshot retry will re-persist both.
	if snapshot.SalvageCheckpoint != nil {
		if err := rs.retryOperationWithBackoff(ctx, rs.Logger().With(zap.String("op", "persistSalvageCheckpoint")), func(ctx context.Context) error {
			return resource.Resource().StreamingNodeCatalog().SaveSalvageCheckpoint(ctx, rs.channel.Name, snapshot.SalvageCheckpoint.IntoProto())
		}); err != nil {
			return err
		}
	}

	if err := rs.persistCheckpointSnapshot(ctx, snapshot, lvl >= zapcore.InfoLevel); err != nil {
		return err
	}
	return
}

func (rs *recoveryStorageImpl) persistCheckpointSnapshot(ctx context.Context, snapshot *RecoverySnapshot, _ bool) error {
	if !snapshot.CheckpointDirty {
		return nil
	}
	task := &walCheckpointPersistTask{
		recoveryStorage: rs,
		checkpoint:      snapshot.Checkpoint.Clone(),
	}
	return task.Run(ctx)
}

type walCheckpointPersistTask struct {
	recoveryStorage *recoveryStorageImpl
	checkpoint      *WALCheckpoint
}

func (t *walCheckpointPersistTask) Run(ctx context.Context) error {
	rs := t.recoveryStorage
	if err := retryOperationWithBackoff(ctx, rs.Logger().With(zap.String("op", "persistCheckpoint")), func(ctx context.Context) error {
		return resource.Resource().StreamingNodeCatalog().
			SaveConsumeCheckpoint(ctx, rs.channel.Name, t.checkpoint.IntoProto())
	}); err != nil {
		return err
	}
	rs.metrics.ObServePersistedMetrics(t.checkpoint.TimeTick)
	rs.simpleTruncateCheckpoint(ctx, t.checkpoint)
	rs.notifyCheckpointPersisted(t.checkpoint)
	return nil
}

func (rs *recoveryStorageImpl) notifyCheckpointPersisted(checkpoint *WALCheckpoint) {
	if checkpoint.DataCheckpoint == nil {
		return
	}
	for _, module := range rs.modules {
		observer, ok := module.(moduleapi.CheckpointPersistedObserver)
		if !ok {
			continue
		}
		observer.NotifyCheckpointPersisted(checkpoint.TimeTick, checkpoint.DataCheckpoint.TimeTick)
	}
}

func (rs *recoveryStorageImpl) refreshSnapshotCheckpoint(snapshot *RecoverySnapshot) {
	rs.mu.Lock()

	if rs.checkpointManager == nil {
		rs.mu.Unlock()
		return
	}
	rs.checkpointManager.TryAdvanceMetaCheckpoint()
	rs.checkpointManager.TryAdvanceDataCheckpoint()
	channelCheckpoints := rs.updateDataCheckpointFromViewsLocked()
	if checkpointDirty := rs.checkpointManager.ConsumeDirty(); checkpointDirty {
		snapshot.Checkpoint = rs.checkpointManager.Snapshot()
		snapshot.CheckpointDirty = true
	}
	rs.mu.Unlock()
	rs.updateChannelCheckpoints(channelCheckpoints)
}

func (rs *recoveryStorageImpl) updateDataCheckpointFromViewsLocked() []*msgpb.MsgPosition {
	if dataTimeTick := rs.dataCheckpointTimeTickLocked(); dataTimeTick != math.MaxUint64 {
		rs.checkpointManager.UpdateDataCheckpointFromPhysicalCheckpoint(dataTimeTick)
	}
	return rs.channelDataCheckpointPositionsLocked()
}

func (rs *recoveryStorageImpl) dataCheckpointTimeTickLocked() uint64 {
	dataTimeTick := uint64(math.MaxUint64)
	for _, module := range rs.modules {
		view, ok := module.(moduleapi.DataCheckpointView)
		if !ok {
			continue
		}
		if timetick := view.DataCheckpointTimeTick(); timetick < dataTimeTick {
			dataTimeTick = timetick
		}
	}
	return dataTimeTick
}

func (rs *recoveryStorageImpl) channelDataCheckpointPositionsLocked() []*msgpb.MsgPosition {
	if rs.checkpointManager == nil || rs.checkpointManager.Checkpoint().MessageID == nil {
		return nil
	}
	channelTimeTicks := make(map[string]uint64)
	for _, module := range rs.modules {
		view, ok := module.(moduleapi.ChannelDataCheckpointView)
		if !ok {
			continue
		}
		for vchannel, timetick := range view.ChannelDataCheckpointTimeTicks() {
			if vchannel == "" || timetick == 0 || timetick == math.MaxUint64 {
				continue
			}
			current, ok := channelTimeTicks[vchannel]
			if !ok || timetick < current {
				channelTimeTicks[vchannel] = timetick
			}
		}
	}
	if len(channelTimeTicks) == 0 {
		return nil
	}
	msgID, walName := msgadaptor.MustGetMQWrapperIDAndWALNameFromMessage(rs.checkpointManager.Checkpoint().MessageID)
	msgIDBytes := msgID.Serialize()
	positions := make([]*msgpb.MsgPosition, 0, len(channelTimeTicks))
	for vchannel, timetick := range channelTimeTicks {
		positions = append(positions, &msgpb.MsgPosition{
			ChannelName: vchannel,
			MsgID:       msgIDBytes,
			Timestamp:   timetick,
			WALName:     commonpb.WALName(walName),
		})
	}
	return positions
}

func (rs *recoveryStorageImpl) updateChannelCheckpoints(positions []*msgpb.MsgPosition) {
	if rs.channelCheckpointUpdater == nil {
		return
	}
	for _, pos := range positions {
		rs.channelCheckpointUpdater.AddTask(pos, true, func() {})
	}
}

func (rs *recoveryStorageImpl) simpleTruncateCheckpoint(ctx context.Context, checkpoint *WALCheckpoint) {
	if rs.truncator == nil || checkpoint.DataCheckpoint == nil || checkpoint.DataCheckpoint.MessageID == nil {
		return
	}
	_ = rs.truncator.Truncate(ctx, checkpoint.DataCheckpoint.MessageID)
}

// retryOperationWithBackoff retries the operation with exponential backoff.
func (rs *recoveryStorageImpl) retryOperationWithBackoff(ctx context.Context, logger *log.MLogger, op func(ctx context.Context) error) error {
	return retryOperationWithBackoff(ctx, logger, op)
}

func retryOperationWithBackoff(ctx context.Context, logger *log.MLogger, op func(ctx context.Context) error) error {
	backoff := newBackoff()
	for {
		err := op(ctx)
		if err == nil {
			return nil
		}
		// because underlying kv may report the context.Canceled, context.DeadlineExceeded even if the ctx is not canceled.
		// so we cannot use errors.IsAny(err, context.Canceled, context.DeadlineExceeded) to check the error.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		nextInterval := backoff.NextBackOff()
		logger.Warn("failed to persist operation, wait for retry...", zap.Duration("nextRetryInterval", nextInterval), zap.Error(err))
		select {
		case <-time.After(nextInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func newBackoff() *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 1 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()
	return backoff
}
