package timetick

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/interceptor"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

var _ interceptor.AppendInterceptor = (*timestampAssignAppendInterceptor)(nil)

// timestampAssignAppendInterceptor is an interceptor to assign timestamp to message.
type timestampAssignAppendInterceptor struct {
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan struct{}

	allocator *allocatorWithRecorder
	sourceID  int64
	wal       wal.WAL
}

// Ready implements AppendInterceptor.
func (impl *timestampAssignAppendInterceptor) Ready() <-chan struct{} {
	return impl.ready
}

// Do implements AppendInterceptor.
func (impl *timestampAssignAppendInterceptor) Do(ctx context.Context, msg message.MutableMessage, append interceptor.Append) (message.MessageID, error) {
	// Allocate new ts for message.
	ts, err := impl.allocator.AllocateOne(ctx)
	if err != nil {
		err := status.NewInner("allocate timestamp failed, %s", err.Error())
		return nil, err
	}
	defer impl.allocator.AckAllocated(ts)

	// Assign timestamp to message and call append method.
	msg = msg.WithTimeTick(ts)
	return append(ctx, msg)
}

// Close implements AppendInterceptor.
func (impl *timestampAssignAppendInterceptor) Close() {
	impl.cancel()
}

// execute start a background task.
func (impl *timestampAssignAppendInterceptor) executeSyncTimeTick(interval time.Duration) {
	logger := log.With(zap.Any("channel", impl.wal.Channel()))
	logger.Info("start to sync time tick...")
	defer logger.Info("sync time tick stopped")

	// Send first timetick message to wal before interceptor is ready.
	for count := 0; ; count++ {
		// Sent first timetick message to wal before ready.
		select {
		case <-impl.ctx.Done():
			return
		default:
		}
		if err := impl.sendTsMsg(impl.ctx); err != nil {
			log.Warn("send first timestamp message failed", zap.Error(err), zap.Int("retryCount", count))
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	// interceptor is ready now.
	close(impl.ready)
	logger.Info("start to sync time ready")

	// TODO: sync time tick message to wal periodically.
	// And underlying allocator should gather the consume bytes information, active trigger time tick sync to make tt package more smaller.
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-impl.ctx.Done():
			return
		case <-ticker.C:
			if err := impl.sendTsMsg(impl.ctx); err != nil {
				log.Warn("send time tick sync message failed", zap.Error(err))
			}
		}
	}
}

// sendTsMsg sends first timestamp message to wal.
// TODO: TT lag warning.
func (impl *timestampAssignAppendInterceptor) sendTsMsg(ctx context.Context) error {
	// Sync up and get last confirmed timestamp.
	consumedTimestamp, err := impl.allocator.SyncAndGetLastAllConfirmed(impl.ctx)
	if err != nil {
		return errors.Wrap(err, "sync allocator and recorder failed")
	}

	// construct time tick message.
	msg, err := newTimeTickMsg(consumedTimestamp.latestAllConfirmedTimestamp, impl.sourceID)
	if err != nil {
		return errors.Wrap(err, "at build time tick msg")
	}

	// Append it to wal.
	_, err = impl.wal.Append(impl.ctx, msg)
	if err != nil {
		return errors.Wrapf(err,
			"append time tick msg to wal failed, timestamp: %d, before message: %d",
			consumedTimestamp.latestAllConfirmedTimestamp,
			consumedTimestamp.countAfterPreviousAllConfirmedTimestamp,
		)
	}
	// ack the timestamp.
	impl.allocator.AckConsumed(consumedTimestamp.latestAllConfirmedTimestamp)
	return nil
}
