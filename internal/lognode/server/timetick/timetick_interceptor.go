package timetick

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/timetick/ack"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

var _ walimpls.AppendInterceptor = (*timeTickAppendInterceptor)(nil)

// timeTickAppendInterceptor is a append interceptor.
type timeTickAppendInterceptor struct {
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan struct{}

	ackManager *ack.AckManager
	ackDetails *ackDetails
	sourceID   int64
}

// Ready implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) Ready() <-chan struct{} {
	return impl.ready
}

// Do implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append walimpls.Append) (msgID message.MessageID, err error) {
	if msg.MessageType() != message.MessageTypeTimeTick {
		// Allocate new acker for message.
		acker, err := impl.ackManager.Allocate(ctx)
		if err != nil {
			err := status.NewInner("allocate timestamp failed, %s", err.Error())
			return nil, err
		}
		defer func() {
			acker.Ack(ack.OptError(err))
			impl.ackManager.AdvanceLastConfirmedMessageID(msgID)
		}()

		// Assign timestamp to message and call append method.
		msg = msg.
			WithTimeTick(acker.Timestamp()).                  // message assigned with these timetick.
			WithLastConfirmed(acker.LastConfirmedMessageID()) // start consuming from these message id, the message which timetick greater than current timetick will never be lost.
	}
	return append(ctx, msg)
}

// Close implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) Close() {
	impl.cancel()
}

// execute start a background task.
func (impl *timeTickAppendInterceptor) executeSyncTimeTick(interval time.Duration, walReady <-chan walimpls.WALImpls) {
	wal := <-walReady

	logger := log.With(zap.Any("channel", wal.Channel()))
	logger.Info("start to sync time tick...")
	defer logger.Info("sync time tick stopped")

	// Send first timetick message to wal before interceptor is ready.
	for count := 0; ; count++ {
		// Sent first timetick message to wal before ready.
		// New TT is always greater than all tt on previous lognode.
		// A fencing operation of underlying WAL is needed to make exclusive produce of topic.
		// Otherwise, the TT principle may be violated.
		// And sendTsMsg must be done, to help ackManager to get first LastConfirmedMessageID
		select {
		case <-impl.ctx.Done():
			return
		default:
		}
		if err := impl.sendTsMsg(impl.ctx, wal); err != nil {
			log.Warn("send first timestamp message failed", zap.Error(err), zap.Int("retryCount", count))
			// TODO: exponential backoff.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	// interceptor is ready now.
	close(impl.ready)
	logger.Info("start to sync time ready")

	// TODO: sync time tick message to wal periodically.
	// Add a trigger on `AckManager` to sync time tick message without periodically.
	// `AckManager` gather detail information, time tick sync can check it and make the message between tt more smaller.
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-impl.ctx.Done():
			return
		case <-ticker.C:
			if err := impl.sendTsMsg(impl.ctx, wal); err != nil {
				log.Warn("send time tick sync message failed", zap.Error(err))
			}
		}
	}
}

// syncAcknowledgedDetails syncs the timestamp acknowledged details.
func (impl *timeTickAppendInterceptor) syncAcknowledgedDetails() {
	// Sync up and get last confirmed timestamp.
	ackDetails, err := impl.ackManager.SyncAndGetAcknowledged(impl.ctx)
	if err != nil {
		log.Warn("sync timestamp ack manager failed", zap.Error(err))
	}

	// Add ack details to ackDetails.
	impl.ackDetails.AddDetails(ackDetails)
}

// sendTsMsg sends first timestamp message to wal.
// TODO: TT lag warning.
func (impl *timeTickAppendInterceptor) sendTsMsg(_ context.Context, wal walimpls.WALImpls) error {
	// Sync the timestamp acknowledged details.
	impl.syncAcknowledgedDetails()

	if impl.ackDetails.Empty() {
		// No acknowledged info can be sent.
		// Some message sent operation is blocked, new TT cannot be pushed forward.
		return nil
	}

	// Construct time tick message.
	msg, err := newTimeTickMsg(impl.ackDetails.LastAllAcknowledgedTimestamp(), impl.sourceID)
	if err != nil {
		return errors.Wrap(err, "at build time tick msg")
	}

	// Append it to wal.
	msgID, err := wal.Append(impl.ctx, msg)
	if err != nil {
		return errors.Wrapf(err,
			"append time tick msg to wal failed, timestamp: %d, previous message counter: %d",
			impl.ackDetails.LastAllAcknowledgedTimestamp(),
			impl.ackDetails.Len(),
		)
	}

	// Ack details has been committed to wal, clear it.
	impl.ackDetails.Clear()
	impl.ackManager.AdvanceLastConfirmedMessageID(msgID)
	return nil
}
