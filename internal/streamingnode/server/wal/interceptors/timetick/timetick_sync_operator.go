package timetick

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"go.uber.org/zap"
)

var _ inspector.TimeTickSyncOperator = &timeTickSyncOperator{}

type timeTickSyncOperator struct {
	ctx    context.Context
	cancel context.CancelFunc

	logger                *log.MLogger
	pchannel              types.PChannelInfo                        // pchannel info belong to.
	notifier              *syncutil.Future[*inspector.SyncNotifier] // used to notify the inspector to perform a sync operation.
	ready                 chan struct{}                             // hint the time tick operator is ready to use.
	interceptorBuildParam interceptors.InterceptorBuildParam        // interceptor build param.
	ackManager            *ack.AckManager                           // ack manager.
	ackDetails            *ackDetails                               // all acknowledged details, all acked messages but not sent to wal will be kept here.
	sourceID              int64                                     // the current node id.
	timeTickInfoListener  *inspector.TimeTickInfoListener
}

// Channel returns the pchannel info.
func (impl *timeTickSyncOperator) Channel() types.PChannelInfo {
	return impl.pchannel
}

// TimeTickListener returns the time tick listener.
func (impl *timeTickSyncOperator) TimeTickListener() *inspector.TimeTickInfoListener {
	return impl.timeTickInfoListener
}

// SetNotifier sets the sync notifier.
func (impl *timeTickSyncOperator) SetNotifier(notifier *inspector.SyncNotifier) {
	impl.notifier.Set(notifier)
}

// Sync trigger a sync operation.
func (impl *timeTickSyncOperator) Sync(ctx context.Context) {
	// Sync operation cannot trigger until isReady.
	if !impl.isReady() {
		return
	}

	wal := impl.interceptorBuildParam.WAL.Get()
	err := impl.sendTsMsg(ctx, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendResult, err := wal.Append(ctx, msg)
		if err != nil {
			return nil, err
		}
		return appendResult.MessageID, nil
	})
	if err != nil {
		impl.logger.Warn("send time tick sync message failed", zap.Error(err))
	}
}

// intialize initializes the time tick sync operator.
func (impl *timeTickSyncOperator) initialize() {
	impl.blockUntilSyncTimeTickReady()
}

// blockUntilSyncTimeTickReady blocks until the first time tick message is sent.
func (impl *timeTickSyncOperator) blockUntilSyncTimeTickReady() error {
	underlyingWALImpls := impl.interceptorBuildParam.WALImpls

	impl.logger.Info("start to sync first time tick")
	defer impl.logger.Info("sync first time tick done")

	// Send first timetick message to wal before interceptor is ready.
	for count := 0; ; count++ {
		// Sent first timetick message to wal before ready.
		// New TT is always greater than all tt on previous streamingnode.
		// A fencing operation of underlying WAL is needed to make exclusive produce of topic.
		// Otherwise, the TT principle may be violated.
		// And sendTsMsg must be done, to help ackManager to get first LastConfirmedMessageID
		// !!! Send a timetick message into walimpls directly is safe.
		select {
		case <-impl.ctx.Done():
			return impl.ctx.Err()
		default:
		}
		resource.Resource().TSOAllocator().Sync()
		ts, err := resource.Resource().TSOAllocator().Allocate(impl.ctx)
		if err != nil {
			impl.logger.Warn("allocate timestamp failed", zap.Error(err))
			// TODO: exponential backoff.
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if err := impl.sendPersistentTsMsg(impl.ctx, ts, nil, underlyingWALImpls.Append); err != nil {
			impl.logger.Warn("send first timestamp message failed", zap.Error(err), zap.Int("retryCount", count))
			// TODO: exponential backoff.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	// interceptor is ready now.
	close(impl.ready)
	return nil
}

// Ready implements AppendInterceptor.
func (impl *timeTickSyncOperator) Ready() <-chan struct{} {
	return impl.ready
}

// isReady returns true if the operator is ready.
func (impl *timeTickSyncOperator) isReady() bool {
	select {
	case <-impl.ready:
		return true
	default:
		return false
	}
}

// AckManager returns the ack manager.
func (impl *timeTickSyncOperator) AckManager() *ack.AckManager {
	return impl.ackManager
}

// Notify notifies the time tick sync operator.
func (impl *timeTickSyncOperator) Notify() {
	impl.notifier.Get().AddAndNotify(impl.pchannel)
}

// Close close the time tick sync operator.
func (impl *timeTickSyncOperator) Close() {
	impl.cancel()
}

// sendTsMsg sends first timestamp message to wal.
// TODO: TT lag warning.
func (impl *timeTickSyncOperator) sendTsMsg(ctx context.Context, appender func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)) error {
	// Sync the timestamp acknowledged details.
	impl.syncAcknowledgedDetails(ctx)

	if impl.ackDetails.Empty() {
		// No acknowledged info can be sent.
		// Some message sent operation is blocked, new TT cannot be pushed forward.
		return nil
	}

	// Construct time tick message.
	ts := impl.ackDetails.LastAllAcknowledgedTimestamp()
	lastConfirmedMessageID := impl.ackDetails.LastConfirmedMessageID()

	if impl.ackDetails.IsNoPersistedMessage() {
		// there's no persisted message, so no need to send persistent time tick message.
		// only update it to notify the scanner.
		return impl.notifyNoPersistentTsMsg(ts)
	}
	// otherwise, send persistent time tick message.
	return impl.sendPersistentTsMsg(ctx, ts, lastConfirmedMessageID, appender)
}

// sendPersistentTsMsg sends persistent time tick message to wal.
func (impl *timeTickSyncOperator) sendPersistentTsMsg(ctx context.Context,
	ts uint64,
	lastConfirmedMessageID message.MessageID,
	appender func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error),
) error {
	msg, err := NewTimeTickMsg(ts, lastConfirmedMessageID, impl.sourceID)
	if err != nil {
		return errors.Wrap(err, "at build time tick msg")
	}

	// Append it to wal.
	msgID, err := appender(ctx, msg)
	if err != nil {
		return errors.Wrapf(err,
			"append time tick msg to wal failed, timestamp: %d, previous message counter: %d",
			impl.ackDetails.LastAllAcknowledgedTimestamp(),
			impl.ackDetails.Len(),
		)
	}

	impl.ackDetails.Clear()
	// Ack details has been committed to wal, clear it.
	impl.ackManager.AdvanceLastConfirmedMessageID(msgID)
	// Update last time tick message id.
	impl.timeTickInfoListener.Update(inspector.TimeTickInfo{
		MessageID:              msgID,
		LastTimeTick:           ts,
		LastConfirmedMessageID: lastConfirmedMessageID,
	})
	return nil
}

// notifyNoPersistentTsMsg sends no persistent time tick message.
func (impl *timeTickSyncOperator) notifyNoPersistentTsMsg(ts uint64) error {
	impl.ackDetails.Clear()
	impl.timeTickInfoListener.OnlyUpdateTs(ts)
	return nil
}

// syncAcknowledgedDetails syncs the timestamp acknowledged details.
func (impl *timeTickSyncOperator) syncAcknowledgedDetails(ctx context.Context) {
	// Sync up and get last confirmed timestamp.
	ackDetails, err := impl.ackManager.SyncAndGetAcknowledged(ctx)
	if err != nil {
		impl.logger.Warn("sync timestamp ack manager failed", zap.Error(err))
	}

	// Add ack details to ackDetails.
	impl.ackDetails.AddDetails(ackDetails)
}
