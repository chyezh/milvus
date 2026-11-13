package pulsar

import (
	"context"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

const (
	backlogClearHelperName = "backlog-clear"
)

// backlogClearHelper is a helper to clear the backlog of pulsar.
type backlogClearHelper struct {
	mlog.Binder

	notifier          *syncutil.AsyncTaskNotifier[struct{}]
	cond              *syncutil.ContextCond
	written           int64
	threshold         int64
	checkpoint        message.MessageID // protected by cond.L; never inferred from append traffic
	checkpointPending bool
	channelName       types.PChannelInfo
	c                 pulsar.Client
	reusedConsumer    pulsar.Consumer
	tenant            tenant
}

// newBacklogClearHelper creates a new backlog clear helper.
func newBacklogClearHelper(ctx context.Context, c pulsar.Client, channelName types.PChannelInfo, threshold int64, tenant tenant, checkpoint message.MessageID) (*backlogClearHelper, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if checkpoint != nil && checkpoint.WALName() != message.WALNamePulsar {
		return nil, merr.WrapErrServiceInternalMsg("Pulsar recovery checkpoint belongs to a different WAL")
	}
	h := &backlogClearHelper{
		notifier:          syncutil.NewAsyncTaskNotifier[struct{}](),
		cond:              syncutil.NewContextCond(&sync.Mutex{}),
		written:           threshold, // trigger the backlog clear immediately.
		threshold:         threshold,
		checkpoint:        checkpoint,
		checkpointPending: checkpoint != nil,
		channelName:       channelName,
		c:                 c,
		reusedConsumer:    nil,
		tenant:            tenant,
	}
	h.SetLogger(mlog.With(mlog.String("channel", channelName.String()), mlog.FieldComponent("backlog-clear")))
	// Establish a durable earliest cursor before any producer can append. With
	// no published checkpoint, retaining the backlog is the only safe choice.
	if _, err := h.getConsumer(); err != nil {
		h.notifier.Cancel()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		h.closeConsumer()
		h.notifier.Cancel()
		return nil, err
	}
	go h.background()
	return h, nil
}

// AdvanceCheckpoint is called only from the published checkpoint/truncation
// path. A traffic threshold schedules work; it never grants deletion authority.
func (h *backlogClearHelper) AdvanceCheckpoint(checkpoint message.MessageID) {
	h.cond.L.Lock()
	defer h.cond.L.Unlock()
	if h.checkpoint == nil || h.checkpoint.LT(checkpoint) {
		h.checkpoint = checkpoint
		h.checkpointPending = true
		h.cond.UnsafeBroadcast()
	}
}

// ObserveAppend observes the append traffic.
func (h *backlogClearHelper) ObserveAppend(size int) {
	h.cond.L.Lock()
	h.written += int64(size)
	if h.written >= h.threshold {
		h.cond.UnsafeBroadcast()
	}
	h.cond.L.Unlock()
}

// background is the background goroutine to clear the backlog.
func (h *backlogClearHelper) background() {
	defer func() {
		h.notifier.Finish(struct{}{})
		h.Logger().Info(context.TODO(), "backlog clear helper exit")
	}()

	for {
		h.cond.L.Lock()
		for (h.written < h.threshold && !h.checkpointPending) || h.checkpoint == nil {
			if err := h.cond.Wait(h.notifier.Context()); err != nil {
				return
			}
		}
		h.written = 0
		h.checkpointPending = false
		checkpoint := h.checkpoint.(pulsarID)
		h.cond.L.Unlock()

		if err := retry.Do(h.notifier.Context(), func() error {
			if h.notifier.Context().Err() != nil {
				return h.notifier.Context().Err()
			}
			if err := h.performBacklogClear(checkpoint); err != nil {
				h.Logger().Warn(context.TODO(), "failed to perform backlog clear", mlog.Err(err))
				return err
			}
			h.Logger().Debug(context.TODO(), "perform backlog clear done")
			return nil
		}, retry.AttemptAlways()); err != nil {
			return
		}
	}
}

// performBacklogClear performs the backlog clear.
func (h *backlogClearHelper) performBacklogClear(checkpoint pulsarID) error {
	consumer, err := h.getConsumer()
	if err != nil {
		return merr.Wrap(err, "create backlog subscription")
	}

	if err := consumer.Seek(checkpoint.PulsarID()); err != nil {
		// close the reused consumer if seek failed.
		h.closeConsumer()
		return merr.Wrap(err, "seek backlog cursor to published checkpoint")
	}
	return nil
}

// getConsumer creates a new consumer.
func (h *backlogClearHelper) getConsumer() (pulsar.Consumer, error) {
	if h.reusedConsumer != nil {
		return h.reusedConsumer, nil
	}
	topic := h.tenant.MustGetFullTopicName(h.channelName.Name)
	consumer, err := h.c.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            backlogClearHelperName,
		Type:                        pulsar.Shared, // use shared subscription to avoid the subscription is rejected because of consumer exists.
		MaxPendingChunkedMessage:    1,             // We cannot set it to 0, because the 0 means 100.
		ReceiverQueueSize:           1,             // We cannot set it to 0, because the 0 means 1000.
		StartMessageIDInclusive:     true,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		return nil, merr.Wrap(err, "create backlog subscription")
	}
	h.reusedConsumer = consumer
	h.Logger().Info(context.TODO(), "created a new consumer")
	return h.reusedConsumer, nil
}

// closeConsumer closes the reused consumer.
func (h *backlogClearHelper) closeConsumer() {
	if h.reusedConsumer != nil {
		h.reusedConsumer.Close()
		h.reusedConsumer = nil
		h.Logger().Info(context.TODO(), "closed the reused consumer")
	}
}

// Close closes the backlog clear helper.
func (h *backlogClearHelper) Close() {
	h.notifier.Cancel()
	h.notifier.BlockUntilFinish()
	h.closeConsumer()
}
