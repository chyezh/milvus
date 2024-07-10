package consumer

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// NewResumableConsumer creates a new resumable consumer.
func NewResumableConsumer(factory factory, opts *options.ConsumerOptions) ResumableConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &resumableConsumerImpl{
		ctx:            ctx,
		cancel:         cancel,
		stopResumingCh: make(chan struct{}),
		resumingExitCh: make(chan struct{}),
		logger:         log.With(zap.String("channel", opts.VChannel), zap.Any("initialDeliverPolicy", opts.DeliverPolicy)),
		opts:           opts,
		mh: &timeTickOrderMessageHandler{
			inner:                  opts.MessageHandler,
			lastConfirmedMessageID: nil,
			lastTimeTick:           0,
		},
		factory: factory,
	}
	go consumer.resumeLoop()
	return consumer
}

// resumableConsumerImpl is a consumer implementation.
type resumableConsumerImpl struct {
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *log.MLogger
	stopResumingCh chan struct{}
	resumingExitCh chan struct{}

	opts    *options.ConsumerOptions
	mh      *timeTickOrderMessageHandler
	factory factory
}

type factory = func(ctx context.Context, opts *options.ConsumerOptions) (consumer.Consumer, error)

// Channel returns the channel name.
func (rc *resumableConsumerImpl) Channel() string {
	return rc.opts.VChannel
}

// resumeLoop starts a background task to consume messages from log service to message handler.
func (rc *resumableConsumerImpl) resumeLoop() {
	defer func() {
		// close the message handler.
		rc.mh.Close()
		rc.logger.Info("resumable consumer is closed")
		close(rc.resumingExitCh)
	}()

	// Use the initialized deliver policy at first running.
	deliverPolicy := rc.opts.DeliverPolicy
	deliverFilters := rc.opts.DeliverFilters
	// consumer need to resume when error occur, so message handler shouldn't close if the internal consumer encounter failure.
	nopCloseMH := message.NopCloseHandler{
		Handler: rc.mh,
	}

	for {
		// Get last checkpoint sent.
		// Consume ordering is always time tick order now.
		if rc.mh.lastConfirmedMessageID != nil {
			// set the deliver policy start after the last message id.
			deliverPolicy = options.DeliverPolicyStartAfter(rc.mh.lastConfirmedMessageID)
			newDeliverFilters := make([]options.DeliverFilter, 0, len(deliverFilters)+1)
			for _, filter := range deliverFilters {
				if !options.IsDeliverFilterTimeTick(filter) {
					newDeliverFilters = append(newDeliverFilters, filter)
				}
			}
			newDeliverFilters = append(newDeliverFilters, options.DeliverFilterTimeTickGT(rc.mh.lastTimeTick))
			deliverFilters = newDeliverFilters
		}
		opts := &options.ConsumerOptions{
			VChannel:       rc.opts.VChannel,
			DeliverPolicy:  deliverPolicy,
			DeliverFilters: deliverFilters,
			MessageHandler: nopCloseMH,
		}

		// Create a new consumer.
		// Return if context canceled.
		consumer, err := rc.createNewConsumer(opts)
		if err != nil {
			return
		}

		// Wait until the consumer is unavailable or context canceled.
		if err := rc.waitUntilUnavailable(consumer); err != nil {
			return
		}
	}
}

func (rc *resumableConsumerImpl) createNewConsumer(opts *options.ConsumerOptions) (consumer.Consumer, error) {
	// Mark as unavailable.
	metrics.StreamingServiceClientConsumerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.StreamingServiceClientProducerUnAvailable).Inc()
	defer metrics.StreamingServiceClientConsumerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.StreamingServiceClientProducerUnAvailable).Dec()

	logger := rc.logger.With(zap.Any("deliverPolicy", opts.DeliverPolicy))

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 100 * time.Millisecond
	backoff.MaxInterval = 10 * time.Second
	for {
		// Create a new consumer.
		// a underlying stream consumer life time should be equal to the resumable producer.
		// so ctx of resumable consumer is passed to underlying stream producer creation.
		consumer, err := rc.factory(rc.ctx, opts)
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		if err != nil {
			nextBackoff := backoff.NextBackOff()
			logger.Warn("create consumer failed, retry...", zap.Error(err), zap.Duration("nextRetryInterval", nextBackoff))
			time.Sleep(nextBackoff)
			continue
		}

		logger.Info("resume on new consumer at new start message id")
		return consumer, nil
	}
}

// waitUntilUnavailable is used to wait until the consumer is unavailable or context canceled.
func (rc *resumableConsumerImpl) waitUntilUnavailable(consumer handler.Consumer) error {
	// Mark as available.
	metrics.StreamingServiceClientConsumerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.StreamingServiceClientProducerAvailable).Inc()
	defer func() {
		consumer.Close()
		if consumer.Error() != nil {
			rc.logger.Warn("consumer is closed with error", zap.Error(consumer.Error()))
		}
		metrics.StreamingServiceClientConsumerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.StreamingServiceClientProducerAvailable).Dec()
	}()

	select {
	case <-rc.stopResumingCh:
		return errors.New("stop resuming")
	case <-rc.ctx.Done():
		return rc.ctx.Err()
	case <-consumer.Done():
		rc.logger.Warn("consumer is done or encounter error, try to resume...",
			zap.Error(consumer.Error()),
			zap.Any("lastConfirmedMessageID", rc.mh.lastConfirmedMessageID),
			zap.Uint64("lastTimeTick", rc.mh.lastTimeTick),
		)
		return nil
	}
}

// gracefulClose graceful close the consumer.
func (rc *resumableConsumerImpl) gracefulClose() error {
	close(rc.stopResumingCh)
	select {
	case <-rc.resumingExitCh:
		return nil
	case <-time.After(50 * time.Millisecond):
		return context.DeadlineExceeded
	}
}

// Close the scanner, release the underlying resources.
func (rc *resumableConsumerImpl) Close() {
	if err := rc.gracefulClose(); err != nil {
		rc.logger.Warn("graceful close a consumer fail, force close is applied")
	}

	// cancel is always need to be called, even graceful close is success.
	// force close is applied by cancel context if graceful close is failed.
	rc.cancel()
	<-rc.resumingExitCh
}

// Done returns a channel which will be closed when scanner is finished or closed.
func (rc *resumableConsumerImpl) Done() <-chan struct{} {
	return rc.resumingExitCh
}
