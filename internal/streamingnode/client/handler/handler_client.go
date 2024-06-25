package handler

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/options"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazyconn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ HandlerClient = (*handlerClientImpl)(nil)

type (
	Producer = producer.Producer
	Consumer = consumer.Consumer
)

// HandlerClient is the interface that wraps streamingpb.StreamingNodeHandlerServiceClient.
type HandlerClient interface {
	// CreateProducer creates a producer.
	// Producer is a stream client, it will be available until context canceled or active close.
	CreateProducer(ctx context.Context, opts *options.ProducerOptions) (Producer, error)

	// CreateConsumer creates a consumer.
	// Consumer is a stream client, it will be available until context canceled or active close.
	CreateConsumer(ctx context.Context, opts *options.ConsumerOptions) (Consumer, error)

	// Close closes the handler client.
	Close()
}

type handlerClientImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]
	conn     *lazyconn.LazyGRPCConn // TODO: use conn pool if there's huge amount of stream.
	// But there's low possibility that there's 100 stream on one streamingnode.
	rb                    resolver.Builder
	watcher               assignment.Watcher
	sharedProducers       map[string]*typeutil.WeakReference[Producer] // map the pchannel to shared producer.
	sharedProducerKeyLock *lock.KeyLock[string]
}

// getHandlerService returns a handler service client.
func (c *handlerClientImpl) getHandlerService(ctx context.Context) (streamingpb.StreamingNodeHandlerServiceClient, error) {
	conn, err := c.conn.Get(ctx)
	if err != nil {
		return nil, err
	}
	return streamingpb.NewStreamingNodeHandlerServiceClient(conn), nil
}

// CreateProducer creates a producer.
func (hc *handlerClientImpl) CreateProducer(ctx context.Context, opts *options.ProducerOptions) (Producer, error) {
	pChannel := funcutil.ToPhysicalChannel(opts.VChannel)
	p, err := hc.createHandlerUntilStreamingNodeReady(ctx, pChannel, func(ctx context.Context, assign *assignment.Assignment) (any, error) {
		return hc.createProducer(ctx, &producer.ProducerOptions{
			PChannel: pChannel,
		}, assign)
	})
	if err != nil {
		return nil, err
	}
	return p.(Producer), nil
}

// CreateConsumer creates a consumer.
func (hc *handlerClientImpl) CreateConsumer(ctx context.Context, opts *options.ConsumerOptions) (Consumer, error) {
	pChannel := funcutil.ToPhysicalChannel(opts.VChannel)
	c, err := hc.createHandlerUntilStreamingNodeReady(ctx, pChannel, func(ctx context.Context, assign *assignment.Assignment) (any, error) {
		return hc.createConsumer(ctx, &consumer.ConsumerOptions{
			PChannel:      pChannel,
			DeliverPolicy: opts.DeliverPolicy,
			DeliverFilters: []options.DeliverFilter{
				options.DeliverFilterVChannel(opts.VChannel),
			},
			MessageHandler: opts.MessageHandler,
		}, assign)
	})
	if err != nil {
		return nil, err
	}
	return c.(Consumer), nil
}

// createHandlerUntilStreamingNodeReady creates a handler until log node ready.
// If log node is not ready, it will block until new assignment term is coming or context timeout.
func (hc *handlerClientImpl) createHandlerUntilStreamingNodeReady(ctx context.Context, pchannel string, create func(ctx context.Context, assign *assignment.Assignment) (any, error)) (any, error) {
	for {
		assign := hc.watcher.Get(ctx, pchannel)
		if assign != nil {
			// Find assignment, try to create producer on this assignment.
			c, err := create(ctx, assign)
			if err == nil {
				return c, nil
			}

			// Check if wrong log node.
			logServiceErr := status.AsStreamingError(err)
			if !logServiceErr.IsWrongStreamingNode() {
				// stop retry if not wrong log node error.
				return nil, logServiceErr
			}
		}

		// Block until new assignment term is coming if wrong log node or no assignment.
		if err := hc.watcher.Watch(ctx, pchannel, assign); err != nil {
			// Context timeout
			log.Warn("wait for watch channel assignment timeout", zap.String("channel", pchannel), zap.Any("oldAssign", assign))
			return nil, err
		}
	}
}

// createProducer creates a producer.
func (hc *handlerClientImpl) createProducer(ctx context.Context, opts *producer.ProducerOptions, assign *assignment.Assignment) (Producer, error) {
	if hc.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("handler client is closed")
	}
	defer hc.lifetime.Done()

	// Wait for handler service is ready.
	handlerService, err := hc.getHandlerService(ctx)
	if err != nil {
		return nil, err
	}

	// producer on same pchannel can be shared on multiple vchannel.
	return hc.createOrGetSharedProducer(ctx, opts, handlerService, assign)
}

// getFromSharedProducers gets a shared producer from shared producers.
func (hc *handlerClientImpl) getFromSharedProducers(pchannel string) (Producer, bool) {
	weakProducerRef, ok := hc.sharedProducers[pchannel]
	if !ok {
		return nil, false
	}
	if strongProducerRef := weakProducerRef.Upgrade(); strongProducerRef != nil {
		return newSharedProducer(strongProducerRef), true
	}
	// upgrade failure means the outer producer is all closed.
	// remove the weak ref and create again.
	delete(hc.sharedProducers, pchannel)
	return nil, false
}

// createOrGetSharedProducer creates or get a shared producer.
// because vchannel in same pchannel can share the same producer.
func (hc *handlerClientImpl) createOrGetSharedProducer(
	ctx context.Context,
	opts *producer.ProducerOptions,
	handlerService streamingpb.StreamingNodeHandlerServiceClient,
	assign *assignment.Assignment,
) (Producer, error) {
	hc.sharedProducerKeyLock.Lock(opts.PChannel)
	defer hc.sharedProducerKeyLock.Unlock(opts.PChannel)

	// check if shared producer is created within key lock.
	p, ok := hc.getFromSharedProducers(opts.PChannel)
	if ok {
		return p, nil
	}

	// create a new producer and insert it into shared producers.
	newProducer, err := producer.CreateProducer(ctx, opts, handlerService, assign)
	if err != nil {
		return nil, err
	}
	newStrongProducerRef := typeutil.NewSharedReference[Producer](newProducer)
	// store a weak ref and return a strong ref.
	returned := newStrongProducerRef.Clone()
	stored := newStrongProducerRef.Downgrade()
	hc.sharedProducers[opts.PChannel] = stored
	return newSharedProducer(returned), nil
}

func (hc *handlerClientImpl) createConsumer(ctx context.Context, opts *consumer.ConsumerOptions, assign *assignment.Assignment) (Consumer, error) {
	if hc.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("handler client is closed")
	}
	defer hc.lifetime.Done()

	// Wait for handler service is ready.
	handlerService, err := hc.getHandlerService(ctx)
	if err != nil {
		return nil, err
	}

	return consumer.CreateConsumer(ctx, opts, handlerService, assign)
}

// Close closes the handler client.
func (hc *handlerClientImpl) Close() {
	hc.lifetime.SetState(lifetime.Stopped)
	hc.lifetime.Wait()
	hc.lifetime.Close()
	hc.watcher.Close()
	hc.conn.Close()
	hc.rb.Close()
}
