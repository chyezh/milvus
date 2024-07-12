package streaming

import (
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/consumer"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/streamingcoord/client"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
)

type clientImpl struct {
	// All services
	streamingCoordAssignmentClient client.Client
	handlerClient                  handler.HandlerClient
}

// CreateProducer creates a producer.
func (c *clientImpl) CreateProducer(opts *ProducerOptions) Producer {
	return producer.NewResumableProducer(c.handlerClient.CreateProducer, opts)
}

// CreateConsumer creates a consumer.
func (c *clientImpl) CreateConsumer(opts *ConsumerOptions) Consumer {
	return consumer.NewResumableConsumer(c.handlerClient.CreateConsumer, opts)
}

// Close closes the handler client.
func (c *clientImpl) Close() {
	c.handlerClient.Close()
	c.streamingCoordAssignmentClient.Close()
}
