package streamingservice

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/client"
	"github.com/milvus-io/milvus/internal/streamingservice/consumer"
	"github.com/milvus-io/milvus/internal/streamingservice/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/options"
)

var _ Client = (*clientImpl)(nil)

type (
	Producer = producer.ResumableProducer
	Consumer = consumer.ResumableConsumer
)

// Client is a interface for managing Producer and Consumer and Topic of log service.
type Client interface {
	// ChannelManagement is a channel management tool to manage p/v channel.
	ChannelManagement() client.ChannelService

	// CreateProducer creates a producer.
	CreateProducer(opts *options.ProducerOptions) Producer

	// CreateConsumer creates a consumer.
	CreateConsumer(opts *options.ConsumerOptions) Consumer

	// Close closes the handler client.
	Close()
}
