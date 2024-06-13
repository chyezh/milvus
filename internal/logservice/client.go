package logservice

import (
	"github.com/milvus-io/milvus/internal/logcoord/client"
	"github.com/milvus-io/milvus/internal/logservice/consumer"
	"github.com/milvus-io/milvus/internal/logservice/producer"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
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
