package handler

import (
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
)

// ProducerOptions is the options for creating a producer.
type ProducerOptions struct {
	VChannel string // the target vchannel name
}

// ConsumerOptions is the options for creating a consumer.
type ConsumerOptions struct {
	// The consume target vchannel
	VChannel string

	// DeliverFilters is the filters of the consumer.
	DeliverFilters []options.DeliverFilter

	// DeliverPolicy is the deliver policy of the consumer.
	DeliverPolicy options.DeliverPolicy

	// Handler is the message handler used to handle message after recv from consumer.
	MessageHandler message.Handler
}
