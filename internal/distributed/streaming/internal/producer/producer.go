package producer

import (
	"context"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var _ ResumableProducer = (*resumableProducerImpl)(nil)

// ProducerOptions is the options for creating a producer.
type ProducerOptions struct {
	// VChannel is the vchannel of the producer.
	VChannel string
}

// ResumableProducer is a interface for producing message to log service.
// ResumableProducer select a right log node to produce automatically.
// ResumableProducer will do automatic resume from stream broken and log node re-balance.
// All error in these package should be marked by streamingservice/errs package.
type ResumableProducer interface {
	// Channel returns the vchannel of producer.
	VChannel() string

	// Produce produce a new message to log service.
	Produce(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Close close the producer client.
	Close()
}
