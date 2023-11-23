package writer

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"go.uber.org/zap"
)

var _ Writer = (*mqBasedWriter)(nil)

// NewMQBasedWriter creates a new writer based on message queue.
func NewMQBasedWriter(p mqwrapper.Producer, channel logpb.PChannelInfo) Writer {
	return &mqBasedWriter{
		p:       p,
		channel: channel,
		logger: log.With(zap.Any("channel", channel)).WithRateGroup(
			"lognode:wal:writer:"+channel.Name,
			2,
			4,
		),
	}
}

// Writer is the interface for writing records to the wal.
type Writer interface {
	// Channel returns the channel assignment info of the writer.
	Channel() logpb.PChannelInfo

	// Append appends a message to the writer.
	Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Close closes the writer.
	Close()
}
