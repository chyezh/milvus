package writer

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"go.uber.org/zap"
)

// mqBasedWriter is a writer based on message queue.
type mqBasedWriter struct {
	p       mqwrapper.Producer
	channel logpb.PChannelInfo
	logger  *log.MLogger
}

// Channel returns the channel assignment info of the writer.
func (w *mqBasedWriter) Channel() logpb.PChannelInfo {
	return w.channel
}

// Append appends a message to the writer.
func (w *mqBasedWriter) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	msgID, err := w.p.Send(ctx, message.NewMQProducerMessageFromMutableMessage(msg))
	if err != nil {
		w.logger.RatedWarn(1, "send message to mq failed", zap.Error(err))
		return nil, err
	}
	return msgID, nil
}

// Close closes the writer.
func (w *mqBasedWriter) Close() {
	w.p.Close()
	w.logger.Info("writer closed")
}
