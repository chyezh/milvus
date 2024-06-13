package rmq

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/client"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
)

var _ walimpls.ScannerImpls = (*scannerImpl)(nil)

// newScanner creates a new scanner.
func newScanner(
	scannerName string,
	exclude *rmqID,
	consumer client.Consumer,
) *scannerImpl {
	s := &scannerImpl{
		ScannerHelper: helper.NewScannerHelper(scannerName),
		exclude:       exclude,
		consumer:      consumer,
		msgChannel:    make(chan message.ImmutableMessage, 1024),
	}
	go s.executeConsume()
	return s
}

// scannerImpl is the implementation of ScannerImpls for rmq.
type scannerImpl struct {
	*helper.ScannerHelper
	exclude    *rmqID
	consumer   client.Consumer
	msgChannel chan message.ImmutableMessage
}

// Chan returns the channel of message.
func (s *scannerImpl) Chan() <-chan message.ImmutableMessage {
	return s.msgChannel
}

// Close the scanner, release the underlying resources.
// Return the error same with `Error`
func (s *scannerImpl) Close() error {
	err := s.ScannerHelper.Close()
	s.consumer.Close()
	return err
}

// executeConsume consumes the message from the consumer.
func (s *scannerImpl) executeConsume() {
	defer close(s.msgChannel)
	for {
		select {
		case <-s.Context().Done():
			s.Finish(nil)
			return
		case msg, ok := <-s.consumer.Chan():
			if !ok {
				s.Finish(status.NewInner("mq consumer unexpected channel closed"))
				return
			}
			msgID := rmqID(msg.ID().(*server.RmqID).MessageID)
			// record the last message id to avoid repeated consume message.
			// and exclude message id should be filterred.
			if s.exclude == nil || !s.exclude.EQ(msgID) {
				s.msgChannel <- message.NewBuilder().
					WithMessageID(msgID).
					WithPayload(msg.Payload()).
					WithProperties(msg.Properties()).
					BuildImmutable()
			}
		}
	}
}
