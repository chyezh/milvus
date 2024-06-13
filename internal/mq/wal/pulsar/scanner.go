package pulsar

import (
	"context"

	gopulsar "github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

var _ walimpls.ScannerImpls = (*scannerImpl)(nil)

func newScanner(
	scannerName string,
	reader gopulsar.Reader,
) *scannerImpl {
	s := &scannerImpl{
		ScannerHelper: helper.NewScannerHelper(scannerName),
		reader:        reader,
		// TODO, configurable
		msgChannel: make(chan message.ImmutableMessage, 1024),
	}
	go s.executeConsume()
	return s
}

type scannerImpl struct {
	*helper.ScannerHelper
	reader     gopulsar.Reader
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
	s.reader.Close()
	return err
}

func (s *scannerImpl) executeConsume() {
	defer close(s.msgChannel)
	for {
		msg, err := s.reader.Next(s.Context())
		if err != nil {
			if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
				s.Finish(nil)
				return
			}
			s.Finish(err)
			return
		}
		newImmutableMessage := message.NewBuilder().
			WithMessageID(pulsarID{msg.ID()}).
			WithPayload(msg.Payload()).
			WithProperties(msg.Properties()).
			BuildImmutable()

		select {
		case <-s.Context().Done():
			s.Finish(nil)
			return
		case s.msgChannel <- newImmutableMessage:
		}
	}
}
