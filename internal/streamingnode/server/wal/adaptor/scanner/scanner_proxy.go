package scanner

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
)

var errScannerProxyClosed = errors.New("scanner proxy closed")

var _ walimpls.ScannerImpls = (*scannerProxy)(nil)

// newScannerProxy creates a new scanner proxy with the given wal name and read options.
func newScannerProxy(
	walName string,
	vchannel string,
	opt walimpls.ReadOption,
) *scannerProxy {
	var inclusive bool
	var messageID message.MessageID
	switch policy := opt.DeliverPolicy.Policy.(type) {
	case *streamingpb.DeliverPolicy_StartFrom:
		inclusive = true
		messageID = message.MustUnmarshalMessageID(walName, policy.StartFrom.GetId())
	case *streamingpb.DeliverPolicy_StartAfter:
		inclusive = false
		messageID = message.MustUnmarshalMessageID(walName, policy.StartAfter.GetId())
	default:
		panic("scanner proxy can not be created with this deliver policy")
	}
	return &scannerProxy{
		opt:              opt,
		vchannel:         vchannel,
		inclusive:        inclusive,
		currentMessageID: messageID,
		ch:               make(chan message.ImmutableMessage, opt.ReadAheadBufferSize),
		closed:           make(chan struct{}),
	}
}

// scannerProxy is a proxy type for message communication between underlying walimpls scanner and wal scanner
// scannerProxy implements the walimpls.ScannerImpls interface.
type scannerProxy struct {
	opt              walimpls.ReadOption
	vchannel         string
	inclusive        bool
	currentMessageID message.MessageID
	ch               chan message.ImmutableMessage
	onceClosed       sync.Once
	closed           chan struct{}
	err              error
}

// ExpectedMessageID returns the expected message ID of the scanner proxy.
func (s *scannerProxy) ExpectedMessageID() message.MessageID {
	return s.currentMessageID
}

// VChannel returns the vchannel of the scanner proxy.
func (s *scannerProxy) VChannel() string {
	return s.vchannel
}

// Push pushes a message to the scanner proxy channel.
func (s *scannerProxy) Push(ctx context.Context, msg message.ImmutableMessage) error {
	select {
	case <-s.closed:
		return errScannerProxyClosed
	default:
	}

	if !s.filterMsg(msg) {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.ch <- msg:
		s.currentMessageID = msg.MessageID()
		return nil
	case <-s.closed:
		return errScannerProxyClosed
	}
}

// fliterMsg filters the message before pushing it to the scanner proxy channel.
func (s *scannerProxy) filterMsg(msg message.ImmutableMessage) bool {
	if s.inclusive && s.currentMessageID.EQ(msg.MessageID()) {
		s.inclusive = false
		return true
	}
	if s.currentMessageID.LT(msg.MessageID()) {
		return true
	}
	return false
}

// Name returns the name of the scanner.
func (s *scannerProxy) Name() string {
	return s.opt.Name
}

// Chan returns the channel of message.
func (s *scannerProxy) Chan() <-chan message.ImmutableMessage {
	return s.ch
}

// Error returns the error of scanner failed.
func (s *scannerProxy) Error() error {
	<-s.closed
	return s.err
}

// closeByGateway closes the scanner proxy by the gateway.
func (s *scannerProxy) closeByGateway(err error) {
	s.onceClosed.Do(func() {
		s.err = err
		close(s.ch)
	})
}

// Close closes the scanner, release the underlying resources.
func (s *scannerProxy) Close() error {
	s.onceClosed.Do(func() {
		close(s.closed)
	})
	return s.Error()
}
