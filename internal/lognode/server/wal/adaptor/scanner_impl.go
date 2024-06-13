package adaptor

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/helper"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

var _ wal.Scanner = (*scannerAdaptorImpl)(nil)

// newScannerAdaptor creates a new scanner adaptor.
func newScannerAdaptor(
	name string,
	l walimpls.WALImpls,
	readOption wal.ReadOption,
	cleanup func(),
) wal.Scanner {
	s := &scannerAdaptorImpl{
		innerWAL:      l,
		readOption:    readOption,
		sendingCh:     make(chan message.ImmutableMessage, 1),
		reorderBuffer: utility.NewReOrderBuffer(),
		pendingQueue:  utility.NewImmutableMessageQueue(),
		cleanup:       cleanup,
		ScannerHelper: helper.NewScannerHelper(name),
	}
	go s.executeConsume()
	return s
}

// scannerAdaptorImpl is a wrapper of ScannerImpls to extend it into a Scanner interface.
type scannerAdaptorImpl struct {
	*helper.ScannerHelper
	innerWAL      walimpls.WALImpls
	readOption    wal.ReadOption
	sendingCh     chan message.ImmutableMessage
	reorderBuffer *utility.ReOrderByTimeTickBuffer // only support time tick reorder now.
	pendingQueue  *utility.ImmutableMessageQueue   //
	cleanup       func()
}

// Chan returns the channel of message.
func (s *scannerAdaptorImpl) Chan() <-chan message.ImmutableMessage {
	return s.sendingCh
}

// Close the scanner, release the underlying resources.
// Return the error same with `Error`
func (s *scannerAdaptorImpl) Close() error {
	err := s.ScannerHelper.Close()
	return err
}

func (s *scannerAdaptorImpl) executeConsume() {
	innerScanner, err := s.innerWAL.Read(s.Context(), walimpls.ReadOption{
		Name:          s.Name(),
		DeliverPolicy: s.readOption.DeliverPolicy,
	})
	if err != nil {
		s.Finish(err)
		return
	}
	defer func() {
		close(s.sendingCh)
		innerScanner.Close()
	}()
	for {
		// generate the event channel and do the event loop.
		// TODO: Consume from local cache.
		upstream, sending := s.getEventCh(innerScanner)
		select {
		case <-s.Context().Done():
			s.Finish(err)
			return
		case msg, ok := <-upstream:
			if !ok {
				s.Finish(innerScanner.Error())
				return
			}
			s.handleUpstream(msg)
		case sending <- s.pendingQueue.Next():
			s.pendingQueue.UnsafeAdvance()
		}
	}
}

func (s *scannerAdaptorImpl) getEventCh(scanner walimpls.ScannerImpls) (<-chan message.ImmutableMessage, chan<- message.ImmutableMessage) {
	if s.pendingQueue.Len() == 0 {
		// If pending queue is empty,
		// no more message can be sent,
		// we always need to recv message from upstream to avoid starve.
		return scanner.Chan(), nil
	}
	// TODO: configurable pending count.
	if s.pendingQueue.Len()+s.reorderBuffer.Len() > 1024 {
		return nil, s.sendingCh
	}
	return scanner.Chan(), s.sendingCh
}

func (s *scannerAdaptorImpl) handleUpstream(msg message.ImmutableMessage) {
	if msg.MessageType() == message.MessageTypeTimeTick {
		// If the time tick message incoming,
		// the reorder buffer can be consumed into a pending queue with latest timetick.
		s.pendingQueue.Add(s.reorderBuffer.PopUtilTimeTick(msg.TimeTick()))
		return
	}
	// Filtering the message if needed.
	if s.readOption.MessageFilter != nil && !s.readOption.MessageFilter(msg) {
		return
	}
	// otherwise add message into reorder buffer directly.
	s.reorderBuffer.Push(msg)
}
