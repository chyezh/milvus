package scanner

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

// NewScannerDispatcher creates a new scanner dispatcher.
func NewScannerDispatcher(l walimpls.ROWALImpls) *scannerDispatcher {
	s := &scannerDispatcher{
		ScannerHelper:     helper.NewScannerHelper("scanner-dispatcher"),
		wal:               l,
		incomingProxy:     make(chan []*scannerProxy, 1),
		currentMessageID:  &atomic.Pointer[message.MessageID]{},
		underlyingScanner: nil,
		proxies:           make(map[string]map[*scannerProxy]struct{}),
	}
	go s.background()
	return s
}

// scannerDispatcher is a scanner that preemptively scans the WAL.
type scannerDispatcher struct {
	*helper.ScannerHelper
	log.Binder
	wal               walimpls.ROWALImpls
	incomingProxy     chan []*scannerProxy               // incoming scanner proxy channel.
	currentMessageID  *atomic.Pointer[message.MessageID] // the current message ID.
	underlyingScanner walimpls.ScannerImpls
	proxies           map[string]map[*scannerProxy]struct{} // map vchannel name to scanner list.
}

// RegisterNewProxy registers a new scanner proxy to the scanner gateway.
func (s *scannerDispatcher) RegisterNewProxy(proxies []*scannerProxy) error {
	select {
	case <-s.Context().Done():
		return s.Context().Err()
	case s.incomingProxy <- proxies:
		return nil
	}
}

// CurrentMessageID returns the current message ID of the scanner gateway.
func (s *scannerDispatcher) CurrentMessageID() message.MessageID {
	msgID := s.currentMessageID.Load()
	if msgID == nil {
		return nil
	}
	return *msgID
}

// saveMessageID saves the message ID to the scanner gateway.
func (s *scannerDispatcher) saveMessageID(msgID message.MessageID) {
	s.currentMessageID.Store(&msgID)
}

// background starts the background goroutine to handle incoming messages and scanner proxies.
func (s *scannerDispatcher) background() {
	defer s.Finish(nil)

	if err := s.waitFirstIncomingProxy(); err != nil {
		return
	}
	for {
		if err := s.createScannerWithBackoff(s.Context()); err != nil {
			return
		}
		if err := s.consumeWithScanner(); err != nil {
			return
		}
	}
}

// waitFirstIncomingProxy waits for the first incoming scanner proxy.
func (s *scannerDispatcher) waitFirstIncomingProxy() error {
	// wait for the first incoming proxy.
	select {
	case <-s.Context().Done():
		return s.Context().Err()
	case newProxies := <-s.incomingProxy:
		for _, newProxy := range newProxies {
			_ = s.whenNewIncomingProxy(newProxy)
		}
		return nil
	}
}

// consumeWithScanner consumes the messages from the underlying scanner.
func (s *scannerDispatcher) consumeWithScanner() error {
	defer s.underlyingScanner.Close()
	for {
		select {
		case <-s.Context().Done():
			return s.Context().Err()
		case msg, ok := <-s.underlyingScanner.Chan():
			if !ok {
				return s.underlyingScanner.Error()
			}
			if err := s.dispatch(msg); err != nil {
				return err
			}
			s.saveMessageID(msg.MessageID())
		case newProxies := <-s.incomingProxy:
			for _, newProxy := range newProxies {
				_ = s.whenNewIncomingProxy(newProxy)
			}
		}
	}
}

// whenNewIncomingProxy handles the new incoming scanner proxy.
// return true if the new incoming scanner proxy reset the consuming position.
func (s *scannerDispatcher) whenNewIncomingProxy(newProxy *scannerProxy) bool {
	reset := false
	currentMessageID := s.CurrentMessageID()
	if currentMessageID == nil || newProxy.ExpectedMessageID().LTE(currentMessageID) {
		// the new incoming scanner proxy want the message which is less than current consuming position.
		// so we need to re-read the message from the expected position of new proxy.
		s.resetConsumingPosition(newProxy.ExpectedMessageID())
		reset = true
	}
	// register the new proxy into the scanner gateway.
	if _, ok := s.proxies[newProxy.VChannel()]; !ok {
		s.proxies[newProxy.VChannel()] = make(map[*scannerProxy]struct{})
	}
	s.proxies[newProxy.VChannel()][newProxy] = struct{}{}
	return reset
}

// resetConsumingPosition resets the consuming position of the scanner.
func (s *scannerDispatcher) resetConsumingPosition(newMessageID message.MessageID) {
	if err := s.underlyingScanner.Close(); err != nil {
		// the close failure is not fatal, just log it.
		s.Logger().Warn("close underlying scanner failed", zap.Error(err))
	}
	s.saveMessageID(newMessageID)
}

// createScannerWithBackoff creates a new reader with backoff.
// It will keep trying to create a new reader until it succeeds or the context is done.
// only return error if the context is done.
func (s *scannerDispatcher) createScannerWithBackoff(ctx context.Context) error {
	currentMessageID := s.CurrentMessageID()
	if currentMessageID == nil {
		panic("current message ID is nil")
	}
	deliverPolicy := options.DeliverPolicyStartFrom(currentMessageID)
	backoffTimer := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: 5 * time.Second,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     5 * time.Second,
		},
	})
	backoffTimer.EnableBackoff()
	for {
		innerScanner, err := s.wal.Read(ctx, walimpls.ReadOption{
			Name:          s.Name(),
			DeliverPolicy: deliverPolicy,
		})
		if err == nil {
			s.underlyingScanner = innerScanner
			return nil
		}
		if ctx.Err() != nil {
			// The scanner is closing, so stop the backoff.
			return ctx.Err()
		}
		waker, nextInterval := backoffTimer.NextTimer()
		s.Logger().Warn("create underlying scanner for wal scanner, start a backoff",
			zap.Duration("nextInterval", nextInterval),
			zap.Error(err),
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waker:
		}
	}
}

// dispatch dispatches the message to the scanner proxy.
func (s *scannerDispatcher) dispatch(msg message.ImmutableMessage) error {
	if msg.VChannel() == "" {
		s.broadcast(msg)
	}
	for scannerProxy := range s.proxies[msg.VChannel()] {
		if err := scannerProxy.Push(s.Context(), msg); err != nil {
			if errors.Is(err, errScannerProxyClosed) {
				delete(s.proxies[msg.VChannel()], scannerProxy)
				continue
			}
			return err
		}
	}
	return nil
}

// broadcast sends the message to all scanner proxies.
func (s *scannerDispatcher) broadcast(msg message.ImmutableMessage) error {
	for vchannel, proxies := range s.proxies {
		for scannerProxy := range proxies {
			if err := scannerProxy.Push(s.Context(), msg); err != nil {
				if errors.Is(err, errScannerProxyClosed) {
					delete(s.proxies[vchannel], scannerProxy)
					continue
				}
				return err
			}
		}
	}
	return nil
}

// Close closes the dispatcher and return the working scanner proxies.
func (s *scannerDispatcher) Close() []*scannerProxy {
	s.ScannerHelper.Close()
	allProxies := make([]*scannerProxy, 0, len(s.proxies))
	for _, proxies := range s.proxies {
		for scannerProxy := range proxies {
			allProxies = append(allProxies, scannerProxy)
		}
	}
	return allProxies
}
