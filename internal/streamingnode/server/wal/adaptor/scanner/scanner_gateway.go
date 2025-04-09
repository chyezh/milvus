package scanner

import (
	"math"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

const (
	mergeDistanceThreshold = 100
	maximumDispatcherCount = 10
)

var (
	defaultMergeInterval = 10 * time.Second
	errGatewayShutdown   = errors.New("scanner gateway shutdown")
)

type scannerGateway struct {
	notifier      *syncutil.AsyncTaskNotifier[struct{}]
	wal           walimpls.ROWALImpls
	incomingProxy chan *scannerProxy
	dispatchers   []*scannerDispatcher
}

func (s *scannerGateway) OpenNewProxy(vchannel string, readOption walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	proxy := newScannerProxy(s.wal.WALName(), vchannel, readOption)

	select {
	case s.incomingProxy <- proxy:
		return proxy, nil
	case <-s.notifier.Context().Done():
		return nil, s.notifier.Context().Err()
	}
}

func (s *scannerGateway) background() {
	defer s.notifier.Finish(struct{}{})

	var mergeTimer <-chan time.Time
	for {
		if len(s.dispatchers) > 0 && mergeTimer != nil {
			mergeTimer = time.After(defaultMergeInterval)
		}
		select {
		case <-s.notifier.Context().Done():
			return
		case proxy := <-s.incomingProxy:
			s.whenNewIncomingProxy(proxy)
		case <-mergeTimer:
			s.mergeDispatchers()
		}
	}
}

// newIncomingProxy creates a new incoming proxy.
func (s *scannerGateway) whenNewIncomingProxy(proxy *scannerProxy) {
	dispatcher := s.findClosestGateway(proxy.ExpectedMessageID())
	dispatcher.RegisterNewProxy([]*scannerProxy{proxy})
	s.dispatchers = append(s.dispatchers, dispatcher)
}

// mergeDispatchers merges the dispatchers if the distance is less than the threshold.
func (s *scannerGateway) mergeDispatchers() {
	if len(s.dispatchers) <= 1 {
		return
	}
	messageIDs := make([]message.MessageID, 0, len(s.dispatchers))
	for _, dispatcher := range s.dispatchers {
		messageIDs = append(messageIDs, dispatcher.CurrentMessageID())
	}
	// find the two dispatchers with the minimum distance.
	// the targetDispatchers[0].CurrentMessageID is less than targetDispatchers[1].CurrentMessageID.
	var targetDispatchers [2]int
	minDistance := int64(math.MaxInt64)
	for i := 0; i < len(messageIDs); i++ {
		for j := i + 1; j < len(messageIDs); j++ {
			distance := messageIDs[i].DistanceHint(messageIDs[j])
			if distance < 0 {
				distance = -distance
			}
			if distance < minDistance {
				minDistance = distance
				if messageIDs[i].LT(messageIDs[j]) {
					targetDispatchers[0] = i
					targetDispatchers[1] = j
				} else {
					targetDispatchers[0] = j
					targetDispatchers[1] = i
				}
			}
		}
	}
	if minDistance < mergeDistanceThreshold {
		// Remove the dispatcher with the larger message ID, and merge all proxy into the smaller one.
		dispatcher1 := s.dispatchers[targetDispatchers[0]]
		dispatcher2 := s.dispatchers[targetDispatchers[1]]
		dispatcher1.RegisterNewProxy(dispatcher2.Close())
		s.dispatchers = append(s.dispatchers[:targetDispatchers[1]], s.dispatchers[targetDispatchers[1]+1:]...)
	}
}

// findClosestGateway finds the closest gateway for the given message ID.
func (s *scannerGateway) findClosestGateway(messageID message.MessageID) *scannerDispatcher {
	minDistance := int64(math.MaxInt64)
	var dispatcher *scannerDispatcher
	for _, dispatcher := range s.dispatchers {
		distance := dispatcher.CurrentMessageID().DistanceHint(messageID)
		if distance < 0 {
			distance = -distance
		}
		if distance < minDistance {
			minDistance = distance
			dispatcher = dispatcher
		}
	}
	if minDistance <= mergeDistanceThreshold || len(s.dispatchers) > maximumDispatcherCount {
		// if the distance is less than 100, we can merge two dispatcher.
		return dispatcher
	}
	return NewScannerDispatcher(s.wal)
}

// Close closes the scanner gateway and all dispatchers.
func (s *scannerGateway) Close() {
	s.notifier.Cancel()
	s.notifier.BlockUntilFinish()

	// close all dispatchers.
	for _, dispatcher := range s.dispatchers {
		scanners := dispatcher.Close()
		for _, scanner := range scanners {
			scanner.closeByGateway(errGatewayShutdown)
		}
	}
}
