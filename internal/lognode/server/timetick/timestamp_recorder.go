package timetick

import "sync"

// newTimestampRecorder creates a new timestamp recorder.
func newTimestampRecorder() *timestampRecorder {
	return &timestampRecorder{
		mu:                 sync.Mutex{},
		ackTimestamps:      make(map[uint64]struct{}),
		notAckTimestamps:   make(map[uint64]struct{}),
		consumedTimestamps: make([]consumedTimestamp, 0),
	}
}

// consumedTimestamp records the timestamps that have been consumed.
type consumedTimestamp struct {
	latestAllConfirmedTimestamp             uint64 // latest timestamp that all messages before it have been confirmed.
	countAfterPreviousAllConfirmedTimestamp int    // number of timestamps between (previousAllConfirmedTimestamp, latestAllConfirmedTimestamp].
}

// timestampRecorder records the timestamps for gather the allocated .
type timestampRecorder struct {
	mu                 sync.Mutex
	ackTimestamps      map[uint64]struct{} // records the timestamps that have been acknowledged but timestamps before it hasn't acknowledged.
	notAckTimestamps   map[uint64]struct{} // records the timestamps allocated but have not acknowledged.
	consumedTimestamps []consumedTimestamp // records the consumed timestamps.
}

// GetConsumedTimestamps merge the consumed timestamps and return.
func (r *timestampRecorder) GetConsumedTimestamps() *consumedTimestamp {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.consumedTimestamps) == 0 {
		return nil
	}
	consumedTimestamp := &consumedTimestamp{
		latestAllConfirmedTimestamp:             0,
		countAfterPreviousAllConfirmedTimestamp: 0,
	}
	for _, ts := range r.consumedTimestamps {
		consumedTimestamp.latestAllConfirmedTimestamp = ts.latestAllConfirmedTimestamp
		consumedTimestamp.countAfterPreviousAllConfirmedTimestamp += ts.countAfterPreviousAllConfirmedTimestamp
	}
	return consumedTimestamp
}

// allocated records the timestamp has been allocated.
func (r *timestampRecorder) Allocated(ts uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.notAckTimestamps[ts] = struct{}{}
}

// ack acknowledges the timestamp has been used.
func (r *timestampRecorder) AckAllocated(ts uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If ts is the minimum not ack timestamp, so all timestamps before it have been acknowledged.
	// count and clean up all ack timestamps before it in ackTimestamps, update consumedTimestamps.
	// Otherwise, just add it to ackTimestamps.
	if r.isMinimumNotAckTS(ts) {
		count := 1 // contain ts itself.
		for ts2 := range r.ackTimestamps {
			if ts2 < ts {
				count++
				delete(r.ackTimestamps, ts2)
			}
		}
		r.consumedTimestamps = append(r.consumedTimestamps, consumedTimestamp{
			latestAllConfirmedTimestamp:             ts,
			countAfterPreviousAllConfirmedTimestamp: count,
		})
	} else {
		r.ackTimestamps[ts] = struct{}{}
	}

	// remove it from notAckTimestamps.
	delete(r.notAckTimestamps, ts)
}

// AckConsumed records the timestamp has been consumed.
func (r *timestampRecorder) AckConsumed(ts uint64) {
	r.mu.Lock()
	r.mu.Unlock()

	// Filter all consumed timestamps LTE ts.
	idx := -1
	for i, info := range r.consumedTimestamps {
		idx = i
		if info.latestAllConfirmedTimestamp > ts {
			break
		}
	}
	r.consumedTimestamps = r.consumedTimestamps[idx+1:]
}

// isMinimumNotAckTS checks if ts is the minimum not ack timestamp.
func (r *timestampRecorder) isMinimumNotAckTS(ts uint64) bool {
	// Check if ts is the minimum not ack timestamp.
	// notAckTimestamps should be small enough,
	// so we don't need to use a tree-based data structure to find minimum in notAckTimestamps.
	// TODO: heap would be better.
	isMinimumNotAckTS := true
	for ts2 := range r.notAckTimestamps {
		if ts2 < ts {
			isMinimumNotAckTS = false
			break
		}
	}
	return isMinimumNotAckTS
}
