package timetick

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/timetick/timestamp"
)

// newAllocatorWithRecorder creates a new allocator with recorder.
func newAllocatorWithRecorder(allocator timestamp.Allocator) *allocatorWithRecorder {
	return &allocatorWithRecorder{
		underlying: allocator,
		recorder:   newTimestampRecorder(),
	}
}

// allocatorWithRecorder allocates timestamp for a channel.
type allocatorWithRecorder struct {
	underlying timestamp.Allocator
	recorder   *timestampRecorder
}

// AllocateOne allocates a timestamp.
func (a *allocatorWithRecorder) AllocateOne(ctx context.Context) (uint64, error) {
	ts, err := a.underlying.AllocateOne(ctx)
	if err != nil {
		return 0, err
	}
	a.recorder.Allocated(ts)
	return ts, nil
}

// SyncAndGetLastAllConfirmed allocates a timestamp and syncs the recorder.
func (a *allocatorWithRecorder) SyncAndGetLastAllConfirmed(ctx context.Context) (*consumedTimestamp, error) {
	if err := a.sync(ctx); err != nil {
		return nil, err
	}
	consumedTimestamp := a.recorder.GetConsumedTimestamps()
	if consumedTimestamp == nil {
		panic("unreachable: consumedTimestamp is nil")
	}
	return consumedTimestamp, nil
}

// sync syncs the recorder with allocator.
func (a *allocatorWithRecorder) sync(ctx context.Context) error {
	// local timestamp may out of date, sync the underlying allocator before send tt.
	a.underlying.Sync()

	// AllocateOne may be uncalled in long term, and the recorder may be out of date.
	// Do a Allocate and Ack, can sync up the recorder with internal timetick.TimestampAllocator latest time.
	ts, err := a.AllocateOne(ctx)
	if err != nil {
		return err
	}
	a.AckAllocated(ts)
	return nil
}

// Ack acknowledges the timestamp has been used.
func (a *allocatorWithRecorder) AckAllocated(ts uint64) {
	a.recorder.AckAllocated(ts)
}

// AckConsumed records the timestamp has been consumed.
func (a *allocatorWithRecorder) AckConsumed(ts uint64) {
	a.recorder.AckConsumed(ts)
}
