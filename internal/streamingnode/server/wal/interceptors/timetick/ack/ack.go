package ack

import (
	"context"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	_ typeutil.HeapInterface = (*ackersOrderByTimestamp)(nil)
	_ typeutil.HeapInterface = (*ackersOrderByEndTimestamp)(nil)
)

// AckerRef is a reference of the acker.
type AckerRef struct {
	*acker
}

// RefreshTimeTick refreshes the timetick of the current acker.
// The underlying acker will be acknowledged with refresh control mark and new acker will be allocated.
// If the error is not nil, the acker will be kept without acknowledged.
// !!!WARNING: RefreshTimeTick can be only called before the related message is not committed, otherwise it's unsound.
func (ar *AckerRef) RefreshTimeTick(ctx context.Context) error {
	return ar.manager.refreshTimeTick(ctx, ar)
}

// swap swaps the underlying acker in the ref.
func (ar *AckerRef) swap(acker *acker) *acker {
	old := ar.acker
	ar.acker = acker
	return old
}

// acker records the timestamp and last confirmed message id that has not been acknowledged.
type acker struct {
	acknowledged bool        // is acknowledged.
	detail       *AckDetail  // info is available after acknowledged.
	manager      *AckManager // the manager of the acker.
}

// LastConfirmedMessageID returns the last confirmed message id.
func (ta *acker) LastConfirmedMessageID() message.MessageID {
	return ta.detail.LastConfirmedMessageID
}

// Timestamp returns the timestamp.
func (ta *acker) Timestamp() uint64 {
	return ta.detail.BeginTimestamp
}

// Ack marks the timestamp as acknowledged.
func (ta *acker) Ack(opts ...AckOption) {
	for _, opt := range opts {
		opt(ta.detail)
	}
	ta.manager.ack(ta)
}

// ackDetail returns the ack info, only can be called after acknowledged.
func (ta *acker) ackDetail() *AckDetail {
	if !ta.acknowledged {
		panic("unreachable: ackDetail can only be called after acknowledged")
	}
	return ta.detail
}

// ackersOrderByTimestamp is a heap underlying represent of timestampAck.
type ackersOrderByTimestamp struct {
	ackers
}

// Less returns true if the element at index i is less than the element at index j.
func (h ackersOrderByTimestamp) Less(i, j int) bool {
	return h.ackers[i].detail.BeginTimestamp < h.ackers[j].detail.BeginTimestamp
}

// ackersOrderByEndTimestamp is a heap underlying represent of timestampAck.
type ackersOrderByEndTimestamp struct {
	ackers
}

// Less returns true if the element at index i is less than the element at index j.
func (h ackersOrderByEndTimestamp) Less(i, j int) bool {
	return h.ackers[i].detail.EndTimestamp < h.ackers[j].detail.EndTimestamp
}

// ackers is a heap underlying represent of timestampAck.
type ackers []*acker

// Len returns the length of the heap.
func (h ackers) Len() int {
	return len(h)
}

// Swap swaps the elements at indexes i and j.
func (h ackers) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes the last one at len.
func (h *ackers) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*acker))
}

// Pop pop the last one at len.
func (h *ackers) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
func (h *ackers) Peek() interface{} {
	return (*h)[0]
}
