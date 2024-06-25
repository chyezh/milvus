package cache

import (
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
)

// newMessageWithClock creates a new message with clock.
func newMessageWithClock(m message.ImmutableMessage, beginClock, endClock int64) messageWithClock {
	return messageWithClock{
		ImmutableMessage: m,
		beginClock:       beginClock,
		endClock:         endClock,
	}
}

// messageWithClock is the message with clock.
type messageWithClock struct {
	message.ImmutableMessage
	beginClock int64
	endClock   int64 // endClock is always greater than beginClock.
}

// BeginClock returns the begin clock of the message.
func (mwc messageWithClock) BeginClock() int64 {
	return mwc.beginClock
}

// EndClock returns the end clock of the message.
func (mwc messageWithClock) EndClock() int64 {
	return mwc.endClock
}

// mesasgeWithClockHeap is a heap underlying represent of messageWithClock.
type mesasgeWithClockHeap []messageWithClock

// Len returns the length of the heap.
func (h mesasgeWithClockHeap) Len() int {
	return len(h)
}

// Less returns true if the element at index i is less than the element at index j.
func (h mesasgeWithClockHeap) Less(i, j int) bool {
	return h[i].MessageID().LT(h[j].MessageID())
}

// Swap swaps the elements at indexes i and j.
func (h mesasgeWithClockHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes the last one at len.
func (h *mesasgeWithClockHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(messageWithClock))
}

// Pop pop the last one at len.
func (h *mesasgeWithClockHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek returns the element at the top of the heap.
func (h *mesasgeWithClockHeap) Peek() interface{} {
	return (*h)[0]
}
