package cache

import "github.com/milvus-io/milvus/internal/util/logserviceutil/message"

// newPendingQueue creates a new pending queue.
func newPendingQueue() *pendingQueue {
	return &pendingQueue{
		h: make(mesasgeWithClockHeap, 0),
	}
}

// pendingQueue is a queue of messages.
type pendingQueue struct {
	h mesasgeWithClockHeap
}

// Add add a message as pending one
func (pq *pendingQueue) Add(msg messageWithClock) {
	pq.h.Push(msg)
}

// PopUntilEndClckReachLAC pops all messages's end clock reach the last confirmed message
func (pq *pendingQueue) PopUntilEndClckReachLAC(lac int64) []message.ImmutableMessage {
	result := make([]message.ImmutableMessage, 0)
	for pq.h.Len() > 0 {
		msg := pq.h.Peek().(messageWithClock)
		if msg.EndClock() < lac {
			result = append(result, pq.h.Pop().(messageWithClock).ImmutableMessage)
		} else {
			break
		}
	}
	return result
}

// PopUtilBeginClockReachLF pops all messages's begin clock reach the last fenced message
func (pq *pendingQueue) PopUntilBeginClockReachLF(lf int64) {
	for pq.h.Len() > 0 {
		msg := pq.h.Peek().(messageWithClock)
		if msg.BeginClock() < lf {
			pq.h.Pop()
		} else {
			break
		}
	}
}
