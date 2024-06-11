package cache

import "github.com/milvus-io/milvus/internal/lognode/server/wal/cache/lclock"

// pendingQueue is a queue of messages.
type pendingQueue struct {
	pendings []messageWithClock
}

// Add add a message as pending one
func (pq *pendingQueue) Add(msg messageWithClock) {
	pq.pendings = append(pq.pendings, msg)
}

// Pop return the message that is no more pending
func (pq *pendingQueue) Pop(confirmInfo lclock.ConfirmedInfo) []messageWithClock {
	confirmedResult := make([]messageWithClock, 0)
	pendings := make([]messageWithClock, 0, len(pq.pendings))
	var droped bool
	for _, msg := range pq.pendings {
		state := msg.CheckState(confirmInfo)
		switch state {
		case messageStateConfirmed:
			confirmedResult = append(confirmedResult, msg)
		case messageStatePending:
			pendings = append(pendings, msg)
		case messageStateDropped:
			continue
		}
	}
}
