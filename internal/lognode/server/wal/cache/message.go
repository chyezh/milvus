package cache

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal/cache/lclock"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

const (
	messageStatePending messageState = iota + 1
	messageStateDropped
	messageStateConfirmed
)

// messageState is the state of the message.
type messageState int

// messageWithClock is the message with clock.
type messageWithClock struct {
	message.MessageID
	beginClock int64
	endClock   int64 // endClock is always greater than beginClock.
}

func (mwc *messageWithClock) CheckState(info lclock.ConfirmedInfo) messageState {
	// If the begin clock is less than or equal to LEC, the message cannot keep consistent order with underlying storage.
	// It should be dropped.
	if mwc.beginClock <= info.LEC {
		return messageStateDropped
	}
	if mwc.endClock >= info.LAC {
		return messageStatePending
	}
	return messageStateConfirmed
}
