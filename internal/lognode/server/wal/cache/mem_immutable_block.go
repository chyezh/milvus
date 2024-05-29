package cache

import (
	"context"
	"io"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// ImmutableBlock is a block of messages.
var _ ImmutableBlock = &memImmutableBlockImpl{}

// memImmutableBlockImpl is a block that stores messages in memory.
type memImmutableBlockImpl struct {
	data []message.ImmutableMessage
	r    MessageIDRange
}

// ReadFrom return a scanner that starts from the given message id.
func (b *memImmutableBlockImpl) ReadFrom(started message.MessageID) BlockScanner {
	if b.Range().In(started) {
		return emptyScanner{}
	}
	k := b.findIndex(started)
	if k == -1 {
		return emptyScanner{}
	}
	return &memImmutableBlockScanner{
		data:   b.data[k:],
		offset: -1,
	}
}

// findIndex finds the index of the message that is greater than or equal to the given message id.
func (b *memImmutableBlockImpl) findIndex(started message.MessageID) int {
	// TODO: perform a binary search here.
	for i, msg := range b.data {
		if started.LTE(msg.MessageID()) {
			return i
		}
	}
	return -1
}

// Range returns the range of message id in the block.
func (b *memImmutableBlockImpl) Range() MessageIDRange {
	return b.r
}

// memImmutableBlockScanner is a scanner that scans messages in a block.
type memImmutableBlockScanner struct {
	data   []message.ImmutableMessage
	offset int
}

// Scan scans the next message.
func (s *memImmutableBlockScanner) Scan(_ context.Context) error {
	if s.offset+1 >= len(s.data) {
		return io.EOF
	}
	s.offset++
	return nil
}

// Message returns the current message.
func (s *memImmutableBlockScanner) Message() message.ImmutableMessage {
	return s.data[s.offset]
}
