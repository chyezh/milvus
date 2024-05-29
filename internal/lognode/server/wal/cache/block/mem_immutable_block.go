package block

import (
	"context"
	"io"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

var (
	_ ImmutableBlock = &memImmutableBlockImpl{}
	_ BlockScanner   = &memImmutableBlockScanner{}
)

// memImmutableBlockImpl is a block that stores messages in memory.
type memImmutableBlockImpl struct {
	data []message.ImmutableMessage
	size int
	r    MessageIDRange
}

// EstimateSize returns the size of the block.
func (b *memImmutableBlockImpl) EstimateSize() int {
	return b.size
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
		data:      b.data[k:],
		firstScan: true,
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
// Implement to fast drop element to gc.
type memImmutableBlockScanner struct {
	data      []message.ImmutableMessage
	firstScan bool
}

// Scan scans the next message.
func (s *memImmutableBlockScanner) Scan(_ context.Context) error {
	if len(s.data) == 0 {
		return io.EOF
	}
	if s.firstScan {
		s.firstScan = false
		return nil
	}
	s.data = s.data[1:]
	if len(s.data) == 0 {
		return io.EOF
	}
	return nil
}

// Message returns the current message.
func (s *memImmutableBlockScanner) Message() message.ImmutableMessage {
	return s.data[0]
}
