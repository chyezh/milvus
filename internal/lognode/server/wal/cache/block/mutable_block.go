package block

import (
	"context"
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

var _ MutableBlock = &mutableBlockImpl{}

// NewMutableBlock creates a new mutable block.
func NewMutableBlock() MutableBlock {
	return &mutableBlockImpl{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
		// TODO: preallocate.
		data:   make([]message.ImmutableMessage, 0),
		sealed: false,
		size:   0,
	}
}

// mutableBlockImpl is a mutable block that stores messages in memory.
type mutableBlockImpl struct {
	cond   *syncutil.ContextCond
	data   []message.ImmutableMessage
	sealed bool
	size   int
}

// Append appends a message into the block.
func (b *mutableBlockImpl) Append(msg []message.ImmutableMessage) {
	b.cond.LockAndBroadcast()
	b.data = append(b.data, msg...)
	for _, msg := range msg {
		b.size += msg.EstimateSize()
	}
	b.cond.L.Unlock()
}

// EstimateSize returns the size of the block.
func (b *mutableBlockImpl) EstimateSize() int {
	b.cond.L.Lock()
	bytes := b.size
	b.cond.L.Unlock()
	return bytes
}

// Seal seals the block and return a immutable block.
func (b *mutableBlockImpl) Seal() ImmutableBlock {
	b.cond.LockAndBroadcast()
	var immutableBlock *memImmutableBlockImpl
	if len(b.data) > 0 {
		immutableBlock = &memImmutableBlockImpl{
			data: b.data,
			r: MessageIDRange{
				Begin: b.data[0].MessageID(),
				Last:  b.data[len(b.data)-1].MessageID(),
			},
			size: b.size,
		}
	}
	b.sealed = true
	b.cond.L.Unlock()
	return immutableBlock
}

// ReadFrom return a scanner that starts from the given message id.
func (b *mutableBlockImpl) ReadFrom(started message.MessageID) BlockScanner {
	// TODO: unsafe promise here, should be fixup.
	k := b.findIndex(started)
	if k == -1 {
		return emptyScanner{}
	}
	return &mutableBlockScannerImpl{
		memMutableBlock: b,
		offset:          k - 1,
	}
}

// findIndex finds the index of the message that is greater than or equal to the given message id.
func (b *mutableBlockImpl) findIndex(started message.MessageID) int {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	// TODO: perform a binary search here.
	for i, msg := range b.data {
		if started.LTE(msg.MessageID()) {
			return i
		}
	}
	return -1
}

// blockUntilIndexReady blocks until the message at the given index is ready or sealed.
func (b *mutableBlockImpl) blockUntilIndexReady(ctx context.Context, idx int) error {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()
	for {
		if idx < len(b.data) {
			return nil
		}
		if b.sealed {
			return io.EOF
		}
		if err := b.cond.Wait(ctx); err != nil {
			return err
		}
	}
}

// mustGetMessageAtIndex returns the message at the given index.
func (b *mutableBlockImpl) mustGetMessageAtIndex(idx int) message.ImmutableMessage {
	b.cond.L.Lock()
	msg := b.data[idx]
	b.cond.L.Unlock()
	return msg
}

// mutableBlockScannerImpl is a scanner that scans messages in a mutable block.
type mutableBlockScannerImpl struct {
	memMutableBlock *mutableBlockImpl
	offset          int
}

// Scan scans the next message.
func (s *mutableBlockScannerImpl) Scan(ctx context.Context) error {
	if err := s.memMutableBlock.blockUntilIndexReady(ctx, s.offset+1); err != nil {
		return err
	}
	s.offset += 1
	return nil
}

// Message returns the current message.
func (s *mutableBlockScannerImpl) Message() message.ImmutableMessage {
	return s.memMutableBlock.mustGetMessageAtIndex(s.offset)
}
