package block

import (
	"context"
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

var (
	_ MutableBlock   = &mutableBlockImpl{}
	_ BlockScanner   = &mutableBlockScannerImpl{}
	_ ImmutableBlock = &memImmutableBlockImpl{}
	_ BlockScanner   = &memImmutableBlockScanner{}
)

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
func (b *mutableBlockImpl) Append(msgs []message.ImmutableMessage) {
	b.cond.LockAndBroadcast()
	b.data = append(b.data, msgs...)
	for _, msg := range msgs {
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
			size: b.size,
		}
	}
	b.sealed = true
	b.cond.L.Unlock()
	return immutableBlock
}

// ReadFrom return a scanner that starts from the given message id.
func (b *mutableBlockImpl) ReadFrom(started message.MessageID) BlockScanner {
	return &mutableBlockScannerImpl{
		memMutableBlock: b,
		started:         started,
		offset:          -1,
	}
}

// blockUntilFindIndex finds the index of the message that is greater than or equal to the given message id.
func (b *mutableBlockImpl) blockUntilFindIndex(ctx context.Context, started message.MessageID) (int, error) {
	if err := b.blockUntilMessageIDReady(ctx, started); err != nil {
		return 0, err
	}
	b.cond.L.Lock()
	data := b.data
	b.cond.L.Unlock()

	k := lowerboundOfMessageList(data, started)
	if k >= len(data) {
		return 0, io.EOF
	}
	return k, nil
}

func (b *mutableBlockImpl) blockUntilMessageIDReady(ctx context.Context, started message.MessageID) error {
	b.cond.L.Lock()

	for {
		if (len(b.data) != 0 && started.LTE(b.data[len(b.data)-1].MessageID())) || b.sealed {
			b.cond.L.Unlock()
			return nil
		}
		if err := b.cond.Wait(ctx); err != nil {
			return err
		}
	}
}

// blockUntilIndexReady blocks until the message at the given index is ready or sealed.
func (b *mutableBlockImpl) blockUntilIndexReady(ctx context.Context, idx int) error {
	b.cond.L.Lock()
	for {
		if idx < len(b.data) {
			b.cond.L.Unlock()
			return nil
		}
		if b.sealed {
			b.cond.L.Unlock()
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
	started         message.MessageID
	offset          int
}

// Scan scans the next message.
func (s *mutableBlockScannerImpl) Scan(ctx context.Context) error {
	if s.started != nil {
		var err error
		s.offset, err = s.memMutableBlock.blockUntilFindIndex(ctx, s.started)
		if err != nil {
			return err
		}
		s.started = nil
		return nil
	}

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

// memImmutableBlockImpl is a block that stores messages in memory.
type memImmutableBlockImpl struct {
	data []message.ImmutableMessage
	size int
}

// EstimateSize returns the size of the block.
func (b *memImmutableBlockImpl) EstimateSize() int {
	return b.size
}

// ReadFrom return a scanner that starts from the given message id.
func (b *memImmutableBlockImpl) ReadFrom(started message.MessageID) BlockScanner {
	if !b.Range().In(started) {
		return emptyScanner{}
	}
	// find the lowerbound of the message list by started message.
	k := lowerboundOfMessageList(b.data, started)
	if k >= len(b.data) {
		return emptyScanner{}
	}
	return &memImmutableBlockScanner{
		data:      b.data[k:],
		firstScan: true,
	}
}

// Range returns the range of message id in the block.
func (b *memImmutableBlockImpl) Range() MessageIDRange {
	return MessageIDRange{
		Begin: b.data[0].MessageID(),
		Last:  b.data[len(b.data)-1].MessageID(),
	}
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

// lowerboundOfMessageList returns the lowerbound of the message list.
func lowerboundOfMessageList(data []message.ImmutableMessage, target message.MessageID) int {
	// perform a lowerbound search here.
	left, right, mid := 0, len(data)-1, 0
	for left <= right {
		mid = (left + right) / 2
		if target.LTE(data[mid].MessageID()) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return left
}
