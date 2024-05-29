package cache

import (
	"context"
	"io"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// MutableBlock is a block of messages that can be modified.
// A block of message should always be continuous and ordered by asc.
type MutableBlock interface {
	// ReadFrom return a scanner that starts from the given message id.
	// Scanner of a mutable block will block until new message append into the block.
	// Or stopped scanner when a mutable block is sealed.
	ReadFrom(started message.MessageID) BlockScanner

	// Append appends a message to the block.
	// !!! user should ensure the message is continuous with existed message
	// and ordered by the asc.
	Append(msg []message.ImmutableMessage)

	// Seal return a immutable block that can't be modified.
	// If the block is empty, return nil.
	Seal() ImmutableBlock
}

// ImmutableBlock is a immutable block of messages.
// A block of message should always be continuous with increasing order.
// A immutable block should never be empty.
type ImmutableBlock interface {
	// ReadFrom return a scanner that starts from the given message id.
	ReadFrom(started message.MessageID) BlockScanner

	// Range returns the range of message id in the block.
	Range() MessageIDRange
}

// BlockScanner is a scanner that scans messages in a block.
type BlockScanner interface {
	// Scan scans the next message.
	// Return io.EOF if there is no more message.
	// Return nil continue to scan.
	// Return other error if any error occurs.
	Scan(ctx context.Context) error

	// Message returns the current message.
	Message() message.ImmutableMessage
}

// emptyScanner is an Scanner that iterates nothing.
type emptyScanner struct{}

func (e emptyScanner) Scan(_ context.Context) error {
	return io.EOF
}

func (e emptyScanner) Message() message.ImmutableMessage {
	return nil
}
