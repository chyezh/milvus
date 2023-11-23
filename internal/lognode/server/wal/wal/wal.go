package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/scanner"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

var (
	_ Opener = (*mqBasedOpener)(nil)
	_ WAL    = (*walImpl)(nil)
)

// WAL is the interface for writing and reading records to the wal.
type WAL interface {
	// Channel returns the channel assignment info of the wal.
	Channel() logpb.PChannelInfo

	// Append writes a record to the log.
	Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

	// Read returns a scanner for reading records from the wal.
	Read(ctx context.Context, deliverPolicy ReadOption) (scanner.Scanner, error)

	// GetLatestMessageID returns the latest message id of the channel.
	GetLatestMessageID(ctx context.Context) (message.MessageID, error)

	// Close closes the wal instance.
	Close()
}

// Opener is the interface for opening wal instance.
type Opener interface {
	// Open opens a wal instance for the channel.
	Open(channel logpb.PChannelInfo) (WAL, error)

	// Close closes the opener resources.
	Close()
}
