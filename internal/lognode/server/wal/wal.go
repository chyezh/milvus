package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/interceptor"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/scanner"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/wal"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

var _ Allocator = (*allocatorImpl)(nil)

// Scanner is the interface for reading records from the wal.
type (
	Scanner    = scanner.Scanner
	ReadOption = wal.ReadOption
)

// AllocateOption is the option for allocating wal instance.
type AllocateOption struct {
	Channel  *logpb.PChannelInfo
	Builders []interceptor.Builder
}

// Allocator is the interface for allocating wal instances.
type Allocator interface {
	// Allocate opens a wal instance for the channel.
	Allocate(opt *AllocateOption) (WALExtend, error)

	// Close the wal allocator, release all related wal resources.
	Close()
}

// WALExtend is the interface for extending wal.
type WALExtend interface {
	wal.WAL

	// AppendAsync writes a record to the log asynchronously.
	AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(message.MessageID, error))
}
