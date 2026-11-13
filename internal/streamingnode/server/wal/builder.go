package wal

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// OpenerBuilder is the interface for build wal opener.
type OpenerBuilder interface {
	// Name of the wal builder, should be a lowercase string.
	Name() message.WALName

	Build() (Opener, error)
}

// OpenOption is the option for allocating wal instance.
type OpenOption struct {
	Channel        types.PChannelInfo
	DisableFlusher bool // disable flusher for test, only use in test.
	Replica        *streamingpb.WALReplicaAssignment
	Primary        *types.PChannelInfoAssigned
}

// Opener is the interface for build wal instance.
type Opener interface {
	// Open open a wal instance.
	Open(ctx context.Context, opt *OpenOption) (WAL, error)

	// Close closes the opener resources.
	Close()
}

// ReadOnlyReplicaOpener creates a query replica from a primary's bootstrap and
// ordered events. It is separate from opening a raw backend ROWAL: that path
// may acquire a backend writer and does not construct a query runtime.
type ReadOnlyReplicaOpener interface {
	OpenReadOnlyReplica(context.Context, *OpenOption) (WAL, error)
}
