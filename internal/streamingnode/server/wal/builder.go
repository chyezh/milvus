package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
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
	DisableFlusher bool                        // disable flusher for test, only use in test.
	StateStore     *utility.StateProgressStore // optional state store for progress tracking
}

// GetStateStore returns the state store, or a new noop store if nil.
func (o *OpenOption) GetStateStore() *utility.StateProgressStore {
	if o.StateStore != nil {
		return o.StateStore
	}
	// Return a new store that won't be watched - acts as noop
	return utility.NewStateProgressStore()
}

// Opener is the interface for build wal instance.
type Opener interface {
	// Open open a wal instance.
	Open(ctx context.Context, opt *OpenOption) (WAL, error)

	// Close closes the opener resources.
	Close()
}
