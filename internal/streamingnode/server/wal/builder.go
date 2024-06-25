package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
)

// OpenerBuilder is the interface for build wal opener.
type OpenerBuilder interface {
	// Name of the wal builder, should be a lowercase string.
	Name() string

	Build() (Opener, error)
}

// OpenOption is the option for allocating wal instance.
type OpenOption struct {
	Channel *streamingpb.PChannelInfo
}

// Opener is the interface for build wal instance.
type Opener interface {
	// Open open a wal instance.
	Open(ctx context.Context, opt *OpenOption) (WAL, error)

	// Close closes the opener resources.
	Close()
}
