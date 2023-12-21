package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
)

// OpenerBuilder is the interface for build wal opener.
type OpenerBuilder interface {
	// Name of the wal builder, should be a lowercase string.
	Name() string

	Build() (Opener, error)
}

// OpenOption is the option for allocating wal instance.
type OpenOption struct {
	Channel             *logpb.PChannelInfo  // Channel to open.
	InterceptorBuilders []InterceptorBuilder // Interceptor builders to build when open.
}

// Opener is the interface for build wal instance.
type Opener interface {
	// Open open a wal instance.
	Open(ctx context.Context, opt *OpenOption) (WAL, error)

	// Close closes the opener resources.
	Close()
}
