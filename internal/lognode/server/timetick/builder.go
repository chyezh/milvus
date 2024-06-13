package timetick

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/lognode/server/timetick/ack"
	"github.com/milvus-io/milvus/internal/lognode/server/timetick/timestamp"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ walimpls.InterceptorBuilder = (*interceptorBuilder)(nil)

// NewInterceptorBuilder creates a new interceptor builder.
// 1. Add timetick to all message before append to wal.
// 2. Collect timetick info, and generate sync-timetick message to wal.
func NewInterceptorBuilder(rc types.RootCoordClient) walimpls.InterceptorBuilder {
	return &interceptorBuilder{
		allocator: timestamp.NewAllocator(rc),
	}
}

// interceptorBuilder is a builder to build timeTickAppendInterceptor.
type interceptorBuilder struct {
	allocator timestamp.Allocator
}

// Build implements Builder.
func (b *interceptorBuilder) Build(walReady <-chan walimpls.WALImpls) walimpls.BasicInterceptor {
	ctx, cancel := context.WithCancel(context.Background())
	interceptor := &timeTickAppendInterceptor{
		ctx:        ctx,
		cancel:     cancel,
		ready:      make(chan struct{}),
		ackManager: ack.NewAckManager(b.allocator),
		ackDetails: &ackDetails{},
		sourceID:   paramtable.GetNodeID(),
	}
	go interceptor.executeSyncTimeTick(
		// TODO: move the configuration to lognode.
		paramtable.Get().ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond),
		walReady,
	)
	return interceptor
}
