package timetick

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/lognode/server/timetick/timestamp"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/interceptor"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/wal"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ interceptor.Builder = (*interceptorBuilder)(nil)

func NewInterceptorBuilder(rc types.RootCoordClient) interceptor.Builder {
	return &interceptorBuilder{
		allocator: timestamp.NewAllocator(rc),
	}
}

type interceptorBuilder struct {
	allocator timestamp.Allocator
}

func (b *interceptorBuilder) Build(wal wal.WAL) interceptor.AppendInterceptor {
	ctx, cancel := context.WithCancel(context.Background())
	interceptor := &timestampAssignAppendInterceptor{
		ctx:       ctx,
		cancel:    cancel,
		ready:     make(chan struct{}),
		allocator: newAllocatorWithRecorder(b.allocator),
		sourceID:  paramtable.GetNodeID(),
		wal:       wal,
	}
	go interceptor.executeSyncTimeTick(
		// TODO: move the configuration to lognode.
		paramtable.Get().ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond),
	)
	return interceptor
}
