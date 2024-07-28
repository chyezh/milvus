package timetick

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"go.uber.org/zap"
)

var _ interceptors.InterceptorBuilder = (*interceptorBuilder)(nil)

// NewInterceptorBuilder creates a new interceptor builder.
// 1. Add timetick to all message before append to wal.
// 2. Collect timetick info, and generate sync-timetick message to wal.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

// interceptorBuilder is a builder to build timeTickAppendInterceptor.
type interceptorBuilder struct{}

// Build implements Builder.
func (b *interceptorBuilder) Build(param interceptors.InterceptorBuildParam) interceptors.Interceptor {
	ctx, cancel := context.WithCancel(context.Background())
	operator := &timeTickSyncOperator{
		ctx:                   ctx,
		cancel:                cancel,
		logger:                log.With(zap.Any("pchannel", param.WALImpls.Channel())),
		pchannel:              param.WALImpls.Channel(),
		notifier:              syncutil.NewFuture[*inspector.SyncNotifier](),
		ready:                 make(chan struct{}),
		interceptorBuildParam: param,
		ackManager:            ack.NewAckManager(),
		ackDetails:            &ackDetails{},
		sourceID:              paramtable.GetNodeID(),
		timeTickInfoListener:  inspector.NewTimeTickInfoListener(),
	}
	go operator.initialize()
	resource.Resource().TimeTickInspector().RegisterSyncOperator(operator)
	return &timeTickAppendInterceptor{
		operator: operator,
	}
}
