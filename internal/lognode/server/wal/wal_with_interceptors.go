package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/interceptor"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

func newWALWithInterceptors(wal wal.WAL, builders ...interceptor.Builder) wal.WAL {
	if len(builders) == 0 {
		return wal
	}
	// Build all interceptors.
	interceptors := make([]interceptor.AppendInterceptor, 0, len(builders))
	for _, b := range builders {
		interceptors = append(interceptors, b.Build(wal))
	}
	return &walWithInterceptor{
		WAL:         wal,
		interceptor: interceptor.NewChainedInterceptor(interceptors...),
	}
}

// walWithInterceptor is a wrapper of wal with interceptors.
type walWithInterceptor struct {
	interceptor interceptor.AppendInterceptorWithReady
	wal.WAL
}

// Append with interceptors.
func (w *walWithInterceptor) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	// Check if interceptor is ready.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.interceptor.Ready():
	}

	// Execute the interceptor and wal append.
	return w.interceptor.Do(ctx, msg, w.WAL.Append)
}

// close all interceptor and underlying wal.
func (w *walWithInterceptor) Close() {
	w.interceptor.Close()
	w.WAL.Close()
}
