package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// newWALWithInterceptors creates a new wal with interceptors.
func newWALWithInterceptors(l walimpls.WALImpls, builders ...walimpls.InterceptorBuilder) walimpls.WALImpls {
	if len(builders) == 0 {
		return l
	}
	// Build all interceptors.
	interceptors := make([]walimpls.BasicInterceptor, 0, len(builders))
	chs := make([]chan walimpls.WALImpls, 0, len(builders))
	for _, b := range builders {
		ch := make(chan walimpls.WALImpls, 1)
		chs = append(chs, ch)
		interceptors = append(interceptors, b.Build(ch))
	}
	walWithInterceptors := &walWithInterceptor{
		WALImpls:    l,
		interceptor: newChainedInterceptor(interceptors...),
	}
	for _, ch := range chs {
		ch <- walWithInterceptors
		close(ch)
	}
	return walWithInterceptors
}

// walWithInterceptor is a wrapper of wal with interceptors.
type walWithInterceptor struct {
	interceptor walimpls.InterceptorWithReady
	walimpls.WALImpls
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
	return w.interceptor.DoAppend(ctx, msg, w.WALImpls.Append)
}

// close all interceptor and underlying wal.
func (w *walWithInterceptor) Close() {
	w.WALImpls.Close()
	w.interceptor.Close()
}
