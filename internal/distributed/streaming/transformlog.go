package streaming

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (w *walAccesserImpl) TransformLog() transformlog.Accesser {
	return transformLogAccesser{w: w}
}

type transformLogAccesser struct {
	w *walAccesserImpl
}

func (a transformLogAccesser) Read(ctx context.Context, opts transformlog.ReadOption) transformlog.Scanner {
	if !a.w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return transformlog.NewErrorScanner(opts.Name, ErrWALAccesserClosed)
	}
	defer a.w.lifetime.Done()

	return a.w.handlerClient.ReadTransformLog(ctx, opts)
}
