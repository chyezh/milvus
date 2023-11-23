package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal/interceptor"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/scanner"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/wal"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
)

var _ WALExtend = (*walExtendImpl)(nil)

func newWALExtend(wal wal.WAL, cleanup func(), builders ...interceptor.Builder) WALExtend {
	return &walExtendImpl{
		lifetime: lifetime.NewLifetime(lifetime.Working),
		inner:    newWALWithInterceptors(wal, builders...),
		pool:     conc.NewPool[struct{}](10),
		cleanup:  cleanup,
	}
}

// walExtendImpl is a wrapper of WAL to extend the WAL interface.
type walExtendImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]
	inner    wal.WAL
	pool     *conc.Pool[struct{}]
	cleanup  func()
}

func (w *walExtendImpl) Channel() logpb.PChannelInfo {
	return w.inner.Channel()
}

func (w *walExtendImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	return w.inner.Append(ctx, msg)
}

func (w *walExtendImpl) Read(ctx context.Context, deliverPolicy wal.ReadOption) (scanner.Scanner, error) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	return w.inner.Read(ctx, deliverPolicy)
}

func (w *walExtendImpl) GetLatestMessageID(ctx context.Context) (message.MessageID, error) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	return w.inner.GetLatestMessageID(ctx)
}

// AppendAsync writes a record to the log asynchronously.
func (w *walExtendImpl) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(message.MessageID, error)) {
	_ = w.pool.Submit(func() (struct{}, error) {
		msgID, err := w.Append(ctx, msg)
		cb(msgID, err)
		return struct{}{}, nil
	})
}

// scannerCleanupWrapper overrides Scanner Close function.
func (w *walExtendImpl) Close() {
	w.lifetime.SetState(lifetime.Stopped)
	w.lifetime.Wait()
	w.lifetime.Close()

	w.inner.Close()
	w.cleanup()
}
