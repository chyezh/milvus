package extends

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ wal.WAL = (*walExtendImpl)(nil)

// walExtendImpl is a wrapper of BasicWAL to extend it into a WAL interface.
type walExtendImpl struct {
	lifetime            lifetime.Lifetime[lifetime.State]
	idAllocator         *util.IDAllocator
	inner               wal.BasicWAL
	appendExecutionPool *conc.Pool[struct{}]
	scanners            *typeutil.ConcurrentMap[int64, wal.Scanner]
}

// Channel returns the channel info of wal.
func (w *walExtendImpl) Channel() *logpb.PChannelInfo {
	return w.inner.Channel()
}

// Append writes a record to the log.
func (w *walExtendImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	return w.inner.Append(ctx, msg)
}

// Read returns a scanner for reading records from the wal.
func (w *walExtendImpl) Read(ctx context.Context, deliverPolicy wal.ReadOption) (wal.Scanner, error) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	scanner, err := w.inner.Read(ctx, deliverPolicy)
	if err != nil {
		return nil, err
	}
	// wrap the scanner with cleanup function.
	id := w.idAllocator.Allocate()
	scanner = ScannerWithCleanup(scanner, func() {
		w.scanners.Remove(id)
	})
	w.scanners.Insert(id, scanner)
	return scanner, nil
}

// GetLatestMessageID returns the latest message id of the channel.
func (w *walExtendImpl) GetLatestMessageID(ctx context.Context) (message.MessageID, error) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	return w.inner.GetLatestMessageID(ctx)
}

// AppendAsync writes a record to the log asynchronously.
func (w *walExtendImpl) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(message.MessageID, error)) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		cb(nil, status.NewOnShutdownError("wal is on shutdown"))
	}
	defer w.lifetime.Done()

	_ = w.appendExecutionPool.Submit(func() (struct{}, error) {
		msgID, err := w.inner.Append(ctx, msg)
		cb(msgID, err)
		return struct{}{}, nil
	})
}

// Close overrides Scanner Close function.
func (w *walExtendImpl) Close() {
	w.lifetime.SetState(lifetime.Stopped)
	w.lifetime.Wait()
	w.lifetime.Close()

	w.inner.Close()
	w.appendExecutionPool.Free()
}
