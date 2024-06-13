package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

var _ wal.WAL = (*walAdaptorImpl)(nil)

// adaptImplsToWAL creates a new wal from wal impls.
func adaptImplsToWAL(basicWAL walimpls.WALImpls, cleanup func()) wal.WAL {
	return &walAdaptorImpl{
		lifetime:    lifetime.NewLifetime(lifetime.Working),
		idAllocator: util.NewIDAllocator(),
		inner:       basicWAL,
		// TODO: make the pool size configurable.
		appendExecutionPool: conc.NewPool[struct{}](10),
		scannerRegistry: scannerRegistry{
			channel:     basicWAL.Channel(),
			idAllocator: util.NewIDAllocator(),
		},
		scanners: typeutil.NewConcurrentMap[int64, wal.Scanner](),
		cleanup:  cleanup,
	}
}

// walAdaptorImpl is a wrapper of WALImpls to extend it into a WAL interface.
type walAdaptorImpl struct {
	lifetime            lifetime.Lifetime[lifetime.State]
	idAllocator         *util.IDAllocator
	inner               walimpls.WALImpls
	appendExecutionPool *conc.Pool[struct{}]
	scannerRegistry     scannerRegistry
	scanners            *typeutil.ConcurrentMap[int64, wal.Scanner]
	cleanup             func()
}

// Channel returns the channel info of wal.
func (w *walAdaptorImpl) Channel() *logpb.PChannelInfo {
	return w.inner.Channel()
}

// Append writes a record to the log.
func (w *walAdaptorImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	return w.inner.Append(ctx, msg)
}

// AppendAsync writes a record to the log asynchronously.
func (w *walAdaptorImpl) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(message.MessageID, error)) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		cb(nil, status.NewOnShutdownError("wal is on shutdown"))
		return
	}

	// Submit async append to a background execution pool.
	_ = w.appendExecutionPool.Submit(func() (struct{}, error) {
		defer w.lifetime.Done()

		msgID, err := w.inner.Append(ctx, msg)
		cb(msgID, err)
		return struct{}{}, nil
	})
}

// Read returns a scanner for reading records from the wal.
func (w *walAdaptorImpl) Read(ctx context.Context, opts wal.ReadOption) (wal.Scanner, error) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	name, err := w.scannerRegistry.AllocateScannerName()
	if err != nil {
		return nil, err
	}
	// wrap the scanner with cleanup function.
	id := w.idAllocator.Allocate()
	s := newScannerAdaptor(w.inner, walimpls.ReadOption{
		DeliverPolicy: opts.DeliverPolicy,
		Name:          name,
	}, func() {
		w.scanners.Remove(id)
	})
	w.scanners.Insert(id, s)
	return s, nil
}

// Close overrides Scanner Close function.
func (w *walAdaptorImpl) Close() {
	w.lifetime.SetState(lifetime.Stopped)
	w.lifetime.Wait()
	w.lifetime.Close()

	// close all wal instances.
	w.scanners.Range(func(id int64, s wal.Scanner) bool {
		s.Close()
		log.Info("close scanner by wal extend", zap.Int64("id", id), zap.Any("channel", w.Channel()))
		return true
	})
	w.inner.Close()
	w.appendExecutionPool.Free()
	w.cleanup()
}
