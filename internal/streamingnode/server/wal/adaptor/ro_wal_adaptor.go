package adaptor

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor/rate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ wal.WAL = (*roWALAdaptorImpl)(nil)

type roWALAdaptorImpl struct {
	*rate.WALRateLimitComponent
	mlog.Binder

	lifetime        *typeutil.Lifetime
	availableCtx    context.Context
	availableCancel context.CancelFunc
	idAllocator     *typeutil.IDAllocator
	roWALImpls      walimpls.ROWALImpls
	scannerRegistry scannerRegistry
	scanners        *typeutil.ConcurrentMap[int64, wal.Scanner]
	cleanup         func()
	scanMetrics     *metricsutil.ScanMetrics

	queryViewHandler    *snview.SNQueryViewHandler
	viewResourceManager *vchannel.PChannelRecoveryManager
	projectionCancel    context.CancelFunc
	projectionDone      <-chan struct{}
	functionRunners     []readOnlyFunctionRunnerLifecycle
}

type readOnlyFunctionRunnerLifecycle struct {
	collectionID int64
	key          string
}

func (w *roWALAdaptorImpl) WALName() message.WALName {
	return w.roWALImpls.WALName()
}

// Channel returns the channel info of wal.
func (w *roWALAdaptorImpl) Channel() types.PChannelInfo {
	return w.roWALImpls.Channel()
}

// Metrics returns the metrics of the wal.
func (w *roWALAdaptorImpl) Metrics() types.WALMetrics {
	return types.ROWALMetrics{
		ChannelInfo: w.Channel(),
	}
}

func (w *roWALAdaptorImpl) GetLatestMVCCTimestamp(ctx context.Context, vchannel string) (uint64, error) {
	return 0, w.notPrimaryError()
}

func (w *roWALAdaptorImpl) TransformLog() wal.TransformLogAccesser {
	if w.viewResourceManager != nil {
		return w.viewResourceManager
	}
	return wal.NewTransformLogErrorAccesser(status.NewOnShutdownError("read only wal query resources are unavailable"))
}

func (w *roWALAdaptorImpl) GetReplicateCheckpoint() (*wal.ReplicateCheckpoint, error) {
	panic("we cannot get replicate checkpoint from a read only wal")
}

func (w *roWALAdaptorImpl) GetSalvageCheckpoint() []*wal.ReplicateCheckpoint {
	panic("we cannot get salvage checkpoint from a read only wal")
}

// Append writes a record to the logger.
func (w *roWALAdaptorImpl) Append(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
	panic("we cannot append message into a read only wal")
}

// Append a record to the log asynchronously.
func (w *roWALAdaptorImpl) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(*wal.AppendResult, error)) {
	panic("we cannot append message into a read only wal")
}

// Read returns a scanner for reading records from the wal.
func (w *roWALAdaptorImpl) Read(ctx context.Context, opts wal.ReadOption) (wal.Scanner, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	// Validate DeliverPolicy: if it's StartFrom or StartAfter, check that the message ID's WALName matches the current WAL name
	if mismatchWALNameErr := w.checkReadOptWALName(opts); mismatchWALNameErr != nil {
		return nil, mismatchWALNameErr
	}

	name, err := w.scannerRegistry.AllocateScannerName()
	if err != nil {
		return nil, err
	}
	// wrap the scanner with cleanup function.
	id := w.idAllocator.Allocate()
	s := newScannerAdaptor(
		name,
		w.roWALImpls,
		opts,
		w.scanMetrics.NewScannerMetrics(),
		func() { w.scanners.Remove(id) })
	w.scanners.Insert(id, s)
	return s, nil
}

func (w *roWALAdaptorImpl) checkReadOptWALName(opts wal.ReadOption) error {
	if opts.DeliverPolicy != nil {
		currentWALName := w.WALName()
		var msgID *commonpb.MessageID

		switch t := opts.DeliverPolicy.GetPolicy().(type) {
		case *streamingpb.DeliverPolicy_StartFrom:
			msgID = t.StartFrom
		case *streamingpb.DeliverPolicy_StartAfter:
			msgID = t.StartAfter
		}

		if msgID != nil {
			msgWALName := message.WALName(msgID.WALName)
			if msgWALName != currentWALName {
				w.Logger().Info(context.TODO(),
					"WAL name mismatch", mlog.String("msgIDWALName", msgWALName.String()), mlog.String("currentWALName", currentWALName.String()))
				return status.NewWALNameMismatchError(currentWALName.String(), msgWALName.String())
			}
		}
	}
	return nil
}

// IsAvailable returns whether the wal is available.
func (w *roWALAdaptorImpl) IsAvailable() bool {
	return w.availableCtx.Err() == nil
}

// Available returns a channel that will be closed when the wal is shut down.
func (w *roWALAdaptorImpl) Available() <-chan struct{} {
	return w.availableCtx.Done()
}

// Close overrides Scanner Close function.
func (w *roWALAdaptorImpl) Close() {
	// begin to close the wal.
	w.Logger().Info(context.TODO(), "wal begin to close...")
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	w.forceCancelAfterGracefulTimeout()
	w.lifetime.Wait()

	w.Logger().Info(context.TODO(), "wal begin to close scanners...")

	if w.projectionCancel != nil {
		w.projectionCancel()
	}
	if w.projectionDone != nil {
		<-w.projectionDone
	}

	// close all wal instances.
	w.scanners.Range(func(id int64, s wal.Scanner) bool {
		s.Close()
		mlog.Info(context.TODO(), "close scanner by wal adaptor", mlog.Int64("id", id), mlog.Any("channel", w.Channel()))
		return true
	})

	if w.queryViewHandler != nil {
		w.queryViewHandler.CloseForHandoff()
	}
	if w.viewResourceManager != nil {
		w.viewResourceManager.Close()
	}
	w.releaseFunctionRunners()

	w.Logger().Info(context.TODO(), "scanner close done, close inner wal...")
	w.roWALImpls.Close()

	w.Logger().Info(context.TODO(), "call wal cleanup function...")
	w.cleanup()
	w.Logger().Info(context.TODO(), "wal closed")

	// close all metrics.
	w.scanMetrics.Close()

	// close the rate limit component.
	w.WALRateLimitComponent.Close()
}

func (w *roWALAdaptorImpl) registerReadOnlyFunctionRunner(collectionID int64, vchannel string, walReplicaID int64, schema *schemapb.CollectionSchema) {
	if schema == nil {
		return
	}
	key := shard.WALReplicaFunctionRunnerKey(vchannel, walReplicaID)
	if err := function.GetManager().Alloc(collectionID, key, schema); err != nil {
		w.Logger().Warn(context.TODO(), "failed to allocate read-only wal function runners",
			mlog.Int64("collectionID", collectionID),
			mlog.String("vchannel", vchannel),
			mlog.Int64("walReplicaID", walReplicaID),
			mlog.String("key", key),
			mlog.Int32("schemaVersion", schema.GetVersion()),
			mlog.Err(err))
		return
	}
	w.functionRunners = append(w.functionRunners, readOnlyFunctionRunnerLifecycle{
		collectionID: collectionID,
		key:          key,
	})
}

func (w *roWALAdaptorImpl) releaseFunctionRunners() {
	for _, lifecycle := range w.functionRunners {
		function.GetManager().Release(lifecycle.collectionID, lifecycle.key)
	}
	w.functionRunners = nil
}

// forceCancelAfterGracefulTimeout forces to cancel the context after the graceful timeout.
func (w *roWALAdaptorImpl) forceCancelAfterGracefulTimeout() {
	if w.availableCtx.Err() != nil {
		return
	}
	time.AfterFunc(3*time.Second, func() {
		// perform a force cancel to avoid resource leak.
		w.availableCancel()
	})
}

func (w *roWALAdaptorImpl) startReadOnlyProjectionScanner(startMessageID message.MessageID) {
	if startMessageID == nil || w.viewResourceManager == nil {
		return
	}
	ctx, cancel := context.WithCancel(w.availableCtx)
	done := make(chan struct{})
	w.projectionCancel = cancel
	w.projectionDone = done

	scanner := newRecoveryScannerAdaptor(
		w.roWALImpls,
		startMessageID,
		w.scanMetrics.NewScannerMetrics(),
		false,
	)
	go w.runReadOnlyProjectionScanner(ctx, scanner, done)
}

func (w *roWALAdaptorImpl) runReadOnlyProjectionScanner(ctx context.Context, scanner wal.Scanner, done chan<- struct{}) {
	defer close(done)
	defer scanner.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-scanner.Chan():
			if !ok {
				if err := scanner.Error(); err != nil && ctx.Err() == nil {
					w.Logger().Warn(context.TODO(), "read-only wal projection scanner stopped with error", mlog.Err(err))
				}
				return
			}
			owner := message.NewOwnedImmutableMessage(msg, nil)
			dispatch := owner.Clone()
			w.viewResourceManager.ObserveMessage(ctx, dispatch)
			dispatch.Release()
			owner.Release()
		}
	}
}
