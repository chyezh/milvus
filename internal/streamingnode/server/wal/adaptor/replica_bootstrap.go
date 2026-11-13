package adaptor

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type replicaBootstrapCatalog interface {
	replicaObjectCatalog
	metastore.WALReplicaRuntimeCatalog
	metastore.WALReplayRetentionCatalog
	metastore.WALQueryViewRetentionCatalog
}

type replicaSnapshotProvider interface {
	CutReplicaSnapshot(context.Context) (*streamingpb.WALReplicaSnapshot, *walevent.Reader, error)
}

var _ wal.ReplicaBootstrapProvider = (*walAdaptorImpl)(nil)

// OpenReplicaBootstrap selects the source's durable restart position. A caller
// checkpoint is a hint, not proof that a new runtime owns the corresponding
// in-memory tail. A fresh metadata baseline therefore declares its own paired
// restart boundary and reconstructs that tail through the ordered WAL scanner.
func (w *walAdaptorImpl) OpenReplicaBootstrap(ctx context.Context, _ *streamingpb.WALCheckpoint, runtime *viewpb.StreamingWorkNode) (_ wal.ReplicaBootstrap, err error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, merr.WrapErrServiceUnavailable("WAL bootstrap source is closing")
	}
	defer w.lifetime.Done()
	if runtime.GetPchannel() != w.Channel().Name || runtime.GetWalTerm() != w.Channel().Term || !runtime.GetReadOnly() ||
		runtime.GetNodeId() <= 0 || runtime.GetRuntimeId() == 0 || runtime.GetWalReplicaId() == 0 || runtime.GetWalReplicaRevision() == 0 {
		return nil, merr.WrapErrServiceInternalMsg("WAL bootstrap receiver identity does not match its source")
	}
	catalog, ok := resource.Resource().StreamingNodeCatalog().(replicaBootstrapCatalog)
	if !ok {
		return nil, merr.WrapErrServiceUnavailable("protected WAL bootstrap catalog is unavailable")
	}
	provider, ok := w.param.RecoveryStorage.(replicaSnapshotProvider)
	if !ok {
		return nil, merr.WrapErrServiceUnavailable("WAL bootstrap metadata source is unavailable")
	}
	ctx, cancel := contextutil.MergeContext(ctx, w.availableCtx)
	b := &protectedReplicaBootstrap{
		ctx: ctx, cancel: cancel, source: w, catalog: catalog, runtime: proto.CloneOf(runtime),
		objects: newReplicaObjectRetention(catalog, runtime, resource.Resource().ChunkManager(), packed.CreateStorageConfig()),
	}
	defer func() {
		if err != nil {
			b.Close()
		}
	}()
	b.snapshot, b.suffix, err = provider.CutReplicaSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err = walevent.ValidateSnapshot(w.Channel().Term, b.snapshot); err != nil {
		return nil, err
	}
	b.through = b.snapshot.Checkpoint.TimeTick
	// A failed pin response may follow a durable acquisition. Record the ID
	// before sending it so every error path can fence and release that attempt.
	b.pinID = uuid.NewString()
	if err = retry.Do(ctx, func() error {
		return catalog.PinWALReplay(ctx, runtime.Pchannel, &streamingpb.WALReplayPin{
			PinId: b.pinID, SourceTerm: runtime.WalTerm, Checkpoint: b.snapshot.Checkpoint, Runtime: runtime,
		})
	}, retry.Attempts(3), retry.Sleep(10*time.Millisecond)); err != nil {
		return nil, err
	}
	for _, view := range b.snapshot.QueryViews {
		if view.Meta.State != viewpb.QueryViewState_QueryViewStatePreparing && view.Meta.State != viewpb.QueryViewState_QueryViewStateUp {
			continue
		}
		if err = retry.Do(ctx, func() error { return catalog.RetainWALQueryView(ctx, runtime, view.Meta) }, retry.Attempts(3), retry.Sleep(10*time.Millisecond)); err != nil {
			return nil, err
		}
	}
	if err = b.objects.RetainSnapshot(ctx, b.snapshot); err != nil {
		return nil, err
	}
	if b.through < b.snapshot.Observed.TimeTick {
		b.scanner, err = w.Read(ctx, wal.ReadOption{
			DeliverPolicy: options.DeliverPolicyStartFrom(message.MustUnmarshalMessageID(b.snapshot.Checkpoint.MessageId)), IgnorePauseConsumption: true,
		})
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

type protectedReplicaBootstrap struct {
	mu        sync.Mutex
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
	source    *walAdaptorImpl
	catalog   replicaBootstrapCatalog
	runtime   *viewpb.StreamingWorkNode
	snapshot  *streamingpb.WALReplicaSnapshot
	suffix    *walevent.Reader
	objects   *replicaObjectRetention
	scanner   wal.Scanner
	pinID     string
	through   uint64
}

func (b *protectedReplicaBootstrap) Snapshot() *streamingpb.WALReplicaSnapshot { return b.snapshot }

func (b *protectedReplicaBootstrap) Next(ctx context.Context) (message.ImmutableMessage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.through < b.snapshot.Observed.TimeTick {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.ctx.Done():
			return nil, b.ctx.Err()
		case msg, ok := <-b.scanner.Chan():
			if !ok {
				if err := b.scanner.Error(); err != nil {
					return nil, err
				}
				return nil, merr.WrapErrServiceUnavailable("WAL replay ended before its protected cut")
			}
			if !msg.IsPersisted() || msg.TimeTick() <= b.snapshot.Checkpoint.TimeTick {
				continue
			}
			// Persisted TimeTick confirmations may repeat the preceding data
			// timestamp. The ordered scanner guarantees a nondecreasing prefix.
			if msg.TimeTick() < b.through || msg.TimeTick() > b.snapshot.Observed.TimeTick {
				return nil, merr.Wrapf(merr.ErrDataIntegrity, "WAL replay does not match its protected prefix: previous=%d next=%d upper=%d type=%s", b.through, msg.TimeTick(), b.snapshot.Observed.TimeTick, msg.MessageType())
			}
			if msg.TimeTick() == b.snapshot.Observed.TimeTick && !proto.Equal(message.MustMarshalMessageID(msg.LastConfirmedMessageID()), b.snapshot.Observed.MessageId) {
				return nil, merr.WrapErrDataIntegrityMsg("WAL replay reached a different physical cut")
			}
			b.through = msg.TimeTick()
			return msg, nil
		}
	}
	return nil, nil
}

func (b *protectedReplicaBootstrap) Complete(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if b.through != b.snapshot.Observed.TimeTick {
		return merr.WrapErrServiceUnavailable("WAL bootstrap replay is incomplete")
	}
	if !b.source.lifetime.Add(typeutil.LifetimeStateWorking) {
		return merr.WrapErrServiceUnavailable("WAL bootstrap source is closing")
	}
	defer b.source.lifetime.Done()
	// The catalog atomically checks the current source term. A replacement
	// writer fences this completion even if the old process is still running.
	return retry.Do(ctx, func() error {
		return b.catalog.PrepareWALReplicaRuntime(ctx, b.runtime, b.snapshot.Cursor)
	}, retry.Attempts(3), retry.Sleep(10*time.Millisecond))
}

func (b *protectedReplicaBootstrap) Close() {
	b.closeOnce.Do(func() {
		b.cancel()
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.scanner != nil {
			_ = b.scanner.Close()
		}
		if b.suffix != nil {
			b.suffix.Close()
		}
		b.objects.Close()
		if b.pinID != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := retry.Do(ctx, func() error { return b.catalog.ReleaseWALReplay(ctx, b.runtime.Pchannel, b.pinID) }, retry.Attempts(3), retry.Sleep(10*time.Millisecond)); err != nil {
				mlog.Warn(ctx, "failed to release WAL bootstrap replay pin", mlog.String("pchannel", b.runtime.Pchannel), mlog.String("pinID", b.pinID), mlog.Err(err))
			}
		}
	})
}
