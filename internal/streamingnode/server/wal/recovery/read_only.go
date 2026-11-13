package recovery

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReadOnlyConfig describes an already reconciled logical baseline. A snapshot
// containing only metadata must finish its retained-WAL replay before the
// subscription owner advertises the runtime as query-ready.
type ReadOnlyConfig struct {
	Channel    types.PChannelInfo
	Applied    walevent.Cursor
	Checkpoint *WALCheckpoint
	Modules    vchannel.PChannelManagerConfig
	Summary    walsummary.ManagerConfig
	Manifest   *streamingpb.PChannelSummaryManifest
	// PrepareEvent acquires dependencies before logical Apply. It may fail
	// without invalidating the previously installed query prefix.
	PrepareEvent walevent.Applier
}

// ReadOnlyRecoveryStorage uses the same Apply operation as the writer. Its
// lifetime is independent of a transport connection: reconnecting keeps this
// consumer and its QueryView resources, including its applied cursor.
type ReadOnlyRecoveryStorage struct {
	*recoveryStorageImpl
	consumer      *walevent.Consumer
	ctx           context.Context
	cancel        context.CancelFunc
	applyMu       sync.RWMutex
	closed        bool
	sourceMu      sync.Mutex
	sourceAttempt uint64
	sourceCancel  context.CancelFunc
}

func NewReadOnlyRecoveryStorage(cfg ReadOnlyConfig, opts ...RecoveryStorageOption) (*ReadOnlyRecoveryStorage, error) {
	if cfg.Channel.Term <= 0 || cfg.Applied.Term != cfg.Channel.Term || cfg.Checkpoint == nil {
		return nil, merr.WrapErrServiceInternalMsg("read-only WAL baseline term does not match its assignment")
	}
	opts = append(opts, func(r *recoveryStorageImpl) { r.readOnly = true })
	rs := newRecoveryStorage(cfg.Channel, cfg.Checkpoint, opts...)
	rs.SetLogger(cfg.Modules.Logger)
	cfg.Summary.ReadOnly = true
	cfg.Summary.PChannel = cfg.Channel.Name
	cfg.Summary.Term = cfg.Channel.Term
	cfg.Summary.Runtime = moduleapi.Runtime{}
	rs.summaryManager = walsummary.NewManager(cfg.Summary)
	if cfg.Manifest != nil {
		if err := rs.summaryManager.ApplyStoragePublished(cfg.Manifest); err != nil {
			rs.closeRecoveryResources()
			return nil, err
		}
	}
	cfg.Modules.ReadOnly = true
	cfg.Modules.PChannel = cfg.Channel.Name
	cfg.Modules.SummaryManager = rs.summaryManager
	cfg.Modules.Runtime = moduleapi.Runtime{Scheduler: rs.taskScheduler, Notifier: rs}
	// RO has query loaders but no capability to register segments, produce
	// binlogs or materialize L0, even when a shared wiring config is supplied.
	cfg.Modules.SegmentLifecycle = nil
	cfg.Modules.SegmentPackWriter = nil
	cfg.Modules.L0Materializer = nil
	metas := make(map[string]*streamingpb.VChannelMeta, len(cfg.Modules.VChannelMetas))
	for name, meta := range cfg.Modules.VChannelMetas {
		metas[name] = proto.CloneOf(meta)
	}
	cfg.Modules.VChannelMetas = metas
	manager, err := vchannel.NewPChannelRecoveryManager(cfg.Modules)
	if err != nil {
		rs.closeRecoveryResources()
		return nil, err
	}
	rs.vchannelManager = manager
	ctx, cancel := context.WithCancel(context.Background())
	return &ReadOnlyRecoveryStorage{
		recoveryStorageImpl: rs, consumer: walevent.NewConsumer(cfg.Applied, rs.applyEvent, walevent.WithPreparation(cfg.PrepareEvent)),
		ctx: ctx, cancel: cancel,
	}, nil
}

func (r *ReadOnlyRecoveryStorage) Applied() walevent.Cursor { return r.consumer.Cursor() }

func (r *ReadOnlyRecoveryStorage) ApplyError() error { return r.consumer.Err() }

func (r *ReadOnlyRecoveryStorage) Apply(ctx context.Context, event walevent.Event) error {
	r.applyMu.RLock()
	defer r.applyMu.RUnlock()
	if r.closed {
		return merr.WrapErrServiceUnavailable("read-only WAL recovery storage is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := contextutil.MergeContext(context.WithoutCancel(ctx), r.ctx)
	defer cancel()
	return r.consumer.Apply(ctx, event)
}

func (r *ReadOnlyRecoveryStorage) Close() {
	r.cancel()
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.backgroundTaskNotifier.Cancel()
	r.closeRecoveryResources()
}

var _ RecoveryStorage = (*ReadOnlyRecoveryStorage)(nil)
