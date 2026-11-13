package adaptor

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/queryplan/provider"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// readOnlyReplica is a query runtime, with no backend writer or persistence
// interceptors. Its owner supplies protected replay and source MVCC resolution.
// The event transport lifetime is deliberately separate from this local WAL.
type readOnlyReplica struct {
	*ratelimit.MuxRateLimitObserverRegistryImpl
	*replicaQueryRuntime
	*snview.SNQueryViewHandler
	storage   *recovery.ReadOnlyRecoveryStorage
	channel   types.PChannelInfo
	walName   message.WALName
	cancel    context.CancelFunc
	closeOnce sync.Once
}

var (
	_ wal.WAL                    = (*readOnlyReplica)(nil)
	_ provider.QueryPlanProvider = (*readOnlyReplica)(nil)
	_ viewquery.TaskProvider     = (*readOnlyReplica)(nil)
)

// newReadOnlyReplica installs query intent only after replay has reached the
// cut and storage overlays have been applied. It never calls a WAL backend Open.
func newReadOnlyReplica(
	ctx context.Context,
	cfg recovery.ReadOnlyConfig,
	snapshot *streamingpb.WALReplicaSnapshot,
	replay recovery.ReplicaReplay,
	resolveMVCC func(context.Context, *viewpb.GetQueryPlanRequest, string) (*viewpb.QueryPlanMVCC, error),
	opts ...recovery.RecoveryStorageOption,
) (*readOnlyReplica, error) {
	available, cancel := context.WithCancel(context.Background())
	r := &readOnlyReplica{
		MuxRateLimitObserverRegistryImpl: ratelimit.NewMuxRateLimitObserverRegistry(),
		channel:                          cfg.Channel, cancel: cancel,
		replicaQueryRuntime: &replicaQueryRuntime{availableCtx: available, lifetime: typeutil.NewLifetime()},
	}
	r.channel.AccessMode = types.AccessModeRO
	r.resolveMVCC = func(ctx context.Context, req *viewpb.GetQueryPlanRequest, vchannel string) (*viewpb.QueryPlanMVCC, error) {
		if fixed, ok := req.GetMvcc().(*viewpb.GetQueryPlanRequest_QueryPlanMvcc); ok {
			return fixed.QueryPlanMvcc, nil
		}
		return resolveMVCC(ctx, req, vchannel)
	}
	cfg.Channel = r.channel
	opts = append(opts, recovery.WithQueryViewEventApplier(func(ctx context.Context, views []*viewpb.QueryViewOfShard) error {
		snview.ApplyQueryViewIntents(ctx, r.SNQueryViewHandler, views)
		return nil
	}))
	storage, err := recovery.BootstrapReadOnlyRecoveryStorage(ctx, cfg, snapshot, replay, opts...)
	if err != nil {
		cancel()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		cancel()
		storage.Close()
		return nil, err
	}
	r.storage = storage
	r.walName = message.WALName(snapshot.Checkpoint.MessageId.WALName)
	r.viewResourceManager = storage.VChannelManager()
	r.SNQueryViewHandler = snview.NewReadOnlyPChannelSNQueryViewHandler(available, r.channel.Name, r.viewResourceManager, snapshot.QueryViews)
	r.queryViewHandler = r.SNQueryViewHandler
	return r, nil
}

func (r *readOnlyReplica) WALName() message.WALName    { return r.walName }
func (r *readOnlyReplica) Channel() types.PChannelInfo { return r.channel }
func (r *readOnlyReplica) Metrics() types.WALMetrics {
	return types.ROWALMetrics{ChannelInfo: r.channel}
}
func (r *readOnlyReplica) IsAvailable() bool            { return r.availableCtx.Err() == nil }
func (r *readOnlyReplica) Unavailable() <-chan struct{} { return r.availableCtx.Done() }

func (r *readOnlyReplica) Append(context.Context, message.MutableMessage) (*wal.AppendResult, error) {
	return nil, status.NewChannelFenced(r.channel.String())
}

func (r *readOnlyReplica) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(*wal.AppendResult, error)) {
	result, err := r.Append(ctx, msg)
	cb(result, err)
}

func (r *readOnlyReplica) Read(context.Context, wal.ReadOption) (wal.Scanner, error) {
	return nil, status.NewChannelFenced(r.channel.String())
}

func (r *readOnlyReplica) GetLatestMVCCTimestamp(context.Context, string) (uint64, error) {
	return 0, status.NewChannelFenced(r.channel.String())
}

func (r *readOnlyReplica) GetMVCCTimestamp(context.Context, *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	return nil, viewerror.NewNotPrimaryError("MVCC allocation requires the primary WAL")
}

func (r *readOnlyReplica) GetReplicateCheckpoint() (*wal.ReplicateCheckpoint, error) {
	return nil, status.NewChannelFenced(r.channel.String())
}

func (r *readOnlyReplica) GetSalvageCheckpoint() []*wal.ReplicateCheckpoint { return nil }

func (r *readOnlyReplica) TransformLog() wal.TransformLogAccesser {
	return wal.NewTransformLogErrorAccesser(status.NewChannelFenced(r.channel.String()))
}

func (r *readOnlyReplica) Close() {
	r.closeOnce.Do(func() {
		r.cancel()
		r.lifetime.SetState(typeutil.LifetimeStateStopped)
		r.lifetime.Wait()
		r.CloseForHandoff()
		r.storage.Close()
	})
}
