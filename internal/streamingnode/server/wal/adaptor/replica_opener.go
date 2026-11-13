package adaptor

import (
	"context"

	replicaclient "github.com/milvus-io/milvus/internal/streamingnode/client/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/idf"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ wal.ReadOnlyReplicaOpener = (*openerAdaptorImpl)(nil)

// OpenReadOnlyReplica prepares a private query runtime from a protected primary
// cut. It deliberately does not open a backend, recover writer interceptors or
// consult the writer's mutable catalog while constructing the local baseline.
func (o *openerAdaptorImpl) OpenReadOnlyReplica(ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
	if !o.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal opener is on shutdown")
	}
	defer o.lifetime.Done()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opt.Primary == nil || opt.Channel.AccessMode != types.AccessModeRO ||
		opt.Primary.Channel.AccessMode != types.AccessModeRW || opt.Channel.Term != opt.Primary.Channel.Term ||
		opt.Channel.Name != opt.Primary.Channel.Name || opt.Channel.Term <= 0 || opt.Primary.Node.ServerID <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("read-only WAL bootstrap requires its assigned primary")
	}
	retention, err := createReplicaRuntimeRetention(ctx, opt)
	if err != nil {
		return nil, err
	}
	installed := false
	defer func() {
		if !installed {
			retention.Close()
		}
	}()
	source := o.getReplicaClient().ForPrimary(*opt.Primary)
	transfer, err := source.Bootstrap(ctx, nil, retention.runtime)
	if err != nil {
		return nil, err
	}
	defer transfer.Close()
	r, err := newReadOnlyReplica(ctx, readOnlyReplicaConfig(opt.Channel), transfer.Snapshot, transfer.Replay, source.ResolveMVCC, retention.views)
	if err != nil {
		return nil, err
	}
	if err := transfer.Complete(ctx); err != nil {
		r.Close()
		return nil, err
	}
	if err := retention.install(ctx, transfer.Snapshot.Cursor); err != nil {
		r.Close()
		return nil, err
	}
	id := o.idAllocator.Allocate()
	r.onClose = func() {
		o.walInstances.Remove(id)
		retention.Close()
	}
	installed = true
	r.startFollowing(source.Events(), opt.Primary.Channel)
	o.walInstances.Insert(id, r)
	return r, nil
}

func (o *openerAdaptorImpl) getReplicaClient() *replicaclient.Client {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.replicaClient == nil {
		o.replicaClient = replicaclient.NewClient(resource.Resource().ETCD())
	}
	return o.replicaClient
}

// Use the same query modules as RW recovery, without any persistence capability.
func readOnlyReplicaConfig(channel types.PChannelInfo) recovery.ReadOnlyConfig {
	r := resource.Resource()
	logger := r.Logger().With(mlog.String("pchannel", channel.Name), mlog.Int64("term", channel.Term))
	return recovery.ReadOnlyConfig{
		Channel: channel,
		Modules: vchannel.PChannelManagerConfig{
			QueryRuntimeModuleBuilders: []queryresource.QueryRuntimeModuleBuilder{
				queryresource.NewGrowingRuntimeModuleBuilder(nil),
				queryresource.NewReadSchemaModuleBuilder(),
				idf.NewFutureProvider(r.MixCoordClient(), idf.WithChunkManager(r.ChunkManager())),
			},
			QueryStatsChunkManager:    r.ChunkManager(),
			QueryViewLoadInfoProvider: queryresource.NewFutureLoadInfoProvider(r.MixCoordClient()),
			Logger:                    logger,
		},
		Summary: walsummary.ManagerConfig{
			Store:         walsummary.NewStore(r.ChunkManager(), channel.Name, channel.Term),
			CacheMaxBytes: uint64(max(0, paramtable.Get().StreamingCfg.SummaryCacheBytesPerPChannel.GetAsSize())),
			Logger:        logger,
		},
	}
}
