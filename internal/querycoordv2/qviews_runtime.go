// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoordv2

import (
	"context"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	streamingcoordclient "github.com/milvus-io/milvus/internal/streamingcoord/client"
	snhandler "github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/nodeview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	streamingtypes "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type qviewsBalancer interface {
	Start(ctx context.Context)
	Stop()
	Trigger(scopes ...balancer.TriggerScope)
}

type qviewsRuntime struct {
	loadConfigStore    *loadmgr.LoadConfigStore
	loadManager        *loadmgr.CollectionLoadManager
	shardViewRegistry  *coordview.ShardViewRegistry
	syncer             syncer.ReliableSyncer
	balancer           qviewsBalancer
	walReplicaProvider balancer.WALReplicaProvider
	walPrimarySwitcher walPrimarySwitchExecutor

	queryNodeClient      nodeview.QueryNodeClient
	queryNodeManager     qnmanager.ManagerClient
	streamingCoordClient streamingcoordclient.Client
	streamingNodeHandler snhandler.HandlerClient
	primaryRGHandler     config.EventHandler
	stopOnce             sync.Once
}

type qviewsRuntimeDependencies struct {
	queryCoordCatalog metastore.QueryCoordCatalog
	queryViewCatalog  queryview.QueryViewCatalog

	viewSyncClient       syncer.ViewSyncClient
	queryNodeClient      nodeview.QueryNodeClient
	resourceGroupManager nodeview.ResourceGroupManager
	dataViewProvider     balancer.DataViewProvider
	dataViewReferences   qviews.DataViewReferenceManager
	walReplicaProvider   balancer.WALReplicaProvider
	walPrimarySwitcher   walPrimarySwitchExecutor

	queryNodeManager            qnmanager.ManagerClient
	streamingCoordClient        streamingcoordclient.Client
	streamingNodeHandler        snhandler.HandlerClient
	streamingNodeViewSyncClient snhandler.QueryViewSyncClient

	balancerFactory func(*balancer.SnapshotBuilder) qviewsBalancer
}

func newQViewsRuntime(ctx context.Context, deps qviewsRuntimeDependencies) (*qviewsRuntime, error) {
	if deps.queryCoordCatalog == nil {
		return nil, merr.WrapErrServiceInternalMsg("querycoord catalog is nil")
	}
	if deps.queryViewCatalog == nil {
		return nil, merr.WrapErrServiceInternalMsg("query view catalog is nil")
	}
	if deps.resourceGroupManager == nil {
		return nil, merr.WrapErrServiceInternalMsg("resource group manager is nil")
	}
	if deps.dataViewProvider == nil {
		deps.dataViewProvider = emptyDataViewProvider{}
	}
	if deps.dataViewReferences == nil {
		deps.dataViewReferences = noopDataViewReferences{}
	}

	if deps.queryNodeClient == nil {
		deps.queryNodeClient = deps.queryNodeManager
	}
	if deps.queryNodeClient == nil {
		return nil, merr.WrapErrServiceInternalMsg("querynode client is nil")
	}
	if deps.streamingNodeViewSyncClient == nil && deps.streamingNodeHandler != nil {
		deps.streamingNodeViewSyncClient = deps.streamingNodeHandler.QueryViewSyncClient()
	}

	if deps.viewSyncClient == nil {
		if deps.queryNodeManager == nil {
			return nil, merr.WrapErrServiceInternalMsg("querynode manager client is nil")
		}
		if deps.streamingNodeViewSyncClient == nil {
			return nil, merr.WrapErrServiceInternalMsg("streamingnode query view sync client is nil")
		}
		deps.viewSyncClient = syncer.NewDefaultViewSyncClient(
			deps.queryNodeManager,
			deps.streamingNodeViewSyncClient,
		)
	}

	loadConfigStore, err := loadmgr.RecoverLoadConfigStore(ctx, deps.queryCoordCatalog)
	if err != nil {
		return nil, err
	}
	reliableSyncer := syncer.NewReliableSyncer(deps.viewSyncClient)
	shardViewRegistry, err := coordview.RecoverShardViewRegistry(ctx, deps.queryViewCatalog, reliableSyncer, deps.dataViewReferences)
	if err != nil {
		_ = reliableSyncer.Close()
		return nil, err
	}

	nodeProvider := nodeview.NewQueryNodeProvider(ctx, deps.queryNodeClient, deps.resourceGroupManager)
	builder := balancer.NewSnapshotBuilder(
		loadConfigStore,
		shardViewRegistry,
		nodeProvider,
		deps.dataViewProvider,
		balancer.DefaultBalanceConfig(),
	)
	if deps.walReplicaProvider == nil && deps.streamingCoordClient != nil {
		deps.walReplicaProvider = streamingCoordWALReplicaProvider{
			assignment: deps.streamingCoordClient.Assignment(),
		}
	}
	if deps.walPrimarySwitcher == nil {
		deps.walPrimarySwitcher = streamingCoordWALPrimarySwitchExecutor{}
	}
	builder.SetWALReplicaProvider(deps.walReplicaProvider)
	balancerController := qviewsBalancer(balancer.NewDefaultBalancer(builder, shardViewRegistry, nil))
	if deps.balancerFactory != nil {
		balancerController = deps.balancerFactory(builder)
	}
	if defaultBalancer, ok := balancerController.(*balancer.DefaultBalancer); ok {
		defaultBalancer.SetWALReplicaDemandExecutor(streamingCoordWALReplicaDemandExecutor{})
	}
	loadManager := loadmgr.NewCollectionLoadManager(
		loadConfigStore,
		func(collectionID int64) {
			balancerController.Trigger(balancer.TriggerScope{DirtyCollections: []int64{collectionID}})
		},
	)
	shardViewRegistry.RegisterStatsObserver(func(shardID qviews.ShardID, stats *coordview.ShardStats) {
		if stats != nil && stats.UpVersion != nil {
			loadManager.ObserveShardUp(shardID, stats.UpWALReplicaID)
			if _, err := maybeSwitchWALPrimaryReplicaForShardUp(
				context.Background(),
				deps.walReplicaProvider,
				deps.walPrimarySwitcher,
				shardViewRegistry.Snapshot(),
				shardID,
				stats.UpWALReplicaID,
			); err != nil {
				mlog.Debug(context.Background(), "failed to switch WAL primary after QueryView Up", mlog.Err(err))
			}
		}
	})
	seedDiscoverableShards(loadManager, shardViewRegistry.Snapshot())

	return &qviewsRuntime{
		loadConfigStore:      loadConfigStore,
		loadManager:          loadManager,
		shardViewRegistry:    shardViewRegistry,
		syncer:               reliableSyncer,
		balancer:             balancerController,
		walReplicaProvider:   deps.walReplicaProvider,
		walPrimarySwitcher:   deps.walPrimarySwitcher,
		queryNodeClient:      deps.queryNodeClient,
		queryNodeManager:     deps.queryNodeManager,
		streamingCoordClient: deps.streamingCoordClient,
		streamingNodeHandler: deps.streamingNodeHandler,
	}, nil
}

func (r *qviewsRuntime) start(ctx context.Context) {
	r.watchPrimaryResourceGroupChanges()
	if err := snmanager.StaticStreamingNodeManager.RegisterShardAssignmentProvider(ctx, r.loadManager); err != nil {
		mlog.Warn(ctx, "failed to register query view shard assignment provider", mlog.Err(err))
	} else {
		r.loadManager.SetShardAssignmentNotifier(func() {
			if err := snmanager.StaticStreamingNodeManager.TriggerShardAssignmentUpdate(context.Background()); err != nil {
				mlog.Warn(context.Background(), "failed to trigger query view shard assignment update", mlog.Err(err))
			}
		})
		if err := snmanager.StaticStreamingNodeManager.TriggerShardAssignmentUpdate(ctx); err != nil {
			mlog.Warn(ctx, "failed to trigger initial query view shard assignment update", mlog.Err(err))
		}
		r.maybeSwitchWALPrimaryReplicasForUpShards(ctx)
	}
	if err := snmanager.StaticStreamingNodeManager.RegisterWALReplicaDependencyProvider(ctx, qviewsWALReplicaDependencyProvider{
		registry: r.shardViewRegistry,
		syncer:   r.syncer,
	}); err != nil {
		mlog.Warn(ctx, "failed to register query view WAL replica dependency provider", mlog.Err(err))
	}
	r.reconcileRecoveredQueryNodeLiveness(ctx)
	r.balancer.Start(ctx)
}

func (r *qviewsRuntime) reconcileRecoveredQueryNodeLiveness(ctx context.Context) {
	if r == nil || r.queryNodeClient == nil || r.shardViewRegistry == nil {
		return
	}
	nodes, err := r.queryNodeClient.GetAllQueryNodes(ctx)
	if err != nil {
		mlog.Warn(ctx, "failed to reconcile recovered query view querynode liveness", mlog.Err(err))
		return
	}

	alive := make(map[int64]struct{}, len(nodes))
	for nodeID, node := range nodes {
		if node == nil {
			continue
		}
		alive[nodeID] = struct{}{}
	}
	for _, node := range r.shardViewRegistry.QueryNodes() {
		if _, ok := alive[node.ID]; ok {
			continue
		}
		mlog.Info(ctx, "mark recovered query view querynode as lost", mlog.Int64("nodeID", node.ID))
		r.handleQueryNodeDown(node.ID)
	}
}

func (r *qviewsRuntime) watchPrimaryResourceGroupChanges() {
	key := paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key
	handler := config.NewHandler("querycoord.qviews.primary-rg", func(_ *config.Event) {
		mlog.Info(context.Background(), "primary resource group config changed, trigger QueryView WAL primary reconciliation")
		r.balancer.Trigger()
		go r.maybeSwitchWALPrimaryReplicasForUpShards(context.Background())
	})
	r.primaryRGHandler = handler
	paramtable.Get().Watch(key, handler)
}

func (r *qviewsRuntime) maybeSwitchWALPrimaryReplicasForUpShards(ctx context.Context) {
	seen := make(map[streamingtypes.ChannelID]struct{})
	snapshot := r.shardViewRegistry.Snapshot()
	for shardID, stats := range snapshot.StatsMap() {
		if stats == nil || stats.UpVersion == nil {
			continue
		}
		replicaKey, ok := walReplicaKeyForShard(shardID, stats.UpWALReplicaID)
		if !ok {
			continue
		}
		if _, exists := seen[replicaKey]; exists {
			continue
		}
		seen[replicaKey] = struct{}{}
		if _, err := maybeSwitchWALPrimaryReplicaForShardUp(
			ctx,
			r.walReplicaProvider,
			r.walPrimarySwitcher,
			snapshot,
			shardID,
			stats.UpWALReplicaID,
		); err != nil {
			mlog.Debug(ctx, "failed to switch WAL primary for recovered QueryView Up shard", mlog.Err(err))
		}
	}
}

func (r *qviewsRuntime) handleQueryNodeDown(nodeID int64) {
	if r == nil || r.shardViewRegistry == nil {
		return
	}
	affected := r.shardViewRegistry.OnQueryNodeLost(qviews.NewQueryNode(nodeID))
	if len(affected) == 0 || r.balancer == nil {
		return
	}
	r.balancer.Trigger(balancer.TriggerScope{DirtyShards: affected})
}

type walReplicaDependencyProvider interface {
	HasWALReplicaDependency(replicaID streamingtypes.ChannelID) bool
}

type qviewsWALReplicaDependencyProvider struct {
	registry walReplicaDependencyProvider
	syncer   walReplicaDependencyProvider
}

func (p qviewsWALReplicaDependencyProvider) HasWALReplicaDependency(replicaID streamingtypes.ChannelID) bool {
	if p.registry != nil && p.registry.HasWALReplicaDependency(replicaID) {
		return true
	}
	return p.syncer != nil && p.syncer.HasWALReplicaDependency(replicaID)
}

func (r *qviewsRuntime) stop() {
	r.stopOnce.Do(func() {
		if r.primaryRGHandler != nil {
			paramtable.Get().Unwatch(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, r.primaryRGHandler)
		}
		r.balancer.Stop()
		if r.shardViewRegistry != nil {
			r.shardViewRegistry.Close()
		}
		_ = r.syncer.Close()
		if r.queryNodeManager != nil {
			r.queryNodeManager.Close()
		}
		if r.streamingNodeHandler != nil {
			r.streamingNodeHandler.Close()
		}
		if r.streamingCoordClient != nil {
			r.streamingCoordClient.Close()
		}
	})
}

func seedDiscoverableShards(loadManager *loadmgr.CollectionLoadManager, snapshot *coordview.ShardViewSnapshot) {
	for shardID, stats := range snapshot.StatsMap() {
		if stats != nil && stats.UpVersion != nil {
			loadManager.MarkShardDiscoverable(shardID, stats.UpWALReplicaID)
		}
	}
}

type streamingCoordWALReplicaProvider struct {
	assignment streamingcoordclient.AssignmentService
}

func (p streamingCoordWALReplicaProvider) WALReplicaSnapshot(ctx context.Context) *balancer.WALReplicaSnapshot {
	if p.assignment == nil {
		return nil
	}
	assignments, err := p.assignment.GetLatestAssignments(ctx)
	if err != nil {
		mlog.Warn(ctx, "failed to get WAL replica assignment snapshot", mlog.Err(err))
		return balancer.NewWALReplicaSnapshot(nil)
	}
	if assignments == nil {
		return balancer.NewWALReplicaSnapshot(nil)
	}
	replicas := make([]streamingtypes.WALReplicaInfo, 0)
	for _, assignment := range assignments.Assignments {
		if len(assignment.WALReplicas) > 0 {
			for _, replica := range assignment.WALReplicas {
				replicas = append(replicas, replica)
			}
			continue
		}
		replicas = append(replicas, legacyWALReplicaInfosFromAssignment(assignment)...)
	}
	return balancer.NewWALReplicaSnapshot(replicas)
}

func legacyWALReplicaInfosFromAssignment(assignment streamingtypes.StreamingNodeAssignment) []streamingtypes.WALReplicaInfo {
	replicas := make([]streamingtypes.WALReplicaInfo, 0, len(assignment.Channels)+len(assignment.SecondaryChannels))
	seen := make(map[string]struct{}, len(assignment.Channels))
	for pchannel, channel := range assignment.Channels {
		replicas = append(replicas, legacyWALReplicaInfo(pchannel, channel, streamingtypes.AccessModeRW))
		seen[pchannel] = struct{}{}
	}
	for pchannel, channel := range assignment.SecondaryChannels {
		if _, ok := seen[pchannel]; ok {
			continue
		}
		replicas = append(replicas, legacyWALReplicaInfo(pchannel, channel, streamingtypes.AccessModeRO))
	}
	return replicas
}

func legacyWALReplicaInfo(pchannel string, channel streamingtypes.PChannelInfo, accessMode streamingtypes.AccessMode) streamingtypes.WALReplicaInfo {
	if channel.Name != "" {
		pchannel = channel.Name
	}
	return streamingtypes.WALReplicaInfo{
		ChannelID: streamingtypes.ChannelID{
			Name:         pchannel,
			WALReplicaID: 0,
		},
		AccessMode:        accessMode,
		PChannelWriteTerm: channel.Term,
		State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
	}
}

type streamingCoordWALReplicaDemandExecutor struct{}

func (streamingCoordWALReplicaDemandExecutor) EnsureReadOnlyWALReplica(ctx context.Context, demand balancer.WALReplicaDemand) error {
	return snmanager.StaticStreamingNodeManager.EnsureReadOnlyWALReplica(ctx, demand.PChannel, demand.ResourceGroup)
}

func (streamingCoordWALReplicaDemandExecutor) ReleaseReadOnlyWALReplica(ctx context.Context, release balancer.WALReplicaRelease) error {
	return snmanager.StaticStreamingNodeManager.ReleaseReadOnlyWALReplica(ctx, release.PChannel, release.WALReplicaID)
}

type walPrimarySwitchExecutor interface {
	SwitchWALPrimaryReplica(ctx context.Context, pchannel string, targetReplicaID int64) error
}

type streamingCoordWALPrimarySwitchExecutor struct{}

func (streamingCoordWALPrimarySwitchExecutor) SwitchWALPrimaryReplica(ctx context.Context, pchannel string, targetReplicaID int64) error {
	return snmanager.StaticStreamingNodeManager.SwitchWALPrimaryReplica(ctx, pchannel, targetReplicaID)
}

func walReplicaKeyForShard(shardID qviews.ShardID, walReplicaID int64) (streamingtypes.ChannelID, bool) {
	ch, err := metautil.ParseChannel(shardID.VChannel, metautil.NewDynChannelMapper())
	if err != nil {
		return streamingtypes.ChannelID{}, false
	}
	return streamingtypes.ChannelID{
		Name:         ch.PhysicalName(),
		WALReplicaID: walReplicaID,
	}, true
}

func maybeSwitchWALPrimaryReplicaForShardUp(
	ctx context.Context,
	provider balancer.WALReplicaProvider,
	switcher walPrimarySwitchExecutor,
	shardSnapshot *coordview.ShardViewSnapshot,
	shardID qviews.ShardID,
	walReplicaID int64,
) (bool, error) {
	primaryRG := paramtable.Get().StreamingCfg.PrimaryResourceGroup.GetValue()
	if primaryRG == "" || provider == nil || switcher == nil {
		return false, nil
	}
	replicaKey, ok := walReplicaKeyForShard(shardID, walReplicaID)
	if !ok {
		return false, nil
	}
	pchannel := replicaKey.Name
	snapshot := provider.WALReplicaSnapshot(ctx)
	if snapshot == nil {
		return false, nil
	}
	mode, ok := snapshot.AccessMode(pchannel, walReplicaID)
	if !ok || mode != streamingtypes.AccessModeRO {
		return false, nil
	}
	if !snapshot.IsServiceableInResourceGroup(pchannel, walReplicaID, primaryRG) {
		return false, nil
	}
	if snapshot.HasAccessModeInResourceGroup(pchannel, primaryRG, streamingtypes.AccessModeRW) {
		return false, nil
	}
	if !targetWALReplicaReadyForPrimaryServing(shardSnapshot, snapshot, pchannel, walReplicaID) {
		return false, nil
	}
	if err := switcher.SwitchWALPrimaryReplica(ctx, pchannel, walReplicaID); err != nil {
		refreshed := provider.WALReplicaSnapshot(ctx)
		if refreshed != nil && refreshed.HasAccessModeInResourceGroup(pchannel, primaryRG, streamingtypes.AccessModeRW) {
			return true, nil
		}
		return true, err
	}
	return true, nil
}

func targetWALReplicaReadyForPrimaryServing(
	shardSnapshot *coordview.ShardViewSnapshot,
	walSnapshot *balancer.WALReplicaSnapshot,
	pchannel string,
	targetWALReplicaID int64,
) bool {
	if shardSnapshot == nil {
		return false
	}
	targetLoadInfoVersions := make(map[string]map[uint64]struct{})
	for shardID, stats := range shardSnapshot.StatsMap() {
		if stats == nil || stats.UpVersion == nil {
			continue
		}
		replicaKey, ok := walReplicaKeyForShard(shardID, stats.UpWALReplicaID)
		if !ok || replicaKey.Name != pchannel {
			continue
		}
		if stats.UpWALReplicaID != targetWALReplicaID {
			continue
		}
		loadInfoVersions := targetLoadInfoVersions[shardID.VChannel]
		if loadInfoVersions == nil {
			loadInfoVersions = make(map[uint64]struct{})
			targetLoadInfoVersions[shardID.VChannel] = loadInfoVersions
		}
		loadInfoVersions[stats.UpLoadInfoVersion] = struct{}{}
	}

	for shardID, stats := range shardSnapshot.StatsMap() {
		if stats == nil || stats.UpVersion == nil {
			continue
		}
		replicaKey, ok := walReplicaKeyForShard(shardID, stats.UpWALReplicaID)
		if !ok || replicaKey.Name != pchannel {
			continue
		}
		if stats.UpWALReplicaID == targetWALReplicaID {
			continue
		}
		mode, ok := walSnapshot.AccessMode(pchannel, stats.UpWALReplicaID)
		if !ok || mode != streamingtypes.AccessModeRW {
			continue
		}
		if _, ok := targetLoadInfoVersions[shardID.VChannel][stats.UpLoadInfoVersion]; !ok {
			return false
		}
	}
	return true
}

func newDefaultQViewsRuntimeDependencies(
	metaKV kv.MetaKv,
	etcdCli *clientv3.Client,
	queryCoordCatalog metastore.QueryCoordCatalog,
	resourceGroupManager nodeview.ResourceGroupManager,
	mixCoord types.MixCoord,
) qviewsRuntimeDependencies {
	queryNodeManager := qnmanager.NewManagerClient(etcdCli)
	streamingCoordClient := streamingcoordclient.NewClient(etcdCli)
	streamingNodeHandler := snhandler.NewHandlerClient(streamingCoordClient.Assignment())
	deps := qviewsRuntimeDependencies{
		queryCoordCatalog:           queryCoordCatalog,
		queryViewCatalog:            queryview.NewQueryViewCatalog(metaKV, "coord"),
		queryNodeClient:             queryNodeManager,
		resourceGroupManager:        resourceGroupManager,
		dataViewProvider:            &mixCoordDataViewProvider{mixCoord: mixCoord},
		queryNodeManager:            queryNodeManager,
		streamingCoordClient:        streamingCoordClient,
		streamingNodeHandler:        streamingNodeHandler,
		streamingNodeViewSyncClient: streamingNodeHandler.QueryViewSyncClient(),
	}
	if references, ok := mixCoord.(qviews.DataViewReferenceManager); ok {
		deps.dataViewReferences = references
	}
	return deps
}

type noopDataViewReferences struct{}

func (noopDataViewReferences) PinDataView(context.Context, int64, qviews.DataVersion) error {
	return nil
}

func (noopDataViewReferences) RecoverDataViewReference(context.Context, int64, qviews.DataVersion) (bool, error) {
	return true, nil
}

func (noopDataViewReferences) UnpinDataView(int64, qviews.DataVersion) {}

type dataViewProviderSource interface {
	DataViewProvider() balancer.DataViewProvider
}

type mixCoordDataViewProvider struct {
	mixCoord types.MixCoord
}

func (p *mixCoordDataViewProvider) DataViewSnapshot(ctx context.Context) *balancer.DataViewSnapshot {
	provider := p.provider()
	if provider == nil {
		return balancer.NewDataViewSnapshot(0, nil, nil)
	}
	return provider.DataViewSnapshot(ctx)
}

func (p *mixCoordDataViewProvider) DataViewSnapshotForCollections(ctx context.Context, collectionIDs map[int64]struct{}) *balancer.DataViewSnapshot {
	provider := p.provider()
	if provider == nil {
		return balancer.NewDataViewSnapshot(0, nil, nil)
	}
	return provider.DataViewSnapshotForCollections(ctx, collectionIDs)
}

func (p *mixCoordDataViewProvider) SegmentSnapshot(ctx context.Context, segmentIDs []int64) balancer.SegmentSnapshot {
	provider := p.provider()
	if provider == nil {
		return nil
	}
	return provider.SegmentSnapshot(ctx, segmentIDs)
}

func (p *mixCoordDataViewProvider) provider() balancer.DataViewProvider {
	source, ok := p.mixCoord.(dataViewProviderSource)
	if !ok {
		return nil
	}
	return source.DataViewProvider()
}

type emptyDataViewProvider struct{}

func (emptyDataViewProvider) DataViewSnapshot(context.Context) *balancer.DataViewSnapshot {
	return balancer.NewDataViewSnapshot(0, nil, nil)
}

func (emptyDataViewProvider) DataViewSnapshotForCollections(context.Context, map[int64]struct{}) *balancer.DataViewSnapshot {
	return balancer.NewDataViewSnapshot(0, nil, nil)
}

func (emptyDataViewProvider) SegmentSnapshot(context.Context, []int64) balancer.SegmentSnapshot {
	return nil
}
