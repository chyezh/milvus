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
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	mockstreamingcoordbalancer "github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	assignmentclient "github.com/milvus-io/milvus/internal/streamingcoord/client/assignment"
	streamingcoordbalancer "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	streamingcoordbalance "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	streamingtypes "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

func TestNewQViewsRuntimeRecoversLoadConfigAndQueryViews(t *testing.T) {
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return([]*querypb.CollectionLoadInfo{
		{DbID: 1, CollectionID: 100},
	}, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, []int64{100}).Return(map[int64][]*querypb.PartitionLoadInfo{
		100: {{CollectionID: 100, PartitionID: 10}},
	}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return([]*querypb.Replica{
		{ID: 1000, CollectionID: 100, ResourceGroup: "rg1"},
	}, nil).Once()

	shardID := qviews.ShardID{ReplicaID: 1000, VChannel: "v0"}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog: catalog,
		queryViewCatalog: &fakeQueryViewCatalog{
			views: []*viewpb.QueryViewOfShard{testPersistedQueryView(100, shardID)},
		},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
	})
	require.NoError(t, err)
	require.NotNil(t, runtime)
	require.NotNil(t, runtime.loadManager)
	require.NotNil(t, runtime.balancer)

	configs := runtime.loadConfigStore.Snapshot().ConfigsMap()
	require.Contains(t, configs, int64(100))
	assert.Equal(t, []int64{10}, configs[100].PartitionIDs)
	require.NotNil(t, runtime.shardViewRegistry.Get(shardID))
	assert.Contains(t, runtime.shardViewRegistry.Snapshot().StatsMap(), shardID)
}

type fakeRuntimeDataViewReferences struct {
	recovered []qviews.DataVersion
}

func (r *fakeRuntimeDataViewReferences) PinDataView(context.Context, int64, qviews.DataVersion) error {
	return nil
}

func (r *fakeRuntimeDataViewReferences) RecoverDataViewReference(_ context.Context, _ int64, version qviews.DataVersion) (bool, error) {
	r.recovered = append(r.recovered, version)
	return true, nil
}

func (r *fakeRuntimeDataViewReferences) UnpinDataView(int64, qviews.DataVersion) {}

func TestQViewsRuntimePassesReferenceManagerToRegistry(t *testing.T) {
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()
	refs := &fakeRuntimeDataViewReferences{}

	_, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog: catalog,
		queryViewCatalog: &fakeQueryViewCatalog{views: []*viewpb.QueryViewOfShard{
			testPersistedQueryView(100, qviews.ShardID{ReplicaID: 1000, VChannel: "v0"}),
		}},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		dataViewReferences:   refs,
	})
	require.NoError(t, err)
	require.Equal(t, []qviews.DataVersion{{StreamingVersion: 1, CompactVersion: 1}}, refs.recovered)
}

func TestNewQViewsRuntimeInstallsWALReplicaProvider(t *testing.T) {
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	walSnapshot := balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
		{
			ChannelID:     streamingtypes.ChannelID{Name: "p0", WALReplicaID: 2},
			AccessMode:    streamingtypes.AccessModeRO,
			ResourceGroup: "rg1",
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
	})
	provider := &fakeRuntimeWALReplicaProvider{snapshot: walSnapshot}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog:    catalog,
		queryViewCatalog:     &fakeQueryViewCatalog{},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		walReplicaProvider:   provider,
	})
	require.NoError(t, err)
	defaultBalancer, ok := runtime.balancer.(*balancer.DefaultBalancer)
	require.True(t, ok)
	defaultBalancer.Trigger()
	require.NoError(t, defaultBalancer.Reconcile(ctx))
	assert.Equal(t, 1, provider.calls)
}

func TestStreamingCoordWALReplicaProviderFlattensAssignments(t *testing.T) {
	provider := streamingCoordWALReplicaProvider{
		assignment: &fakeRuntimeAssignmentService{
			latest: &streamingtypes.VersionedStreamingNodeAssignments{
				Assignments: map[int64]streamingtypes.StreamingNodeAssignment{
					1: {
						WALReplicas: map[streamingtypes.ChannelID]streamingtypes.WALReplicaInfo{
							{Name: "p0", WALReplicaID: 2}: {
								ChannelID:     streamingtypes.ChannelID{Name: "p0", WALReplicaID: 2},
								AccessMode:    streamingtypes.AccessModeRO,
								ResourceGroup: "rg1",
								State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
							},
						},
					},
				},
			},
		},
	}

	snapshot := provider.WALReplicaSnapshot(context.Background())
	replicaID, ok := snapshot.Select("p0", "rg1")

	require.True(t, ok)
	assert.Equal(t, int64(2), replicaID)
}

func TestStreamingCoordWALReplicaProviderProjectsLegacyChannelAssignments(t *testing.T) {
	provider := streamingCoordWALReplicaProvider{
		assignment: &fakeRuntimeAssignmentService{
			latest: &streamingtypes.VersionedStreamingNodeAssignments{
				Assignments: map[int64]streamingtypes.StreamingNodeAssignment{
					1: {
						Channels: map[string]streamingtypes.PChannelInfo{
							"p0": {Name: "p0", Term: 7, AccessMode: streamingtypes.AccessModeRW},
						},
					},
				},
			},
		},
	}

	snapshot := provider.WALReplicaSnapshot(context.Background())
	replicaID, ok := snapshot.SelectByAccessMode("p0", "__default_resource_group", streamingtypes.AccessModeRW)

	require.True(t, ok)
	assert.Equal(t, int64(0), replicaID)
}

func TestStreamingCoordWALReplicaProviderProjectsLegacySecondaryChannelAssignments(t *testing.T) {
	provider := streamingCoordWALReplicaProvider{
		assignment: &fakeRuntimeAssignmentService{
			latest: &streamingtypes.VersionedStreamingNodeAssignments{
				Assignments: map[int64]streamingtypes.StreamingNodeAssignment{
					1: {
						SecondaryChannels: map[string]streamingtypes.PChannelInfo{
							"p0": {Name: "p0", Term: 7, AccessMode: streamingtypes.AccessModeRO},
						},
					},
				},
			},
		},
	}

	snapshot := provider.WALReplicaSnapshot(context.Background())
	replicaID, ok := snapshot.SelectByAccessMode("p0", "__default_resource_group", streamingtypes.AccessModeRO)

	require.True(t, ok)
	assert.Equal(t, int64(0), replicaID)
}

func TestStreamingCoordWALReplicaDemandExecutorDelegatesToStreamingNodeManager(t *testing.T) {
	snmanager.ResetStreamingNodeManager()
	defer func() {
		snmanager.StaticStreamingNodeManager.Close()
		streamingcoordbalance.ResetBalancer()
	}()

	b := mockstreamingcoordbalancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, cb streamingcoordbalancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
	b.EXPECT().EnsureReadOnlyWALReplica(mock.Anything, "p0", "rg1").Return(nil).Once()
	streamingcoordbalance.Register(b)

	err := streamingCoordWALReplicaDemandExecutor{}.EnsureReadOnlyWALReplica(context.Background(), balancer.WALReplicaDemand{
		PChannel:      "p0",
		ResourceGroup: "rg1",
	})

	require.NoError(t, err)
}

func TestStreamingCoordWALReplicaDemandExecutorDelegatesReleaseToStreamingNodeManager(t *testing.T) {
	snmanager.ResetStreamingNodeManager()
	defer func() {
		snmanager.StaticStreamingNodeManager.Close()
		streamingcoordbalance.ResetBalancer()
	}()

	b := mockstreamingcoordbalancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, cb streamingcoordbalancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
	b.EXPECT().ReleaseReadOnlyWALReplica(mock.Anything, "p0", int64(3)).Return(nil).Once()
	streamingcoordbalance.Register(b)

	err := streamingCoordWALReplicaDemandExecutor{}.ReleaseReadOnlyWALReplica(context.Background(), balancer.WALReplicaRelease{
		PChannel:     "p0",
		WALReplicaID: 3,
	})

	require.NoError(t, err)
}

func TestMaybeSwitchWALPrimaryReplicaForShardUpPromotesReadOnlyReplicaInPrimaryRG(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, "rg-new"))
	defer func() {
		assert.NoError(t, paramtable.Get().Remove(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key))
	}()

	executor := &fakeWALPrimarySwitchExecutor{}
	provider := &fakeRuntimeWALReplicaProvider{snapshot: balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
		{
			ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 0},
			AccessMode:    streamingtypes.AccessModeRW,
			ResourceGroup: "rg-old",
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
		{
			ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 1},
			AccessMode:    streamingtypes.AccessModeRO,
			ResourceGroup: "rg-new",
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
	})}

	switched, err := maybeSwitchWALPrimaryReplicaForShardUp(
		context.Background(),
		provider,
		executor,
		coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
			{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}: {
				UpVersion:              &qviews.QueryViewVersion{},
				UpWALReplicaID:         1,
				WALReplicaDependencies: map[int64]struct{}{1: {}},
			},
		}),
		qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"},
		1,
	)

	require.NoError(t, err)
	assert.True(t, switched)
	assert.Equal(t, []walPrimarySwitchCall{{pchannel: "by-dev-rootcoord-dml_0", targetReplicaID: 1}}, executor.calls)
}

func TestMaybeSwitchWALPrimaryReplicaForShardUpWaitsForAllPrimaryServingShards(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, "rg-new"))
	defer func() {
		assert.NoError(t, paramtable.Get().Remove(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key))
	}()

	executor := &fakeWALPrimarySwitchExecutor{}
	provider := &fakeRuntimeWALReplicaProvider{snapshot: balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
		{
			ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 0},
			AccessMode:    streamingtypes.AccessModeRW,
			ResourceGroup: "rg-old",
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
		{
			ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 1},
			AccessMode:    streamingtypes.AccessModeRO,
			ResourceGroup: "rg-new",
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
	})}
	currentPrimaryShard := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	targetReadyShard := qviews.ShardID{ReplicaID: 20, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	missingTargetShard := qviews.ShardID{ReplicaID: 11, VChannel: "by-dev-rootcoord-dml_0_100v1"}
	snapshot := coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
		currentPrimaryShard: {
			UpVersion:              &qviews.QueryViewVersion{},
			UpWALReplicaID:         0,
			WALReplicaDependencies: map[int64]struct{}{0: {}},
		},
		targetReadyShard: {
			UpVersion:              &qviews.QueryViewVersion{},
			UpWALReplicaID:         1,
			WALReplicaDependencies: map[int64]struct{}{1: {}},
		},
		missingTargetShard: {
			UpVersion:              &qviews.QueryViewVersion{},
			UpWALReplicaID:         0,
			WALReplicaDependencies: map[int64]struct{}{0: {}},
		},
	})

	switched, err := maybeSwitchWALPrimaryReplicaForShardUp(
		context.Background(),
		provider,
		executor,
		snapshot,
		targetReadyShard,
		1,
	)

	require.NoError(t, err)
	assert.False(t, switched)
	assert.Empty(t, executor.calls)
}

func TestMaybeSwitchWALPrimaryReplicaForShardUpRequiresSettingsAlignedTarget(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, "rg-new"))
	defer func() {
		assert.NoError(t, paramtable.Get().Remove(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key))
	}()

	newProvider := func() *fakeRuntimeWALReplicaProvider {
		return &fakeRuntimeWALReplicaProvider{snapshot: balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
			{
				ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 0},
				AccessMode:    streamingtypes.AccessModeRW,
				ResourceGroup: "rg-old",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 1},
				AccessMode:    streamingtypes.AccessModeRO,
				ResourceGroup: "rg-new",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		})}
	}

	currentPrimaryShard := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	targetShard := qviews.ShardID{ReplicaID: 20, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	mismatchedSnapshot := coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
		currentPrimaryShard: {
			UpVersion:              &qviews.QueryViewVersion{},
			UpLoadInfoVersion:      7,
			UpWALReplicaID:         0,
			WALReplicaDependencies: map[int64]struct{}{0: {}},
		},
		targetShard: {
			UpVersion:              &qviews.QueryViewVersion{},
			UpLoadInfoVersion:      8,
			UpWALReplicaID:         1,
			WALReplicaDependencies: map[int64]struct{}{1: {}},
		},
	})

	executor := &fakeWALPrimarySwitchExecutor{}
	switched, err := maybeSwitchWALPrimaryReplicaForShardUp(
		context.Background(),
		newProvider(),
		executor,
		mismatchedSnapshot,
		targetShard,
		1,
	)

	require.NoError(t, err)
	assert.False(t, switched)
	assert.Empty(t, executor.calls)

	alignedSnapshot := coordview.NewShardViewSnapshot(2, map[qviews.ShardID]*coordview.ShardStats{
		currentPrimaryShard: {
			UpVersion:              &qviews.QueryViewVersion{},
			UpLoadInfoVersion:      7,
			UpWALReplicaID:         0,
			WALReplicaDependencies: map[int64]struct{}{0: {}},
		},
		targetShard: {
			UpVersion:              &qviews.QueryViewVersion{},
			UpLoadInfoVersion:      7,
			UpWALReplicaID:         1,
			WALReplicaDependencies: map[int64]struct{}{1: {}},
		},
	})

	executor = &fakeWALPrimarySwitchExecutor{}
	switched, err = maybeSwitchWALPrimaryReplicaForShardUp(
		context.Background(),
		newProvider(),
		executor,
		alignedSnapshot,
		targetShard,
		1,
	)

	require.NoError(t, err)
	assert.True(t, switched)
	assert.Equal(t, []walPrimarySwitchCall{{pchannel: "by-dev-rootcoord-dml_0", targetReplicaID: 1}}, executor.calls)
}

func TestMaybeSwitchWALPrimaryReplicaForShardUpSkipsWhenPrimaryRGAlreadyHasReadWrite(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, "rg-new"))
	defer func() {
		assert.NoError(t, paramtable.Get().Remove(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key))
	}()

	executor := &fakeWALPrimarySwitchExecutor{}
	provider := &fakeRuntimeWALReplicaProvider{snapshot: balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
		{
			ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 1},
			AccessMode:    streamingtypes.AccessModeRW,
			ResourceGroup: "rg-new",
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
		{
			ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 2},
			AccessMode:    streamingtypes.AccessModeRO,
			ResourceGroup: "rg-new",
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
	})}

	switched, err := maybeSwitchWALPrimaryReplicaForShardUp(
		context.Background(),
		provider,
		executor,
		coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
			{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}: {
				UpVersion:              &qviews.QueryViewVersion{},
				UpWALReplicaID:         2,
				WALReplicaDependencies: map[int64]struct{}{2: {}},
			},
		}),
		qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"},
		2,
	)

	require.NoError(t, err)
	assert.False(t, switched)
	assert.Empty(t, executor.calls)
}

func TestMaybeSwitchWALPrimaryReplicaForShardUpSkipsWhenPrimaryRGHasAssigningReadWrite(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, "rg-new"))
	defer func() {
		assert.NoError(t, paramtable.Get().Remove(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key))
	}()

	executor := &fakeWALPrimarySwitchExecutor{}
	provider := &fakeRuntimeWALReplicaProvider{snapshot: balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
		{
			ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 1},
			AccessMode:    streamingtypes.AccessModeRW,
			ResourceGroup: "rg-new",
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
		},
		{
			ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 2},
			AccessMode:    streamingtypes.AccessModeRO,
			ResourceGroup: "rg-new",
			State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		},
	})}

	switched, err := maybeSwitchWALPrimaryReplicaForShardUp(
		context.Background(),
		provider,
		executor,
		coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
			{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}: {
				UpVersion:              &qviews.QueryViewVersion{},
				UpWALReplicaID:         2,
				WALReplicaDependencies: map[int64]struct{}{2: {}},
			},
		}),
		qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"},
		2,
	)

	require.NoError(t, err)
	assert.False(t, switched)
	assert.Empty(t, executor.calls)
}

func TestMaybeSwitchWALPrimaryReplicaForShardUpIgnoresConcurrentSwitchErrorAfterPrimaryReady(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, "rg-new"))
	defer func() {
		assert.NoError(t, paramtable.Get().Remove(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key))
	}()

	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	provider := &sequenceRuntimeWALReplicaProvider{snapshots: []*balancer.WALReplicaSnapshot{
		balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
			{
				ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 0},
				AccessMode:    streamingtypes.AccessModeRW,
				ResourceGroup: "rg-old",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 1},
				AccessMode:    streamingtypes.AccessModeRO,
				ResourceGroup: "rg-new",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		}),
		balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
			{
				ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 1},
				AccessMode:    streamingtypes.AccessModeRW,
				ResourceGroup: "rg-new",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		}),
	}}
	executor := &fakeWALPrimarySwitchExecutor{err: errors.New("concurrent switch already changed meta")}

	switched, err := maybeSwitchWALPrimaryReplicaForShardUp(
		context.Background(),
		provider,
		executor,
		coordview.NewShardViewSnapshot(1, map[qviews.ShardID]*coordview.ShardStats{
			shardID: {
				UpVersion:              &qviews.QueryViewVersion{},
				UpWALReplicaID:         1,
				WALReplicaDependencies: map[int64]struct{}{1: {}},
			},
		}),
		shardID,
		1,
	)

	require.NoError(t, err)
	assert.True(t, switched)
	assert.Equal(t, []walPrimarySwitchCall{{pchannel: "by-dev-rootcoord-dml_0", targetReplicaID: 1}}, executor.calls)
}

func TestNewQViewsRuntimeSwitchesWALPrimaryForRecoveredUpShard(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, paramtable.Get().Save(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, "rg-new"))
	defer func() {
		assert.NoError(t, paramtable.Get().Remove(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key))
	}()
	snmanager.ResetStreamingNodeManager()
	defer func() {
		snmanager.StaticStreamingNodeManager.Close()
		streamingcoordbalance.ResetBalancer()
	}()

	b := mockstreamingcoordbalancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, cb streamingcoordbalancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		})
	b.EXPECT().SetShardAssignmentProvider(mock.Anything).Return().Once()
	b.EXPECT().SetWALReplicaDependencyProvider(mock.Anything).Return().Once()
	b.EXPECT().TriggerShardAssignmentUpdate().Return().Once()
	streamingcoordbalance.Register(b)

	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	executor := &fakeWALPrimarySwitchExecutor{}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog: catalog,
		queryViewCatalog: &fakeQueryViewCatalog{
			views: []*viewpb.QueryViewOfShard{
				testPersistedQueryViewWithWALReplica(100, shardID, 1),
			},
		},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		walReplicaProvider: &fakeRuntimeWALReplicaProvider{snapshot: balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
			{
				ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 0},
				AccessMode:    streamingtypes.AccessModeRW,
				ResourceGroup: "rg-old",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 1},
				AccessMode:    streamingtypes.AccessModeRO,
				ResourceGroup: "rg-new",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		})},
		walPrimarySwitcher: executor,
		balancerFactory: func(*balancer.SnapshotBuilder) qviewsBalancer {
			return &fakeRuntimeBalancer{}
		},
	})

	require.NoError(t, err)
	defer runtime.stop()

	runtime.start(ctx)

	assert.Equal(t, []walPrimarySwitchCall{{pchannel: "by-dev-rootcoord-dml_0", targetReplicaID: 1}}, executor.calls)
}

func TestQViewsRuntimeSwitchesWALPrimaryWhenPrimaryResourceGroupChanges(t *testing.T) {
	paramtable.Init()
	assert.NoError(t, paramtable.Get().Remove(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key))
	defer func() {
		assert.NoError(t, paramtable.Get().Remove(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key))
	}()
	snmanager.ResetStreamingNodeManager()
	defer func() {
		snmanager.StaticStreamingNodeManager.Close()
		streamingcoordbalance.ResetBalancer()
	}()

	b := mockstreamingcoordbalancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, cb streamingcoordbalancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		})
	b.EXPECT().SetShardAssignmentProvider(mock.Anything).Return().Once()
	b.EXPECT().SetWALReplicaDependencyProvider(mock.Anything).Return().Once()
	b.EXPECT().TriggerShardAssignmentUpdate().Return().Once()
	streamingcoordbalance.Register(b)

	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	executor := &fakeWALPrimarySwitchExecutor{}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog: catalog,
		queryViewCatalog: &fakeQueryViewCatalog{
			views: []*viewpb.QueryViewOfShard{
				testPersistedQueryViewWithWALReplica(100, shardID, 1),
			},
		},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		walReplicaProvider: &fakeRuntimeWALReplicaProvider{snapshot: balancer.NewWALReplicaSnapshot([]streamingtypes.WALReplicaInfo{
			{
				ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 0},
				AccessMode:    streamingtypes.AccessModeRW,
				ResourceGroup: "rg-old",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ChannelID:     streamingtypes.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 1},
				AccessMode:    streamingtypes.AccessModeRO,
				ResourceGroup: "rg-new",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		})},
		walPrimarySwitcher: executor,
		balancerFactory: func(*balancer.SnapshotBuilder) qviewsBalancer {
			return &fakeRuntimeBalancer{}
		},
	})

	require.NoError(t, err)
	defer runtime.stop()

	runtime.start(ctx)
	require.Empty(t, executor.calls)

	require.NoError(t, paramtable.Get().Save(paramtable.Get().StreamingCfg.PrimaryResourceGroup.Key, "rg-new"))
	require.Eventually(t, func() bool {
		return len(executor.calls) == 1
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, []walPrimarySwitchCall{{pchannel: "by-dev-rootcoord-dml_0", targetReplicaID: 1}}, executor.calls)
}

func TestQViewsWALReplicaDependencyProviderCombinesRegistryAndSyncer(t *testing.T) {
	replicaID := streamingtypes.ChannelID{Name: "p0", WALReplicaID: 3}

	provider := qviewsWALReplicaDependencyProvider{
		registry: staticRuntimeWALReplicaDependencyProvider{
			dependencies: map[streamingtypes.ChannelID]bool{replicaID: true},
		},
		syncer: staticRuntimeWALReplicaDependencyProvider{},
	}
	assert.True(t, provider.HasWALReplicaDependency(replicaID))

	provider = qviewsWALReplicaDependencyProvider{
		registry: staticRuntimeWALReplicaDependencyProvider{},
		syncer: staticRuntimeWALReplicaDependencyProvider{
			dependencies: map[streamingtypes.ChannelID]bool{replicaID: true},
		},
	}
	assert.True(t, provider.HasWALReplicaDependency(replicaID))

	provider = qviewsWALReplicaDependencyProvider{
		registry: staticRuntimeWALReplicaDependencyProvider{},
		syncer:   staticRuntimeWALReplicaDependencyProvider{},
	}
	assert.False(t, provider.HasWALReplicaDependency(replicaID))
}

func TestQViewsRuntimeStartRegistersStreamingCoordProviders(t *testing.T) {
	snmanager.ResetStreamingNodeManager()
	defer func() {
		snmanager.StaticStreamingNodeManager.Close()
		streamingcoordbalance.ResetBalancer()
	}()

	b := mockstreamingcoordbalancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, cb streamingcoordbalancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		})
	b.EXPECT().SetShardAssignmentProvider(mock.Anything).Return().Once()
	b.EXPECT().SetWALReplicaDependencyProvider(mock.Anything).Return().Once()
	b.EXPECT().TriggerShardAssignmentUpdate().Return().Once()
	streamingcoordbalance.Register(b)

	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	fakeBalancer := &fakeRuntimeBalancer{}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog:    catalog,
		queryViewCatalog:     &fakeQueryViewCatalog{},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		balancerFactory: func(*balancer.SnapshotBuilder) qviewsBalancer {
			return fakeBalancer
		},
	})
	require.NoError(t, err)
	defer runtime.stop()

	runtime.start(ctx)

	assert.True(t, fakeBalancer.started)
}

func TestQViewsRuntimeLoadManagerTriggersBalancer(t *testing.T) {
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	fakeBalancer := &fakeRuntimeBalancer{}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog:    catalog,
		queryViewCatalog:     &fakeQueryViewCatalog{},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		balancerFactory: func(*balancer.SnapshotBuilder) qviewsBalancer {
			return fakeBalancer
		},
	})
	require.NoError(t, err)

	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, runtime.loadManager.UpdateLoadConfig(ctx, testAlterLoadConfigResult()))

	assert.Empty(t, runtime.shardViewRegistry.ShardIDs())
	assert.Equal(t, []balancer.TriggerScope{{DirtyCollections: []int64{100}}}, fakeBalancer.triggers)
}

func TestQViewsRuntimeQueryNodeDownMarksAffectedViewsAndTriggersBalancer(t *testing.T) {
	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	fakeBalancer := &fakeRuntimeBalancer{}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog: catalog,
		queryViewCatalog: &fakeQueryViewCatalog{
			views: []*viewpb.QueryViewOfShard{
				testPersistedQueryViewWithQueryNode(100, shardID, 7, 101),
			},
		},
		viewSyncClient:       &fakeRuntimeViewSyncClient{},
		queryNodeClient:      &fakeRuntimeQueryNodeClient{},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		balancerFactory: func(*balancer.SnapshotBuilder) qviewsBalancer {
			return fakeBalancer
		},
	})
	require.NoError(t, err)

	runtime.handleQueryNodeDown(7)

	assert.Equal(t, []balancer.TriggerScope{{DirtyShards: []qviews.ShardID{shardID}}}, fakeBalancer.triggers)
	stats := runtime.shardViewRegistry.Snapshot().StatsMap()[shardID]
	require.NotNil(t, stats)
	assert.Nil(t, stats.UpVersion)
	assert.Equal(t, coordview.SegmentStateUnrecoverable, stats.Segments[101].Nodes[7])
}

func TestQViewsRuntimeStartMarksRecoveredViewsOnMissingQueryNodes(t *testing.T) {
	snmanager.ResetStreamingNodeManager()
	defer func() {
		snmanager.StaticStreamingNodeManager.Close()
		streamingcoordbalance.ResetBalancer()
	}()

	b := mockstreamingcoordbalancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, cb streamingcoordbalancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		})
	b.EXPECT().SetShardAssignmentProvider(mock.Anything).Return().Once()
	b.EXPECT().SetWALReplicaDependencyProvider(mock.Anything).Return().Once()
	b.EXPECT().TriggerShardAssignmentUpdate().Return().Once()
	streamingcoordbalance.Register(b)

	ctx := context.Background()
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().GetCollections(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).
		Return(map[int64][]*querypb.PartitionLoadInfo{}, nil).Once()
	catalog.EXPECT().GetReplicas(mock.Anything).Return(nil, nil).Once()

	shardID := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	fakeBalancer := &fakeRuntimeBalancer{}
	runtime, err := newQViewsRuntime(ctx, qviewsRuntimeDependencies{
		queryCoordCatalog: catalog,
		queryViewCatalog: &fakeQueryViewCatalog{
			views: []*viewpb.QueryViewOfShard{
				testPersistedQueryViewWithQueryNode(100, shardID, 7, 101),
			},
		},
		viewSyncClient: &fakeRuntimeViewSyncClient{},
		queryNodeClient: &fakeRuntimeQueryNodeClient{
			nodes: map[int64]*qnmanager.NodeInfo{
				17: {ServerID: 17},
				20: {ServerID: 20},
			},
		},
		resourceGroupManager: &fakeRuntimeResourceGroupManager{},
		dataViewProvider:     &fakeRuntimeDataViewProvider{},
		balancerFactory: func(*balancer.SnapshotBuilder) qviewsBalancer {
			return fakeBalancer
		},
	})
	require.NoError(t, err)
	defer runtime.stop()

	runtime.start(ctx)

	assert.Equal(t, []balancer.TriggerScope{{DirtyShards: []qviews.ShardID{shardID}}}, fakeBalancer.triggers)
	stats := runtime.shardViewRegistry.Snapshot().StatsMap()[shardID]
	require.NotNil(t, stats)
	assert.Nil(t, stats.UpVersion)
	assert.Equal(t, coordview.SegmentStateUnrecoverable, stats.Segments[101].Nodes[7])
}

func testPersistedQueryView(collectionID int64, shardID qviews.ShardID) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: collectionID,
			ReplicaId:    shardID.ReplicaID,
			Vchannel:     shardID.VChannel,
			State:        viewpb.QueryViewState_QueryViewStateUp,
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
				QueryVersion: 1,
			},
		},
	}
}

func testPersistedQueryViewWithWALReplica(collectionID int64, shardID qviews.ShardID, walReplicaID int64) *viewpb.QueryViewOfShard {
	view := testPersistedQueryView(collectionID, shardID)
	view.StreamingNode = &viewpb.QueryViewOfStreamingNode{WalReplicaId: walReplicaID}
	return view
}

func testPersistedQueryViewWithQueryNode(collectionID int64, shardID qviews.ShardID, nodeID int64, segmentID int64) *viewpb.QueryViewOfShard {
	view := testPersistedQueryView(collectionID, shardID)
	view.StreamingNode = &viewpb.QueryViewOfStreamingNode{}
	view.QueryNode = []*viewpb.QueryViewOfQueryNode{
		{
			NodeId: nodeID,
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{segmentID}},
			},
		},
	}
	return view
}

func testAlterLoadConfigResult() message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := funcutil.GetControlChannel("test")
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.AlterLoadConfigMessageHeader{
			DbId:         1,
			CollectionId: 100,
			PartitionIds: []int64{10},
			Replicas: []*messagespb.LoadReplicaConfig{
				{ReplicaId: 1000, ResourceGroupName: "rg1", Priority: commonpb.LoadPriority_HIGH},
			},
		}).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()
	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{controlChannel: {}},
	}
}

type fakeQueryViewCatalog struct {
	views []*viewpb.QueryViewOfShard
	saves [][]*viewpb.QueryViewOfShard
}

func (c *fakeQueryViewCatalog) ListQueryViews(context.Context) ([]*viewpb.QueryViewOfShard, error) {
	return c.views, nil
}

func (c *fakeQueryViewCatalog) SaveQueryViews(_ context.Context, views []*viewpb.QueryViewOfShard) error {
	c.saves = append(c.saves, views)
	return nil
}

type fakeRuntimeViewSyncClient struct{}

func (c *fakeRuntimeViewSyncClient) RegisterNodeChangedNotifier(func()) {}
func (c *fakeRuntimeViewSyncClient) IsNodeAlive(context.Context, qviews.WorkNode) bool {
	return true
}

func (c *fakeRuntimeViewSyncClient) OpenSyncStream(ctx context.Context, _ qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	return newFakeRuntimeViewSyncStream(ctx), nil
}
func (c *fakeRuntimeViewSyncClient) Close() {}

type fakeRuntimeViewSyncStream struct {
	ctx    context.Context
	sendCh chan *viewpb.SyncRequest
}

func newFakeRuntimeViewSyncStream(ctx context.Context) *fakeRuntimeViewSyncStream {
	return &fakeRuntimeViewSyncStream{
		ctx:    ctx,
		sendCh: make(chan *viewpb.SyncRequest, 16),
	}
}

func (s *fakeRuntimeViewSyncStream) Send(req *viewpb.SyncRequest) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.sendCh <- req:
		return nil
	}
}

func (s *fakeRuntimeViewSyncStream) Recv() (*viewpb.SyncResponse, error) {
	<-s.ctx.Done()
	return nil, io.EOF
}

func (s *fakeRuntimeViewSyncStream) Header() (metadata.MD, error) { return nil, nil }
func (s *fakeRuntimeViewSyncStream) Trailer() metadata.MD         { return nil }
func (s *fakeRuntimeViewSyncStream) CloseSend() error             { return nil }
func (s *fakeRuntimeViewSyncStream) Context() context.Context     { return s.ctx }
func (s *fakeRuntimeViewSyncStream) SendMsg(interface{}) error    { return nil }
func (s *fakeRuntimeViewSyncStream) RecvMsg(interface{}) error    { return nil }

type fakeRuntimeQueryNodeClient struct {
	nodes map[int64]*qnmanager.NodeInfo
	err   error
}

func (c *fakeRuntimeQueryNodeClient) RegisterNodeChangedNotifier(func()) {}
func (c *fakeRuntimeQueryNodeClient) GetAllQueryNodes(context.Context) (map[int64]*qnmanager.NodeInfo, error) {
	return c.nodes, c.err
}

type fakeRuntimeResourceGroupManager struct{}

func (m *fakeRuntimeResourceGroupManager) ListResourceGroups(context.Context) []string {
	return nil
}

func (m *fakeRuntimeResourceGroupManager) GetNodes(context.Context, string) ([]int64, error) {
	return nil, nil
}

type fakeRuntimeDataViewProvider struct{}

func (p *fakeRuntimeDataViewProvider) DataViewSnapshot(context.Context) *balancer.DataViewSnapshot {
	return balancer.NewDataViewSnapshot(0, nil, nil)
}

func (p *fakeRuntimeDataViewProvider) DataViewSnapshotForCollections(context.Context, map[int64]struct{}) *balancer.DataViewSnapshot {
	return balancer.NewDataViewSnapshot(0, nil, nil)
}

func (p *fakeRuntimeDataViewProvider) SegmentSnapshot(context.Context, []int64) balancer.SegmentSnapshot {
	return nil
}

type fakeRuntimeWALReplicaProvider struct {
	snapshot *balancer.WALReplicaSnapshot
	calls    int
}

func (p *fakeRuntimeWALReplicaProvider) WALReplicaSnapshot(context.Context) *balancer.WALReplicaSnapshot {
	p.calls++
	return p.snapshot
}

type sequenceRuntimeWALReplicaProvider struct {
	snapshots []*balancer.WALReplicaSnapshot
	next      int
}

func (p *sequenceRuntimeWALReplicaProvider) WALReplicaSnapshot(context.Context) *balancer.WALReplicaSnapshot {
	if len(p.snapshots) == 0 {
		return balancer.NewWALReplicaSnapshot(nil)
	}
	if p.next >= len(p.snapshots) {
		return p.snapshots[len(p.snapshots)-1]
	}
	snapshot := p.snapshots[p.next]
	p.next++
	return snapshot
}

type walPrimarySwitchCall struct {
	pchannel        string
	targetReplicaID int64
}

type fakeWALPrimarySwitchExecutor struct {
	calls []walPrimarySwitchCall
	err   error
}

func (e *fakeWALPrimarySwitchExecutor) SwitchWALPrimaryReplica(_ context.Context, pchannel string, targetReplicaID int64) error {
	e.calls = append(e.calls, walPrimarySwitchCall{
		pchannel:        pchannel,
		targetReplicaID: targetReplicaID,
	})
	return e.err
}

type fakeRuntimeAssignmentService struct {
	latest *streamingtypes.VersionedStreamingNodeAssignments
	err    error
}

func (s *fakeRuntimeAssignmentService) AssignmentDiscover(context.Context, func(*streamingtypes.VersionedStreamingNodeAssignments) error) error {
	return nil
}

func (s *fakeRuntimeAssignmentService) ReportAssignmentError(context.Context, streamingtypes.PChannelInfo, error) error {
	return nil
}

func (s *fakeRuntimeAssignmentService) ReportWALReplicaAssignmentError(context.Context, streamingtypes.PChannelInfoAssigned, error) error {
	return nil
}

func (s *fakeRuntimeAssignmentService) GetLatestStreamingVersion(context.Context) (*streamingpb.StreamingVersion, error) {
	return nil, nil
}

func (s *fakeRuntimeAssignmentService) UpdateReplicateConfiguration(context.Context, *milvuspb.UpdateReplicateConfigurationRequest) error {
	return nil
}

func (s *fakeRuntimeAssignmentService) GetReplicateConfiguration(context.Context, ...assignmentclient.GetReplicateConfigurationOpt) (*replicateutil.ConfigHelper, error) {
	return nil, nil
}

func (s *fakeRuntimeAssignmentService) GetLatestAssignments(context.Context) (*streamingtypes.VersionedStreamingNodeAssignments, error) {
	return s.latest, s.err
}

func (s *fakeRuntimeAssignmentService) UpdateWALBalancePolicy(context.Context, *streamingtypes.UpdateWALBalancePolicyRequest) (*streamingtypes.UpdateWALBalancePolicyResponse, error) {
	return nil, nil
}

type staticRuntimeWALReplicaDependencyProvider struct {
	dependencies map[streamingtypes.ChannelID]bool
}

func (p staticRuntimeWALReplicaDependencyProvider) HasWALReplicaDependency(replicaID streamingtypes.ChannelID) bool {
	return p.dependencies[replicaID]
}

type fakeRuntimeBalancer struct {
	started  bool
	stopped  bool
	triggers []balancer.TriggerScope
}

func (b *fakeRuntimeBalancer) Start(context.Context) {
	b.started = true
}

func (b *fakeRuntimeBalancer) Stop() {
	b.stopped = true
}

func (b *fakeRuntimeBalancer) Trigger(scopes ...balancer.TriggerScope) {
	b.triggers = append(b.triggers, scopes...)
}

var (
	_ syncer.ViewSyncClient           = (*fakeRuntimeViewSyncClient)(nil)
	_ balancer.DataViewProvider       = (*fakeRuntimeDataViewProvider)(nil)
	_ loadmgr.DirtyCollectionNotifier = func(int64) {}
)
