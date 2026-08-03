package balancer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type testSnapshotSource struct {
	snapshot     *BalancerSnapshot
	afterCapture func()
}

func (s *testSnapshotSource) build(_ context.Context, pending triggerBatch) (*BalancerSnapshot, []qviews.ShardID) {
	snapshot := s.snapshot
	if s.afterCapture != nil {
		s.afterCapture()
	}
	shardSet := make(map[qviews.ShardID]struct{}, len(snapshot.ShardStatsMap())+len(pending.dirtyShards))
	for shardID := range snapshot.ShardStatsMap() {
		shardSet[shardID] = struct{}{}
	}
	for shardID := range pending.dirtyShards {
		shardSet[shardID] = struct{}{}
	}
	shards := make([]qviews.ShardID, 0, len(shardSet))
	for shardID := range shardSet {
		shards = append(shards, shardID)
	}
	return snapshot, shards
}

func TestBalancer_ReconcileDirtyShardAppliesPrepare(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}

	store := storeWithConfig(t, collID, replicaID, []int64{100}, []int64{1, 2})
	reg := emptyRegistry(t)
	reg.Ensure(shardID)

	builder := NewSnapshotBuilder(
		store,
		reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{
			1: {NodeID: 1, Alive: true},
			2: {NodeID: 2, Alive: true},
		}},
		&fakeDataViewProvider{
			collections: []*viewpb.DataViewOfCollection{{
				CollectionId: collID,
				DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
				Shards: []*viewpb.DataViewOfShard{
					shardDataView(shardID.VChannel, 100, 101, 102),
				},
			}},
			segments: map[int64]*SegmentInfo{
				101: {SegmentID: 101, PartitionID: 100, RowNum: 600},
				102: {SegmentID: 102, PartitionID: 100, RowNum: 200},
			},
		},
		policyTestConfig(),
	)
	b := NewDefaultBalancer(builder, reg, nil)

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
	require.NoError(t, b.Reconcile(context.Background()))

	stats := reg.Get(shardID).Stats()
	require.NotNil(t, stats)
	assert.NotNil(t, stats.PreparingVersion)
	assert.NotEmpty(t, stats.Segments)
}

func TestBalancer_ReconcileFullScanRecreatesRecoveredUnrecoverableView(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}

	recoveredView := qviews.NewQueryViewAtCoordBuilder(
		replicaID,
		&viewpb.DataViewOfCollection{
			CollectionId: collID,
			DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
			Shards:       []*viewpb.DataViewOfShard{shardDataView(shardID.VChannel, 100, 101)},
		},
		shardID.VChannel,
	).
		SetWALReplicaID(2).
		SetAssignments(map[int64]map[int64][]int64{
			7: {100: {101}},
		}).
		Build()
	recoveredView.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable

	reg, err := coordview.RecoverShardViewRegistry(
		context.Background(),
		&staticQueryViewCatalog{views: []*viewpb.QueryViewOfShard{recoveredView}},
		&stubSyncer{},
	)
	require.NoError(t, err)
	store := storeWithConfigInResourceGroup(t, collID, replicaID, []int64{100}, []int64{20}, "rg1")
	builder := NewSnapshotBuilder(
		store,
		reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{
			20: {NodeID: 20, Alive: true, ResourceGroup: "rg1"},
		}},
		&fakeDataViewProvider{
			collections: []*viewpb.DataViewOfCollection{{
				CollectionId: collID,
				DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
				Shards:       []*viewpb.DataViewOfShard{shardDataView(shardID.VChannel, 100, 101)},
			}},
			segments: map[int64]*SegmentInfo{
				101: {SegmentID: 101, PartitionID: 100, RowNum: 100},
			},
		},
		policyTestConfig(),
	)
	builder.SetWALReplicaProvider(&fakeWALReplicaProvider{
		snapshot: NewWALReplicaSnapshot([]types.WALReplicaInfo{
			{
				ChannelID:     types.ChannelID{Name: "by-dev-rootcoord-dml_0", WALReplicaID: 2},
				AccessMode:    types.AccessModeRW,
				ResourceGroup: "rg1",
				State:         streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		}),
	})
	b := NewDefaultBalancer(builder, reg, nil)

	b.Trigger()
	require.NoError(t, b.Reconcile(context.Background()))

	stats := reg.Get(shardID).Stats()
	require.NotNil(t, stats.PreparingVersion)
	assert.Nil(t, stats.UpVersion)
	assert.Equal(t, map[int64]coordview.SegmentState{20: coordview.SegmentStatePreparing}, stats.Segments[101].Nodes)
}

func TestBalancer_ReconcileAppliesWALReplicaDemand(t *testing.T) {
	const replicaID int64 = 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}

	reg := emptyRegistry(t)
	reg.Ensure(shardID)
	executor := &fakeWALReplicaDemandExecutor{}
	b := &DefaultBalancer{
		snapshotBuilder: &testSnapshotSource{snapshot: &BalancerSnapshot{}},
		viewRegistry:    reg,
		policy: fakeBalancePolicy{
			plan: &BalancePlan{
				WALReplicaDemands: []WALReplicaDemand{{PChannel: "by-dev-rootcoord-dml_0", ResourceGroup: "rg1"}},
			},
		},
		queue: newTriggerQueue(),
	}
	b.SetWALReplicaDemandExecutor(executor)

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
	require.NoError(t, b.Reconcile(context.Background()))

	assert.Equal(t, []WALReplicaDemand{{PChannel: "by-dev-rootcoord-dml_0", ResourceGroup: "rg1"}}, executor.demands)
	assert.Nil(t, reg.Get(shardID).Stats().PreparingVersion)
	assert.False(t, b.queue.takePending().empty())
}

func TestBalancer_ReconcileAppliesWALReplicaRelease(t *testing.T) {
	const replicaID int64 = 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "by-dev-rootcoord-dml_0_1v0"}

	reg := emptyRegistry(t)
	reg.Ensure(shardID)
	executor := &fakeWALReplicaDemandExecutor{}
	b := &DefaultBalancer{
		snapshotBuilder: &testSnapshotSource{snapshot: &BalancerSnapshot{}},
		viewRegistry:    reg,
		policy: fakeBalancePolicy{
			plan: &BalancePlan{
				WALReplicaReleases: []WALReplicaRelease{{PChannel: "by-dev-rootcoord-dml_0", WALReplicaID: 3}},
			},
		},
		queue: newTriggerQueue(),
	}
	b.SetWALReplicaDemandExecutor(executor)

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
	require.NoError(t, b.Reconcile(context.Background()))

	assert.Equal(t, []WALReplicaRelease{{PChannel: "by-dev-rootcoord-dml_0", WALReplicaID: 3}}, executor.releases)
}

func TestBalancer_ReconcileDirtyCollectionCreatesDataViewShards(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}

	store := storeWithConfig(t, collID, replicaID, []int64{100}, []int64{1})
	reg := emptyRegistry(t)
	builder := NewSnapshotBuilder(
		store,
		reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{
			1: {NodeID: 1, Alive: true},
		}},
		&fakeDataViewProvider{
			collections: []*viewpb.DataViewOfCollection{{
				CollectionId: collID,
				DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
				Shards:       []*viewpb.DataViewOfShard{shardDataView(shardID.VChannel, 100, 101)},
			}},
			segments: map[int64]*SegmentInfo{
				101: {SegmentID: 101, PartitionID: 100, RowNum: 100},
			},
		},
		policyTestConfig(),
	)
	b := NewDefaultBalancer(builder, reg, nil)

	b.Trigger(TriggerScope{DirtyCollections: []int64{collID}})
	require.NoError(t, b.Reconcile(context.Background()))

	mgr := reg.Get(shardID)
	require.NotNil(t, mgr)
	stats := mgr.Stats()
	assert.NotNil(t, stats.PreparingVersion)
}

func TestBalancer_ReconcilePreservesTriggerArrivingDuringSnapshotBuild(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{
		ReplicaID: replicaID,
		VChannel:  "by-dev-rootcoord-dml_0_1v0",
	}

	reg := emptyRegistry(t)
	addShardWithPreparingView(t, reg, shardID, map[int64]map[int64][]int64{
		1: {100: {101}},
	})

	viewSnapshot := reg.Snapshot()
	source := &testSnapshotSource{
		snapshot: &BalancerSnapshot{
			LoadConfigSnapshot: loadmgr.NewLoadConfigSnapshot(1, map[int64]*loadmgr.LoadConfig{
				collID: cfgFor(collID, replicaID, nil, nil),
			}),
			ShardViewSnapshot: viewSnapshot,
		},
	}
	b := &DefaultBalancer{
		snapshotBuilder: source,
		viewRegistry:    reg,
		policy:          NewDefaultBalancePolicy(),
		queue:           newTriggerQueue(),
	}
	source.afterCapture = func() {
		source.afterCapture = nil
		source.snapshot = &BalancerSnapshot{
			LoadConfigSnapshot: loadmgr.NewLoadConfigSnapshot(2, map[int64]*loadmgr.LoadConfig{}),
			ShardViewSnapshot:  viewSnapshot,
		}
		b.Trigger(TriggerScope{DirtyCollections: []int64{collID}})
	}

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
	require.NoError(t, b.Reconcile(context.Background()))
	require.NotNil(t, reg.Get(shardID).Stats().PreparingVersion)

	require.NoError(t, b.Reconcile(context.Background()))
	stats := reg.Get(shardID).Stats()
	assert.Nil(t, stats.PreparingVersion)
	assert.Empty(t, stats.Segments)
}

func TestBalancer_NodeChangedNotifierTriggersFullScan(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}

	store := storeWithConfig(t, collID, replicaID, []int64{100}, []int64{1})
	reg := emptyRegistry(t)
	reg.Ensure(shardID)
	nodeProvider := &fakeNodeProvider{infos: map[int64]*NodeInfo{
		1: {NodeID: 1, Alive: true},
	}}
	builder := NewSnapshotBuilder(
		store,
		reg,
		nodeProvider,
		&fakeDataViewProvider{
			collections: []*viewpb.DataViewOfCollection{{
				CollectionId: collID,
				DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
				Shards:       []*viewpb.DataViewOfShard{shardDataView(shardID.VChannel, 100, 101)},
			}},
			segments: map[int64]*SegmentInfo{
				101: {SegmentID: 101, PartitionID: 100, RowNum: 100},
			},
		},
		policyTestConfig(),
	)
	b := NewDefaultBalancer(builder, reg, nil)

	nodeProvider.notifyNodeChanged()
	require.NoError(t, b.Reconcile(context.Background()))

	stats := reg.Get(shardID).Stats()
	assert.NotNil(t, stats.PreparingVersion)
}

func TestBalancer_ReconcileFullScanDoesNotRestackPreparing(t *testing.T) {
	const collID, replicaID int64 = 1, 10
	shardID := qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}

	store := storeWithConfig(t, collID, replicaID, []int64{100}, []int64{1})
	reg := emptyRegistry(t)
	reg.Ensure(shardID)
	builder := NewSnapshotBuilder(
		store,
		reg,
		&fakeNodeProvider{infos: map[int64]*NodeInfo{
			1: {NodeID: 1, Alive: true},
		}},
		&fakeDataViewProvider{
			collections: []*viewpb.DataViewOfCollection{{
				CollectionId: collID,
				DataVersion:  (&qviews.DataVersion{StreamingVersion: 1}).IntoProto(),
				Shards:       []*viewpb.DataViewOfShard{shardDataView(shardID.VChannel, 100, 101)},
			}},
			segments: map[int64]*SegmentInfo{
				101: {SegmentID: 101, PartitionID: 100, RowNum: 100},
			},
		},
		policyTestConfig(),
	)
	b := NewDefaultBalancer(builder, reg, nil)

	b.Trigger(TriggerScope{DirtyShards: []qviews.ShardID{shardID}})
	require.NoError(t, b.Reconcile(context.Background()))
	stats := reg.Get(shardID).Stats()
	require.NotNil(t, stats.PreparingVersion)
	before := stats.Segments

	b.Trigger()
	require.NoError(t, b.Reconcile(context.Background()))
	after := reg.Get(shardID).Stats().Segments
	assert.Equal(t, before, after)
}

func TestBalancer_StartStop(t *testing.T) {
	reg := emptyRegistry(t)
	b := NewDefaultBalancer(nil, reg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	b.Trigger()
	time.Sleep(10 * time.Millisecond)
	b.Stop()
	b.Stop()
}
