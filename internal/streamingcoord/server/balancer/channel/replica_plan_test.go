package channel

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestReplicaPlanIdentityAndTerm(t *testing.T) {
	m := NewPChannelMeta("p", types.AccessModeRW).CopyForWrite()
	require.Nil(t, m.ReplicaPlan())
	target := ReplicaPlanTarget{
		PrimaryResourceGroup: "b",
		Nodes: map[string]*types.StreamingNodeInfo{
			"a": {ServerID: 1, Address: "a"},
			"b": {ServerID: 2, Address: "b"},
			"c": {ServerID: 3, Address: "c"},
		},
	}
	changed, err := m.assignReplicaPlan(target)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int64(2), m.CurrentTerm())
	require.Equal(t, int64(2), m.CurrentServerID())
	plan := m.ReplicaPlan()
	require.Equal(t, uint64(1), plan.Revision)
	require.Len(t, plan.Replicas, 3)
	for i, member := range plan.Replicas {
		require.Equal(t, uint64(i+1), member.ReplicaId)
		require.EqualValues(t, 1, member.ReplicaRevision)
		if member.ResourceGroup == "b" {
			require.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE, member.AccessMode)
		} else {
			require.Equal(t, streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, member.AccessMode)
		}
	}
	m.AssignToServerDone()
	changed, err = m.assignReplicaPlan(target)
	require.NoError(t, err)
	require.False(t, changed)

	// Join, placement loss and an intra-RG move never fence a healthy writer.
	target.Nodes["d"] = nil
	target.Nodes["a"] = &types.StreamingNodeInfo{ServerID: 4, Address: "a2"}
	changed, err = m.assignReplicaPlan(target)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int64(2), m.CurrentTerm())
	require.Equal(t, uint64(1), m.ReplicaPlan().Replicas[0].ReplicaId)
	require.EqualValues(t, 2, m.ReplicaPlan().Replicas[0].ReplicaRevision)
	require.EqualValues(t, 1, m.ReplicaPlan().Replicas[1].ReplicaRevision)
	require.Len(t, m.ReplicaPlan().RetiringReplicas, 1)
	require.Nil(t, m.ReplicaPlan().Replicas[3].Node)

	// Retirement followed by rejoin must not reuse the old member identity.
	delete(target.Nodes, "c")
	_, err = m.assignReplicaPlan(target)
	require.NoError(t, err)
	target.Nodes["c"] = &types.StreamingNodeInfo{ServerID: 5, Address: "c2"}
	target.PrimaryResourceGroup = "c"
	_, err = m.assignReplicaPlan(target)
	require.NoError(t, err)
	plan = m.ReplicaPlan()
	require.Equal(t, int64(3), plan.WalTerm)
	require.Equal(t, uint64(5), plan.Replicas[2].ReplicaId)
	require.EqualValues(t, 2, plan.Replicas[0].ReplicaRevision, "a remote term change preserves the RO instance")
	require.Len(t, plan.RetiringReplicas, 3)
	require.Len(t, m.AssignHistories(), 1)
	require.Equal(t, int64(2), m.AssignHistories()[0].Node.ServerID)
	plan.Replicas[0].ResourceGroup = "mutated"
	require.Equal(t, "a", m.ReplicaPlan().Replicas[0].ResourceGroup)

	// Re-delivery during preparation preserves the accepted source lifetime.
	_, err = m.assignReplicaPlan(target)
	require.NoError(t, err)
	require.EqualValues(t, 3, m.CurrentTerm())
	// An explicit failure, including failure before READY, permits a new term.
	m.inner.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE
	_, err = m.assignReplicaPlan(target)
	require.NoError(t, err)
	require.Equal(t, int64(4), m.CurrentTerm())
	require.EqualValues(t, 2, m.ReplicaPlan().Replicas[2].ReplicaRevision, "same-node writer recovery replaces the local instance")
	m.AssignToServerDone()
	target.Nodes["c"].Address = "c3"
	_, err = m.assignReplicaPlan(target)
	require.NoError(t, err)
	require.EqualValues(t, 5, m.CurrentTerm())
	require.Equal(t, "c3", m.inner.Node.Address, "writer projection and full plan publish the same endpoint")
}

func TestReplicaPlanRejectsInvalidPlacement(t *testing.T) {
	for _, target := range []ReplicaPlanTarget{
		{},
		{PrimaryResourceGroup: "a", Nodes: map[string]*types.StreamingNodeInfo{"a": nil}},
		{PrimaryResourceGroup: "", Nodes: map[string]*types.StreamingNodeInfo{"": {ServerID: 1, Address: "x"}}},
		{PrimaryResourceGroup: "a", Nodes: map[string]*types.StreamingNodeInfo{"a": {ServerID: 1, Address: "x"}, "b": {ServerID: 1, Address: "x"}}},
		{PrimaryResourceGroup: "a", Nodes: map[string]*types.StreamingNodeInfo{"a": {ServerID: 0, Address: "x"}}},
		{PrimaryResourceGroup: "a", Nodes: map[string]*types.StreamingNodeInfo{"a": {ServerID: 1}}},
	} {
		m := NewPChannelMeta("p", types.AccessModeRW).CopyForWrite()
		before := proto.Clone(m.inner)
		changed, err := m.assignReplicaPlan(target)
		require.Error(t, err)
		require.False(t, changed)
		require.True(t, proto.Equal(before, m.inner))
	}
}

func TestReplicaPlanCatalogCommit(t *testing.T) {
	paramtable.Init()
	catalog := catalogkv.NewCataLog(nil)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	id := types.ChannelID{Name: "p"}
	m := &ChannelManager{
		cond: syncutil.NewContextCond(&sync.Mutex{}), metrics: newPChannelMetrics(),
		channels: map[ChannelID]*PChannelMeta{id: NewPChannelMeta("p", types.AccessModeRW)},
	}
	m.SetLogger(mlog.With())
	target := ReplicaPlanTarget{PrimaryResourceGroup: "a", Nodes: map[string]*types.StreamingNodeInfo{
		"a": {ServerID: 1, Address: "a"}, "b": {ServerID: 2, Address: "b"},
	}}
	var saved *streamingpb.PChannelMeta
	persistErr := merr.WrapErrServiceUnavailable("injected catalog failure")
	patch := mockey.Mock(mockey.GetMethod(catalog, "SavePChannels")).To(func(_ context.Context, metas []*streamingpb.PChannelMeta) error {
		require.Len(t, metas, 1)
		saved = proto.Clone(metas[0]).(*streamingpb.PChannelMeta)
		return persistErr
	}).Build()
	defer patch.UnPatch()
	ctx := context.Background()
	_, err := m.AssignReplicaPlans(ctx, map[ChannelID]ReplicaPlanTarget{id: target})
	require.ErrorIs(t, err, persistErr)
	require.Nil(t, m.channels[id].ReplicaPlan())
	require.Equal(t, int64(1), m.channels[id].CurrentTerm())
	persistErr = nil
	updates, err := m.AssignReplicaPlans(ctx, map[ChannelID]ReplicaPlanTarget{id: target})
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.True(t, proto.Equal(saved, m.channels[id].inner))
	// Catalog recovery retains both IDs and the exact plan revision.
	recovered := newPChannelMetaFromProto(saved, nil)
	require.True(t, proto.Equal(recovered.ReplicaPlan(), m.channels[id].ReplicaPlan()))
	require.NoError(t, m.AssignPChannelsDone(ctx, []ChannelID{id}))
	updates, err = m.AssignReplicaPlans(ctx, map[ChannelID]ReplicaPlanTarget{id: target})
	require.NoError(t, err)
	require.Empty(t, updates)
	_, err = m.AssignReplicaPlans(ctx, map[ChannelID]ReplicaPlanTarget{{Name: "missing"}: target})
	require.ErrorIs(t, err, ErrChannelNotExist)
	_, err = m.AssignReplicaPlans(ctx, map[ChannelID]ReplicaPlanTarget{id: {}})
	require.Error(t, err)
	require.NoError(t, m.CompleteReplicaRetirements(ctx, id, nil))
	require.ErrorIs(t, m.CompleteReplicaRetirements(ctx, ChannelID{Name: "missing"}, nil), ErrChannelNotExist)
	// Move the RO within its RG, keeping the former exact placement until a
	// matching drain acknowledgement has itself been committed to the catalog.
	target.Nodes["b"] = &types.StreamingNodeInfo{ServerID: 3, Address: "b2"}
	_, err = m.AssignReplicaPlans(ctx, map[ChannelID]ReplicaPlanTarget{id: target})
	require.NoError(t, err)
	retired := m.channels[id].ReplicaPlan().RetiringReplicas[0]
	report := &streamingpb.WALReplicaObservation{ReplicaId: retired.ReplicaId, ReplicaRevision: retired.ReplicaRevision, NodeId: 3, State: streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED}
	require.NoError(t, m.CompleteReplicaRetirements(ctx, id, []*streamingpb.WALReplicaObservation{report}))
	require.Len(t, m.channels[id].ReplicaPlan().RetiringReplicas, 1, "replacement cannot acknowledge the old server's drain")
	report.NodeId = retired.Node.ServerId
	report.State = streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING
	require.NoError(t, m.CompleteReplicaRetirements(ctx, id, []*streamingpb.WALReplicaObservation{report}))
	require.Len(t, m.channels[id].ReplicaPlan().RetiringReplicas, 1)
	report.State = streamingpb.WALReplicaState_WAL_REPLICA_STATE_CLOSED
	persistErr = merr.WrapErrServiceUnavailable("drain commit failed")
	require.ErrorIs(t, m.CompleteReplicaRetirements(ctx, id, []*streamingpb.WALReplicaObservation{report}), persistErr)
	require.Len(t, m.channels[id].ReplicaPlan().RetiringReplicas, 1)
	persistErr = nil
	before := m.channels[id].ReplicaPlan().Revision
	require.NoError(t, m.CompleteReplicaRetirements(ctx, id, []*streamingpb.WALReplicaObservation{report}))
	require.Empty(t, m.channels[id].ReplicaPlan().RetiringReplicas)
	require.Equal(t, before+1, m.channels[id].ReplicaPlan().Revision)
}

func TestReplicaObservationsRequireExactWriterIncarnation(t *testing.T) {
	paramtable.Init()
	catalog := catalogkv.NewCataLog(nil)
	resource.InitForTest(resource.OptStreamingCatalog(catalog))
	id := ChannelID{Name: "p"}
	m := &ChannelManager{cond: syncutil.NewContextCond(&sync.Mutex{}), metrics: newPChannelMetrics(), channels: map[ChannelID]*PChannelMeta{id: NewPChannelMeta("p", types.AccessModeRW)}}
	m.SetLogger(mlog.With())
	var persistErr error
	patch := mockey.Mock(mockey.GetMethod(catalog, "SavePChannels")).To(func(context.Context, []*streamingpb.PChannelMeta) error { return persistErr }).Build()
	defer patch.UnPatch()
	response := &streamingpb.ReconcileWALReplicasResponse{PlanRevision: 1}
	changed, err := m.ApplyReplicaObservations(t.Context(), id, 1, response)
	require.NoError(t, err)
	require.False(t, changed)
	_, err = m.ApplyReplicaObservations(t.Context(), ChannelID{Name: "missing"}, 1, response)
	require.ErrorIs(t, err, ErrChannelNotExist)
	target := ReplicaPlanTarget{PrimaryResourceGroup: "a", Nodes: map[string]*types.StreamingNodeInfo{"a": {ServerID: 1, Address: "a"}, "b": {ServerID: 2, Address: "b"}}}
	_, err = m.AssignReplicaPlans(t.Context(), map[ChannelID]ReplicaPlanTarget{id: target})
	require.NoError(t, err)
	plan := m.channels[id].ReplicaPlan()
	primary := plan.Replicas[0]
	report := &streamingpb.WALReplicaObservation{NodeId: 1, ReplicaId: primary.ReplicaId, ReplicaRevision: primary.ReplicaRevision, SourceTerm: plan.WalTerm, State: streamingpb.WALReplicaState_WAL_REPLICA_STATE_READY}
	response.Replicas = []*streamingpb.WALReplicaObservation{report}
	// Neither stale plan, stale local revision, stale source term nor a
	// follower's readiness can publish this writer in service discovery.
	for _, mutation := range []func(*streamingpb.ReconcileWALReplicasResponse){
		func(r *streamingpb.ReconcileWALReplicasResponse) { r.PlanRevision++ },
		func(r *streamingpb.ReconcileWALReplicasResponse) { r.Replicas[0].ReplicaRevision++ },
		func(r *streamingpb.ReconcileWALReplicasResponse) { r.Replicas[0].SourceTerm-- },
		func(r *streamingpb.ReconcileWALReplicasResponse) {
			r.Replicas[0].State = streamingpb.WALReplicaState_WAL_REPLICA_STATE_PREPARING
		},
		func(r *streamingpb.ReconcileWALReplicasResponse) {
			r.Replicas[0].NodeId = 2
			r.Replicas[0].ReplicaId = plan.Replicas[1].ReplicaId
		},
	} {
		stale := proto.CloneOf(response)
		mutation(stale)
		changed, err := m.ApplyReplicaObservations(t.Context(), id, stale.Replicas[0].NodeId, stale)
		require.NoError(t, err)
		require.False(t, changed)
		require.False(t, m.channels[id].IsAssigned())
	}
	_, err = m.ApplyReplicaObservations(t.Context(), id, 2, response)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	persistErr = merr.ErrServiceUnavailable
	_, err = m.ApplyReplicaObservations(t.Context(), id, 1, response)
	require.ErrorIs(t, err, persistErr)
	require.False(t, m.channels[id].IsAssigned())
	persistErr = nil
	changed, err = m.ApplyReplicaObservations(t.Context(), id, 1, response)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, m.channels[id].IsAssigned())
	report.State = streamingpb.WALReplicaState_WAL_REPLICA_STATE_FAILED
	changed, err = m.ApplyReplicaObservations(t.Context(), id, 1, response)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE, m.channels[id].State())
	_, err = m.AssignReplicaPlans(t.Context(), map[ChannelID]ReplicaPlanTarget{id: target})
	require.NoError(t, err)
	newPlan := m.channels[id].ReplicaPlan()
	require.Equal(t, plan.WalTerm+1, newPlan.WalTerm)
	require.Equal(t, primary.ReplicaRevision+1, newPlan.Replicas[0].ReplicaRevision)
	// A failed open may not have an applied source term yet.
	response.PlanRevision = newPlan.Revision
	report.ReplicaRevision = newPlan.Replicas[0].ReplicaRevision
	report.SourceTerm = 0
	changed, err = m.ApplyReplicaObservations(t.Context(), id, 1, response)
	require.NoError(t, err)
	require.True(t, changed)
	_, err = m.AssignReplicaPlans(t.Context(), map[ChannelID]ReplicaPlanTarget{id: target})
	require.NoError(t, err)
	require.Equal(t, newPlan.WalTerm+1, m.channels[id].CurrentTerm())
}
