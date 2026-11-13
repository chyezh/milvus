package balancer

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPlanWALReplicasByResourceGroup(t *testing.T) {
	paramtable.Init()
	// The external balancer tests register the production policy builders.
	policy := mustGetPolicy("vchannelFair").Build()
	policy.SetLogger(mlog.With())
	view := &channel.PChannelView{
		Channels: make(map[types.ChannelID]*channel.PChannelMeta),
		Stats:    make(map[types.ChannelID]channel.PChannelStatsView),
	}
	for _, name := range []string{"p1", "p2", "p3", "p4"} {
		meta := channel.NewPChannelMeta(name, types.AccessModeRW)
		view.Channels[meta.ChannelID()] = meta
		view.Stats[meta.ChannelID()] = channel.PChannelStatsView{}
	}
	nodes := map[int64]*types.StreamingNodeStatus{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "a1"}, ResourceGroup: "a"},
		2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "a2"}, ResourceGroup: "a"},
		3: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "b1"}, ResourceGroup: "b"},
		4: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 4, Address: "c1"}, ResourceGroup: "c", Err: types.ErrFrozen},
	}
	plans, err := planWALReplicas(view, nodes, "b", policy)
	require.NoError(t, err)
	require.Len(t, plans, 4)
	for _, plan := range plans {
		require.Equal(t, "b", plan.PrimaryResourceGroup)
		require.Len(t, plan.Nodes, 3)
		require.NotNil(t, plan.Nodes["a"])
		require.Equal(t, int64(3), plan.Nodes["b"].ServerID)
		require.Nil(t, plan.Nodes["c"])
	}
	plans, err = planWALReplicas(view, nodes, "missing", policy)
	require.NoError(t, err)
	for _, plan := range plans {
		require.Equal(t, "a", plan.PrimaryResourceGroup)
	}
	// Keep an existing healthy writer when there is no usable preference.
	for id, meta := range view.Channels {
		m := meta.CopyForWrite()
		m.TryAssignToServerID(types.AccessModeRW, nodes[3].StreamingNodeInfo)
		m.AssignToServerDone()
		view.Channels[id] = m.PChannelMeta
	}
	plans, err = planWALReplicas(view, nodes, "c", policy)
	require.NoError(t, err)
	for _, plan := range plans {
		require.Equal(t, "b", plan.PrimaryResourceGroup)
	}
	// Each RG retains its own follower placement, independent of the writer.
	patch := mockey.Mock((*channel.PChannelMeta).ReplicaPlan).Return(&streamingpb.WALReplicaPlan{Replicas: []*streamingpb.WALReplicaAssignment{
		{ResourceGroup: "a", Node: &streamingpb.StreamingNodeInfo{ServerId: 2}},
		{ResourceGroup: "gone", Node: &streamingpb.StreamingNodeInfo{ServerId: 9}},
	}}).Build()
	defer patch.UnPatch()
	paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.SwapTempValue("false")
	defer paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.SwapTempValue("")
	plans, err = planWALReplicas(view, nodes, "a", policy)
	require.NoError(t, err)
	for _, plan := range plans {
		require.Equal(t, "a", plan.PrimaryResourceGroup)
		require.Equal(t, int64(2), plan.Nodes["a"].ServerID)
	}
	nodes[2].Err = types.ErrNotAlive
	plans, err = planWALReplicas(view, nodes, "a", policy)
	require.NoError(t, err)
	for _, plan := range plans {
		require.Equal(t, int64(1), plan.Nodes["a"].ServerID)
	}
	// No candidate must remain a retriable planning failure.
	for _, node := range nodes {
		node.Err = types.ErrStopping
	}
	_, err = planWALReplicas(view, nodes, "a", policy)
	require.Error(t, err)

	// Unlabelled SNs form the default group, including for initial fallback.
	for id, meta := range view.Channels {
		view.Channels[id] = channel.NewPChannelMeta(meta.Name(), types.AccessModeRW)
	}
	nodes[5] = &types.StreamingNodeStatus{StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 5, Address: "default"}}
	plans, err = planWALReplicas(view, nodes, "", policy)
	require.NoError(t, err)
	for _, plan := range plans {
		require.Equal(t, common.DefaultResourceGroupName, plan.PrimaryResourceGroup)
	}
}

func TestReplicaPlanPolicyFailure(t *testing.T) {
	paramtable.Init()
	policy := mustGetPolicy("vchannelFair").Build()
	id := types.ChannelID{Name: "p"}
	view := &channel.PChannelView{
		Channels: map[types.ChannelID]*channel.PChannelMeta{id: channel.NewPChannelMeta("p", types.AccessModeRW)},
		Stats:    map[types.ChannelID]channel.PChannelStatsView{id: {}},
	}
	nodes := map[int64]*types.StreamingNodeStatus{1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "a"}, ResourceGroup: "a"}}
	patch := mockey.Mock(mockey.GetMethod(policy, "Balance")).Return(ExpectedLayout{}, types.ErrNotAlive).Build()
	_, err := planWALReplicas(view, nodes, "a", policy)
	require.ErrorIs(t, err, types.ErrNotAlive)
	patch.UnPatch()
	patch = mockey.Mock(mockey.GetMethod(policy, "Balance")).Return(ExpectedLayout{}, nil).Build()
	defer patch.UnPatch()
	_, err = planWALReplicas(view, nodes, "a", policy)
	require.ErrorContains(t, err, "omitted or misplaced")
}

func TestManualTransferPreservesOtherReplicaMembers(t *testing.T) {
	id := types.ChannelID{Name: "p"}
	view := &channel.PChannelView{Channels: map[types.ChannelID]*channel.PChannelMeta{id: channel.NewPChannelMeta("p", types.AccessModeRW)}}
	patch := mockey.Mock((*channel.PChannelMeta).ReplicaPlan).Return(&streamingpb.WALReplicaPlan{Replicas: []*streamingpb.WALReplicaAssignment{
		{ResourceGroup: "a", Node: &streamingpb.StreamingNodeInfo{ServerId: 1, Address: "a"}},
		{ResourceGroup: "b", Node: &streamingpb.StreamingNodeInfo{ServerId: 2, Address: "b"}},
		{ResourceGroup: "c"},
	}}).Build()
	defer patch.UnPatch()
	nodes := map[int64]*types.StreamingNodeStatus{3: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "b2"}, ResourceGroup: "b"}}
	assignments := map[types.ChannelID]types.PChannelInfoAssigned{id: {Channel: view.Channels[id].ChannelInfo(), Node: nodes[3].StreamingNodeInfo}}
	plan := replicaPlansForTransfer(view, nodes, assignments)[id]
	require.Equal(t, "b", plan.PrimaryResourceGroup)
	require.Len(t, plan.Nodes, 3)
	require.Equal(t, int64(1), plan.Nodes["a"].ServerID)
	require.Equal(t, int64(3), plan.Nodes["b"].ServerID)
	require.Nil(t, plan.Nodes["c"])
}
