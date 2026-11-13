package queryclient

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestReplicaPlanRetainsRuntimeThroughDispatch(t *testing.T) {
	mockey.PatchConvey("plan conversion, segment selection and RPC dispatch preserve the same runtime", t, func() {
		target := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 2, WalReplicaId: 3, WalReplicaRevision: 4, RuntimeId: 5, WalTerm: 7, ReadOnly: true}
		plan := &viewpb.QueryPlan{WorkNodes: []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: target}}}}
		nodes := workNodesFromPlan(plan)
		require.Len(t, nodes, 1)
		node := nodes[0].(qviews.StreamingNode)
		require.True(t, proto.Equal(target, node.IntoProto()))
		require.Same(t, plan.WorkNodes[0], qviews.WorkNodeSelection(plan, node))
		other := node
		other.RuntimeID++
		require.NotEqual(t, node.Key(), other.Key())
		require.Nil(t, qviews.WorkNodeSelection(plan, other), "a different runtime cannot reuse segment selections")
		check := func(ctx context.Context, channel types.PChannelInfo) {
			actual, err := worknodehandler.DecodeQueryReplicaFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.True(t, proto.Equal(target, actual))
			require.Equal(t, "p", channel.Name)
		}
		mockey.Mock((*fakeStreamingNodeViewQueryServiceClient).SearchOnView).To(func(_ *fakeStreamingNodeViewQueryServiceClient, ctx context.Context, channel types.PChannelInfo, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			check(ctx, channel)
			return &viewpb.SearchOnViewResponse{}, nil
		}).Build()
		mockey.Mock((*fakeStreamingNodeViewQueryServiceClient).QueryOnView).To(func(_ *fakeStreamingNodeViewQueryServiceClient, ctx context.Context, channel types.PChannelInfo, _ *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
			check(ctx, channel)
			return &viewpb.QueryOnViewResponse{}, nil
		}).Build()
		client := NewCompositeViewQueryServiceClient(&fakeStreamingNodeViewQueryServiceClient{}, nil)
		_, err := client.SearchOnView(t.Context(), node, &viewpb.SearchOnViewRequest{})
		require.NoError(t, err)
		_, err = client.QueryOnView(t.Context(), node, &viewpb.QueryOnViewRequest{})
		require.NoError(t, err)
		node.PChannel = "\xff"
		_, err = client.SearchOnView(t.Context(), node, &viewpb.SearchOnViewRequest{})
		require.Error(t, err)
		_, err = client.QueryOnView(t.Context(), node, &viewpb.QueryOnViewRequest{})
		require.Error(t, err)
	})
}
