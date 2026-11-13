package queryclient

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestShardSelectsIndependentWALCopiesOfSameQueryReplica(t *testing.T) {
	mockey.PatchConvey("same query replica on another WAL remains an untried candidate", t, func() {
		first := qviews.StreamingNode{PChannel: "p", NodeID: 10, WALReplicaID: 1, WALReplicaRevision: 1, RuntimeID: 11, WALTerm: 7}
		second := qviews.StreamingNode{PChannel: "p", NodeID: 20, WALReplicaID: 2, WALReplicaRevision: 1, RuntimeID: 22, WALTerm: 7, ReadOnly: true}
		route := &replicaRoute{changed: make(chan struct{}), snapshot: qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 7, StreamingNode: first}, {ReplicaID: 7, StreamingNode: second}}}}
		mockey.Mock((*replicaRouter).route).Return(route, nil).Build()
		var selected []int64
		mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, ctx context.Context, id qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			target, err := worknodehandler.DecodeQueryReplicaFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.NotNil(t, target)
			require.EqualValues(t, 7, id.ReplicaID)
			selected = append(selected, target.NodeId)
			if target.NodeId == 10 {
				return nil, viewerror.NewViewInvalidated("primary runtime disappeared")
			}
			require.Equal(t, second, qviews.StreamingNodeFromProto(target))
			plan := newTestSearchQueryPlan(id, &viewpb.QueryPlanMVCC{})
			plan.WorkNodes = []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: target}}}
			return &viewpb.GetQueryPlanResponse{Plan: plan}, nil
		}).Build()
		mockey.Mock((*fakeViewQueryServiceClient).SearchOnView).To(func(_ *fakeViewQueryServiceClient, _ context.Context, node qviews.WorkNode, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			require.Equal(t, second, node)
			return &viewpb.SearchOnViewResponse{}, nil
		}).Build()
		client := newShardViewQueryClient(1, &fakeQueryPlanClient{}, &fakeViewQueryServiceClient{})
		plan, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: "p_1v0", Req: &internalpb.SearchRequest{}, Reducer: fakeSearchResultReducer{}})
		require.NoError(t, err)
		require.EqualValues(t, 7, plan.ShardID.ReplicaID)
		require.Equal(t, []int64{10, 20}, selected)
	})
}
