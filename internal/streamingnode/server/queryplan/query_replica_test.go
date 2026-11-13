package queryplan

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/queryplan/provider"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type queryRuntimeManager struct{ walManager }

func (*queryRuntimeManager) GetAvailableWAL(types.PChannelInfo) (wal.WAL, error) { panic("mockey") }
func (*queryRuntimeManager) QueryReplicaIdentity(wal.WAL) (*viewpb.StreamingWorkNode, error) {
	panic("mockey")
}

func (*queryRuntimeManager) GetAvailableQueryReplica(*viewpb.StreamingWorkNode) (wal.WAL, error) {
	panic("mockey")
}

type queryRuntimeWAL struct {
	wal.WAL
	provider.QueryPlanProvider
}

func (*queryRuntimeWAL) GetQueryPlan(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	panic("mockey")
}

func TestQueryPlanRecordsTheRuntimeThatPreparedIt(t *testing.T) {
	mockey.PatchConvey("plan identity is captured before local planning and survives later assignment changes", t, func() {
		instance := &queryRuntimeWAL{}
		target := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 2, WalReplicaId: 3, WalReplicaRevision: 4, RuntimeId: 5, WalTerm: 7, ReadOnly: true}
		outgoing, err := worknodehandler.EncodeQueryReplicaToOutgoingContext(t.Context(), target)
		require.NoError(t, err)
		md, _ := metadata.FromOutgoingContext(outgoing)
		ctx := metadata.NewIncomingContext(t.Context(), md)
		var identityErr, lookupErr error
		mockey.Mock((*queryRuntimeManager).GetAvailableQueryReplica).To(func(_ *queryRuntimeManager, actual *viewpb.StreamingWorkNode) (wal.WAL, error) {
			require.EqualValues(t, 5, actual.RuntimeId)
			return instance, lookupErr
		}).Build()
		mockey.Mock((*queryRuntimeManager).QueryReplicaIdentity).To(func(_ *queryRuntimeManager, raw wal.WAL) (*viewpb.StreamingWorkNode, error) {
			require.Same(t, instance, raw)
			return target, identityErr
		}).Build()
		pchannel := "p"
		mockey.Mock((*queryRuntimeWAL).GetQueryPlan).To(func(*queryRuntimeWAL, context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			return &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{WorkNodes: []*viewpb.QueryPlanWorkNode{
				{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 9}}},
				{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: pchannel}}},
			}}}, nil
		}).Build()
		server := NewServer(&queryRuntimeManager{})
		req := &viewpb.GetQueryPlanRequest{ShardId: &viewpb.ShardID{Vchannel: "p_100v0"}}
		response, err := server.GetQueryPlan(ctx, req)
		require.NoError(t, err)
		require.EqualValues(t, 9, response.Plan.WorkNodes[0].GetQueryNode().NodeId)
		require.Same(t, target, response.Plan.WorkNodes[1].GetStreamingNode())
		pchannel = "wrong"
		_, err = server.GetQueryPlan(ctx, req)
		require.Error(t, err)
		identityErr = context.Canceled
		_, err = server.GetQueryPlan(ctx, req)
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		lookupErr = context.Canceled
		_, err = server.GetQueryPlan(ctx, req)
		require.True(t, viewerror.AsViewError(err).IsOnShutdown())
	})
}
