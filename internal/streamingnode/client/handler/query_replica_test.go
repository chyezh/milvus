package handler

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryReplicaDispatchNeverReResolvesWriter(t *testing.T) {
	mockey.PatchConvey("an explicitly selected replica is independent of writer discovery", t, func() {
		cc, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		conn := lazygrpc.NewConn(func(context.Context) (*grpc.ClientConn, error) { return cc, nil })
		defer conn.Close()
		owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), replicaConn: conn}
		client := newQueryViewClient(owner, nil)
		target := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 42, WalReplicaId: 3, WalReplicaRevision: 4, RuntimeId: 5, WalTerm: 7, ReadOnly: true}
		ctx, err := worknodehandler.EncodeQueryReplicaToOutgoingContext(t.Context(), target)
		require.NoError(t, err)
		var rpcErr error
		invoke := mockey.Mock((*grpc.ClientConn).Invoke).To(func(_ *grpc.ClientConn, rpcCtx context.Context, _ string, _, _ any, _ ...grpc.CallOption) error {
			id, ok := contextutil.GetPickServerID(rpcCtx)
			require.True(t, ok)
			require.EqualValues(t, 42, id)
			actual, err := worknodehandler.DecodeQueryReplicaFromOutgoingContext(rpcCtx)
			require.NoError(t, err)
			require.EqualValues(t, 5, actual.RuntimeId)
			return rpcErr
		}).Build()
		channel := types.PChannelInfo{Name: "p"}
		shard := qviews.ShardID{VChannel: "p_100v0"}
		_, err = client.SearchOnView(ctx, channel, &viewpb.SearchOnViewRequest{})
		require.NoError(t, err)
		_, err = client.QueryOnView(ctx, channel, &viewpb.QueryOnViewRequest{})
		require.NoError(t, err)
		_, err = client.GetQueryPlan(ctx, shard, &viewpb.GetQueryPlanRequest{})
		require.NoError(t, err)
		_, err = client.GetMVCCTimestamp(ctx, shard, &viewpb.GetMVCCTimestampRequest{})
		require.NoError(t, err)
		rpcErr = grpcstatus.Error(codes.Unavailable, "chosen replica disappeared")
		_, err = client.SearchOnView(ctx, channel, &viewpb.SearchOnViewRequest{})
		require.Equal(t, codes.Unavailable, grpcstatus.Code(err))
		require.Equal(t, 5, invoke.Times(), "failure must return for replanning without waiting for a new writer")
		_, err = client.QueryOnView(ctx, types.PChannelInfo{Name: "other"}, &viewpb.QueryOnViewRequest{})
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		_, err = client.GetQueryPlan(ctx, qviews.ShardID{VChannel: "other_100v0"}, &viewpb.GetQueryPlanRequest{})
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		target.RuntimeId = 0
		invalid, err := worknodehandler.EncodeQueryReplicaToOutgoingContext(t.Context(), target)
		require.NoError(t, err)
		_, err = client.SearchOnView(invalid, channel, &viewpb.SearchOnViewRequest{})
		require.Error(t, err)
		_, err = client.GetQueryPlan(invalid, shard, &viewpb.GetQueryPlanRequest{})
		require.Error(t, err)
		owner.replicaConn = nil
		_, err = client.SearchOnView(ctx, channel, &viewpb.SearchOnViewRequest{})
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		owner.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = client.GetQueryPlan(ctx, shard, &viewpb.GetQueryPlanRequest{})
		require.ErrorIs(t, err, ErrClientClosed)
		require.Equal(t, 5, invoke.Times())
	})
}
