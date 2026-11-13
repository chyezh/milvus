package replica

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/client/connection"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type replicaTestResolver struct{ resolver.Builder }

func (*replicaTestResolver) Close() { panic("mockey") }

func TestReplicaSourceResolvesMVCCOnExactPrimary(t *testing.T) {
	mockey.PatchConvey("source routing pins the server, WAL term and requested consistency", t, func() {
		cc, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		conn := lazygrpc.NewConn(func(context.Context) (*grpc.ClientConn, error) { return cc, nil })
		defer conn.Close()
		rb := &replicaTestResolver{}
		closed := mockey.Mock((*replicaTestResolver).Close).Return().Build()
		mockey.Mock(connection.NewSession).To(func(_ *clientv3.Client, services ...string) (lazygrpc.Conn, resolver.Builder) {
			require.Equal(t, []string{"milvus.proto.streaming.StreamingNodeReplicaService", "milvus.proto.view.QueryPlanService"}, services)
			return conn, rb
		}).Build()
		client := NewClient(nil)
		primary := types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 8, AccessMode: types.AccessModeRW}, Node: types.StreamingNodeInfo{ServerID: 42}}
		source := client.ForPrimary(primary)
		require.NotNil(t, source.Events())
		req := &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: 100}}
		var rpcErr error
		mockey.Mock((*grpc.ClientConn).Invoke).To(func(_ *grpc.ClientConn, ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
			id, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.EqualValues(t, 42, id)
			md, _ := metadata.FromOutgoingContext(ctx)
			channel, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(metadata.NewIncomingContext(ctx, md))
			require.NoError(t, err)
			require.Equal(t, primary.Channel, channel)
			require.Equal(t, viewpb.QueryPlanService_ResolveQueryPlanMVCC_FullMethodName, method)
			require.Same(t, req, args)
			reply.(*viewpb.QueryPlanMVCC).GrowingTimetick = 100
			return rpcErr
		}).Build()
		mvcc, err := source.ResolveMVCC(t.Context(), req, "v")
		require.NoError(t, err)
		require.EqualValues(t, 100, mvcc.GrowingTimetick)
		rpcErr = viewerror.NewGRPCStatusFromViewError(viewerror.NewNotPrimaryError("writer changed")).Err()
		_, err = source.ResolveMVCC(t.Context(), req, "v")
		require.True(t, viewerror.AsViewError(err).IsNotPrimary())
		client.Close()
		require.Equal(t, 1, closed.Times())
	})
}
