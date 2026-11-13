package handler

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/replicadiscovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReplicaWatchUsesExactRuntimeWithoutWriterDiscovery(t *testing.T) {
	mockey.PatchConvey("each explicit watch uses its own node and local runtime", t, func() {
		cc, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		conn := lazygrpc.NewConn(func(context.Context) (*grpc.ClientConn, error) { return cc, nil })
		defer conn.Close()
		owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), replicaConn: conn}
		target := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 42, WalReplicaId: 3, WalReplicaRevision: 4, RuntimeId: 5, WalTerm: 7, ReadOnly: true}
		ctx, err := worknodehandler.EncodeQueryReplicaToOutgoingContext(t.Context(), target)
		require.NoError(t, err)
		var fail bool
		stream := &remote.EventStream{}
		create := mockey.Mock(remote.CreateEventStream).To(func(ctx context.Context, opts *remote.EventStreamOptions, client streamingpb.StreamingNodeHandlerServiceClient) (*remote.EventStream, error) {
			require.EqualValues(t, 42, opts.Assignment.Node.ServerID)
			require.Equal(t, types.AccessModeRO, opts.Assignment.Channel.AccessMode)
			require.EqualValues(t, 7, opts.Assignment.Channel.Term)
			actual, err := worknodehandler.DecodeQueryReplicaFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.True(t, proto.Equal(target, actual))
			// The generated client has a node-bound connection for every stream.
			_, err = client.SubscribeReplicas(ctx)
			require.Error(t, err)
			if fail {
				return nil, status.NewChannelFenced("p")
			}
			return stream, nil
		}).Build()
		mockey.Mock((*grpc.ClientConn).NewStream).To(func(_ *grpc.ClientConn, ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			id, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.EqualValues(t, 42, id)
			return nil, status.NewChannelFenced("p")
		}).Build()
		actual, err := owner.AcquireReplicaWatchStream(ctx, "p")
		require.NoError(t, err)
		require.Same(t, stream, actual)
		fail = true
		_, err = owner.AcquireReplicaWatchStream(ctx, "p")
		require.Error(t, err)
		require.Equal(t, 2, create.Times(), "rejection must not retry on the primary")
		_, err = owner.AcquireReplicaWatchStream(ctx, "other")
		require.True(t, status.AsStreamingError(err).IsFenced())
		owner.replicaConn = nil
		_, err = owner.AcquireReplicaWatchStream(ctx, "p")
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
		target.RuntimeId = 0
		invalid, err := worknodehandler.EncodeQueryReplicaToOutgoingContext(t.Context(), target)
		require.NoError(t, err)
		_, err = owner.AcquireReplicaWatchStream(invalid, "p")
		require.Error(t, err)
		require.Equal(t, 2, create.Times())
	})
}
