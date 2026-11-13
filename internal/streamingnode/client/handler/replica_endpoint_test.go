package handler

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestWALReplicaEndpointUsesMemberSession(t *testing.T) {
	mockey.PatchConvey("endpoint discovery needs neither writer readiness nor its term", t, func() {
		cc, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		conn := lazygrpc.NewConn(func(context.Context) (*grpc.ClientConn, error) { return cc, nil })
		defer conn.Close()
		owner := &handlerClientImpl{lifetime: typeutil.NewLifetime(), replicaConn: conn}
		member := &streamingpb.WALReplicaAssignment{ReplicaId: 2, ReplicaRevision: 9, Node: &streamingpb.StreamingNodeInfo{ServerId: 42}}
		endpoint := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 42, WalReplicaId: 2, WalReplicaRevision: 8, RuntimeId: 5, WalTerm: 7, ReadOnly: true}
		var rpcErr error
		var malformed bool
		invoke := mockey.Mock((*grpc.ClientConn).Invoke).To(func(_ *grpc.ClientConn, ctx context.Context, method string, arg, reply any, _ ...grpc.CallOption) error {
			id, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.EqualValues(t, 42, id)
			require.Equal(t, streamingpb.StreamingNodeReplicaService_GetWALReplicaEndpoint_FullMethodName, method)
			req := arg.(*streamingpb.WALReplicaEndpointRequest)
			require.Equal(t, "p", req.Pchannel)
			require.EqualValues(t, 2, req.ReplicaId)
			require.EqualValues(t, 42, req.NodeId)
			proto.Merge(reply.(*viewpb.StreamingWorkNode), endpoint)
			if malformed {
				reply.(*viewpb.StreamingWorkNode).NodeId++
			}
			return rpcErr
		}).Build()
		result, err := owner.GetWALReplicaEndpoint(t.Context(), "p", member)
		require.NoError(t, err)
		require.True(t, proto.Equal(endpoint, result), "a retained RO remains discoverable during replacement preparation")
		malformed = true
		_, err = owner.GetWALReplicaEndpoint(t.Context(), "p", member)
		require.Error(t, err)
		rpcErr = status.NewChannelFenced("p")
		_, err = owner.GetWALReplicaEndpoint(t.Context(), "p", member)
		require.Error(t, err)
		require.Equal(t, 3, invoke.Times())
		_, err = owner.GetWALReplicaEndpoint(t.Context(), "", member)
		require.Error(t, err)
		_, err = owner.GetWALReplicaEndpoint(t.Context(), "p", nil)
		require.Error(t, err)
		owner.replicaConn = nil
		_, err = owner.GetWALReplicaEndpoint(t.Context(), "p", member)
		require.Error(t, err)
		owner.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = owner.GetWALReplicaEndpoint(t.Context(), "p", member)
		require.ErrorIs(t, err, ErrClientClosed)
		require.Equal(t, 3, invoke.Times())
	})
}
