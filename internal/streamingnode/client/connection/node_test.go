package connection

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
)

func TestNodeConnectionPinsUnaryAndStream(t *testing.T) {
	mockey.PatchConvey("all calls retain the chosen node identity and never choose an arbitrary writer", t, func() {
		cc, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		conn := lazygrpc.NewConn(func(context.Context) (*grpc.ClientConn, error) { return cc, nil })
		defer conn.Close()
		bound := ForNode(conn, 42)
		request, response := new(int), new(int)
		mockey.Mock((*grpc.ClientConn).Invoke).To(func(_ *grpc.ClientConn, ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
			id, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.EqualValues(t, 42, id)
			require.Equal(t, "unary", method)
			require.Same(t, request, args)
			require.Same(t, response, reply)
			require.Len(t, opts, 1)
			return context.DeadlineExceeded
		}).Build()
		desc := &grpc.StreamDesc{ServerStreams: true}
		mockey.Mock((*grpc.ClientConn).NewStream).To(func(_ *grpc.ClientConn, ctx context.Context, actual *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			id, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.EqualValues(t, 42, id)
			require.Equal(t, "stream", method)
			require.Same(t, desc, actual)
			require.Len(t, opts, 1)
			return nil, context.DeadlineExceeded
		}).Build()
		ctx := contextutil.WithPickServerID(t.Context(), 99)
		require.ErrorIs(t, bound.Invoke(ctx, "unary", request, response, grpc.WaitForReady(true)), context.DeadlineExceeded)
		_, err = bound.NewStream(ctx, desc, "stream", grpc.WaitForReady(true))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		id, _ := contextutil.GetPickServerID(ctx)
		require.EqualValues(t, 99, id, "routing must not mutate the caller's other RPCs")
		conn.Close()
		require.ErrorIs(t, bound.Invoke(t.Context(), "unary", nil, nil), lazygrpc.ErrClosed)
		_, err = bound.NewStream(t.Context(), desc, "stream")
		require.ErrorIs(t, err, lazygrpc.ErrClosed)
	})
}
