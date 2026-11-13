package manager

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReconcileReplicaPlanRoutesExactNode(t *testing.T) {
	client := streamingpb.NewStreamingNodeReplicaServiceClient(nil)
	service := lazygrpc.WithServiceCreator[streamingpb.StreamingNodeReplicaServiceClient](nil, nil)
	m := &managerClientImpl{lifetime: typeutil.NewLifetime(), replicaService: service}
	var serviceErr, rpcErr error
	get := mockey.Mock(mockey.GetMethod(service, "GetService")).To(func(context.Context) (streamingpb.StreamingNodeReplicaServiceClient, error) {
		return client, serviceErr
	}).Build()
	defer get.UnPatch()
	req := &streamingpb.ReconcileWALReplicasRequest{Pchannel: "p", Plan: &streamingpb.WALReplicaPlan{Revision: 8}}
	response := &streamingpb.ReconcileWALReplicasResponse{PlanRevision: 8}
	rpc := mockey.Mock(mockey.GetMethod(client, "ReconcileWALReplicas")).To(func(ctx context.Context, actual *streamingpb.ReconcileWALReplicasRequest, _ ...grpc.CallOption) (*streamingpb.ReconcileWALReplicasResponse, error) {
		id, ok := contextutil.GetPickServerID(ctx)
		require.True(t, ok)
		require.EqualValues(t, 42, id)
		require.Same(t, req, actual)
		return response, rpcErr
	}).Build()
	defer rpc.UnPatch()
	got, err := m.ReconcileWALReplicas(t.Context(), 42, req)
	require.NoError(t, err)
	require.Same(t, response, got)
	rpcErr = picker.ErrSubConnNotExist
	_, err = m.ReconcileWALReplicas(t.Context(), 42, req)
	require.ErrorIs(t, err, types.ErrNotAlive)
	rpcErr = context.DeadlineExceeded
	_, err = m.ReconcileWALReplicas(t.Context(), 42, req)
	require.ErrorIs(t, err, rpcErr)
	serviceErr = context.Canceled
	_, err = m.ReconcileWALReplicas(t.Context(), 42, req)
	require.ErrorIs(t, err, serviceErr)
	m.lifetime.SetState(typeutil.LifetimeStateStopped)
	_, err = m.ReconcileWALReplicas(t.Context(), 42, req)
	require.Error(t, err)
}
