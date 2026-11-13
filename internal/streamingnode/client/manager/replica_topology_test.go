package manager

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReplicaTopologyPreservesFailedAndStoppingMembers(t *testing.T) {
	timeout := &paramtable.Get().StreamingCfg.WALBalancerOperationTimeout
	oldTimeout := timeout.SwapTempValue("50ms")
	defer timeout.SwapTempValue(oldTimeout)
	rb := mock_resolver.NewMockBuilder(t)
	r := mock_resolver.NewMockResolver(t)
	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeManagerServiceClient](t)
	client := streamingpb.NewStreamingNodeManagerServiceClient(nil)
	m := &managerClientImpl{lifetime: typeutil.NewLifetime(), stopped: make(chan struct{}), rb: rb, service: service}
	patches := []*mockey.Mocker{
		mockey.Mock((*mock_resolver.MockBuilder).Resolver).Return(resolver.Resolver(r)).Build(),
		mockey.Mock(mockey.GetMethod(service, "GetService")).Return(client, nil).Build(),
		mockey.Mock(mockey.GetMethod(client, "CollectStatus")).To(func(ctx context.Context, _ *streamingpb.StreamingNodeManagerCollectStatusRequest, _ ...grpc.CallOption) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
			id, _ := contextutil.GetPickServerID(ctx)
			if id == 3 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &streamingpb.StreamingNodeManagerCollectStatusResponse{}, nil
		}).Build(),
	}
	defer func() {
		for _, patch := range patches {
			patch.UnPatch()
		}
	}()
	initial := newVersionedStateWithRG(1, map[uint64]serverInfo{
		1: {}, 2: {resourceGroup: "a", stopping: true}, 3: {resourceGroup: "b"}, 4: {resourceGroup: "c"},
	})
	latest := initial
	calls := 0
	statePatch := mockey.Mock((*mock_resolver.MockResolver).GetLatestState).To(func(context.Context) (discoverer.VersionedState, error) {
		calls++
		if calls%2 == 1 {
			return initial, nil
		}
		return latest, nil
	}).Build()
	defer statePatch.UnPatch()
	nodes, err := m.CollectAllStatus(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, nodes, 4)
	require.Equal(t, common.DefaultResourceGroupName, nodes[1].ResourceGroup)
	require.ErrorIs(t, nodes[2].Err, types.ErrStopping)
	require.ErrorIs(t, nodes[3].Err, context.DeadlineExceeded)
	require.Equal(t, "b", nodes[3].ResourceGroup)
	require.True(t, nodes[4].IsHealthy())

	latest = newVersionedStateWithRG(2, map[uint64]serverInfo{
		1: {resourceGroup: "moved"}, 2: {resourceGroup: "a", stopping: true}, 3: {resourceGroup: "b"},
	})
	nodes, err = m.CollectAllStatus(context.Background(), "")
	require.NoError(t, err)
	require.ErrorIs(t, nodes[1].Err, types.ErrNotAlive)
	require.ErrorIs(t, nodes[4].Err, types.ErrNotAlive)
	require.Equal(t, common.DefaultResourceGroupName, nodes[1].ResourceGroup)
}
