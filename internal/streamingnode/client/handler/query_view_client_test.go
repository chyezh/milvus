package handler

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestHandlerClientGetQueryPlanRoutesByShardPChannel(t *testing.T) {
	shardID := qviews.ShardID{
		ReplicaID: 1,
		VChannel:  funcutil.GetVirtualChannel("p0", 100, 0),
	}
	planReq := &viewpb.GetQueryPlanRequest{ShardId: shardID.IntoProto()}
	planResp := &viewpb.GetQueryPlanResponse{Plan: &viewpb.QueryPlan{ShardId: shardID.IntoProto()}}
	queryPlanService := &fakeQueryPlanServiceClient{
		getQueryPlan: func(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(202), serverID)
			pchannel, err := worknodehandler.DecodeQueryViewPChannelFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{
				Name:       "p0",
				Term:       3,
				AccessMode: types.AccessModeRO,
			}, pchannel)
			walReplicaID, err := worknodehandler.DecodeQueryViewWALReplicaIDFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(2), walReplicaID)
			require.Same(t, planReq, req)
			return planResp, nil
		},
	}
	client := newTestHandlerClient(queryPlanService, nil, nil)

	resp, err := client.QueryViewClient().GetQueryPlan(context.Background(), shardID, 2, planReq)

	require.NoError(t, err)
	require.Same(t, planResp, resp)
}

func TestHandlerClientViewQueryRoutesByStreamingWorkNode(t *testing.T) {
	searchReq := &viewpb.SearchOnViewRequest{}
	searchResp := &viewpb.SearchOnViewResponse{}
	viewQueryService := &fakeViewQueryServiceClient{
		searchOnView: func(ctx context.Context, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(202), serverID)
			pchannel, err := worknodehandler.DecodeQueryViewPChannelFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{
				Name:       "p0",
				Term:       3,
				AccessMode: types.AccessModeRO,
			}, pchannel)
			walReplicaID, err := worknodehandler.DecodeQueryViewWALReplicaIDFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(2), walReplicaID)
			require.Same(t, searchReq, req)
			return searchResp, nil
		},
	}
	client := newTestHandlerClient(nil, viewQueryService, nil)

	resp, err := client.QueryViewClient().SearchOnView(context.Background(), types.PChannelInfo{Name: "p0"}, 2, searchReq)

	require.NoError(t, err)
	require.Same(t, searchResp, resp)
}

func TestHandlerClientConvertsViewQueryRPCError(t *testing.T) {
	viewErr := viewerror.NewViewNotFound("missing view")
	viewQueryService := &fakeViewQueryServiceClient{
		searchOnView: func(context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
			return nil, viewerror.NewGRPCStatusFromViewError(viewErr).Err()
		},
	}
	client := newTestHandlerClient(nil, viewQueryService, nil)

	_, err := client.QueryViewClient().SearchOnView(context.Background(), types.PChannelInfo{Name: "p0"}, 0, &viewpb.SearchOnViewRequest{})

	require.Error(t, err)
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
}

func TestHandlerClientRetriesViewQueryTransportErrorThroughAssignmentLoop(t *testing.T) {
	queryResp := &viewpb.QueryOnViewResponse{}
	attempts := 0
	viewQueryService := &fakeViewQueryServiceClient{
		queryOnView: func(ctx context.Context, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(202), serverID)
			attempts++
			if attempts == 1 {
				return nil, picker.ErrSubConnNotExist
			}
			return queryResp, nil
		},
	}
	client := newTestHandlerClient(nil, viewQueryService, nil)
	trigger := &handlerFakeWALReplicaRebalanceTrigger{}
	client.rebalanceTrigger = trigger
	client.watcher = fakeAssignmentWatcher{
		walReplicaAssignments: map[types.ChannelID]*types.PChannelInfoAssigned{
			{Name: "p0", WALReplicaID: 2}: {
				Channel:         types.PChannelInfo{Name: "p0", Term: 3, AccessMode: types.AccessModeRO},
				WALReplicaID:    2,
				AssignmentEpoch: 7,
				Node:            types.StreamingNodeInfo{ServerID: 202, Address: "localhost-ro"},
			},
		},
		watchWALReplica: func(context.Context, types.ChannelID, *types.PChannelInfoAssigned) error {
			return nil
		},
	}

	resp, err := client.QueryViewClient().QueryOnView(context.Background(), types.PChannelInfo{Name: "p0"}, 2, &viewpb.QueryOnViewRequest{})

	require.NoError(t, err)
	require.Same(t, queryResp, resp)
	require.Equal(t, 2, attempts)
	require.NotNil(t, trigger.reportedWALAssignment)
	require.Equal(t, int64(2), trigger.reportedWALAssignment.WALReplicaID)
	require.Equal(t, int64(7), trigger.reportedWALAssignment.AssignmentEpoch)
}

func TestHandlerClientDoesNotRetryCanceledViewQueryRPC(t *testing.T) {
	attempts := 0
	viewQueryService := &fakeViewQueryServiceClient{
		queryOnView: func(ctx context.Context, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
			attempts++
			return nil, grpcstatus.Error(codes.Canceled, "context canceled")
		},
	}
	client := newTestHandlerClient(nil, viewQueryService, nil)
	client.watcher = fakeAssignmentWatcher{
		walReplicaAssignments: map[types.ChannelID]*types.PChannelInfoAssigned{
			{Name: "p0", WALReplicaID: 2}: {
				Channel:      types.PChannelInfo{Name: "p0", Term: 3, AccessMode: types.AccessModeRO},
				WALReplicaID: 2,
				Node:         types.StreamingNodeInfo{ServerID: 202, Address: "localhost-ro"},
			},
		},
		watchWALReplica: func(context.Context, types.ChannelID, *types.PChannelInfoAssigned) error {
			require.FailNow(t, "canceled RPC should not enter assignment retry loop")
			return nil
		},
	}

	resp, err := client.QueryViewClient().QueryOnView(context.Background(), types.PChannelInfo{Name: "p0"}, 2, &viewpb.QueryOnViewRequest{})

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, 1, attempts)
}

func TestHandlerClientViewSyncRoutesByPChannelAssignment(t *testing.T) {
	viewSyncService := &fakeViewSyncServiceClient{
		syncQueryView: func(ctx context.Context) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
			serverID, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, int64(202), serverID)
			pchannel, err := worknodehandler.DecodeQueryViewPChannelFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{
				Name:       "p0",
				Term:       3,
				AccessMode: types.AccessModeRO,
			}, pchannel)
			walReplicaID, err := worknodehandler.DecodeQueryViewWALReplicaIDFromOutgoingContext(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(2), walReplicaID)
			return &noopViewSyncClientStream{ctx: ctx}, nil
		},
	}
	client := newTestHandlerClient(nil, nil, viewSyncService)

	_, err := client.QueryViewSyncClient().SyncQueryView(context.Background(), "p0", 2)

	require.NoError(t, err)
}

func newTestHandlerClient(queryPlanService viewpb.QueryPlanServiceClient, viewQueryService viewpb.ViewQueryServiceClient, viewSyncService viewpb.ViewSyncServiceClient) *handlerClientImpl {
	client := &handlerClientImpl{
		lifetime: typeutil.NewLifetime(),
		watcher: fakeAssignmentWatcher{
			assignment: &types.PChannelInfoAssigned{
				Channel: types.PChannelInfo{Name: "p0", Term: 1, AccessMode: types.AccessModeRW},
				Node:    types.StreamingNodeInfo{ServerID: 101, Address: "localhost"},
			},
			walReplicaAssignments: map[types.ChannelID]*types.PChannelInfoAssigned{
				{Name: "p0"}: {
					Channel: types.PChannelInfo{Name: "p0", Term: 1, AccessMode: types.AccessModeRW},
					Node:    types.StreamingNodeInfo{ServerID: 101, Address: "localhost"},
				},
				{Name: "p0", WALReplicaID: 2}: {
					Channel:      types.PChannelInfo{Name: "p0", Term: 3, AccessMode: types.AccessModeRO},
					WALReplicaID: 2,
					Node:         types.StreamingNodeInfo{ServerID: 202, Address: "localhost-ro"},
				},
			},
		},
	}
	client.queryViewClient = &queryViewClient{
		owner:            client,
		queryPlanService: fakeLazyService[viewpb.QueryPlanServiceClient]{service: queryPlanService},
		viewQueryService: fakeLazyService[viewpb.ViewQueryServiceClient]{service: viewQueryService},
	}
	client.queryViewSyncClient = &queryViewSyncClient{
		owner:           client,
		viewSyncService: fakeLazyService[viewpb.ViewSyncServiceClient]{service: viewSyncService},
	}
	return client
}

type fakeAssignmentWatcher struct {
	assignment            *types.PChannelInfoAssigned
	walReplicaAssignments map[types.ChannelID]*types.PChannelInfoAssigned
	watchWALReplica       func(context.Context, types.ChannelID, *types.PChannelInfoAssigned) error
}

func (w fakeAssignmentWatcher) Get(context.Context, string) *types.PChannelInfoAssigned {
	return w.assignment
}

func (w fakeAssignmentWatcher) GetWALReplica(_ context.Context, channelID types.ChannelID) *types.PChannelInfoAssigned {
	return w.walReplicaAssignments[channelID]
}

func (w fakeAssignmentWatcher) Watch(context.Context, string, *types.PChannelInfoAssigned) error {
	return context.Canceled
}

func (w fakeAssignmentWatcher) WatchWALReplica(ctx context.Context, channelID types.ChannelID, previous *types.PChannelInfoAssigned) error {
	if w.watchWALReplica != nil {
		return w.watchWALReplica(ctx, channelID, previous)
	}
	return context.Canceled
}

func (w fakeAssignmentWatcher) Close() {}

type fakeLazyService[T any] struct {
	service T
}

func (s fakeLazyService[T]) GetConn(context.Context) (*grpc.ClientConn, error) {
	return nil, nil
}

func (s fakeLazyService[T]) GetService(context.Context) (T, error) {
	return s.service, nil
}

func (s fakeLazyService[T]) Close() {}

type fakeQueryPlanServiceClient struct {
	getQueryPlan     func(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)
	getMVCCTimestamp func(context.Context, *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)
}

func (c *fakeQueryPlanServiceClient) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest, _ ...grpc.CallOption) (*viewpb.GetQueryPlanResponse, error) {
	return c.getQueryPlan(ctx, req)
}

func (c *fakeQueryPlanServiceClient) GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest, _ ...grpc.CallOption) (*viewpb.GetMVCCTimestampResponse, error) {
	return c.getMVCCTimestamp(ctx, req)
}

type fakeViewQueryServiceClient struct {
	searchOnView  func(context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	queryOnView   func(context.Context, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
	requeryOnView func(context.Context, *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error)
}

func (c *fakeViewQueryServiceClient) SearchOnView(ctx context.Context, req *viewpb.SearchOnViewRequest, _ ...grpc.CallOption) (*viewpb.SearchOnViewResponse, error) {
	return c.searchOnView(ctx, req)
}

func (c *fakeViewQueryServiceClient) QueryOnView(ctx context.Context, req *viewpb.QueryOnViewRequest, _ ...grpc.CallOption) (*viewpb.QueryOnViewResponse, error) {
	return c.queryOnView(ctx, req)
}

func (c *fakeViewQueryServiceClient) RequeryOnView(ctx context.Context, req *viewpb.RequeryOnViewRequest, _ ...grpc.CallOption) (*viewpb.RequeryOnViewResponse, error) {
	return c.requeryOnView(ctx, req)
}

type fakeViewSyncServiceClient struct {
	syncQueryView func(context.Context) (viewpb.ViewSyncService_SyncQueryViewClient, error)
}

func (c *fakeViewSyncServiceClient) SyncQueryView(ctx context.Context, _ ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	return c.syncQueryView(ctx)
}

func (c *fakeViewSyncServiceClient) SyncDataView(context.Context, *viewpb.SyncDataViewRequest, ...grpc.CallOption) (*viewpb.SyncDataViewResponse, error) {
	return &viewpb.SyncDataViewResponse{}, nil
}

type noopViewSyncClientStream struct {
	ctx context.Context
}

func (s *noopViewSyncClientStream) Send(*viewpb.SyncRequest) error {
	return nil
}

func (s *noopViewSyncClientStream) Recv() (*viewpb.SyncResponse, error) {
	return nil, io.EOF
}

func (s *noopViewSyncClientStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *noopViewSyncClientStream) Trailer() metadata.MD {
	return nil
}

func (s *noopViewSyncClientStream) CloseSend() error {
	return nil
}

func (s *noopViewSyncClientStream) Context() context.Context {
	return s.ctx
}

func (s *noopViewSyncClientStream) SendMsg(interface{}) error {
	return nil
}

func (s *noopViewSyncClientStream) RecvMsg(interface{}) error {
	return io.EOF
}
