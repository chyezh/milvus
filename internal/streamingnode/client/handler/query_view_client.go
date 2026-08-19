package handler

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// QueryViewClient is the QueryView domain client under HandlerClient.
type QueryViewClient interface {
	// GetQueryPlan generates a shard-level query plan from the StreamingNode owning the shard pchannel.
	GetQueryPlan(ctx context.Context, shardID qviews.ShardID, walReplicaID int64, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error)

	// GetMVCCTimestamp returns query-plan MVCC frontiers from the StreamingNode owning the shard pchannel.
	GetMVCCTimestamp(ctx context.Context, shardID qviews.ShardID, walReplicaID int64, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)

	// SearchOnView executes a QueryView search on the StreamingNode owning the pchannel.
	SearchOnView(ctx context.Context, pchannel types.PChannelInfo, walReplicaID int64, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)

	// QueryOnView executes a QueryView retrieve on the StreamingNode owning the pchannel.
	QueryOnView(ctx context.Context, pchannel types.PChannelInfo, walReplicaID int64, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)

	// RequeryOnView fetches fields from the StreamingNode owning the pchannel for a previous QueryView plan.
	RequeryOnView(ctx context.Context, pchannel types.PChannelInfo, walReplicaID int64, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error)
}

type queryViewClient struct {
	owner            *handlerClientImpl
	queryPlanService lazygrpc.Service[viewpb.QueryPlanServiceClient]
	viewQueryService lazygrpc.Service[viewpb.ViewQueryServiceClient]
}

type viewRPCResult[T any] struct {
	resp T
	err  error
}

func newQueryViewClient(owner *handlerClientImpl, conn lazygrpc.Conn) QueryViewClient {
	return &queryViewClient{
		owner:            owner,
		queryPlanService: lazygrpc.WithServiceCreator(conn, viewpb.NewQueryPlanServiceClient),
		viewQueryService: lazygrpc.WithServiceCreator(conn, viewpb.NewViewQueryServiceClient),
	}
}

func (hc *handlerClientImpl) QueryViewClient() QueryViewClient {
	return hc.queryViewClient
}

func (qvc *queryViewClient) GetQueryPlan(ctx context.Context, shardID qviews.ShardID, walReplicaID int64, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	result, err := executeQueryPlanRPC(ctx, qvc, shardID.VChannel, walReplicaID, "QueryPlanService.GetQueryPlan", func(ctx context.Context, client viewpb.QueryPlanServiceClient) (*viewpb.GetQueryPlanResponse, error) {
		return client.GetQueryPlan(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) GetMVCCTimestamp(ctx context.Context, shardID qviews.ShardID, walReplicaID int64, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	result, err := executeQueryPlanRPC(ctx, qvc, shardID.VChannel, walReplicaID, "QueryPlanService.GetMVCCTimestamp", func(ctx context.Context, client viewpb.QueryPlanServiceClient) (*viewpb.GetMVCCTimestampResponse, error) {
		return client.GetMVCCTimestamp(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) SearchOnView(ctx context.Context, pchannel types.PChannelInfo, walReplicaID int64, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, walReplicaID, "ViewQueryService.SearchOnView", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.SearchOnViewResponse, error) {
		return client.SearchOnView(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) QueryOnView(ctx context.Context, pchannel types.PChannelInfo, walReplicaID int64, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, walReplicaID, "ViewQueryService.QueryOnView", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.QueryOnViewResponse, error) {
		return client.QueryOnView(ctx, req)
	})
	return result, err
}

func (qvc *queryViewClient) RequeryOnView(ctx context.Context, pchannel types.PChannelInfo, walReplicaID int64, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	result, err := executeViewQueryRPC(ctx, qvc, pchannel, walReplicaID, "ViewQueryService.RequeryOnView", func(ctx context.Context, client viewpb.ViewQueryServiceClient) (*viewpb.RequeryOnViewResponse, error) {
		return client.RequeryOnView(ctx, req)
	})
	return result, err
}

func executeQueryPlanRPC[T any](
	ctx context.Context,
	qvc *queryViewClient,
	vchannel string,
	walReplicaID int64,
	method string,
	call func(context.Context, viewpb.QueryPlanServiceClient) (T, error),
) (T, error) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	logger := mlog.With(mlog.FieldPChannel(pchannel), mlog.Int64("walReplicaID", walReplicaID), mlog.String("handler", method))
	return executeViewRPC(ctx, qvc, logger, pchannel, walReplicaID, func(ctx context.Context, _ *types.PChannelInfoAssigned) (viewRPCResult[T], error) {
		client, err := qvc.queryPlanService.GetService(ctx)
		if err != nil {
			return viewRPCResult[T]{}, err
		}
		resp, err := call(ctx, client)
		return viewRPCResult[T]{resp: resp, err: viewerror.ConvertViewError(method, err)}, nil
	})
}

func executeViewQueryRPC[T any](
	ctx context.Context,
	qvc *queryViewClient,
	pchannel types.PChannelInfo,
	walReplicaID int64,
	method string,
	call func(context.Context, viewpb.ViewQueryServiceClient) (T, error),
) (T, error) {
	logger := mlog.With(mlog.FieldPChannel(pchannel.Name), mlog.Int64("walReplicaID", walReplicaID), mlog.String("handler", method))
	return executeViewRPC(ctx, qvc, logger, pchannel.Name, walReplicaID, func(ctx context.Context, _ *types.PChannelInfoAssigned) (viewRPCResult[T], error) {
		client, err := qvc.viewQueryService.GetService(ctx)
		if err != nil {
			return viewRPCResult[T]{}, err
		}
		resp, err := call(ctx, client)
		return viewRPCResult[T]{resp: resp, err: viewerror.ConvertViewError(method, err)}, nil
	})
}

func executeViewRPC[T any](
	ctx context.Context,
	qvc *queryViewClient,
	logger *mlog.Logger,
	pchannel string,
	walReplicaID int64,
	call func(context.Context, *types.PChannelInfoAssigned) (viewRPCResult[T], error),
) (T, error) {
	channelID := types.ChannelID{Name: pchannel, WALReplicaID: walReplicaID}
	result, err := qvc.owner.createHandlerAfterWALReplicaReady(ctx, logger, channelID, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		rpcCtx := worknodehandler.EncodeQueryViewWALReplicaToOutgoingContext(ctx, assign.Channel, assign.WALReplicaID)
		rpcResult, err := call(rpcCtx, assign)
		if err != nil {
			return nil, err
		}
		if shouldRetryViewRPCAtHandler(rpcResult.err) {
			return nil, rpcResult.err
		}
		return rpcResult, nil
	})
	if err != nil {
		var zero T
		return zero, err
	}
	rpcResult := result.(viewRPCResult[T])
	return rpcResult.resp, rpcResult.err
}

func shouldRetryViewRPCAtHandler(err error) bool {
	if err == nil {
		return false
	}
	var clientStatus *viewerror.ViewClientStatus
	if !errors.As(err, &clientStatus) {
		return false
	}
	return clientStatus.Code() == codes.Unavailable && clientStatus.TryIntoViewError() == nil
}
