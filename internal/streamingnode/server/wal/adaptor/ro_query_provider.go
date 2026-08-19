package adaptor

import (
	"context"

	"google.golang.org/protobuf/proto"

	queryplanprovider "github.com/milvus-io/milvus/internal/streamingnode/server/queryplan/provider"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	_ queryplanprovider.QueryPlanProvider = (*roWALAdaptorImpl)(nil)
	_ snview.QueryViewHandlerProvider     = (*roWALAdaptorImpl)(nil)
	_ viewquery.TaskProvider              = (*roWALAdaptorImpl)(nil)
)

func (w *roWALAdaptorImpl) QueryViewHandler() worknodehandler.QueryViewHandler {
	return w.queryViewHandler
}

func (w *roWALAdaptorImpl) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlan, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, viewerror.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	if req == nil || req.GetShardId() == nil {
		return nil, viewerror.NewUnknownError("query plan request misses shard id")
	}
	if w.queryViewHandler == nil {
		return nil, viewerror.NewViewNotFound("query view handler is unavailable")
	}

	shardID := qviews.FromProtoShardID(req.GetShardId())
	lease, err := w.queryViewHandler.AcquireLatestUpView(ctx, shardID)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	if req.GetCollectionId() != 0 && lease.Meta.GetCollectionId() != req.GetCollectionId() {
		return nil, viewerror.NewViewNotFound("query view collection mismatch, expected %d, got %d", req.GetCollectionId(), lease.Meta.GetCollectionId())
	}

	mvcc, err := w.resolveReadOnlyQueryPlanMVCC(req)
	if err != nil {
		return nil, err
	}

	var runtime *queryresource.QueryRuntime
	if w.viewResourceManager != nil {
		runtime, _ = w.viewResourceManager.GetQueryRuntime(qviews.QueryViewKey{
			ShardID:          shardID,
			WALReplicaID:     lease.View.GetStreamingNode().GetWalReplicaId(),
			QueryViewVersion: lease.Version,
		})
	}
	walReplicaID := lease.View.GetStreamingNode().GetWalReplicaId()
	optimizer := queryresource.NewGlobalOptimizer(runtime, lease.Version.DataVersion, shard.WALReplicaFunctionRunnerKey(shardID.VChannel, walReplicaID))
	plan := &viewpb.QueryPlan{
		Version: lease.Version.IntoProto(),
		ShardId: shardID.IntoProto(),
		Mvcc:    mvcc,
	}
	switch request := req.GetRequest().(type) {
	case *viewpb.GetQueryPlanRequest_LegacySearchRequest:
		if request.LegacySearchRequest == nil {
			return nil, viewerror.NewUnknownError("query plan request misses legacy search request")
		}
		searchReq := proto.Clone(request.LegacySearchRequest).(*internalpb.SearchRequest)
		fillSearchRequestPartitionIDs(searchReq, req.GetPartitionIds())
		optimization, err := optimizer.OptimizeSearch(ctx, searchReq)
		if err != nil {
			return nil, err
		}
		plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: searchReq}
		if !optimization.Skip {
			plan.WorkNodes = buildQueryPlanWorkNodes(lease.View, queryPlanWorkNodeOptions{
				ignoreGrowing: searchReq.GetIgnoreGrowing(),
				partitionIDs:  searchReq.GetPartitionIDs(),
				runtime:       runtime,
				mvcc:          mvcc,
			})
		}
	case *viewpb.GetQueryPlanRequest_LegacyRetrieveRequest:
		if request.LegacyRetrieveRequest == nil {
			return nil, viewerror.NewUnknownError("query plan request misses legacy retrieve request")
		}
		retrieveReq := proto.Clone(request.LegacyRetrieveRequest).(*internalpb.RetrieveRequest)
		fillRetrieveRequestPartitionIDs(retrieveReq, req.GetPartitionIds())
		if err := optimizer.OptimizeRetrieve(ctx, retrieveReq); err != nil {
			return nil, err
		}
		plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: retrieveReq}
		plan.WorkNodes = buildQueryPlanWorkNodes(lease.View, queryPlanWorkNodeOptions{
			ignoreGrowing: retrieveReq.GetIgnoreGrowing(),
			partitionIDs:  retrieveReq.GetPartitionIDs(),
			runtime:       runtime,
			mvcc:          mvcc,
		})
	default:
		return nil, viewerror.NewUnknownError("query plan request misses legacy request")
	}
	return plan, nil
}

func (w *roWALAdaptorImpl) GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	if req == nil || req.GetVchannel() == "" {
		return nil, viewerror.NewUnknownError("mvcc request misses vchannel")
	}
	return nil, w.notPrimaryError()
}

func (w *roWALAdaptorImpl) resolveReadOnlyQueryPlanMVCC(req *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlanMVCC, error) {
	switch mvcc := req.GetMvcc().(type) {
	case *viewpb.GetQueryPlanRequest_QueryPlanMvcc:
		return mvcc.QueryPlanMvcc, nil
	case *viewpb.GetQueryPlanRequest_ConsistencyLevel:
		return nil, w.notPrimaryError()
	default:
		return nil, viewerror.NewUnknownError("query plan request misses mvcc source")
	}
}

func (w *roWALAdaptorImpl) AcquireSearchSegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	walReplicaID int64,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
) (viewquery.SearchSegmentTasks, error) {
	h, err := w.queryViewTaskHandler(shardID)
	if err != nil {
		return nil, err
	}
	return h.AcquireSearchSegmentTasks(ctx, shardID, walReplicaID, version, mvcc, req)
}

func (w *roWALAdaptorImpl) AcquireQuerySegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	walReplicaID int64,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.RetrieveRequest,
) (viewquery.QuerySegmentTasks, error) {
	h, err := w.queryViewTaskHandler(shardID)
	if err != nil {
		return nil, err
	}
	return h.AcquireQuerySegmentTasks(ctx, shardID, walReplicaID, version, mvcc, req)
}

func (w *roWALAdaptorImpl) queryViewTaskHandler(shardID qviews.ShardID) (*snview.SNQueryViewHandler, error) {
	if !w.IsAvailable() {
		return nil, viewerror.NewOnShutdownError("wal is on shutdown")
	}
	if funcutil.ToPhysicalChannel(shardID.VChannel) != w.Channel().Name {
		return nil, viewerror.NewViewNotFound("query view shard %s is not on wal %s", shardID.String(), w.Channel().String())
	}
	if w.queryViewHandler == nil {
		return nil, viewerror.NewViewNotFound("query view handler is unavailable")
	}
	return w.queryViewHandler, nil
}

func (w *roWALAdaptorImpl) notPrimaryError() *viewerror.ViewError {
	return viewerror.NewNotPrimaryError("wal %s is not primary", w.Channel().String())
}
