package queryclient

import (
	"context"

	"golang.org/x/sync/errgroup"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/views/queryclient/reducer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// shardViewQueryClient executes two-phase queries at the shard granularity.
// It owns replica resolution, replica selection, consistency routing,
// Phase 1 (GetQueryPlan), Phase 2 (SearchOnView/QueryOnView) dispatch,
// and shard-level retry.
//
// Both Search and Query share the same executeShard framework, differing only
// in what request goes into the GetQueryPlanRequest and which Phase 2 RPC is called.
type shardViewQueryClient struct {
	maxRetries         int
	queryPlanClient    QueryPlanClient
	queryServiceClient ViewQueryServiceClient
	shardResolver      ShardResolver
	replicaPicker      ReplicaPicker
}

func newShardViewQueryClient(
	maxRetries int,
	queryPlanClient QueryPlanClient,
	queryServiceClient ViewQueryServiceClient,
	shardResolver ShardResolver,
	replicaPicker ReplicaPicker,
) *shardViewQueryClient {
	return &shardViewQueryClient{
		maxRetries:         maxRetries,
		queryPlanClient:    queryPlanClient,
		queryServiceClient: queryServiceClient,
		shardResolver:      shardResolver,
		replicaPicker:      replicaPicker,
	}
}

// ShardSearchRequest contains the parameters for a shard-level search execution.
type ShardSearchRequest struct {
	VChannel string
	Req      *internalpb.SearchRequest
	Reducer  reducer.SearchResultReducer
}

// ShardQueryRequest contains the parameters for a shard-level query (retrieve) execution.
type ShardQueryRequest struct {
	VChannel string
	Req      *internalpb.RetrieveRequest
	Reducer  reducer.RetrieveResultReducer
}

// Search executes Phase 1 + Phase 2 for a single shard's search.
// Replica resolution is handled internally. Results are fed into the provided reducer.
// Returns the ShardPlan for potential requery.
func (s *shardViewQueryClient) Search(ctx context.Context, req *ShardSearchRequest) (*ShardPlan, error) {
	return s.executeShard(ctx, req.Req.CollectionID, req.VChannel, &shardExecParams{
		consistencyLevel: req.Req.ConsistencyLevel,
		guaranteeTs:      req.Req.GuaranteeTimestamp,
		buildPlanReq: func(targetShardID qviews.ShardID) *viewpb.GetQueryPlanRequest {
			return &viewpb.GetQueryPlanRequest{
				CollectionId: req.Req.CollectionID,
				ShardId:      targetShardID.IntoProto(),
				PartitionIds: req.Req.PartitionIDs,
				Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{
					LegacySearchRequest: req.Req,
				},
			}
		},
		dispatchNode: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error {
			resp, err := s.queryServiceClient.SearchOnView(ctx, node, &viewpb.SearchOnViewRequest{
				LegacyReq: plan.GetLegacySearchRequest(),
				ShardId:   shardID.IntoProto(),
				Version:   plan.Version,
			})
			if err != nil {
				return err
			}
			return req.Reducer.Add(shardID, resp)
		},
		resetShard: req.Reducer.ResetShard,
	})
}

// Query executes Phase 1 + Phase 2 for a single shard's query (retrieve).
// Replica resolution is handled internally. Results are fed into the provided reducer.
// Returns the ShardPlan for potential requery.
func (s *shardViewQueryClient) Query(ctx context.Context, req *ShardQueryRequest) (*ShardPlan, error) {
	return s.executeShard(ctx, req.Req.CollectionID, req.VChannel, &shardExecParams{
		consistencyLevel: req.Req.ConsistencyLevel,
		guaranteeTs:      req.Req.GuaranteeTimestamp,
		buildPlanReq: func(targetShardID qviews.ShardID) *viewpb.GetQueryPlanRequest {
			return &viewpb.GetQueryPlanRequest{
				CollectionId: req.Req.CollectionID,
				ShardId:      targetShardID.IntoProto(),
				PartitionIds: req.Req.PartitionIDs,
				Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{
					LegacyRetrieveRequest: req.Req,
				},
			}
		},
		dispatchNode: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error {
			resp, err := s.queryServiceClient.QueryOnView(ctx, node, &viewpb.QueryOnViewRequest{
				LegacyReq: plan.GetLegacyRetrieveRequest(),
				ShardId:   shardID.IntoProto(),
				Version:   plan.Version,
			})
			if err != nil {
				return err
			}
			return req.Reducer.Add(shardID, resp)
		},
		resetShard: req.Reducer.ResetShard,
	})
}

// ============================================================================
// Shared shard execution framework
// ============================================================================

// shardExecParams parameterizes the per-shard Phase 1 + Phase 2 execution.
// Both Search and Query use the same executeShard loop, differing only in these callbacks.
type shardExecParams struct {
	consistencyLevel commonpb.ConsistencyLevel
	guaranteeTs      uint64
	// buildPlanReq creates the GetQueryPlanRequest for a target shard.
	buildPlanReq func(targetShardID qviews.ShardID) *viewpb.GetQueryPlanRequest
	// dispatchNode executes a Phase 2 RPC on a single work node.
	// Called concurrently for each work node in the plan.
	dispatchNode func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error
	// resetShard resets the reducer state for the given shard on retry.
	resetShard func(shardID qviews.ShardID)
}

// executeShard runs Phase 1 + Phase 2 for a single shard with retry.
// Replica resolution is performed at the beginning of each attempt (including the first),
// so stale primary mappings are automatically refreshed on retry.
func (s *shardViewQueryClient) executeShard(
	ctx context.Context,
	collectionID int64,
	vchannel string,
	params *shardExecParams,
) (*ShardPlan, error) {
	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		// Resolve shard replicas (every attempt, including first).
		// ShardResolver uses a local cache, so this is a zero-overhead lookup.
		shardReplicas, err := s.shardResolver.ResolveShard(ctx, collectionID, vchannel)
		if err != nil {
			return nil, err
		}

		// Select target replica via picker.
		pickResult, err := s.replicaPicker.Pick(ctx, ReplicaPickInfo{ShardReplicas: shardReplicas})
		if err != nil {
			return nil, err
		}
		targetShardID := pickResult.ShardID

		// Phase 1: GetQueryPlan with consistency routing.
		planReq := params.buildPlanReq(targetShardID)
		plan, err := s.executeGetQueryPlan(ctx, params.consistencyLevel, params.guaranteeTs, targetShardID, shardReplicas, planReq)
		if err != nil {
			if pickResult.Done != nil {
				pickResult.Done(ReplicaDoneInfo{Err: err})
			}
			if ve := viewerror.AsViewError(err); ve != nil && ve.IsRetryable() {
				lastErr = err
				continue
			}
			return nil, err
		}

		shardID := qviews.FromProtoShardID(plan.ShardId)
		workNodes := workNodesFromPlan(plan)

		// Phase 2: Fan out to all work nodes concurrently.
		// Set MVCC timestamp into the plan's request before dispatch.
		// Both SearchRequest and RetrieveRequest have MvccTimestamp field.
		if sr := plan.GetLegacySearchRequest(); sr != nil {
			sr.MvccTimestamp = plan.MvccTimestamp
		}
		if rr := plan.GetLegacyRetrieveRequest(); rr != nil {
			rr.MvccTimestamp = plan.MvccTimestamp
		}
		err = s.fanOutToWorkNodes(ctx, workNodes, plan, shardID, params.dispatchNode)
		if pickResult.Done != nil {
			pickResult.Done(ReplicaDoneInfo{Err: err})
		}
		if err != nil {
			if ve := viewerror.AsViewError(err); ve != nil && ve.IsRetryable() {
				lastErr = err
				params.resetShard(shardID)
				continue
			}
			return nil, err
		}

		return &ShardPlan{
			ShardID:   shardID,
			Version:   plan.Version,
			WorkNodes: workNodes,
		}, nil
	}
	return nil, lastErr
}

// executeGetQueryPlan handles consistency-level routing and dispatches Phase 1.
//
// Routing logic per consistency level:
//   - Strong on primary: GetQueryPlan(consistency_level=Strong)
//   - Strong cross-replica: GetMVCCTimestamp from primary → GetQueryPlan(mvcc_timestamp=ts)
//   - Session: GetQueryPlan(mvcc_timestamp=guaranteeTs) — Proxy provides session timestamp
//   - Bounded/Eventually: GetQueryPlan(consistency_level=...) — SN generates MVCC from WAL
func (s *shardViewQueryClient) executeGetQueryPlan(
	ctx context.Context,
	consistencyLevel commonpb.ConsistencyLevel,
	guaranteeTs uint64,
	targetShardID qviews.ShardID,
	shardReplicas *ShardReplicas,
	planReq *viewpb.GetQueryPlanRequest,
) (*viewpb.QueryPlan, error) {
	switch consistencyLevel {
	case commonpb.ConsistencyLevel_Strong:
		if targetShardID != shardReplicas.PrimaryShardID {
			mvccResp, err := s.queryPlanClient.GetMVCCTimestamp(ctx, shardReplicas.PrimaryShardID,
				&viewpb.GetMVCCTimestampRequest{
					Vchannel: targetShardID.VChannel,
				})
			if err != nil {
				return nil, err
			}
			planReq.Mvcc = &viewpb.GetQueryPlanRequest_MvccTimestamp{
				MvccTimestamp: mvccResp.MvccTimestamp,
			}
		} else {
			planReq.Mvcc = &viewpb.GetQueryPlanRequest_ConsistencyLevel{
				ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			}
		}
	case commonpb.ConsistencyLevel_Session:
		planReq.Mvcc = &viewpb.GetQueryPlanRequest_MvccTimestamp{
			MvccTimestamp: guaranteeTs,
		}
	default:
		planReq.Mvcc = &viewpb.GetQueryPlanRequest_ConsistencyLevel{
			ConsistencyLevel: consistencyLevel,
		}
	}

	resp, err := s.queryPlanClient.GetQueryPlan(ctx, targetShardID, planReq)
	if err != nil {
		return nil, err
	}
	return resp.Plan, nil
}

// fanOutToWorkNodes dispatches Phase 2 to all work nodes concurrently.
// Uses errgroup.WithContext for fast-fail: if any node fails, gCtx is canceled
// and in-flight RPCs on other nodes are aborted. The caller (executeShard) then
// retries the entire shard.
func (s *shardViewQueryClient) fanOutToWorkNodes(
	ctx context.Context,
	workNodes []qviews.WorkNode,
	plan *viewpb.QueryPlan,
	shardID qviews.ShardID,
	dispatchNode func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error,
) error {
	g, gCtx := errgroup.WithContext(ctx)
	for _, node := range workNodes {
		node := node
		g.Go(func() error {
			return dispatchNode(gCtx, node, plan, shardID)
		})
	}
	return g.Wait()
}

// workNodesFromPlan converts proto QueryPlanWorkNode list to domain WorkNode types.
func workNodesFromPlan(plan *viewpb.QueryPlan) []qviews.WorkNode {
	nodes := make([]qviews.WorkNode, 0, len(plan.WorkNodes))
	for _, n := range plan.WorkNodes {
		switch v := n.Node.(type) {
		case *viewpb.QueryPlanWorkNode_QueryNode:
			nodes = append(nodes, qviews.NewQueryNode(v.QueryNode.NodeId))
		case *viewpb.QueryPlanWorkNode_StreamingNode:
			nodes = append(nodes, qviews.StreamingNode{PChannel: v.StreamingNode.Pchannel})
		}
	}
	return nodes
}
