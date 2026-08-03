package queryclient

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestShardSearchReturnsQueryPlanMVCCForRequery(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	mvcc := &viewpb.QueryPlanMVCC{
		GrowingTimetick:      100,
		TransformingTimetick: 90,
	}
	version := &viewpb.QueryViewVersion{
		QueryVersion: 10,
	}
	queryNode := qviews.NewQueryNode(11)
	planClient := &fakeQueryPlanClient{
		plan: &viewpb.QueryPlan{
			ShardId: shardID.IntoProto(),
			Version: version,
			Mvcc:    mvcc,
			Request: &viewpb.QueryPlan_LegacySearchRequest{
				LegacySearchRequest: &internalpb.SearchRequest{},
			},
			WorkNodes: []*viewpb.QueryPlanWorkNode{
				{
					Node: &viewpb.QueryPlanWorkNode_QueryNode{
						QueryNode: &viewpb.QueryWorkNode{NodeId: queryNode.ID},
					},
				},
			},
		},
	}
	queryService := &fakeViewQueryServiceClient{}
	client := newShardViewQueryClient(
		1,
		planClient,
		queryService,
		&fakeShardResolver{replicas: &resolver.ShardReplicas{
			VChannel:       shardID.VChannel,
			PrimaryShardID: shardID,
			ShardIDs:       []qviews.ShardID{shardID},
			WALReplicaIDs:  map[qviews.ShardID]int64{shardID: 2},
		}},
		fixedReplicaPicker{shardID: shardID},
	)

	shardPlan, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: shardID.VChannel,
		Req: &internalpb.SearchRequest{
			CollectionID:     100,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		},
		Reducer: fakeSearchResultReducer{},
	})
	require.NoError(t, err)

	require.True(t, proto.Equal(mvcc, shardPlan.Mvcc))
	require.Equal(t, int64(2), shardPlan.WALReplicaID)
	require.Equal(t, int64(2), planClient.planWALReplicaID)
	require.Equal(t, int64(2), queryService.searchReq.GetWalReplicaId())
	require.True(t, proto.Equal(mvcc, queryService.searchReq.GetMvcc()))
	require.Equal(t, mvcc.GetTransformingTimetick(), queryService.searchReq.GetLegacyReq().GetMvccTimestamp())
}

func TestSessionSearchOnPrimaryLetsSNGenerateQueryPlanMVCC(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	planClient := &fakeQueryPlanClient{
		plan: newTestSearchQueryPlan(shardID, &viewpb.QueryPlanMVCC{
			GrowingTimetick:      100,
			TransformingTimetick: 90,
		}),
	}
	queryService := &fakeViewQueryServiceClient{}
	client := newShardViewQueryClient(
		1,
		planClient,
		queryService,
		&fakeShardResolver{replicas: &resolver.ShardReplicas{
			VChannel:       shardID.VChannel,
			PrimaryShardID: shardID,
			ShardIDs:       []qviews.ShardID{shardID},
			WALReplicaIDs:  map[qviews.ShardID]int64{shardID: 2},
		}},
		fixedReplicaPicker{shardID: shardID},
	)

	_, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: shardID.VChannel,
		Req: &internalpb.SearchRequest{
			CollectionID:       100,
			ConsistencyLevel:   commonpb.ConsistencyLevel_Session,
			GuaranteeTimestamp: 999,
		},
		Reducer: fakeSearchResultReducer{},
	})
	require.NoError(t, err)

	require.Equal(t, commonpb.ConsistencyLevel_Strong, planClient.planReq.GetConsistencyLevel())
	require.Nil(t, planClient.planReq.GetQueryPlanMvcc())
	require.Equal(t, int64(2), planClient.planWALReplicaID)
	require.Equal(t, 0, planClient.mvccReqCount)
}

func TestStrongSearchPrefersPrimaryWALReplicaWhenPickerSelectsSecondary(t *testing.T) {
	vchannel := "by-dev-rootcoord-dml_0_100v0"
	primaryShardID := qviews.ShardID{ReplicaID: 1, VChannel: vchannel}
	secondaryShardID := qviews.ShardID{ReplicaID: 2, VChannel: vchannel}
	mvcc := &viewpb.QueryPlanMVCC{
		GrowingTimetick:      100,
		TransformingTimetick: 90,
	}
	planClient := &fakeQueryPlanClient{plan: newTestSearchQueryPlan(primaryShardID, mvcc)}
	queryService := &fakeViewQueryServiceClient{}
	client := newShardViewQueryClient(
		1,
		planClient,
		queryService,
		&fakeShardResolver{replicas: &resolver.ShardReplicas{
			VChannel:       vchannel,
			PrimaryShardID: primaryShardID,
			ShardIDs:       []qviews.ShardID{primaryShardID, secondaryShardID},
			WALReplicaIDs: map[qviews.ShardID]int64{
				primaryShardID:   0,
				secondaryShardID: 2,
			},
		}},
		fixedReplicaPicker{shardID: secondaryShardID},
	)

	_, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: vchannel,
		Req: &internalpb.SearchRequest{
			CollectionID:       100,
			ConsistencyLevel:   commonpb.ConsistencyLevel_Strong,
			GuaranteeTimestamp: 999,
		},
		Reducer: fakeSearchResultReducer{},
	})
	require.NoError(t, err)

	require.Equal(t, 0, planClient.mvccReqCount)
	require.Nil(t, planClient.planReq.GetQueryPlanMvcc())
	require.Equal(t, commonpb.ConsistencyLevel_Strong, planClient.planReq.GetConsistencyLevel())
	require.Equal(t, primaryShardID, qviews.FromProtoShardID(planClient.planReq.GetShardId()))
	require.Equal(t, int64(0), planClient.planWALReplicaID)
	require.Equal(t, int64(0), queryService.searchReq.GetWalReplicaId())
}

func TestNonPrimarySearchUsesPrimaryWALMVCC(t *testing.T) {
	for _, consistencyLevel := range []commonpb.ConsistencyLevel{
		commonpb.ConsistencyLevel_Bounded,
		commonpb.ConsistencyLevel_Eventually,
	} {
		t.Run(consistencyLevel.String(), func(t *testing.T) {
			vchannel := "by-dev-rootcoord-dml_0_100v0"
			primaryShardID := qviews.ShardID{ReplicaID: 1, VChannel: vchannel}
			secondaryShardID := qviews.ShardID{ReplicaID: 2, VChannel: vchannel}
			mvcc := &viewpb.QueryPlanMVCC{
				GrowingTimetick:      100,
				TransformingTimetick: 90,
			}
			planClient := &fakeQueryPlanClient{plan: newTestSearchQueryPlan(secondaryShardID, mvcc)}
			queryService := &fakeViewQueryServiceClient{}
			client := newShardViewQueryClient(
				1,
				planClient,
				queryService,
				&fakeShardResolver{replicas: &resolver.ShardReplicas{
					VChannel:       vchannel,
					PrimaryShardID: primaryShardID,
					ShardIDs:       []qviews.ShardID{primaryShardID, secondaryShardID},
					WALReplicaIDs: map[qviews.ShardID]int64{
						primaryShardID:   0,
						secondaryShardID: 2,
					},
				}},
				fixedReplicaPicker{shardID: secondaryShardID},
			)

			_, err := client.Search(context.Background(), &ShardSearchRequest{
				VChannel: vchannel,
				Req: &internalpb.SearchRequest{
					CollectionID:     100,
					ConsistencyLevel: consistencyLevel,
				},
				Reducer: fakeSearchResultReducer{},
			})
			require.NoError(t, err)

			require.Equal(t, 1, planClient.mvccReqCount)
			require.Equal(t, primaryShardID, planClient.mvccShardID)
			require.Equal(t, int64(0), planClient.mvccWALReplicaID)
			require.Equal(t, vchannel, planClient.mvccReq.GetVchannel())
			require.True(t, proto.Equal(mvcc, planClient.planReq.GetQueryPlanMvcc()))
			require.Equal(t, commonpb.ConsistencyLevel(0), planClient.planReq.GetConsistencyLevel())
			require.Equal(t, int64(2), planClient.planWALReplicaID)
		})
	}
}

func TestSessionSearchOnSecondaryFailsWhenPrimaryWALIsNotServiceable(t *testing.T) {
	vchannel := "by-dev-rootcoord-dml_0_100v0"
	secondaryShardID := qviews.ShardID{ReplicaID: 2, VChannel: vchannel}
	planClient := &fakeQueryPlanClient{plan: newTestSearchQueryPlan(secondaryShardID, &viewpb.QueryPlanMVCC{})}
	client := newShardViewQueryClient(
		1,
		planClient,
		&fakeViewQueryServiceClient{},
		&fakeShardResolver{replicas: &resolver.ShardReplicas{
			VChannel:       vchannel,
			PrimaryShardID: qviews.ShardID{},
			ShardIDs:       []qviews.ShardID{secondaryShardID},
			WALReplicaIDs:  map[qviews.ShardID]int64{secondaryShardID: 2},
		}},
		fixedReplicaPicker{shardID: secondaryShardID},
	)

	_, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: vchannel,
		Req: &internalpb.SearchRequest{
			CollectionID:     100,
			ConsistencyLevel: commonpb.ConsistencyLevel_Session,
		},
		Reducer: fakeSearchResultReducer{},
	})

	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, 0, planClient.mvccReqCount)
}

func TestShardQueryRetriesTransportUnavailableFromPhase2(t *testing.T) {
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "by-dev-rootcoord-dml_0_100v0"}
	planClient := &fakeQueryPlanClient{
		plan: newTestQueryPlan(shardID, &viewpb.QueryPlanMVCC{
			GrowingTimetick:      100,
			TransformingTimetick: 90,
		}),
	}
	queryService := &fakeViewQueryServiceClient{
		queryErrs: []error{
			viewerror.ConvertViewError("ViewQueryService.QueryOnView", grpcstatus.Error(codes.Unavailable, "subConn not exist")),
			nil,
		},
	}
	reducer := &fakeRetrieveResultReducer{}
	client := newShardViewQueryClient(
		2,
		planClient,
		queryService,
		&fakeShardResolver{replicas: &resolver.ShardReplicas{
			VChannel:       shardID.VChannel,
			PrimaryShardID: shardID,
			ShardIDs:       []qviews.ShardID{shardID},
			WALReplicaIDs:  map[qviews.ShardID]int64{shardID: 2},
		}},
		fixedReplicaPicker{shardID: shardID},
	)

	_, err := client.Query(context.Background(), &ShardQueryRequest{
		VChannel: shardID.VChannel,
		Req: &internalpb.RetrieveRequest{
			CollectionID:     100,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		},
		Reducer: reducer,
	})

	require.NoError(t, err)
	require.Equal(t, 2, planClient.planReqCount)
	require.Equal(t, 2, queryService.queryReqCount)
	require.Equal(t, 1, reducer.resetCount)
	require.Equal(t, 1, reducer.addCount)
}

func TestShardSearchWaitsForTopologyChangeBeforeRetryingStalePrimaryMVCC(t *testing.T) {
	vchannel := "by-dev-rootcoord-dml_0_100v0"
	oldPrimaryShardID := qviews.ShardID{ReplicaID: 1, VChannel: vchannel}
	newPrimaryShardID := qviews.ShardID{ReplicaID: 2, VChannel: vchannel}
	mvcc := &viewpb.QueryPlanMVCC{
		GrowingTimetick:      100,
		TransformingTimetick: 90,
	}
	stalePrimaryErr := viewerror.ConvertViewError(
		"QueryPlanService.GetMVCCTimestamp",
		viewerror.NewGRPCStatusFromViewError(viewerror.NewOnShutdownError("local WAL is unavailable")).Err(),
	)
	planClient := &fakeQueryPlanClient{
		plan:     newTestSearchQueryPlan(newPrimaryShardID, mvcc),
		mvccErrs: []error{stalePrimaryErr},
	}
	queryService := &fakeViewQueryServiceClient{}
	shardResolver := &waitForChangeShardResolver{
		vchannel: vchannel,
		current: &resolver.ShardReplicas{
			VChannel:       vchannel,
			PrimaryShardID: oldPrimaryShardID,
			ShardIDs:       []qviews.ShardID{oldPrimaryShardID, newPrimaryShardID},
			WALReplicaIDs: map[qviews.ShardID]int64{
				oldPrimaryShardID: 2,
				newPrimaryShardID: 3,
			},
		},
		next: &resolver.ShardReplicas{
			VChannel:       vchannel,
			PrimaryShardID: newPrimaryShardID,
			ShardIDs:       []qviews.ShardID{oldPrimaryShardID, newPrimaryShardID},
			WALReplicaIDs: map[qviews.ShardID]int64{
				oldPrimaryShardID: 2,
				newPrimaryShardID: 3,
			},
		},
	}
	client := newShardViewQueryClient(
		2,
		planClient,
		queryService,
		shardResolver,
		fixedReplicaPicker{shardID: newPrimaryShardID},
	)

	_, err := client.Search(context.Background(), &ShardSearchRequest{
		VChannel: vchannel,
		Req: &internalpb.SearchRequest{
			CollectionID:     100,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		},
		Reducer: fakeSearchResultReducer{},
	})

	require.NoError(t, err)
	require.Equal(t, 1, shardResolver.waitCount)
	require.Equal(t, 2, shardResolver.resolveCount)
	require.Equal(t, 1, planClient.mvccReqCount)
	require.Equal(t, oldPrimaryShardID, planClient.mvccShardID)
	require.Equal(t, int64(2), planClient.mvccWALReplicaID)
	require.Equal(t, 1, planClient.planReqCount)
	require.Equal(t, int64(3), planClient.planWALReplicaID)
	require.Equal(t, int64(3), queryService.searchReq.GetWalReplicaId())
}

func TestShardQueryWaitsForTopologyChangeBeforeRetryingInvalidatedView(t *testing.T) {
	vchannel := "by-dev-rootcoord-dml_0_100v0"
	oldPrimaryShardID := qviews.ShardID{ReplicaID: 1, VChannel: vchannel}
	newPrimaryShardID := qviews.ShardID{ReplicaID: 2, VChannel: vchannel}
	mvcc := &viewpb.QueryPlanMVCC{
		GrowingTimetick:      100,
		TransformingTimetick: 90,
	}
	invalidatedErr := viewerror.ConvertViewError(
		"ViewQueryService.QueryOnView",
		viewerror.NewGRPCStatusFromViewError(viewerror.NewViewInvalidated("query view dropped")).Err(),
	)
	planClient := &fakeQueryPlanClient{
		plan: newTestQueryPlan(oldPrimaryShardID, mvcc),
	}
	queryService := &fakeViewQueryServiceClient{
		queryErrs: []error{invalidatedErr, nil},
	}
	shardResolver := &waitForChangeShardResolver{
		vchannel: vchannel,
		current: &resolver.ShardReplicas{
			VChannel:       vchannel,
			PrimaryShardID: oldPrimaryShardID,
			ShardIDs:       []qviews.ShardID{oldPrimaryShardID, newPrimaryShardID},
			WALReplicaIDs: map[qviews.ShardID]int64{
				oldPrimaryShardID: 2,
				newPrimaryShardID: 3,
			},
		},
		next: &resolver.ShardReplicas{
			VChannel:       vchannel,
			PrimaryShardID: newPrimaryShardID,
			ShardIDs:       []qviews.ShardID{oldPrimaryShardID, newPrimaryShardID},
			WALReplicaIDs: map[qviews.ShardID]int64{
				oldPrimaryShardID: 2,
				newPrimaryShardID: 3,
			},
		},
	}
	client := newShardViewQueryClient(
		2,
		planClient,
		queryService,
		shardResolver,
		fixedReplicaPicker{shardID: oldPrimaryShardID},
	)

	_, err := client.Query(context.Background(), &ShardQueryRequest{
		VChannel: vchannel,
		Req: &internalpb.RetrieveRequest{
			CollectionID:     100,
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		},
		Reducer: &fakeRetrieveResultReducer{},
	})

	require.NoError(t, err)
	require.Equal(t, 1, shardResolver.waitCount)
	require.Equal(t, 2, shardResolver.resolveCount)
	require.Equal(t, 2, queryService.queryReqCount)
}

type fakeShardResolver struct {
	replicas *resolver.ShardReplicas
}

func (f *fakeShardResolver) ResolveVChannels(context.Context, int64) ([]string, error) {
	return []string{f.replicas.VChannel}, nil
}

func (f *fakeShardResolver) ResolveShard(context.Context, int64, string) (*resolver.ShardReplicas, error) {
	return f.replicas, nil
}

type fixedReplicaPicker struct {
	shardID qviews.ShardID
}

func (p fixedReplicaPicker) Pick(context.Context, ReplicaPickInfo) (ReplicaPickResult, error) {
	return ReplicaPickResult{ShardID: p.shardID}, nil
}

type waitForChangeShardResolver struct {
	mu           sync.Mutex
	vchannel     string
	current      *resolver.ShardReplicas
	next         *resolver.ShardReplicas
	waitCount    int
	resolveCount int
}

func (r *waitForChangeShardResolver) ResolveVChannels(context.Context, int64) ([]string, error) {
	return []string{r.vchannel}, nil
}

func (r *waitForChangeShardResolver) ResolveShard(context.Context, int64, string) (*resolver.ShardReplicas, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolveCount++
	return r.current, nil
}

func (r *waitForChangeShardResolver) WaitForShardChange(context.Context, int64, string, *resolver.ShardReplicas) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waitCount++
	r.current = r.next
	return nil
}

type fakeQueryPlanClient struct {
	plan             *viewpb.QueryPlan
	planReq          *viewpb.GetQueryPlanRequest
	planWALReplicaID int64
	planReqCount     int
	mvccReq          *viewpb.GetMVCCTimestampRequest
	mvccShardID      qviews.ShardID
	mvccWALReplicaID int64
	mvccReqCount     int
	mvccErrs         []error
}

func (f *fakeQueryPlanClient) GetQueryPlan(_ context.Context, _ qviews.ShardID, walReplicaID int64, req *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	f.planReq = proto.Clone(req).(*viewpb.GetQueryPlanRequest)
	f.planWALReplicaID = walReplicaID
	f.planReqCount++
	return &viewpb.GetQueryPlanResponse{Plan: f.plan}, nil
}

func (f *fakeQueryPlanClient) GetMVCCTimestamp(_ context.Context, shardID qviews.ShardID, walReplicaID int64, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	f.mvccReq = proto.Clone(req).(*viewpb.GetMVCCTimestampRequest)
	f.mvccShardID = shardID
	f.mvccWALReplicaID = walReplicaID
	f.mvccReqCount++
	if len(f.mvccErrs) > 0 {
		err := f.mvccErrs[0]
		f.mvccErrs = f.mvccErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &viewpb.GetMVCCTimestampResponse{Mvcc: f.plan.GetMvcc()}, nil
}

type fakeViewQueryServiceClient struct {
	searchReq     *viewpb.SearchOnViewRequest
	queryErrs     []error
	queryReqCount int
}

func (f *fakeViewQueryServiceClient) SearchOnView(_ context.Context, _ qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	f.searchReq = req
	return &viewpb.SearchOnViewResponse{}, nil
}

func (f *fakeViewQueryServiceClient) QueryOnView(_ context.Context, _ qviews.WorkNode, _ *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	f.queryReqCount++
	if len(f.queryErrs) > 0 {
		err := f.queryErrs[0]
		f.queryErrs = f.queryErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &viewpb.QueryOnViewResponse{}, nil
}

func (f *fakeViewQueryServiceClient) RequeryOnView(context.Context, qviews.WorkNode, *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	return &viewpb.RequeryOnViewResponse{}, nil
}

type fakeSearchResultReducer struct{}

func (fakeSearchResultReducer) Add(qviews.ShardID, *viewpb.SearchOnViewResponse) error {
	return nil
}

func (fakeSearchResultReducer) ResetShard(qviews.ShardID) {}

func (fakeSearchResultReducer) Finish() (*internalpb.SearchResults, error) {
	return &internalpb.SearchResults{}, nil
}

type fakeRetrieveResultReducer struct {
	addCount   int
	resetCount int
}

func (f *fakeRetrieveResultReducer) Add(qviews.ShardID, *viewpb.QueryOnViewResponse) error {
	f.addCount++
	return nil
}

func (f *fakeRetrieveResultReducer) ResetShard(qviews.ShardID) {
	f.resetCount++
}

func (f *fakeRetrieveResultReducer) Finish() (*internalpb.RetrieveResults, error) {
	return &internalpb.RetrieveResults{}, nil
}

func newTestSearchQueryPlan(shardID qviews.ShardID, mvcc *viewpb.QueryPlanMVCC) *viewpb.QueryPlan {
	queryNode := qviews.NewQueryNode(11)
	return &viewpb.QueryPlan{
		ShardId: shardID.IntoProto(),
		Version: &viewpb.QueryViewVersion{QueryVersion: 10},
		Mvcc:    mvcc,
		Request: &viewpb.QueryPlan_LegacySearchRequest{
			LegacySearchRequest: &internalpb.SearchRequest{},
		},
		WorkNodes: []*viewpb.QueryPlanWorkNode{
			{
				Node: &viewpb.QueryPlanWorkNode_QueryNode{
					QueryNode: &viewpb.QueryWorkNode{NodeId: queryNode.ID},
				},
			},
		},
	}
}

func newTestQueryPlan(shardID qviews.ShardID, mvcc *viewpb.QueryPlanMVCC) *viewpb.QueryPlan {
	queryNode := qviews.NewQueryNode(11)
	return &viewpb.QueryPlan{
		ShardId: shardID.IntoProto(),
		Version: &viewpb.QueryViewVersion{QueryVersion: 10},
		Mvcc:    mvcc,
		Request: &viewpb.QueryPlan_LegacyRetrieveRequest{
			LegacyRetrieveRequest: &internalpb.RetrieveRequest{},
		},
		WorkNodes: []*viewpb.QueryPlanWorkNode{
			{
				Node: &viewpb.QueryPlanWorkNode_QueryNode{
					QueryNode: &viewpb.QueryWorkNode{NodeId: queryNode.ID},
				},
			},
		},
	}
}
