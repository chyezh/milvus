package snview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type fencedQueryWAL struct{ *fakeViewQueryWAL }

func (*fencedQueryWAL) ValidateQueryServing(context.Context) error { panic("mockey") }

func TestQueryServingFenceDiscardsCompletedNativeResults(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		mockey.PatchConvey("final publication follows execution, conversion, and handle release", t, func() {
			raw := &fencedQueryWAL{&fakeViewQueryWAL{
				searchTasks: &fakeViewQuerySearchTasks{tasks: []viewquery.SearchSegmentTask{struct{}{}}},
				queryTasks:  &fakeViewQueryQueryTasks{tasks: []viewquery.QuerySegmentTask{struct{}{}}},
			}}
			check := mockey.Mock((*fencedQueryWAL).ValidateQueryServing).To(func(*fencedQueryWAL, context.Context) error {
				require.Positive(t, raw.searchTasks.releaseCount+raw.queryTasks.releaseCount)
				if fenced {
					return viewerror.NewViewInvalidated("lost runtime")
				}
				return nil
			}).Build()
			server := NewPChannelViewQueryServer(&fakeViewQueryWALManager{wal: raw}, &fakeViewQueryScheduler{searchResult: &internalpb.SearchResults{}, queryResult: &internalpb.RetrieveResults{}})
			ctx := newIncomingViewSyncContext(types.PChannelInfo{Name: "p0", Term: 7, AccessMode: types.AccessModeRO})
			shard := &viewpb.ShardID{ReplicaId: 1, Vchannel: "p0_100v0"}
			mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 10, TransformingTimetick: 9}
			search, err := server.SearchOnView(ctx, &viewpb.SearchOnViewRequest{LegacyReq: &internalpb.SearchRequest{CollectionID: 100}, ShardId: shard, Version: testViewQueryVersion().IntoProto(), Mvcc: mvcc})
			if fenced {
				require.Nil(t, search)
				require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
			} else {
				require.NoError(t, err)
				require.NotNil(t, search)
			}
			query, err := server.QueryOnView(ctx, &viewpb.QueryOnViewRequest{LegacyReq: &internalpb.RetrieveRequest{CollectionID: 100}, ShardId: shard, Version: testViewQueryVersion().IntoProto(), Mvcc: mvcc})
			if fenced {
				require.Nil(t, query)
				require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
			} else {
				require.NoError(t, err)
				require.NotNil(t, query)
			}
			require.Equal(t, 2, check.Times())
			require.Equal(t, 1, raw.queryTasks.releaseCount)
		})
	}
}
