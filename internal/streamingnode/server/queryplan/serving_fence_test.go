package queryplan

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type fencedPlanWAL struct{ *fakeProviderWAL }

func (*fencedPlanWAL) ValidateQueryServing(context.Context) error { panic("mockey") }

func TestPlanServingFenceDiscardsCompletedInlineResults(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		mockey.PatchConvey("validate only after planning and inline execution complete", t, func() {
			raw := &fencedPlanWAL{&fakeProviderWAL{}}
			completed := false
			mockey.Mock((*fakeProviderWAL).GetQueryPlan).To(func(*fakeProviderWAL, context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
				completed = true
				return &viewpb.GetQueryPlanResponse{GrowingInlineResponse: &viewpb.GrowingInlineResponse{}}, nil
			}).Build()
			check := mockey.Mock((*fencedPlanWAL).ValidateQueryServing).To(func(*fencedPlanWAL, context.Context) error {
				require.True(t, completed)
				if fenced {
					return viewerror.NewViewInvalidated("lost runtime")
				}
				return nil
			}).Build()
			server := NewServer(&fakeWALManager{wal: raw})
			response, err := server.GetQueryPlan(newIncomingPChannelContext("by-dev-rootcoord-dml_0"), &viewpb.GetQueryPlanRequest{ShardId: &viewpb.ShardID{ReplicaId: 1, Vchannel: testVChannel}})
			require.Equal(t, 1, check.Times())
			if fenced {
				require.Nil(t, response)
				require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
			} else {
				require.NoError(t, err)
				require.NotNil(t, response.GrowingInlineResponse)
			}
		})
	}
}
