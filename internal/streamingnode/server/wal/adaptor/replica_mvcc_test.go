package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReplicaMVCCResolutionNeedsNoPrimaryQueryView(t *testing.T) {
	mockey.PatchConvey("a writer with no ready local QueryView can resolve a replica's read frontier", t, func() {
		channel := types.PChannelInfo{Name: "p", Term: 7, AccessMode: types.AccessModeRW}
		mockey.Mock((*roWALAdaptorImpl).Channel).To(func(*roWALAdaptorImpl) types.PChannelInfo { return channel }).Build()
		manager := mvcc.NewMVCCManager(100)
		manager.ApplyRecoveryBarrier("v", 100)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		w := &walAdaptorImpl{
			roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: ctx, lifetime: typeutil.NewLifetime()},
			param:            &interceptors.InterceptorBuildParam{MVCCManager: manager},
		}
		req := &viewpb.GetQueryPlanRequest{ShardId: &viewpb.ShardID{Vchannel: "v"}, Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{ConsistencyLevel: commonpb.ConsistencyLevel_Strong}}
		frontier, err := w.ResolveQueryPlanMVCC(t.Context(), req)
		require.NoError(t, err)
		require.EqualValues(t, 100, frontier.GrowingTimetick)
		require.EqualValues(t, 100, frontier.TransformingTimetick)
		require.Nil(t, w.queryViewHandler)
		channel.AccessMode = types.AccessModeRO
		_, err = w.ResolveQueryPlanMVCC(t.Context(), req)
		require.True(t, viewerror.AsViewError(err).IsNotPrimary())
		cancel()
		_, err = w.ResolveQueryPlanMVCC(ctx, req)
		require.ErrorIs(t, err, context.Canceled)
		w.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = w.ResolveQueryPlanMVCC(t.Context(), req)
		require.True(t, viewerror.AsViewError(err).IsOnShutdown())
	})
}
