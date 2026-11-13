package snview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestReadOnlyQueryViewServesAfterLocalPreparationWithoutCatalog(t *testing.T) {
	ctx := context.Background()
	mgr, reports := newMockResourceManager(), &reportCollector{}
	h := NewReadOnlyPChannelSNQueryViewHandler(ctx, testPChannel, mgr, nil)
	h.leaseDuration = 0
	preparing := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{{View: preparing, OnReport: reports.onReport}})
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: reports.onReport}})
	require.Equal(t, qviews.QueryViewStatePreparing, reports.last().State())
	resource, ok := mgr.getAcquired(preparing.QueryViewKey())
	require.True(t, ok)
	resource.OnReady()
	require.Equal(t, qviews.QueryViewStateUp, reports.last().State())
	lease, err := h.AcquireLatestUpView(ctx, preparing.ShardID())
	require.NoError(t, err)
	lease.Release()
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown), OnReport: reports.onReport}})
	require.Equal(t, qviews.QueryViewStateDown, reports.last().State())
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: reports.onReport}})
	mgr.invokeReleaseCallback(preparing.QueryViewKey())
	require.Equal(t, qviews.QueryViewStateDropped, reports.last().State())
	h.CloseForHandoff()
}

func TestReadOnlyBaselinePreservesIntentAndReportsLocalDrain(t *testing.T) {
	mgr := newMockResourceManager()
	preparing := newPreparingSNView(2)
	up := newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp)
	down := newSNViewWithState(3, viewpb.QueryViewState_QueryViewStateDown)
	reports := make(map[int64]viewpb.QueryViewState)
	baseline := []*viewpb.QueryViewOfShard{preparing.IntoProto(), down.IntoProto(), up.IntoProto()}
	h := NewReadOnlyPChannelSNQueryViewHandler(t.Context(), testPChannel, mgr, baseline, func(meta *viewpb.QueryViewMeta) {
		reports[meta.Version.QueryVersion] = meta.State
	})
	require.Len(t, mgr.acquiredKeys(), 2)
	require.Equal(t, []qviews.QueryViewKey{up.QueryViewKey(), preparing.QueryViewKey()}, mgr.acquiredKeys())
	require.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, reports[3])
	require.Equal(t, viewpb.QueryViewState_QueryViewStateDown, baseline[1].Meta.State, "caller snapshot remains immutable")
	for _, view := range []qviews.QueryViewAtWorkNode{up, preparing} {
		resource, ok := mgr.getAcquired(view.QueryViewKey())
		require.True(t, ok)
		resource.OnReady()
	}
	require.Equal(t, viewpb.QueryViewState_QueryViewStateUp, reports[1])
	require.Equal(t, viewpb.QueryViewState_QueryViewStateReady, reports[2], "local readiness cannot invent an Up intent")
	lease, err := h.AcquireLatestUpView(t.Context(), up.ShardID())
	require.NoError(t, err)
	lease.Release()
	for _, view := range []qviews.QueryViewAtWorkNode{up, preparing} {
		dropped := view.IntoProto()
		dropped.Meta.State = viewpb.QueryViewState_QueryViewStateDropped
		h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtWorkNodeFromProto(dropped)}})
		require.NotEqual(t, viewpb.QueryViewState_QueryViewStateDropped, reports[view.Version().QueryVersion], "receiving Dropped does not prove local drain")
		mgr.invokeReleaseCallback(view.QueryViewKey())
		require.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, reports[view.Version().QueryVersion])
	}
	// Unknown late targets also reach the local observer without a stream callback.
	unknown := proto.CloneOf(baseline[0])
	unknown.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtWorkNodeFromProto(unknown)}})
	require.Equal(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, reports[2])
	h.CloseForHandoff()
}

func TestReadOnlyQueryViewBaselineWaitsForItsOwnResources(t *testing.T) {
	ctx := context.Background()
	mgr := newMockResourceManager()
	up := newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp)
	h := NewReadOnlyPChannelSNQueryViewHandler(ctx, testPChannel, mgr, []*viewpb.QueryViewOfShard{up.IntoProto()})
	resource, ok := mgr.getAcquired(up.QueryViewKey())
	require.True(t, ok)
	shard := h.shards[up.ShardID()]
	require.Equal(t, qviews.QueryViewStatePreparing, shard.views[up.QueryViewKey().QueryViewVersion].sm.State())
	resource.OnReady()
	lease, err := h.AcquireLatestUpView(ctx, up.ShardID())
	require.NoError(t, err)
	lease.Release()
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped)}})
	mgr.invokeReleaseCallback(up.QueryViewKey())
	h.CloseForHandoff()
}
