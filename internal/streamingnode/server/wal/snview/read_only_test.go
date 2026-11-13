package snview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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

func TestReadOnlyQueryViewBaselineWaitsForItsOwnResources(t *testing.T) {
	ctx := context.Background()
	mgr := newMockResourceManager()
	up := newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp)
	h := NewReadOnlyPChannelSNQueryViewHandler(ctx, testPChannel, mgr, []*viewpb.QueryViewOfShard{up.IntoProto()})
	resource, ok := mgr.getAcquired(up.QueryViewKey())
	require.True(t, ok)
	shard := h.shards[up.ShardID()]
	require.Equal(t, qviews.QueryViewStateUpRecovering, shard.views[up.QueryViewKey().QueryViewVersion].sm.State())
	resource.OnReady()
	lease, err := h.AcquireLatestUpView(ctx, up.ShardID())
	require.NoError(t, err)
	lease.Release()
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped)}})
	mgr.invokeReleaseCallback(up.QueryViewKey())
	h.CloseForHandoff()
}
