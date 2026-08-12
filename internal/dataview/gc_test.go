package dataview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestDataViewSnapshotRefProtectsAllCollectionViewsUntilRelease(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 100)
	store.segments[200] = newDataViewTestSegment(1, 10, 200, "ch-1", 100)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	ref, err := manager.DataViewSnapshotRefForCollections(ctx, map[int64]struct{}{1: {}})
	require.NoError(t, err)
	require.NotNil(t, ref.Snapshot())
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{200}})))

	// The old view remains protected by the snapshot ref even though it is not
	// in the retention window.
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	views, err := catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Len(t, views, 2)

	ref.Release()
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	views, err = catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Len(t, views, 1)
}

func TestDataViewDropWaitsForRefsAndFinalizeBarrier(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 100)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	ref, err := manager.Get(ctx, 1, qviews.DataVersion{StreamingVersion: 1})
	require.NoError(t, err)

	require.NoError(t, noErrorVersion(manager.OnDropCollection(ctx, 1)))
	views, err := catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Len(t, views, 1, "terminal drop must retain snapshots while refs are live")

	ref.Deref()
	require.NoError(t, manager.FinalizeDropCollection(ctx, 1))
	views, err = catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, views)
	_, err = manager.Get(ctx, 1, qviews.DataVersion{StreamingVersion: 1})
	require.Error(t, err)
}

func TestDataViewDropWithoutRefsStillWaitsForFinalizeBarrier(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 100)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	require.NoError(t, noErrorVersion(manager.OnDropCollection(ctx, 1)))

	views, err := catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.NoError(t, manager.FinalizeDropCollection(ctx, 1))
	views, err = catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, views)
}

func TestDataViewFinalizeDropRejectsLiveRefs(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 100)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	ref, err := manager.Get(ctx, 1, qviews.DataVersion{StreamingVersion: 1})
	require.NoError(t, err)
	require.NoError(t, noErrorVersion(manager.OnDropCollection(ctx, 1)))

	require.Error(t, manager.FinalizeDropCollection(ctx, 1))
	ref.Deref()
	require.NoError(t, manager.FinalizeDropCollection(ctx, 1))
}

func TestDataViewRetainedMembershipTracksGarbageCollectedViews(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 100)
	store.segments[200] = newDataViewTestSegment(1, 10, 200, "ch-1", 100)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{CollectionID: 1, CompactFrom: []int64{100}, CompactTo: []int64{200}})))

	referenced, err := manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, referenced, "retained old DataView must protect physical segment GC")
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	referenced, err = manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.False(t, referenced, "segment becomes eligible after its last DataView is collected")
}
