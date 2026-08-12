package dataview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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

func TestDataViewSnapshotRefRecoversPublishedHeadForScopedLazyManager(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 1,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}
	manager := NewManager(catalog, &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
	}})

	ref, err := manager.DataViewSnapshotRefForCollections(ctx, map[int64]struct{}{1: {}})
	require.NoError(t, err)
	t.Cleanup(ref.Release)
	version, ok := ref.Snapshot().DataVersion(1)
	require.True(t, ok)
	require.Equal(t, qviews.DataVersion{StreamingVersion: 1}, version)
	shard, ok := ref.Snapshot().ShardView(1, "ch-1")
	require.True(t, ok)
	require.Equal(t, []int64{100}, shard.GetPartitions()[0].GetSegmentIds())
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

func TestDataViewRetainedMembershipIgnoresNewerOrphanBeforeGarbageCollect(t *testing.T) {
	ctx := context.Background()
	head := newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100))
	orphan := newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 200))
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{head, orphan},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 2,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}
	manager := NewManager(catalog, &fakeDataViewSegmentStore{segments: make(map[int64]*Segment)})

	referenced, err := manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, referenced, "published head membership must protect physical segment GC")

	referenced, err = manager.IsSegmentReferenced(ctx, 1, 200)
	require.NoError(t, err)
	require.False(t, referenced, "newer orphan membership must not block physical segment GC")
}

func TestDataViewRetainedMembershipIgnoresSnapshotsWithoutPublishedHead(t *testing.T) {
	ctx := context.Background()
	orphan := newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100))
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{orphan},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 1,
			},
		},
	}
	manager := NewManager(catalog, &fakeDataViewSegmentStore{segments: make(map[int64]*Segment)})

	referenced, err := manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.False(t, referenced, "snapshot without a published head must not block physical segment GC")
}

func TestDataViewGarbageCollectDropsSnapshotsWithoutPublishedHead(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 200)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 2,
			},
		},
	}
	manager := NewManager(catalog, &fakeDataViewSegmentStore{segments: make(map[int64]*Segment)})

	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	views, err := catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, views, "all snapshots are orphaned when durable state has no published head")
}

func TestDataViewRetainedMembershipIncludesPublicationAfterCacheInitialization(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 100)
	store.segments[200] = newDataViewTestSegment(1, 10, 200, "ch-1", 100)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	referenced, err := manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, referenced, "initialize retained-membership cache")
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{200}})))

	referenced, err = manager.IsSegmentReferenced(ctx, 1, 200)
	require.NoError(t, err)
	require.True(t, referenced, "newly published member must be added to an initialized cache")
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	referenced, err = manager.IsSegmentReferenced(ctx, 1, 200)
	require.NoError(t, err)
	require.True(t, referenced, "published head remains retained after GC")
}

func TestDataViewRetainedMembershipDropsRemovedSegmentAfterPublicationAndGC(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 100)
	store.segments[200] = newDataViewTestSegment(1, 10, 200, "ch-1", 100)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	referenced, err := manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, referenced)

	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{CollectionID: 1, CompactFrom: []int64{100}, CompactTo: []int64{200}})))
	referenced, err = manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, referenced, "old retained DataView still protects the removed segment")
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	referenced, err = manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.False(t, referenced, "removed segment must leave the cache after its DataView is collected")
}
