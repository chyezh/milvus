package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMetadataFirstTrimPlanReturnsExplicitPublications(t *testing.T) {
	segments := []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{
			ID:                  100,
			CollectionID:        1,
			SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 2},
		}),
		NewSegmentInfo(&datapb.SegmentInfo{
			ID:                  200,
			CollectionID:        1,
			SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 2},
		}),
		NewSegmentInfo(&datapb.SegmentInfo{ID: 300, CollectionID: 1}),
	}

	plan, err := metadataFirstTrimPlan(1, segments)

	require.NoError(t, err)
	require.Equal(t, []AssignedMutation{{
		Version:  &viewpb.DataVersion{StreamingVersion: 2},
		Mutation: PublishedMutation{Remove: []int64{100, 200}},
	}}, plan.Assigned)
	require.Equal(t, PublishedMutation{Remove: []int64{100, 200, 300}}, plan.Rewrite)
}

func TestLoadablePublishedMembershipsUsesLockedMetaAccessor(t *testing.T) {
	mt := &meta{ctx: context.Background(), segments: NewSegmentsInfo()}
	getCalled := false
	patch := mockey.Mock((*meta).GetSegment).To(func(_ *meta, _ context.Context, _ int64) *SegmentInfo {
		getCalled = true
		return NewSegmentInfo(&datapb.SegmentInfo{
			ID:           100,
			CollectionID: 1,
			State:        commonpb.SegmentState_Flushed,
			Level:        datapb.SegmentLevel_L1,
			NumOfRows:    1,
		})
	}).Build()
	defer patch.UnPatch()

	memberships, ready := mt.loadablePublishedMemberships([]int64{100})

	require.True(t, getCalled)
	require.True(t, ready)
	require.Equal(t, []SegmentMembership{{SegmentID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1}}, memberships)
}

func TestLoadableCompactionMembershipsUsesLockedMetaAccessor(t *testing.T) {
	mt := &meta{ctx: context.Background(), segments: NewSegmentsInfo()}
	getCalled := false
	patch := mockey.Mock((*meta).GetSegment).To(func(_ *meta, _ context.Context, _ int64) *SegmentInfo {
		getCalled = true
		return NewSegmentInfo(&datapb.SegmentInfo{
			ID:           100,
			CollectionID: 1,
			State:        commonpb.SegmentState_Flushed,
			Level:        datapb.SegmentLevel_L1,
			NumOfRows:    1,
		})
	}).Build()
	defer patch.UnPatch()

	memberships, ready := mt.loadableCompactionMemberships([]int64{100})

	require.True(t, getCalled)
	require.True(t, ready)
	require.Equal(t, []SegmentMembership{{SegmentID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1}}, memberships)
}

func (m *fakeGCDataViewManager) SegmentSnapshot(ctx context.Context, segmentIDs []int64) dataview.SegmentSnapshot {
	return nil
}

func TestServerCreateCollectionDataViewDelegatesToDataViewManager(t *testing.T) {
	manager := &fakeGCDataViewManager{}
	server := &Server{dataViewManager: manager}

	version, err := server.CreateCollectionDataView(context.Background(), 10, []string{"ch-0", "ch-1"})

	require.NoError(t, err)
	require.Nil(t, version)
	require.Equal(t, []DataViewCollectionInitialization{{
		CollectionID: 10,
		VChannels:    []string{"ch-0", "ch-1"},
	}}, manager.createEvents)
}

func TestServerCreateCollectionDataViewReturnsEmptyWithoutDataViewManager(t *testing.T) {
	server := &Server{}

	version, err := server.CreateCollectionDataView(context.Background(), 10, []string{"ch-0"})

	require.NoError(t, err)
	require.Nil(t, version)
}

func TestServerDropCollectionDataViewDelegatesToDataViewManager(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: make(map[int64]struct{})}
	dataViews := &testDataViewLifecycleDataViews{
		garbageCollectFn: func(context.Context, int64, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
	}
	server := &Server{dataViewLifecycle: newTestDataViewLifecycle(t, catalog, dataViews)}

	err := server.DropCollectionDataView(context.Background(), 10)

	require.NoError(t, err)
	require.Equal(t, []int64{10}, catalog.marked)
	require.NoError(t, server.FinalizeDropCollectionDataView(context.Background(), 10))
	require.Equal(t, []int64{10}, catalog.unmarked)
}

func TestServerDropCollectionDataViewReturnsNilWithoutDataViewManager(t *testing.T) {
	server := &Server{}

	require.NoError(t, server.DropCollectionDataView(context.Background(), 10))
}

func TestDataViewSegmentStoreGetSegmentsMapsBatch(t *testing.T) {
	m := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments:    NewSegmentsInfo(),
	}
	first := NewSegmentInfo(&datapb.SegmentInfo{
		ID:                            100,
		CollectionID:                  1,
		PartitionID:                   10,
		InsertChannel:                 "ch-1",
		NumOfRows:                     11,
		State:                         commonpb.SegmentState_Flushed,
		Level:                         datapb.SegmentLevel_L1,
		DeleteApplyStartAfterTimetick: 500,
	})
	second := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   11,
		InsertChannel: "ch-1",
		NumOfRows:     22,
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
	})
	m.segments.SetSegment(100, first)
	m.segments.SetSegment(101, second)

	segments := (&dataViewSegmentStore{meta: m}).GetSegments(context.Background(), []int64{101, 404, 100})

	require.Len(t, segments, 2)
	require.Equal(t, int64(101), segments[0].GetID())
	require.Equal(t, int64(22), segments[0].GetNumOfRows())
	require.Equal(t, int64(100), segments[1].GetID())
	require.Equal(t, int64(10), segments[1].GetPartitionID())
	require.Equal(t, uint64(500), segments[1].GetTransformStartAfterTimetick())
	first.NumOfRows = 99
	require.Equal(t, int64(11), segments[1].GetNumOfRows())
}

func TestFlushVersionRecoveryIncludesSegmentsOutsideCurrentPartitions(t *testing.T) {
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	m.AddCollection(&collectionInfo{
		ID:         1,
		Partitions: []int64{10},
	})
	require.NoError(t, m.catalog.SaveDataViewVersionState(ctx, &viewpb.CollectionDataVersionState{
		CollectionId:              1,
		AllocatedStreamingVersion: 4,
	}))
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:                  100,
		CollectionID:        1,
		PartitionID:         11,
		InsertChannel:       "ch-1",
		State:               commonpb.SegmentState_Dropped,
		Level:               datapb.SegmentLevel_L1,
		SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 7},
	})))
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   10,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Sealed,
		Level:         datapb.SegmentLevel_L1,
	})))
	manager := newDataViewManager(m.catalog, m)

	assigned, err := manager.AssignFlushVersion(ctx, 1, 101)
	require.NoError(t, err)
	require.Equal(t, int64(8), assigned.GetStreamingVersion())
	durable, err := m.catalog.GetDataViewVersionState(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, int64(8), durable.GetAllocatedStreamingVersion())
}

func TestPublishDataViewAfterVisibleSortCompactionCommitsRewrite(t *testing.T) {
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	manager := &fakeGCDataViewManager{publishedVersion: &viewpb.DataVersion{StreamingVersion: 7, CompactVersion: 1}}
	m.dataViewManager = manager
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:                  100,
		CollectionID:        1,
		PartitionID:         10,
		InsertChannel:       "ch-0",
		State:               commonpb.SegmentState_Dropped,
		Level:               datapb.SegmentLevel_L1,
		SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 7},
	})))
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   10,
		InsertChannel: "ch-0",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		NumOfRows:     1,
	})))

	err = m.publishDataViewAfterCompaction(ctx, &datapb.CompactionTask{
		Type:          datapb.CompactionType_SortCompaction,
		CollectionID:  1,
		InputSegments: []int64{100},
	}, []int64{101})

	require.NoError(t, err)
	require.Empty(t, manager.publishedMutations)
	require.Len(t, manager.rewriteMutations, 1)
}

func TestGetCollectionIDsByPartitionUsesSegmentMeta(t *testing.T) {
	m := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments:    NewSegmentsInfo(),
	}
	m.collections.Insert(1, &collectionInfo{
		ID:         1,
		Partitions: []int64{10},
	})
	m.segments.SetSegment(100, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   11,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
	}))

	collectionIDs := m.GetCollectionIDsByPartition(context.Background(), []int64{11})

	require.Equal(t, []int64{1}, collectionIDs)
}

func TestPublishDataViewAfterSortCompactionUsesInheritedFlushVersion(t *testing.T) {
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	manager := &fakeGCDataViewManager{publishedVersion: &viewpb.DataVersion{StreamingVersion: 7}}
	m.dataViewManager = manager
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:                  100,
		CollectionID:        1,
		PartitionID:         10,
		InsertChannel:       "ch-0",
		State:               commonpb.SegmentState_Dropped,
		Level:               datapb.SegmentLevel_L1,
		IsInvisible:         true,
		SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 7},
	})))
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   10,
		InsertChannel: "ch-0",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		NumOfRows:     1,
	})))

	m.publishDataViewAfterCompaction(ctx, &datapb.CompactionTask{
		Type:          datapb.CompactionType_SortCompaction,
		CollectionID:  1,
		InputSegments: []int64{100},
	}, []int64{101})

	require.Len(t, manager.publishedMutations, 1)
	require.Empty(t, manager.rewriteMutations)
	require.Equal(t, int64(7), manager.publishedAssigned[0].GetStreamingVersion())
	require.Equal(t, []int64{100}, manager.publishedMutations[0].Remove)
	require.Equal(t, int64(101), manager.publishedMutations[0].Add[0].SegmentID)
}

func TestPublishDataViewAfterMixCompactionCommitsRewrite(t *testing.T) {
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	manager := &fakeGCDataViewManager{publishedVersion: &viewpb.DataVersion{StreamingVersion: 7, CompactVersion: 1}}
	m.dataViewManager = manager
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   10,
		InsertChannel: "ch-0",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		NumOfRows:     1,
	})))

	m.publishDataViewAfterCompaction(ctx, &datapb.CompactionTask{
		Type:          datapb.CompactionType_MixCompaction,
		CollectionID:  1,
		InputSegments: []int64{100},
	}, []int64{101})

	require.Empty(t, manager.publishedMutations)
	require.Len(t, manager.rewriteMutations, 1)
	require.Equal(t, []int64{100}, manager.rewriteMutations[0].Remove)
	require.Equal(t, int64(101), manager.rewriteMutations[0].Add[0].SegmentID)
}

func TestPublishDataViewAfterCompactionReturnsPublicationFailure(t *testing.T) {
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	m.dataViewManager = &fakeGCDataViewManager{
		publishVersionErr: merr.WrapErrServiceUnavailableMsg("publication failed"),
	}
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   10,
		InsertChannel: "ch-0",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
	})))

	err = m.publishDataViewAfterCompaction(ctx, &datapb.CompactionTask{
		Type:          datapb.CompactionType_MixCompaction,
		CollectionID:  1,
		InputSegments: []int64{100},
	}, []int64{101})

	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func TestPublishDataViewAfterCompactionCommitsRemoveOnlyForDroppedOutput(t *testing.T) {
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	manager := &fakeGCDataViewManager{}
	m.dataViewManager = manager
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   10,
		InsertChannel: "ch-0",
		State:         commonpb.SegmentState_Dropped,
		Level:         datapb.SegmentLevel_L1,
	})))

	err = m.publishDataViewAfterCompaction(ctx, &datapb.CompactionTask{
		Type:          datapb.CompactionType_MixCompaction,
		CollectionID:  1,
		InputSegments: []int64{100},
	}, []int64{101})

	require.NoError(t, err)
	require.Len(t, manager.rewriteMutations, 1)
	require.Empty(t, manager.rewriteMutations[0].Add)
	require.Equal(t, []int64{100}, manager.rewriteMutations[0].Remove)
}
