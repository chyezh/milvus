package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCommitDataViewTrimPassesExplicitIDsWithoutReadingSegmentMetadata(t *testing.T) {
	manager := &recordingSegmentTrimManager{}
	meta := &meta{dataViewManager: manager}

	require.NotPanics(t, func() {
		_, err := meta.commitDataViewTrim(context.Background(), 1, func(context.Context) []int64 {
			return []int64{100, 200}
		}, nil)
		require.NoError(t, err)
	})
	require.Equal(t, []int64{100, 200}, manager.targets)
}

func TestSegmentIDsForDataViewPartitionTrimUsesLockedMetaAccessor(t *testing.T) {
	mt := &meta{ctx: context.Background(), segments: NewSegmentsInfo()}
	selectCalled := false
	patch := mockey.Mock((*meta).SelectSegments).To(func(_ *meta, _ context.Context, _ ...SegmentFilter) []*SegmentInfo {
		selectCalled = true
		return []*SegmentInfo{NewSegmentInfo(&datapb.SegmentInfo{ID: 100})}
	}).Build()
	defer patch.UnPatch()

	segmentIDs := mt.segmentIDsForDataViewTrim(
		context.Background(),
		1,
		dataViewPartitionTrimFilter(map[int64]struct{}{10: {}}),
	)

	require.True(t, selectCalled)
	require.Equal(t, []int64{100}, segmentIDs)
}

func TestSegmentIDsForDataViewTruncateTrimUsesLockedMetaAccessor(t *testing.T) {
	mt := &meta{ctx: context.Background(), segments: NewSegmentsInfo()}
	selectCalled := false
	patch := mockey.Mock((*meta).SelectSegments).To(func(_ *meta, _ context.Context, _ ...SegmentFilter) []*SegmentInfo {
		selectCalled = true
		return []*SegmentInfo{NewSegmentInfo(&datapb.SegmentInfo{ID: 100})}
	}).Build()
	defer patch.UnPatch()

	segmentIDs := mt.segmentIDsForDataViewTrim(context.Background(), 1, dataViewTruncateTrimFilter("ch-0", 100))

	require.True(t, selectCalled)
	require.Equal(t, []int64{100}, segmentIDs)
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

type recordingSegmentTrimManager struct {
	DataViewManager
	targets []int64
}

func (m *recordingSegmentTrimManager) CommitSegmentTrim(
	ctx context.Context,
	collectionID int64,
	resolveTargets SegmentTrimTargetResolver,
	finalize SegmentTrimFinalize,
) (*viewpb.DataVersion, error) {
	m.targets = append([]int64(nil), resolveTargets(ctx)...)
	if finalize != nil {
		if err := finalize(ctx); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (m *fakeGCDataViewManager) DataViewSnapshotForCollections(ctx context.Context, collectionIDs map[int64]struct{}) *balancerapi.DataViewSnapshot {
	return balancerapi.NewDataViewSnapshot(0, m.snapshotViews, nil)
}

func (m *fakeGCDataViewManager) SegmentSnapshot(ctx context.Context, segmentIDs []int64) balancerapi.SegmentSnapshot {
	return nil
}

func TestServerCreateCollectionDataViewDelegatesToDataViewManager(t *testing.T) {
	manager := &fakeGCDataViewManager{}
	server := &Server{dataViewManager: manager}

	version, err := server.CreateCollectionDataView(context.Background(), 10, []string{"ch-0", "ch-1"})

	require.NoError(t, err)
	require.Nil(t, version)
	require.Equal(t, []CreateCollectionDataViewEvent{{
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
	catalog := &testDataViewReferenceCatalog{markerPresent: make(map[int64]struct{})}
	dataViews := &testDataViewReferenceDataViews{
		dataViewFn: func(context.Context, int64, *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
			return nil, nil
		},
		garbageCollectFn: func(context.Context, int64, []*viewpb.DataVersion, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
	}
	server := &Server{dataViewReferences: newTestDataViewReferenceManager(t, catalog, dataViews, func(int64) bool { return true })}

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

func TestServerSnapshotDelegatesToDataViewManager(t *testing.T) {
	manager := &fakeGCDataViewManager{
		snapshotViews: []*viewpb.DataViewOfCollection{
			{CollectionId: 10, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}},
		},
	}
	server := &Server{dataViewManager: manager}

	views, err := server.Snapshot(context.Background(), []int64{10})

	require.NoError(t, err)
	require.Equal(t, []int64{10}, manager.snapshotRequested)
	require.Equal(t, manager.snapshotViews, views)
}

func TestServerSnapshotReturnsEmptyWithoutDataViewManager(t *testing.T) {
	server := &Server{}

	views, err := server.Snapshot(context.Background(), []int64{10})

	require.NoError(t, err)
	require.Nil(t, views)
}

func TestDataViewSegmentStoreSelectSegmentsSkipsDroppedPartition(t *testing.T) {
	m := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments:    NewSegmentsInfo(),
	}
	m.collections.Insert(1, &collectionInfo{
		ID:         1,
		Partitions: []int64{10},
	})
	m.segments.SetSegment(100, NewSegmentInfo(&datapb.SegmentInfo{
		ID:                            100,
		CollectionID:                  1,
		PartitionID:                   10,
		InsertChannel:                 "ch-1",
		NumOfRows:                     11,
		Binlogs:                       []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 1024}, {LogSize: 256}}}},
		Statslogs:                     []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 128}}}},
		State:                         commonpb.SegmentState_Flushed,
		Level:                         datapb.SegmentLevel_L1,
		StartPosition:                 &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 500},
		DmlPosition:                   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
		DeleteApplyStartAfterTimetick: 500,
	}))
	m.segments.SetSegment(101, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   11,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
	}))

	store := &dataViewSegmentStore{meta: m}
	segments := store.SelectSegments(context.Background(), 1)

	require.Len(t, segments, 1)
	require.Equal(t, int64(100), segments[0].GetID())
	require.Equal(t, int64(11), segments[0].GetNumOfRows())
	require.Equal(t, int64(1408), segments[0].GetMemSize())
	require.Equal(t, uint64(500), segments[0].GetStartPosition().GetTimestamp())
	require.Equal(t, uint64(500), segments[0].GetTransformStartAfterTimetick())
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

func TestDataViewRecoveryUsesCollectionPartitions(t *testing.T) {
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	m.AddCollection(&collectionInfo{
		ID:         1,
		Partitions: []int64{10},
	})
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   10,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
	})))
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID:            101,
		CollectionID:  1,
		PartitionID:   11,
		InsertChannel: "ch-1",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		DmlPosition:   &msgpb.MsgPosition{ChannelName: "ch-1", Timestamp: 1000},
	})))
	manager := newDataViewManager(m.catalog, m)

	require.NoError(t, manager.RepairCollection(ctx, 1))
	view, err := manager.LatestVisibleDataView(ctx, 1)

	require.NoError(t, err)
	require.NotNil(t, view)
	require.Len(t, view.GetShards(), 1)
	require.Len(t, view.GetShards()[0].GetPartitions(), 1)
	require.Equal(t, int64(10), view.GetShards()[0].GetPartitions()[0].GetPartitionId())
	require.Equal(t, []int64{100}, view.GetShards()[0].GetPartitions()[0].GetSegmentIds())
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
