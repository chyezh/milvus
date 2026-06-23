//go:build test && dynamic

package qnview

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestDefaultSegmentLoadScheduler_ReservesAndReleasesResourceAroundLoad(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	provider := &fakeMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{Schema: &schemapb.CollectionSchema{Name: "test"}},
		segments:   []*datapb.SegmentInfo{{ID: 1000, PartitionID: 10, InsertChannel: testVChannel}},
	}
	planner := &fakeLoadPlanner{plan: &LoadPlan{
		CollectionID:     testCollectionID,
		Segments:         []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
		ReadyByPartition: map[int64][]int64{10: {1000}},
	}}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	estimator := &fakeSegmentResourceEstimator{}
	scheduler := NewDefaultSegmentLoadScheduler(provider, planner, loader, estimator)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:     context.Background(),
		Meta:        meta,
		SegmentID:   1000,
		PartitionID: 10,
		VChannel:    testVChannel,
		Collection:  runtime,
		OnLoaded:    func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(error) {
			t.Fatal("unexpected unrecoverable")
		},
	})

	require.Eventually(t, func() bool {
		return len(loadedCh) == 1
	}, time.Second, 10*time.Millisecond)
	require.Len(t, estimator.infos, 1)
	assert.Equal(t, int64(1000), estimator.infos[0].GetSegmentID())
	require.Len(t, estimator.collections, 1)
	assert.Same(t, runtime, estimator.collections[0])
	require.Len(t, estimator.reservations, 1)
	assert.True(t, estimator.reservations[0].released)
	require.Len(t, loader.loadInfos, 1)
	require.Len(t, loader.collections, 1)
	assert.Same(t, runtime, loader.collections[0])
}

func TestDefaultSegmentLoadScheduler_UsesPackedSegmentLoadInfoFromMetadataProvider(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	provider := &fakeMetadataProvider{
		loadInfos:      []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10, CollectionID: testCollectionID}},
		loadIndexInfos: indexes,
	}
	planner := &fakeLoadPlanner{err: errors.New("planner should not be called")}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	scheduler := NewDefaultSegmentLoadScheduler(provider, planner, loader)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:     context.Background(),
		Meta:        meta,
		SegmentID:   1000,
		PartitionID: 10,
		VChannel:    testVChannel,
		Collection:  runtime,
		OnLoaded:    func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(err error) {
			t.Fatalf("unexpected unrecoverable: %v", err)
		},
	})

	select {
	case loaded := <-loadedCh:
		assert.Equal(t, int64(1000), loaded.ID())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loaded segment")
	}
	assert.Equal(t, []int64{1000}, provider.loadInfoCalled)
	assert.False(t, provider.describeCalled)
	assert.Empty(t, provider.segmentsCalled)
	assert.False(t, provider.indexesCalled)
	assert.Empty(t, provider.indexInfoCalled)
	assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
	require.Len(t, loader.loadInfos, 1)
	assert.Equal(t, int64(1000), loader.loadInfos[0].GetSegmentID())
}

func TestDefaultSegmentLoadScheduler_UsesTaskTransformStartTick(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	provider := &fakeMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{Schema: &schemapb.CollectionSchema{Name: "test"}},
		segments:   []*datapb.SegmentInfo{{ID: 1000, PartitionID: 10, InsertChannel: testVChannel}},
	}
	planner := &fakeLoadPlanner{plan: &LoadPlan{
		CollectionID:     testCollectionID,
		Segments:         []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
		ReadyByPartition: map[int64][]int64{10: {1000}},
	}}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID(), startAfter: 10}, nil
		},
	}
	scheduler := NewDefaultSegmentLoadScheduler(provider, planner, loader)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:                     context.Background(),
		Meta:                        meta,
		SegmentID:                   1000,
		PartitionID:                 10,
		VChannel:                    testVChannel,
		TransformStartAfterTimeTick: 99,
		OnLoaded:                    func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(error) {
			t.Fatal("unexpected unrecoverable")
		},
	})

	var loaded TransformSegment
	select {
	case loaded = <-loadedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loaded segment")
	}
	assert.Equal(t, uint64(99), loaded.TransformStartAfterTimeTick())
}

func TestDefaultSegmentLoadScheduler_UpdatesCollectionIndexMetaBeforeLoad(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	provider := &fakeMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{Schema: &schemapb.CollectionSchema{Name: "test"}},
		segments:   []*datapb.SegmentInfo{{ID: 1000, PartitionID: 10, InsertChannel: testVChannel}},
		indexes:    indexes,
	}
	planner := &fakeLoadPlanner{plan: &LoadPlan{
		CollectionID:     testCollectionID,
		IndexInfos:       indexes,
		Segments:         []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
		ReadyByPartition: map[int64][]int64{10: {1000}},
	}}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	scheduler := NewDefaultSegmentLoadScheduler(provider, planner, loader)

	loadedCh := make(chan TransformSegment, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:     context.Background(),
		Meta:        meta,
		SegmentID:   1000,
		PartitionID: 10,
		VChannel:    testVChannel,
		Collection:  runtime,
		OnLoaded:    func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(error) {
			t.Fatal("unexpected unrecoverable")
		},
	})

	select {
	case <-loadedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loaded segment")
	}
	assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
}

func TestDefaultSegmentLoadScheduler_IndexMetaUpdateFailureSkipsReserveAndLoad(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID, updateErr: errors.New("index meta update failed")}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	provider := &fakeMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{Schema: &schemapb.CollectionSchema{Name: "test"}},
		segments:   []*datapb.SegmentInfo{{ID: 1000, PartitionID: 10, InsertChannel: testVChannel}},
		indexes:    indexes,
	}
	planner := &fakeLoadPlanner{plan: &LoadPlan{
		CollectionID:     testCollectionID,
		IndexInfos:       indexes,
		Segments:         []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
		ReadyByPartition: map[int64][]int64{10: {1000}},
	}}
	loader := &fakePhysicalLoader{}
	estimator := &fakeSegmentResourceEstimator{}
	scheduler := NewDefaultSegmentLoadScheduler(provider, planner, loader, estimator)

	unrecoverableCh := make(chan error, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:         context.Background(),
		Meta:            meta,
		SegmentID:       1000,
		PartitionID:     10,
		VChannel:        testVChannel,
		Collection:      runtime,
		OnLoaded:        func(TransformSegment) { t.Fatal("unexpected loaded") },
		OnUnrecoverable: func(err error) { unrecoverableCh <- err },
	})

	select {
	case err := <-unrecoverableCh:
		require.Error(t, err)
		assert.ErrorContains(t, err, "index meta update failed")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unrecoverable")
	}
	assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
	assert.Empty(t, estimator.infos)
	assert.Empty(t, loader.loadInfos)
}

func TestDefaultSegmentLoadScheduler_ReservationFailureSkipsPhysicalLoad(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	provider := &fakeMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{Schema: &schemapb.CollectionSchema{Name: "test"}},
		segments:   []*datapb.SegmentInfo{{ID: 1000, PartitionID: 10, InsertChannel: testVChannel}},
	}
	planner := &fakeLoadPlanner{plan: &LoadPlan{
		CollectionID:     testCollectionID,
		Segments:         []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
		ReadyByPartition: map[int64][]int64{10: {1000}},
	}}
	loader := &fakePhysicalLoader{}
	estimator := &fakeSegmentResourceEstimator{err: errors.New("resource rejected")}
	scheduler := NewDefaultSegmentLoadScheduler(provider, planner, loader, estimator)

	unrecoverableCh := make(chan error, 1)
	scheduler.Submit(SegmentLoadTask{
		Context:         context.Background(),
		Meta:            meta,
		SegmentID:       1000,
		PartitionID:     10,
		VChannel:        testVChannel,
		Collection:      runtime,
		OnLoaded:        func(TransformSegment) { t.Fatal("unexpected loaded") },
		OnUnrecoverable: func(err error) { unrecoverableCh <- err },
	})

	select {
	case err := <-unrecoverableCh:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unrecoverable")
	}
	require.Len(t, estimator.infos, 1)
	require.Len(t, estimator.collections, 1)
	assert.Same(t, runtime, estimator.collections[0])
	assert.Empty(t, loader.loadInfos)
}
