//go:build test && dynamic

package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestViewAwareSealedSegmentManager_SubmitsSegmentLoadTasks(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	meta.DeleteApplyStartAfterTimetick = 99
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	scheduler := &fakeSegmentLoadScheduler{}
	mgr := NewViewAwareSealedSegmentManagerWithScheduler(scheduler)

	loadedCh := make(chan *LoadedSegments, 2)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view, Collection: runtime,
		OnLoaded:        func(loaded *LoadedSegments) { loadedCh <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})

	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond)
	taskBySegment := make(map[int64]SegmentLoadTask, len(scheduler.tasks))
	for _, task := range scheduler.tasks {
		taskBySegment[task.SegmentID] = task
	}
	for _, segmentID := range []int64{1000, 1001} {
		task := taskBySegment[segmentID]
		assert.Equal(t, int64(10), task.PartitionID)
		assert.Equal(t, testVChannel, task.VChannel)
		assert.Same(t, runtime, task.Collection)
		assert.Equal(t, uint64(99), task.TransformStartAfterTimeTick)
	}

	taskBySegment[1000].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
	taskBySegment[1001].OnLoaded(&fakeTransformSegment{id: 1001, partitionID: 10})

	require.Eventually(t, func() bool {
		return len(loadedCh) == 2
	}, time.Second, 10*time.Millisecond)
}

func TestViewAwareSealedSegmentManager_CancelsLoadingSegmentAfterLastViewRelease(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	scheduler := &fakeSegmentLoadScheduler{}
	mgr := NewViewAwareSealedSegmentManagerWithScheduler(scheduler)

	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func(*LoadedSegments) { t.Fatal("unexpected loaded") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)

	dropped := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})

	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]int64{1000}, scheduler.canceled)
	}, time.Second, 10*time.Millisecond)
	select {
	case <-dropped:
		t.Fatal("release should wait for canceled loading task callback")
	case <-time.After(20 * time.Millisecond):
	}
	scheduler.tasks[0].OnUnrecoverable(assert.AnError)
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dropped after canceled task callback")
	}
}

func TestViewAwareSealedSegmentManager_ReleaseWaitsForInFlightLoadCallback(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	scheduler := &fakeSegmentLoadScheduler{}
	mgr := NewViewAwareSealedSegmentManagerWithScheduler(scheduler)

	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func(*LoadedSegments) { t.Fatal("unexpected loaded after release") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)

	dropped := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})
	select {
	case <-dropped:
		t.Fatal("release dropped before in-flight load callback completed")
	case <-time.After(20 * time.Millisecond):
	}

	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	scheduler.tasks[0].OnLoaded(segment)
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dropped after load callback completed")
	}
	assert.True(t, segment.released)
}

func TestViewAwareSealedSegmentManager_SharedInFlightLoadSurvivesSubmitterRelease(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view).QueryViewKey()

	scheduler := &fakeSegmentLoadScheduler{}
	mgr := NewViewAwareSealedSegmentManagerWithScheduler(scheduler)

	loaded1 := make(chan *LoadedSegments, 1)
	loaded2 := make(chan *LoadedSegments, 1)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key1, Meta: meta1, View: view,
		OnLoaded:        func(loaded *LoadedSegments) { loaded1 <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for first view") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key2, Meta: meta2, View: view,
		OnLoaded:        func(loaded *LoadedSegments) { loaded2 <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for second view") },
	})
	time.Sleep(20 * time.Millisecond)
	require.Len(t, scheduler.tasks, 1, "shared in-flight segment should not submit another load task")

	dropped1 := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key1, OnDropped: func() { dropped1 <- struct{}{} }})
	require.Eventually(t, func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return mgr.views[key1] == nil
	}, time.Second, 10*time.Millisecond)
	assert.NoError(t, scheduler.tasks[0].Context.Err())

	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	scheduler.tasks[0].OnLoaded(segment)
	select {
	case <-dropped1:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first view release")
	}
	select {
	case <-loaded1:
		t.Fatal("released submitter view should not receive loaded callback")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case got := <-loaded2:
		require.Len(t, got.Segments, 1)
		assert.Equal(t, int64(1000), got.Segments[0].ID())
	case <-time.After(time.Second):
		t.Fatal("shared live view did not receive loaded callback")
	}
	assert.False(t, segment.released)
}

func TestViewAwareSealedSegmentManager_CancelsOnlyLastRefTasksOnMixedRelease(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view1 := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}},
	}
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view1).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	view2 := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view2).QueryViewKey()

	scheduler := &fakeSegmentLoadScheduler{}
	mgr := NewViewAwareSealedSegmentManagerWithScheduler(scheduler)

	mgr.Acquire(AcquirePhysicalSegments{
		Key: key1, Meta: meta1, View: view1,
		OnLoaded:        func(*LoadedSegments) { t.Fatal("unexpected loaded for first view") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for first view") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond)
	taskBySegment := make(map[int64]SegmentLoadTask, len(scheduler.tasks))
	for _, task := range scheduler.tasks {
		taskBySegment[task.SegmentID] = task
	}
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key2, Meta: meta2, View: view2,
		OnLoaded:        func(*LoadedSegments) {},
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for second view") },
	})
	time.Sleep(20 * time.Millisecond)
	require.Len(t, scheduler.tasks, 2, "shared in-flight segment should not submit another load task")

	dropped1 := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key1, OnDropped: func() { dropped1 <- struct{}{} }})
	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]int64{1001}, scheduler.canceled)
	}, time.Second, 10*time.Millisecond)

	assert.NoError(t, taskBySegment[1000].Context.Err())
	assert.ErrorIs(t, taskBySegment[1001].Context.Err(), context.Canceled)

	taskBySegment[1001].OnUnrecoverable(context.Canceled)
	taskBySegment[1000].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
	select {
	case <-dropped1:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first view release")
	}
}
func cloneReadyByPartitionForTest(in map[int64][]int64) map[int64][]int64 {
	out := make(map[int64][]int64, len(in))
	for partitionID, segmentIDs := range in {
		out[partitionID] = append([]int64(nil), segmentIDs...)
	}
	return out
}

func TestViewAwareSealedSegmentManager_AcquireFetchesMetadataLoadsAndReports(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	provider := &fakeMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{
			{SegmentID: 1000, PartitionID: 10},
			{SegmentID: 1001, PartitionID: 10},
			{SegmentID: 2000, PartitionID: 20},
		},
	}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	mgr := NewViewAwareSealedSegmentManager(provider, loader)

	loadedCh := make(chan *LoadedSegments, 3)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func(loaded *LoadedSegments) { loadedCh <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})

	require.Eventually(t, func() bool {
		return len(loadedCh) == 3
	}, time.Second, 10*time.Millisecond)
	assert.False(t, provider.describeCalled)
	require.Len(t, loader.loadInfos, 3)
	assert.ElementsMatch(t, []int64{1000, 1001, 2000}, provider.loadInfoCalled)
}

func TestViewAwareSealedSegmentManager_LoadsMissingSegmentsIndependently(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	provider := &fakeMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{
			{SegmentID: 1000, PartitionID: 10},
			{SegmentID: 1001, PartitionID: 10},
		},
	}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	mgr := NewViewAwareSealedSegmentManager(provider, loader)

	loadedCh := make(chan *LoadedSegments, 2)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func(loaded *LoadedSegments) { loadedCh <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})

	require.Eventually(t, func() bool {
		return len(loadedCh) == 2
	}, time.Second, 10*time.Millisecond)
	require.Len(t, loader.loadInfos, 2)
	loadedSegments := []int64{loader.loadInfos[0].GetSegmentID(), loader.loadInfos[1].GetSegmentID()}
	assert.ElementsMatch(t, []int64{1000, 1001}, loadedSegments)
	assert.ElementsMatch(t, []int64{1000, 1001}, provider.loadInfoCalled)
}

func TestViewAwareSealedSegmentManager_ReleaseAfterLastView(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view1 := buildHandlerTestQNView(1)
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view1).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	view2 := buildHandlerTestQNView(1)
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view2).QueryViewKey()
	provider := &fakeMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{
			{SegmentID: 1000, PartitionID: 10},
			{SegmentID: 1001, PartitionID: 10},
			{SegmentID: 2000, PartitionID: 20},
		},
	}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	mgr := NewViewAwareSealedSegmentManager(provider, loader)

	ready1 := make(chan *LoadedSegments, 3)
	ready2 := make(chan *LoadedSegments, 1)
	mgr.Acquire(AcquirePhysicalSegments{Key: key1, Meta: meta1, View: view1, OnLoaded: func(loaded *LoadedSegments) { ready1 <- loaded }, OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") }})
	require.Eventually(t, func() bool {
		return len(ready1) == 3
	}, time.Second, 10*time.Millisecond)
	mgr.Acquire(AcquirePhysicalSegments{Key: key2, Meta: meta2, View: view2, OnLoaded: func(loaded *LoadedSegments) { ready2 <- loaded }, OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") }})
	<-ready2

	dropped1 := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key1, OnDropped: func() { dropped1 <- struct{}{} }})
	<-dropped1
	assert.Empty(t, loader.released)

	dropped2 := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key2, OnDropped: func() { dropped2 <- struct{}{} }})
	<-dropped2
	assert.Empty(t, loader.released)
	assert.Len(t, loader.loadInfos, 3)
}

func TestViewAwareSealedSegmentManager_MissingIndexDoesNotBlockAcquire(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	provider := &fakeMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
	}
	loader := &fakePhysicalLoader{loaded: &fakeTransformSegment{id: 1000, partitionID: 10}}
	mgr := NewViewAwareSealedSegmentManager(provider, loader)

	loadedCh := make(chan *LoadedSegments, 1)
	unrecoverableCh := make(chan struct{}, 1)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func(loaded *LoadedSegments) { loadedCh <- loaded },
		OnUnrecoverable: func() { unrecoverableCh <- struct{}{} },
	})

	select {
	case got := <-loadedCh:
		require.Equal(t, map[int64][]int64{10: {1000}}, got.ReadyByPartition)
		require.Len(t, got.Segments, 1)
		assert.Equal(t, int64(1000), got.Segments[0].ID())
	case <-unrecoverableCh:
		t.Fatal("missing index should not make query view unrecoverable")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for load callback")
	}
}
