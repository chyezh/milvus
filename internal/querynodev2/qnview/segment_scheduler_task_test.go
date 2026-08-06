//go:build test && dynamic

package qnview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type qnTaskFunc func(context.Context) error

func (f qnTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

type capturedNodeScheduler struct {
	tasks []nodescheduler.Task
}

func (s *capturedNodeScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return noopNodeTaskHandle{}
}

type noopNodeTaskHandle struct{}

func (noopNodeTaskHandle) Cancel() {}

func (noopNodeTaskHandle) Wait(context.Context) error { return nil }

type recordingLoadMetadataRuntime struct {
	*fakeCollectionRuntimeGuard
	deliveryVersion uint64
	loadSchema      *schemapb.CollectionSchema
	loadBarrier     uint64
	loadIndexes     []*indexpb.IndexInfo
	loadMetadataErr error
}

func (r *recordingLoadMetadataRuntime) UpdateLoadMetadata(_ context.Context, deliveryVersion uint64, schema *schemapb.CollectionSchema, schemaBarrierTs uint64, indexes []*indexpb.IndexInfo) error {
	r.deliveryVersion = deliveryVersion
	r.loadSchema = schema
	r.loadBarrier = schemaBarrierTs
	r.loadIndexes = append([]*indexpb.IndexInfo(nil), indexes...)
	return r.loadMetadataErr
}

func testSegmentLoadSnapshot(segmentID int64, partitionID int64, indexes ...*indexpb.IndexInfo) SegmentLoadInfoSnapshot {
	return SegmentLoadInfoSnapshot{
		CollectionID: testCollectionID,
		SegmentID:    segmentID,
		Revision:     SegmentLoadInfoRevision{Revision: 1},
		LoadInfo: &querypb.SegmentLoadInfo{
			SegmentID:    segmentID,
			PartitionID:  partitionID,
			CollectionID: testCollectionID,
		},
		IndexInfos: indexes,
	}
}

func TestSegmentLoadTaskUsesNodeSchedulerQueue(t *testing.T) {
	nodeScheduler := nodescheduler.New(1)
	t.Cleanup(nodeScheduler.Close)

	blockStarted := make(chan struct{})
	release := make(chan struct{})
	blocker := nodeScheduler.Submit(qnTaskFunc(func(context.Context) error {
		close(blockStarted)
		<-release
		return nil
	}))
	<-blockStarted

	loaded := make(chan struct{})
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID()}, nil
		},
	}
	nodeScheduler.Submit(newSegmentLoadTask(loader, nil, SegmentLoadTask{
		SegmentID:  1000,
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		Snapshot:   testSegmentLoadSnapshot(1000, 10),
		OnLoaded:   func(TransformSegment) { close(loaded) },
	}))

	assert.Never(t, func() bool {
		select {
		case <-loaded:
			return true
		default:
			return false
		}
	}, 20*time.Millisecond, time.Millisecond)
	close(release)
	require.NoError(t, blocker.Wait(context.Background()))
	require.Eventually(t, func() bool {
		select {
		case <-loaded:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestSegmentLoadTaskContextCancelsRunningLoad(t *testing.T) {
	nodeScheduler := nodescheduler.New(1)
	t.Cleanup(nodeScheduler.Close)

	started := make(chan struct{})
	loader := &fakePhysicalLoader{
		loadFnWithContext: func(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	failed := make(chan error, 1)
	taskCtx, cancel := context.WithCancel(context.Background())
	nodeScheduler.Submit(newSegmentLoadTask(loader, nil, SegmentLoadTask{
		Context:         taskCtx,
		SegmentID:       1000,
		Collection:      &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		Snapshot:        testSegmentLoadSnapshot(1000, 10),
		OnUnrecoverable: func(err error) { failed <- err },
	}))

	<-started
	cancel()
	select {
	case err := <-failed:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for canceled load")
	}
}

func TestSegmentUpdateTaskRetriesSameTaskWithErrDelay(t *testing.T) {
	nodeScheduler := &capturedNodeScheduler{}
	var attempts atomic.Int32
	loader := &fakePhysicalLoader{
		updateFn: func(segment TransformSegment, collection CollectionRuntime, snapshot SegmentLoadInfoSnapshot, action SegmentUpdateAction) error {
			if attempts.Add(1) == 1 {
				return errors.New("update failed")
			}
			return nil
		},
	}
	var updated atomic.Int32
	var failed atomic.Int32
	nodeScheduler.Submit(newSegmentUpdateTask(loader, SegmentUpdateTask{
		Segment:    &fakeTransformSegment{id: 1000},
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		Current:    SegmentLoadInfoRevision{Revision: 1},
		Snapshot: SegmentLoadInfoSnapshot{
			SegmentID: 1000,
			Revision:  SegmentLoadInfoRevision{Revision: 2},
			LoadInfo:  &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
		},
		OnUpdated: func(SegmentLoadInfoRevision) { updated.Add(1) },
		OnFailed:  func(error) { failed.Add(1) },
	}))

	require.Len(t, nodeScheduler.tasks, 1)
	task := nodeScheduler.tasks[0]
	require.ErrorIs(t, task.Execute(context.Background()), nodescheduler.ErrDelay)
	assert.Equal(t, int32(0), updated.Load())
	assert.Equal(t, int32(0), failed.Load())

	require.NoError(t, task.Execute(context.Background()))
	assert.Equal(t, int32(2), attempts.Load())
	assert.Equal(t, int32(1), updated.Load())
	assert.Equal(t, int32(0), failed.Load())
	require.Len(t, nodeScheduler.tasks, 1, "retry must reuse the same scheduled task")
}

func TestSegmentLoadTask_ReservesAndReleasesResourceAroundLoad(t *testing.T) {
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	estimator := &fakeSegmentResourceEstimator{}
	loadedCh := make(chan TransformSegment, 1)
	handle := submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:    context.Background(),
		SegmentID:  1000,
		Collection: runtime,
		Snapshot:   testSegmentLoadSnapshot(1000, 10),
		OnLoaded:   func(segment TransformSegment) { loadedCh <- segment },
		OnUnrecoverable: func(error) {
			t.Fatal("unexpected unrecoverable")
		},
	}, estimator)

	require.NoError(t, handle.Wait(context.Background()))
	select {
	case <-loadedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for loaded segment")
	}
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

func TestSegmentLoadTask_UsesSegmentLoadInfoFromWatchSnapshot(t *testing.T) {
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	provider := &fakeQueryViewLoadMetadataProvider{}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	loadedCh := make(chan TransformSegment, 1)
	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:    context.Background(),
		SegmentID:  1000,
		Collection: runtime,
		Snapshot:   testSegmentLoadSnapshot(1000, 10, indexes...),
		OnLoaded:   func(segment TransformSegment) { loadedCh <- segment },
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
	assert.Empty(t, provider.loadInfoCalled)
	assert.False(t, provider.describeCalled)
	assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
	require.Len(t, loader.loadInfos, 1)
	assert.Equal(t, int64(1000), loader.loadInfos[0].GetSegmentID())
}

func TestSegmentLoadTask_LoadsFromSnapshotWithoutMetadataLookup(t *testing.T) {
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	provider := &fakeQueryViewLoadMetadataProvider{
		err: errors.New("metadata lookup should not be called"),
	}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	loadedCh := make(chan TransformSegment, 1)
	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:    context.Background(),
		SegmentID:  1000,
		Collection: runtime,
		Snapshot: SegmentLoadInfoSnapshot{
			CollectionID: testCollectionID,
			SegmentID:    1000,
			Revision:     SegmentLoadInfoRevision{Revision: 1},
			LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, PartitionID: 10, CollectionID: testCollectionID},
			IndexInfos:   indexes,
		},
		OnLoaded: func(segment TransformSegment) { loadedCh <- segment },
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
	assert.Empty(t, provider.loadInfoCalled)
	assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
}

func TestSegmentLoadTask_RequiresWatchSnapshotForLoad(t *testing.T) {
	provider := &fakeQueryViewLoadMetadataProvider{
		loadInfos: []*querypb.SegmentLoadInfo{{SegmentID: 1000, PartitionID: 10}},
	}
	loader := &fakePhysicalLoader{}
	unrecoverableCh := make(chan error, 1)
	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:    context.Background(),
		SegmentID:  1000,
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		OnLoaded: func(TransformSegment) {
			t.Fatal("unexpected loaded")
		},
		OnUnrecoverable: func(err error) {
			unrecoverableCh <- err
		},
	})

	select {
	case err := <-unrecoverableCh:
		require.Error(t, err)
		assert.ErrorContains(t, err, "watch snapshot")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unrecoverable")
	}
	assert.Empty(t, provider.loadInfoCalled)
}

func TestSegmentLoadTask_UsesTaskTransformStartTick(t *testing.T) {
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID(), startAfter: 10}, nil
		},
	}
	loadedCh := make(chan TransformSegment, 1)
	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:                     context.Background(),
		SegmentID:                   1000,
		TransformStartAfterTimeTick: 99,
		Snapshot:                    testSegmentLoadSnapshot(1000, 10),
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

func TestSegmentLoadTask_PreservesReadableSegment(t *testing.T) {
	collection := &segments.Collection{}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
			return &fakeReadableTransformSegment{
				fakeTransformSegment: fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()},
				collection:           collection,
			}, nil
		},
	}
	loadedCh := make(chan TransformSegment, 1)
	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:                     context.Background(),
		SegmentID:                   1000,
		TransformStartAfterTimeTick: 99,
		Snapshot:                    testSegmentLoadSnapshot(1000, 10),
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
	readable, ok := loaded.(ReadableSealedSegment)
	require.True(t, ok)
	assert.Same(t, collection, readable.Collection())
}

func TestSegmentLoadTask_UpdatesCollectionIndexMetaBeforeLoad(t *testing.T) {
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			assert.ElementsMatch(t, indexes, runtime.updatedIndexes)
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	loadedCh := make(chan TransformSegment, 1)
	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:    context.Background(),
		SegmentID:  1000,
		Collection: runtime,
		Snapshot:   testSegmentLoadSnapshot(1000, 10, indexes...),
		OnLoaded:   func(segment TransformSegment) { loadedCh <- segment },
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

func TestSegmentLoadTaskRefreshesCollectionSchemaBeforePhysicalLoad(t *testing.T) {
	runtime := &fakeCollectionRuntimeGuard{
		collectionID: testCollectionID,
		schema:       &schemapb.CollectionSchema{Version: 1},
	}
	latestSchema := &schemapb.CollectionSchema{Version: 2}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			assert.Same(t, latestSchema, runtime.updatedSchema)
			assert.Equal(t, uint64(200), runtime.schemaBarrier)
			return &fakeTransformSegment{id: info.GetSegmentID()}, nil
		},
	}
	loadedCh := make(chan struct{}, 1)
	snapshot := testSegmentLoadSnapshot(1000, 10)
	snapshot.CollectionSchema = latestSchema
	snapshot.SchemaBarrierTs = 200

	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:    context.Background(),
		SegmentID:  1000,
		Collection: runtime,
		Snapshot:   snapshot,
		OnLoaded:   func(TransformSegment) { loadedCh <- struct{}{} },
		OnUnrecoverable: func(err error) {
			t.Fatalf("unexpected load failure: %v", err)
		},
	}, nil)

	select {
	case <-loadedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for load")
	}
}

func TestSegmentLoadTaskUsesSnapshotDeliveryVersionForCombinedCollectionMetadataUpdate(t *testing.T) {
	latestSchema := &schemapb.CollectionSchema{Version: 2}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	runtime := &recordingLoadMetadataRuntime{
		fakeCollectionRuntimeGuard: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
	}
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			assert.Equal(t, uint64(7), runtime.deliveryVersion)
			assert.Same(t, latestSchema, runtime.loadSchema)
			assert.Equal(t, uint64(200), runtime.loadBarrier)
			assert.ElementsMatch(t, indexes, runtime.loadIndexes)
			return &fakeTransformSegment{id: info.GetSegmentID()}, nil
		},
	}
	loadedCh := make(chan struct{}, 1)
	snapshot := testSegmentLoadSnapshot(1000, 10, indexes...)
	snapshot.DeliveryVersion = 7
	snapshot.CollectionSchema = latestSchema
	snapshot.SchemaBarrierTs = 200

	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:    context.Background(),
		SegmentID:  1000,
		Collection: runtime,
		Snapshot:   snapshot,
		OnLoaded:   func(TransformSegment) { loadedCh <- struct{}{} },
		OnUnrecoverable: func(err error) {
			t.Fatalf("unexpected load failure: %v", err)
		},
	}, nil)

	select {
	case <-loadedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for load")
	}
	assert.Nil(t, runtime.updatedSchema, "combined updater must replace the legacy split update path")
	assert.Nil(t, runtime.updatedIndexes, "combined updater must replace the legacy split update path")
}

func TestSegmentLoadTask_IndexMetaUpdateFailureSkipsReserveAndLoad(t *testing.T) {
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID, updateErr: errors.New("index meta update failed")}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	loader := &fakePhysicalLoader{}
	estimator := &fakeSegmentResourceEstimator{}
	unrecoverableCh := make(chan error, 1)
	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:         context.Background(),
		SegmentID:       1000,
		Collection:      runtime,
		Snapshot:        testSegmentLoadSnapshot(1000, 10, indexes...),
		OnLoaded:        func(TransformSegment) { t.Fatal("unexpected loaded") },
		OnUnrecoverable: func(err error) { unrecoverableCh <- err },
	}, estimator)

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

func TestSegmentLoadTask_ReservationFailureSkipsPhysicalLoad(t *testing.T) {
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	loader := &fakePhysicalLoader{}
	estimator := &fakeSegmentResourceEstimator{err: errors.New("resource rejected")}
	unrecoverableCh := make(chan error, 1)
	submitTestSegmentLoadTask(t, loader, SegmentLoadTask{
		Context:         context.Background(),
		SegmentID:       1000,
		Collection:      runtime,
		Snapshot:        testSegmentLoadSnapshot(1000, 10),
		OnLoaded:        func(TransformSegment) { t.Fatal("unexpected loaded") },
		OnUnrecoverable: func(err error) { unrecoverableCh <- err },
	}, estimator)

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

func TestSegmentUpdateTask_ClassifiesRevisionChange(t *testing.T) {
	loader := &fakePhysicalLoader{}
	updatedCh := make(chan SegmentLoadInfoRevision, 1)

	current := SegmentLoadInfoRevision{Revision: 10}
	next := SegmentLoadInfoRevision{Revision: 11}
	submitTestSegmentUpdateTask(t, loader, SegmentUpdateTask{
		Segment:    &fakeTransformSegment{id: 1000, partitionID: 10},
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		Current:    current,
		Snapshot: SegmentLoadInfoSnapshot{
			CollectionID: testCollectionID,
			SegmentID:    1000,
			Revision:     next,
			LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
		},
		OnUpdated: func(revision SegmentLoadInfoRevision) { updatedCh <- revision },
		OnFailed: func(err error) {
			t.Fatalf("unexpected update failure: %v", err)
		},
	})

	select {
	case got := <-updatedCh:
		require.Equal(t, next, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update")
	}
	require.Len(t, loader.updateActions, 1)
	require.True(t, loader.updateActions[0].Has(SegmentUpdateReopen))
	require.True(t, loader.updateActions[0].Has(SegmentUpdateLoadIndex))
}

func TestSegmentUpdateTaskRefreshesCollectionSchemaBeforePhysicalUpdate(t *testing.T) {
	runtime := &fakeCollectionRuntimeGuard{
		collectionID: testCollectionID,
		schema:       &schemapb.CollectionSchema{Version: 1},
	}
	latestSchema := &schemapb.CollectionSchema{Version: 2}
	loader := &fakePhysicalLoader{
		updateFn: func(TransformSegment, CollectionRuntime, SegmentLoadInfoSnapshot, SegmentUpdateAction) error {
			assert.Same(t, latestSchema, runtime.updatedSchema)
			assert.Equal(t, uint64(200), runtime.schemaBarrier)
			return nil
		},
	}
	updatedCh := make(chan SegmentLoadInfoRevision, 1)

	submitTestSegmentUpdateTask(t, loader, SegmentUpdateTask{
		Segment:    &fakeTransformSegment{id: 1000, partitionID: 10},
		Collection: runtime,
		Current:    SegmentLoadInfoRevision{Revision: 10},
		Snapshot: SegmentLoadInfoSnapshot{
			CollectionID:     testCollectionID,
			SegmentID:        1000,
			Revision:         SegmentLoadInfoRevision{Revision: 11},
			LoadInfo:         &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
			CollectionSchema: latestSchema,
			SchemaBarrierTs:  200,
		},
		OnUpdated: func(revision SegmentLoadInfoRevision) { updatedCh <- revision },
		OnFailed: func(err error) {
			t.Fatalf("unexpected update failure: %v", err)
		},
	})

	select {
	case got := <-updatedCh:
		require.Equal(t, SegmentLoadInfoRevision{Revision: 11}, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update")
	}
}

func TestSegmentUpdateTaskUsesSnapshotDeliveryVersionForCombinedCollectionMetadataUpdate(t *testing.T) {
	latestSchema := &schemapb.CollectionSchema{Version: 2}
	indexes := []*indexpb.IndexInfo{{CollectionID: testCollectionID, FieldID: 101, IndexName: "vec_idx"}}
	runtime := &recordingLoadMetadataRuntime{
		fakeCollectionRuntimeGuard: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
	}
	loader := &fakePhysicalLoader{
		updateFn: func(TransformSegment, CollectionRuntime, SegmentLoadInfoSnapshot, SegmentUpdateAction) error {
			assert.Equal(t, uint64(8), runtime.deliveryVersion)
			assert.Same(t, latestSchema, runtime.loadSchema)
			assert.Equal(t, uint64(300), runtime.loadBarrier)
			assert.ElementsMatch(t, indexes, runtime.loadIndexes)
			return nil
		},
	}
	updatedCh := make(chan SegmentLoadInfoRevision, 1)

	submitTestSegmentUpdateTask(t, loader, SegmentUpdateTask{
		Segment:    &fakeTransformSegment{id: 1000, partitionID: 10},
		Collection: runtime,
		Current:    SegmentLoadInfoRevision{Revision: 10},
		Snapshot: SegmentLoadInfoSnapshot{
			CollectionID:     testCollectionID,
			SegmentID:        1000,
			Revision:         SegmentLoadInfoRevision{Revision: 11},
			DeliveryVersion:  8,
			LoadInfo:         &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
			IndexInfos:       indexes,
			CollectionSchema: latestSchema,
			SchemaBarrierTs:  300,
		},
		OnUpdated: func(revision SegmentLoadInfoRevision) { updatedCh <- revision },
		OnFailed: func(err error) {
			t.Fatalf("unexpected update failure: %v", err)
		},
	})

	select {
	case got := <-updatedCh:
		require.Equal(t, SegmentLoadInfoRevision{Revision: 11}, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update")
	}
	assert.Nil(t, runtime.updatedSchema, "combined updater must replace the legacy split update path")
	assert.Nil(t, runtime.updatedIndexes, "combined updater must replace the legacy split update path")
}

func TestSegmentUpdateTask_ClassifiesDataChange(t *testing.T) {
	loader := &fakePhysicalLoader{}
	updatedCh := make(chan SegmentLoadInfoRevision, 1)

	current := SegmentLoadInfoRevision{Revision: 10}
	next := SegmentLoadInfoRevision{Revision: 12}
	submitTestSegmentUpdateTask(t, loader, SegmentUpdateTask{
		Segment:    &fakeTransformSegment{id: 1000, partitionID: 10},
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		Current:    current,
		Snapshot: SegmentLoadInfoSnapshot{
			CollectionID: testCollectionID,
			SegmentID:    1000,
			Revision:     next,
			LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
		},
		OnUpdated: func(revision SegmentLoadInfoRevision) { updatedCh <- revision },
		OnFailed: func(err error) {
			t.Fatalf("unexpected update failure: %v", err)
		},
	})

	select {
	case got := <-updatedCh:
		require.Equal(t, next, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for update")
	}
	require.Len(t, loader.updateActions, 1)
	require.True(t, loader.updateActions[0].Has(SegmentUpdateReopen))
	require.True(t, loader.updateActions[0].Has(SegmentUpdateLoadIndex))
}
