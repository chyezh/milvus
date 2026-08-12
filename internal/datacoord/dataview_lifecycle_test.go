// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type testDataViewLifecycleCatalog struct {
	mu            sync.Mutex
	dropped       []int64
	marked        []int64
	unmarked      []int64
	markerPresent map[int64]struct{}
}

func (c *testDataViewLifecycleCatalog) MarkDataViewCollectionDropped(_ context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.marked = append(c.marked, collectionID)
	c.markerPresent[collectionID] = struct{}{}
	return nil
}

func (c *testDataViewLifecycleCatalog) ListDroppedDataViewCollections(context.Context) ([]int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]int64, 0, len(c.markerPresent))
	for collectionID := range c.markerPresent {
		result = append(result, collectionID)
	}
	return result, nil
}

func (c *testDataViewLifecycleCatalog) UnmarkDataViewCollectionDropped(_ context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unmarked = append(c.unmarked, collectionID)
	delete(c.markerPresent, collectionID)
	return nil
}

type testDataViewLifecycleDataViews struct {
	garbageCollectFn func(context.Context, int64, int) error
	dropCollectionFn func(context.Context, int64) (*viewpb.DataVersion, error)
	getFn            func(context.Context, int64, qviews.DataVersion) (dataview.DataViewRef, error)
}

func TestDataViewLifecycleSnapshotRefRejectsTerminalCollection(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: map[int64]struct{}{100: {}}}
	dataViews := &testDataViewLifecycleDataViews{
		garbageCollectFn: func(context.Context, int64, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
		getFn: func(context.Context, int64, qviews.DataVersion) (dataview.DataViewRef, error) {
			return nil, nil
		},
	}
	lifecycle := newTestDataViewLifecycle(t, catalog, dataViews)
	_, err := lifecycle.DataViewSnapshotRefForCollections(context.Background(), map[int64]struct{}{100: {}})
	require.Error(t, err)
}

type testLifecycleDataViewRef struct{}

func (*testLifecycleDataViewRef) DataView() *dataview.DataView { return nil }
func (*testLifecycleDataViewRef) Deref()                       {}

type blockingSnapshotLifecycleDataViews struct {
	*testDataViewLifecycleDataViews
	started chan struct{}
	release chan struct{}
}

func (m *blockingSnapshotLifecycleDataViews) DataViewSnapshotRefForCollections(context.Context, map[int64]struct{}) (balancer.DataViewSnapshotRef, error) {
	close(m.started)
	<-m.release
	return &testLifecycleSnapshotRef{snapshot: balancer.NewDataViewSnapshot(0, nil, nil)}, nil
}

type testLifecycleSnapshotRef struct {
	snapshot *balancer.DataViewSnapshot
}

func (r *testLifecycleSnapshotRef) Snapshot() *balancer.DataViewSnapshot { return r.snapshot }
func (*testLifecycleSnapshotRef) Release()                               {}

func TestDataViewLifecycleSnapshotAcquisitionSerializesWithDropMarker(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: make(map[int64]struct{})}
	dataViews := &blockingSnapshotLifecycleDataViews{
		testDataViewLifecycleDataViews: &testDataViewLifecycleDataViews{
			garbageCollectFn: func(context.Context, int64, int) error { return nil },
			dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
			getFn:            func(context.Context, int64, qviews.DataVersion) (dataview.DataViewRef, error) { return nil, nil },
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	lifecycle, err := recoverDataViewLifecycle(context.Background(), catalog, dataViews, func(int64) bool { return true })
	require.NoError(t, err)
	refDone := make(chan error, 1)
	go func() {
		_, err := lifecycle.DataViewSnapshotRefForCollections(context.Background(), map[int64]struct{}{100: {}})
		refDone <- err
	}()
	<-dataViews.started
	dropDone := make(chan error, 1)
	go func() { dropDone <- lifecycle.DropCollection(context.Background(), 100) }()
	select {
	case err := <-dropDone:
		t.Fatalf("drop crossed in-flight snapshot acquisition: %v", err)
	default:
	}
	close(dataViews.release)
	require.NoError(t, <-refDone)
	require.NoError(t, <-dropDone)
}

func TestDataViewLifecycleFullSnapshotAcquisitionFencesUnknownCollectionDrop(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: make(map[int64]struct{})}
	dataViews := &blockingSnapshotLifecycleDataViews{
		testDataViewLifecycleDataViews: &testDataViewLifecycleDataViews{
			garbageCollectFn: func(context.Context, int64, int) error { return nil },
			dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
			getFn:            func(context.Context, int64, qviews.DataVersion) (dataview.DataViewRef, error) { return nil, nil },
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	lifecycle, err := recoverDataViewLifecycle(context.Background(), catalog, dataViews, func(int64) bool { return true })
	require.NoError(t, err)

	refDone := make(chan error, 1)
	go func() {
		_, err := lifecycle.DataViewSnapshotRefForCollections(context.Background(), nil)
		refDone <- err
	}()
	<-dataViews.started

	dropDone := make(chan error, 1)
	go func() { dropDone <- lifecycle.DropCollection(context.Background(), 100) }()
	require.Never(t, func() bool {
		catalog.mu.Lock()
		defer catalog.mu.Unlock()
		return len(catalog.marked) > 0
	}, 100*time.Millisecond, 5*time.Millisecond)
	select {
	case err := <-dropDone:
		t.Fatalf("drop crossed in-flight full snapshot acquisition: %v", err)
	default:
	}

	close(dataViews.release)
	require.NoError(t, <-refDone)
	require.NoError(t, <-dropDone)
	require.Equal(t, []int64{100}, catalog.marked)
}

func (m *testDataViewLifecycleDataViews) GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error {
	return m.garbageCollectFn(ctx, collectionID, retainLatest)
}

func (m *testDataViewLifecycleDataViews) OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error) {
	return m.dropCollectionFn(ctx, collectionID)
}

func (m *testDataViewLifecycleDataViews) Get(ctx context.Context, collectionID int64, version qviews.DataVersion) (dataview.DataViewRef, error) {
	return m.getFn(ctx, collectionID, version)
}

func (m *testDataViewLifecycleDataViews) DataViewSnapshotRefForCollections(context.Context, map[int64]struct{}) (balancer.DataViewSnapshotRef, error) {
	return &testLifecycleSnapshotRef{snapshot: balancer.NewDataViewSnapshot(0, nil, nil)}, nil
}

func (m *testDataViewLifecycleDataViews) SegmentSnapshot(context.Context, []int64) balancer.SegmentSnapshot {
	return nil
}

func newTestDataViewLifecycle(t *testing.T, catalog *testDataViewLifecycleCatalog, dataViews *testDataViewLifecycleDataViews) *dataViewLifecycle {
	t.Helper()
	lifecycle, err := recoverDataViewLifecycle(context.Background(), catalog, dataViews, func(int64) bool { return true })
	require.NoError(t, err)
	return lifecycle
}

func TestDataViewLifecycleRecoversTerminalMarker(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: map[int64]struct{}{100: {}}}
	dataViews := &testDataViewLifecycleDataViews{
		garbageCollectFn: func(context.Context, int64, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
	}

	lifecycle := newTestDataViewLifecycle(t, catalog, dataViews)

	require.True(t, lifecycle.IsTerminal(100))
	require.False(t, lifecycle.IsTerminal(101))
}

func TestDataViewLifecycleDropIsTerminalAndMarkerFirst(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: make(map[int64]struct{})}
	dataViews := &testDataViewLifecycleDataViews{
		garbageCollectFn: func(context.Context, int64, int) error { return nil },
		dropCollectionFn: func(_ context.Context, collectionID int64) (*viewpb.DataVersion, error) {
			catalog.mu.Lock()
			defer catalog.mu.Unlock()
			_, marked := catalog.markerPresent[collectionID]
			require.True(t, marked, "drop marker must be durable before deleting data views")
			catalog.dropped = append(catalog.dropped, collectionID)
			return nil, nil
		},
	}
	lifecycle := newTestDataViewLifecycle(t, catalog, dataViews)

	require.NoError(t, lifecycle.DropCollection(context.Background(), 100))
	require.True(t, lifecycle.IsTerminal(100))
	require.Equal(t, []int64{100}, catalog.marked)
	require.Equal(t, []int64{100}, catalog.dropped)
	require.NoError(t, lifecycle.FinalizeDropCollection(context.Background(), 100))
	require.Equal(t, []int64{100}, catalog.unmarked)
	require.True(t, lifecycle.IsTerminal(100), "finalization must not reopen the collection in this process")
}

func TestDataViewLifecycleGarbageCollectPreservesTask5Boundary(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: make(map[int64]struct{})}
	called := false
	dataViews := &testDataViewLifecycleDataViews{
		garbageCollectFn: func(_ context.Context, collectionID int64, retainLatest int) error {
			called = true
			require.Equal(t, int64(100), collectionID)
			require.Equal(t, 1, retainLatest)
			return nil
		},
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
	}
	lifecycle := newTestDataViewLifecycle(t, catalog, dataViews)

	require.NoError(t, lifecycle.GarbageCollect(context.Background(), 100, 1))
	require.True(t, called)

	require.NoError(t, lifecycle.DropCollection(context.Background(), 100))
	called = false
	require.NoError(t, lifecycle.GarbageCollect(context.Background(), 100, 1))
	require.False(t, called, "terminal collection GC remains suppressed")
}

func TestDataViewLifecycleGetRejectsRecoveredTerminalMarkerWithoutDelegate(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: map[int64]struct{}{100: {}}}
	delegated := false
	dataViews := &testDataViewLifecycleDataViews{
		garbageCollectFn: func(context.Context, int64, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
		getFn: func(context.Context, int64, qviews.DataVersion) (dataview.DataViewRef, error) {
			delegated = true
			return nil, nil
		},
	}
	lifecycle, err := recoverDataViewLifecycle(context.Background(), catalog, dataViews, func(int64) bool { return true })
	require.NoError(t, err)

	_, err = lifecycle.Get(context.Background(), 100, qviews.DataVersion{StreamingVersion: 3})
	require.True(t, dataview.IsUnavailableDataViewError(err))
	require.False(t, delegated)
}

func TestDataViewLifecycleGetRejectsAbsentCollectionWithoutDelegate(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: make(map[int64]struct{})}
	delegated := false
	dataViews := &testDataViewLifecycleDataViews{
		garbageCollectFn: func(context.Context, int64, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
		getFn: func(context.Context, int64, qviews.DataVersion) (dataview.DataViewRef, error) {
			delegated = true
			return nil, nil
		},
	}
	lifecycle, err := recoverDataViewLifecycle(context.Background(), catalog, dataViews, func(int64) bool { return false })
	require.NoError(t, err)

	_, err = lifecycle.Get(context.Background(), 100, qviews.DataVersion{StreamingVersion: 3})
	require.True(t, dataview.IsUnavailableDataViewError(err))
	require.False(t, delegated)
}

func TestDataViewLifecycleGetDelegatesLiveCollection(t *testing.T) {
	catalog := &testDataViewLifecycleCatalog{markerPresent: make(map[int64]struct{})}
	expected := &testLifecycleDataViewRef{}
	dataViews := &testDataViewLifecycleDataViews{
		garbageCollectFn: func(context.Context, int64, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
		getFn: func(_ context.Context, collectionID int64, version qviews.DataVersion) (dataview.DataViewRef, error) {
			require.Equal(t, int64(100), collectionID)
			require.Equal(t, qviews.DataVersion{StreamingVersion: 3}, version)
			return expected, nil
		},
	}
	lifecycle, err := recoverDataViewLifecycle(context.Background(), catalog, dataViews, func(int64) bool { return true })
	require.NoError(t, err)

	actual, err := lifecycle.Get(context.Background(), 100, qviews.DataVersion{StreamingVersion: 3})
	require.NoError(t, err)
	require.Same(t, expected, actual)
}
