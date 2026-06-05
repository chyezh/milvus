//go:build test && dynamic

package qnview

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// ---------------------------------------------------------------------------
// Test mocks
// ---------------------------------------------------------------------------

type mockInfoProvider struct {
	mu    sync.Mutex
	infos map[int64]*querypb.SegmentLoadInfo // segmentID → info
	err   error
}

func (m *mockInfoProvider) GetSegmentLoadInfos(_ context.Context, _ int64, segmentIDs []int64) ([]*querypb.SegmentLoadInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	var result []*querypb.SegmentLoadInfo
	for _, id := range segmentIDs {
		if info, ok := m.infos[id]; ok {
			result = append(result, info)
		}
	}
	return result, nil
}

// mockLoader implements sealedSegmentLoader.
type mockLoader struct {
	mu     sync.Mutex
	loaded []int64
	err    map[int64]error // per-segment error
}

func (m *mockLoader) LoadSealed(_ context.Context, _ int64, infos ...*querypb.SegmentLoadInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, info := range infos {
		if err, ok := m.err[info.GetSegmentID()]; ok && err != nil {
			return err
		}
		m.loaded = append(m.loaded, info.GetSegmentID())
	}
	return nil
}

// blockingLoader allows tests to control load timing.
type blockingLoader struct {
	mu        sync.Mutex
	onLoad    func(segID int64)
	err       map[int64]error
	callCount atomic.Int32
}

func (m *blockingLoader) LoadSealed(_ context.Context, _ int64, infos ...*querypb.SegmentLoadInfo) error {
	for _, info := range infos {
		m.callCount.Add(1)
		if m.onLoad != nil {
			m.onLoad(info.GetSegmentID())
		}
		m.mu.Lock()
		err := m.err[info.GetSegmentID()]
		m.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// mockSegManager implements segmentRemover.
type mockSegManager struct {
	mu       sync.Mutex
	released []int64
}

func (m *mockSegManager) Remove(_ context.Context, segmentID int64, _ querypb.DataScope) (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released = append(m.released, segmentID)
	return 0, 1
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func makeKey(version int64) qviews.QueryViewKey {
	return qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: 1, VChannel: "v0"},
		QueryViewVersion: qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: version, CompactVersion: 1}, QueryVersion: version},
	}
}

func waitAcquire(t *testing.T, s *loadScheduler, key qviews.QueryViewKey, collectionID int64, partitions map[int64][]int64) {
	t.Helper()
	total := 0
	for _, segs := range partitions {
		total += len(segs)
	}
	var wg sync.WaitGroup
	readyCount := atomic.Int32{}
	var once sync.Once
	wg.Add(1)
	s.Acquire(AcquireSegments{
		Key:          key,
		CollectionID: collectionID,
		SegmentIDs:   partitions,
		OnReady: func(m map[int64][]int64) {
			count := int32(0)
			for _, segs := range m {
				count += int32(len(segs))
			}
			if readyCount.Add(count) >= int32(total) {
				once.Do(wg.Done)
			}
		},
		// t.Errorf + release wg so the main goroutine can observe failure
		// and fail fast instead of hanging until test timeout.
		OnUnrecoverable: func() {
			t.Errorf("unexpected OnUnrecoverable")
			once.Do(wg.Done)
		},
	})
	wg.Wait()
}

func waitRelease(t *testing.T, s *loadScheduler, key qviews.QueryViewKey, partitions map[int64][]int64) {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(1)
	s.Release(ReleaseSegments{
		Key:        key,
		SegmentIDs: partitions,
		OnDropped:  func() { wg.Done() },
	})
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Task 2: Constructor
// ---------------------------------------------------------------------------

func TestNewLoadScheduler(t *testing.T) {
	s := newLoadScheduler(&mockInfoProvider{}, &mockLoader{}, &mockSegManager{})
	assert.NotNil(t, s)
	s.Close()
}

// ---------------------------------------------------------------------------
// Task 3: Acquire basic
// ---------------------------------------------------------------------------

func TestAcquire_NewSegments(t *testing.T) {
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
		1001: {SegmentID: 1001, PartitionID: 10, CollectionID: 100},
		2000: {SegmentID: 2000, PartitionID: 20, CollectionID: 100},
	}}
	loader := &mockLoader{}
	s := newLoadScheduler(ip, loader, &mockSegManager{})
	defer s.Close()

	waitAcquire(t, s, makeKey(1), 100, map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	loader.mu.Lock()
	assert.ElementsMatch(t, []int64{1000, 1001, 2000}, loader.loaded)
	loader.mu.Unlock()
}

// TestAcquire_OnReadyPartitionGrouping verifies that OnReady callbacks carry
// the correct partitionID → segmentIDs mapping. This is the core contract of
// the map-shaped SegmentIDs field.
func TestAcquire_OnReadyPartitionGrouping(t *testing.T) {
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
		1001: {SegmentID: 1001, PartitionID: 10, CollectionID: 100},
		2000: {SegmentID: 2000, PartitionID: 20, CollectionID: 100},
	}}
	s := newLoadScheduler(ip, &mockLoader{}, &mockSegManager{})
	defer s.Close()

	var mu sync.Mutex
	accumulated := make(map[int64][]int64)
	var wg sync.WaitGroup
	var once sync.Once
	wg.Add(1)

	s.Acquire(AcquireSegments{
		Key:          makeKey(1),
		CollectionID: 100,
		SegmentIDs:   map[int64][]int64{10: {1000, 1001}, 20: {2000}},
		OnReady: func(m map[int64][]int64) {
			mu.Lock()
			defer mu.Unlock()
			total := 0
			// Every segment reported in this batch must belong to the claimed partition.
			for partID, segs := range m {
				for _, segID := range segs {
					expectedPart := ip.infos[segID].PartitionID
					assert.Equal(t, expectedPart, partID,
						"segment %d reported under partition %d, expected %d", segID, partID, expectedPart)
					accumulated[partID] = append(accumulated[partID], segID)
				}
			}
			for _, segs := range accumulated {
				total += len(segs)
			}
			if total >= 3 {
				once.Do(wg.Done)
			}
		},
		OnUnrecoverable: func() {
			t.Errorf("unexpected OnUnrecoverable")
			once.Do(wg.Done)
		},
	})
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []int64{1000, 1001}, accumulated[10])
	assert.ElementsMatch(t, []int64{2000}, accumulated[20])
}

// ---------------------------------------------------------------------------
// Task 4: Acquire advanced
// ---------------------------------------------------------------------------

func TestAcquire_SharedSegments(t *testing.T) {
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
		2000: {SegmentID: 2000, PartitionID: 20, CollectionID: 100},
		3000: {SegmentID: 3000, PartitionID: 10, CollectionID: 100},
	}}
	loader := &mockLoader{}
	s := newLoadScheduler(ip, loader, &mockSegManager{})
	defer s.Close()

	// View1 loads [1000, 2000].
	waitAcquire(t, s, makeKey(1), 100, map[int64][]int64{10: {1000}, 20: {2000}})

	// View2 loads [2000, 3000]. 2000 should NOT be loaded again.
	waitAcquire(t, s, makeKey(2), 100, map[int64][]int64{20: {2000}, 10: {3000}})

	loader.mu.Lock()
	assert.ElementsMatch(t, []int64{1000, 2000, 3000}, loader.loaded)
	loader.mu.Unlock()
}

func TestAcquire_LoadError(t *testing.T) {
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
	}}
	loader := &mockLoader{err: map[int64]error{1000: fmt.Errorf("OOM")}}
	s := newLoadScheduler(ip, loader, &mockSegManager{})
	defer s.Close()

	var wg sync.WaitGroup
	var once sync.Once
	wg.Add(1)
	s.Acquire(AcquireSegments{
		Key:          makeKey(1),
		CollectionID: 100,
		SegmentIDs:   map[int64][]int64{10: {1000}},
		OnReady: func(m map[int64][]int64) {
			t.Errorf("unexpected OnReady")
			once.Do(wg.Done)
		},
		OnUnrecoverable: func() { once.Do(wg.Done) },
	})
	wg.Wait()
}

func TestAcquire_InfoProviderError(t *testing.T) {
	ip := &mockInfoProvider{err: fmt.Errorf("DataCoord unavailable")}
	s := newLoadScheduler(ip, &mockLoader{}, &mockSegManager{})
	defer s.Close()

	var wg sync.WaitGroup
	var once sync.Once
	wg.Add(1)
	s.Acquire(AcquireSegments{
		Key:          makeKey(1),
		CollectionID: 100,
		SegmentIDs:   map[int64][]int64{10: {1000}},
		OnReady: func(m map[int64][]int64) {
			t.Errorf("unexpected OnReady")
			once.Do(wg.Done)
		},
		OnUnrecoverable: func() { once.Do(wg.Done) },
	})
	wg.Wait()
}

func TestAcquire_ConcurrentDedup(t *testing.T) {
	loadStarted := make(chan struct{})
	loadProceed := make(chan struct{})
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
	}}
	loader := &blockingLoader{
		onLoad: func(id int64) {
			close(loadStarted)
			<-loadProceed
		},
	}
	s := newLoadScheduler(ip, loader, &mockSegManager{})
	defer s.Close()

	var ready1, ready2 sync.WaitGroup
	ready1.Add(1)
	ready2.Add(1)

	var once1, once2 sync.Once
	// View1 starts loading 1000.
	s.Acquire(AcquireSegments{
		Key:          makeKey(1),
		CollectionID: 100,
		SegmentIDs:   map[int64][]int64{10: {1000}},
		OnReady:      func(m map[int64][]int64) { once1.Do(ready1.Done) },
		OnUnrecoverable: func() {
			t.Errorf("unexpected OnUnrecoverable")
			once1.Do(ready1.Done)
		},
	})
	<-loadStarted

	// View2 requests 1000 while it's loading.
	s.Acquire(AcquireSegments{
		Key:          makeKey(2),
		CollectionID: 100,
		SegmentIDs:   map[int64][]int64{10: {1000}},
		OnReady:      func(m map[int64][]int64) { once2.Do(ready2.Done) },
		OnUnrecoverable: func() {
			t.Errorf("unexpected OnUnrecoverable")
			once2.Do(ready2.Done)
		},
	})

	close(loadProceed)

	ready1.Wait()
	ready2.Wait()
	assert.Equal(t, int32(1), loader.callCount.Load())
}

func TestAcquire_Empty(t *testing.T) {
	s := newLoadScheduler(&mockInfoProvider{}, &mockLoader{}, &mockSegManager{})
	defer s.Close()

	var wg sync.WaitGroup
	var once sync.Once
	wg.Add(1)
	s.Acquire(AcquireSegments{
		Key:          makeKey(1),
		CollectionID: 100,
		SegmentIDs:   map[int64][]int64{},
		OnReady:      func(m map[int64][]int64) { once.Do(wg.Done) },
		OnUnrecoverable: func() {
			t.Errorf("unexpected OnUnrecoverable")
			once.Do(wg.Done)
		},
	})
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Task 5: Release
// ---------------------------------------------------------------------------

func TestRelease_ExclusiveSegment(t *testing.T) {
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
	}}
	segMgr := &mockSegManager{}
	s := newLoadScheduler(ip, &mockLoader{}, segMgr)
	defer s.Close()

	key := makeKey(1)
	waitAcquire(t, s, key, 100, map[int64][]int64{10: {1000}})

	waitRelease(t, s, key, map[int64][]int64{10: {1000}})

	segMgr.mu.Lock()
	assert.ElementsMatch(t, []int64{1000}, segMgr.released)
	segMgr.mu.Unlock()
}

func TestRelease_SharedSegmentNotRemoved(t *testing.T) {
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
	}}
	segMgr := &mockSegManager{}
	s := newLoadScheduler(ip, &mockLoader{}, segMgr)
	defer s.Close()

	key1 := makeKey(1)
	key2 := makeKey(2)
	waitAcquire(t, s, key1, 100, map[int64][]int64{10: {1000}})
	waitAcquire(t, s, key2, 100, map[int64][]int64{10: {1000}})

	// Release view1 → segment 1000 still held by view2, NOT removed.
	waitRelease(t, s, key1, map[int64][]int64{10: {1000}})

	segMgr.mu.Lock()
	assert.Empty(t, segMgr.released)
	segMgr.mu.Unlock()

	// Release view2 → now segment 1000 is removed.
	waitRelease(t, s, key2, map[int64][]int64{10: {1000}})

	segMgr.mu.Lock()
	assert.ElementsMatch(t, []int64{1000}, segMgr.released)
	segMgr.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Task 6: Double Buffer lifecycle
// ---------------------------------------------------------------------------

func TestDoubleBufferLifecycle(t *testing.T) {
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1: {SegmentID: 1, PartitionID: 10, CollectionID: 100},
		2: {SegmentID: 2, PartitionID: 10, CollectionID: 100},
		3: {SegmentID: 3, PartitionID: 10, CollectionID: 100},
		4: {SegmentID: 4, PartitionID: 10, CollectionID: 100},
	}}
	segMgr := &mockSegManager{}
	s := newLoadScheduler(ip, &mockLoader{}, segMgr)
	defer s.Close()

	key1 := makeKey(1)
	key2 := makeKey(2)

	// View1 acquires [1, 2, 3].
	waitAcquire(t, s, key1, 100, map[int64][]int64{10: {1, 2, 3}})

	// View2 acquires [2, 3, 4] (overlapping).
	waitAcquire(t, s, key2, 100, map[int64][]int64{10: {2, 3, 4}})

	// Drop view1 → only segment 1 should be released (2,3 still held by view2).
	waitRelease(t, s, key1, map[int64][]int64{10: {1, 2, 3}})

	segMgr.mu.Lock()
	assert.ElementsMatch(t, []int64{1}, segMgr.released)
	segMgr.mu.Unlock()

	// Drop view2 → segments 2, 3, 4 released.
	waitRelease(t, s, key2, map[int64][]int64{10: {2, 3, 4}})

	segMgr.mu.Lock()
	assert.ElementsMatch(t, []int64{1, 2, 3, 4}, segMgr.released)
	segMgr.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Task 7: Failure + retry
// ---------------------------------------------------------------------------

func TestAcquire_RetryAfterFailure(t *testing.T) {
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
	}}
	loader := &mockLoader{err: map[int64]error{1000: fmt.Errorf("OOM")}}
	s := newLoadScheduler(ip, loader, &mockSegManager{})
	defer s.Close()

	// First attempt fails.
	var failWg sync.WaitGroup
	failWg.Add(1)
	s.Acquire(AcquireSegments{
		Key:             makeKey(1),
		CollectionID:    100,
		SegmentIDs:      map[int64][]int64{10: {1000}},
		OnReady:         func(m map[int64][]int64) {},
		OnUnrecoverable: func() { failWg.Done() },
	})
	failWg.Wait()

	// Clear error for retry.
	loader.mu.Lock()
	delete(loader.err, 1000)
	loader.mu.Unlock()

	// Second attempt succeeds (ref was cleaned, so it's toLoad again).
	waitAcquire(t, s, makeKey(2), 100, map[int64][]int64{10: {1000}})
}

func TestAcquire_SharedSegmentLoadFailure(t *testing.T) {
	loadStarted := make(chan struct{})
	loadProceed := make(chan struct{})
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
	}}
	loader := &blockingLoader{
		onLoad: func(id int64) {
			close(loadStarted)
			<-loadProceed
		},
		err: map[int64]error{1000: fmt.Errorf("OOM")},
	}
	s := newLoadScheduler(ip, loader, &mockSegManager{})
	defer s.Close()

	var fail1, fail2 sync.WaitGroup
	var once1, once2 sync.Once
	fail1.Add(1)
	fail2.Add(1)

	// View1 starts loading.
	s.Acquire(AcquireSegments{
		Key:          makeKey(1),
		CollectionID: 100,
		SegmentIDs:   map[int64][]int64{10: {1000}},
		OnReady: func(m map[int64][]int64) {
			t.Errorf("unexpected OnReady on load failure (view1)")
			once1.Do(fail1.Done)
		},
		OnUnrecoverable: func() { once1.Do(fail1.Done) },
	})
	<-loadStarted

	// View2 queues up to wait.
	s.Acquire(AcquireSegments{
		Key:          makeKey(2),
		CollectionID: 100,
		SegmentIDs:   map[int64][]int64{10: {1000}},
		OnReady: func(m map[int64][]int64) {
			t.Errorf("unexpected OnReady on load failure (view2)")
			once2.Do(fail2.Done)
		},
		OnUnrecoverable: func() { once2.Do(fail2.Done) },
	})

	close(loadProceed)

	fail1.Wait()
	fail2.Wait()
}

// ---------------------------------------------------------------------------
// Task 8: Close and concurrency
// ---------------------------------------------------------------------------

func TestClose(t *testing.T) {
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
	}}
	loadStarted := make(chan struct{})
	loader := &blockingLoader{
		onLoad: func(id int64) {
			close(loadStarted)
			time.Sleep(100 * time.Millisecond)
		},
	}
	s := newLoadScheduler(ip, loader, &mockSegManager{})

	s.Acquire(AcquireSegments{
		Key:             makeKey(1),
		CollectionID:    100,
		SegmentIDs:      map[int64][]int64{10: {1000}},
		OnReady:         func(m map[int64][]int64) {},
		OnUnrecoverable: func() {},
	})
	<-loadStarted

	done := make(chan struct{})
	go func() {
		s.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not complete in time")
	}
}

func TestConcurrentAcquireRelease(t *testing.T) {
	ip := &mockInfoProvider{infos: make(map[int64]*querypb.SegmentLoadInfo)}
	for i := int64(1); i <= 100; i++ {
		ip.infos[i] = &querypb.SegmentLoadInfo{SegmentID: i, PartitionID: 10, CollectionID: 100}
	}
	s := newLoadScheduler(ip, &mockLoader{}, &mockSegManager{})

	const numViews = 50
	var wg sync.WaitGroup

	for i := int64(1); i <= numViews; i++ {
		wg.Add(1)
		go func(version int64) {
			defer wg.Done()
			segs := []int64{version, version%100 + 1}

			var acquireWg sync.WaitGroup
			acquireWg.Add(1)
			readyCount := atomic.Int32{}
			var acquireOnce sync.Once
			s.Acquire(AcquireSegments{
				Key:          makeKey(version),
				CollectionID: 100,
				SegmentIDs:   map[int64][]int64{10: segs},
				OnReady: func(m map[int64][]int64) {
					count := int32(0)
					for _, ss := range m {
						count += int32(len(ss))
					}
					if readyCount.Add(count) >= int32(len(segs)) {
						acquireOnce.Do(acquireWg.Done)
					}
				},
				OnUnrecoverable: func() { acquireOnce.Do(acquireWg.Done) },
			})
			acquireWg.Wait()

			var releaseWg sync.WaitGroup
			releaseWg.Add(1)
			s.Release(ReleaseSegments{
				Key:        makeKey(version),
				SegmentIDs: map[int64][]int64{10: segs},
				OnDropped:  func() { releaseWg.Done() },
			})
			releaseWg.Wait()
		}(i)
	}

	wg.Wait()
	s.Close()
}

// ---------------------------------------------------------------------------
// Release during loading
// ---------------------------------------------------------------------------

func TestRelease_DuringLoading(t *testing.T) {
	loadStarted := make(chan struct{})
	loadProceed := make(chan struct{})
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
	}}
	loader := &blockingLoader{
		onLoad: func(id int64) {
			close(loadStarted)
			<-loadProceed
		},
	}
	segMgr := &mockSegManager{}
	s := newLoadScheduler(ip, loader, segMgr)
	defer s.Close()

	key := makeKey(1)
	s.Acquire(AcquireSegments{
		Key:             key,
		CollectionID:    100,
		SegmentIDs:      map[int64][]int64{10: {1000}},
		OnReady:         func(m map[int64][]int64) {},
		OnUnrecoverable: func() {},
	})
	<-loadStarted

	// Release while segment is still loading.
	var releaseDone sync.WaitGroup
	releaseDone.Add(1)
	s.Release(ReleaseSegments{
		Key:        key,
		SegmentIDs: map[int64][]int64{10: {1000}},
		OnDropped:  func() { releaseDone.Done() },
	})

	// Let the load complete.
	close(loadProceed)

	// OnDropped should fire after load completes + segment is removed.
	releaseDone.Wait()

	segMgr.mu.Lock()
	require.ElementsMatch(t, []int64{1000}, segMgr.released)
	segMgr.mu.Unlock()
}

func TestRelease_DuringLoadingWithFailure(t *testing.T) {
	loadStarted := make(chan struct{})
	loadProceed := make(chan struct{})
	ip := &mockInfoProvider{infos: map[int64]*querypb.SegmentLoadInfo{
		1000: {SegmentID: 1000, PartitionID: 10, CollectionID: 100},
	}}
	loader := &blockingLoader{
		onLoad: func(id int64) {
			close(loadStarted)
			<-loadProceed
		},
		err: map[int64]error{1000: fmt.Errorf("OOM")},
	}
	segMgr := &mockSegManager{}
	s := newLoadScheduler(ip, loader, segMgr)
	defer s.Close()

	key := makeKey(1)
	s.Acquire(AcquireSegments{
		Key:             key,
		CollectionID:    100,
		SegmentIDs:      map[int64][]int64{10: {1000}},
		OnReady:         func(m map[int64][]int64) {},
		OnUnrecoverable: func() {},
	})
	<-loadStarted

	var releaseDone sync.WaitGroup
	releaseDone.Add(1)
	s.Release(ReleaseSegments{
		Key:        key,
		SegmentIDs: map[int64][]int64{10: {1000}},
		OnDropped:  func() { releaseDone.Done() },
	})

	close(loadProceed)
	releaseDone.Wait()

	// Load failed → Remove should NOT be called (segment was never loaded).
	segMgr.mu.Lock()
	assert.Empty(t, segMgr.released)
	segMgr.mu.Unlock()
}
