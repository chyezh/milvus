package qnview

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// sealedSegmentLoader loads sealed segments into memory.
// segment_manager_impl.go adapts segments.Loader to this interface.
type sealedSegmentLoader interface {
	LoadSealed(ctx context.Context, collectionID int64, infos ...*querypb.SegmentLoadInfo) error
}

// segmentRemover removes segments from memory.
// segments.SegmentManager satisfies this interface directly (typeutil.UniqueID = int64).
type segmentRemover interface {
	Remove(ctx context.Context, segmentID int64, scope querypb.DataScope) (int, int)
}

// loadTask tracks an in-flight segment load. Waiters block on done.
type loadTask struct {
	done chan struct{} // closed when load completes (success or failure)
	err  error         // nil on success; set before close(done)
}

// segPartition pairs a segment ID with its partition ID for OnReady reporting.
type segPartition struct {
	segID       int64
	partitionID int64
}

// loadScheduler is the core implementation of segment lifecycle management.
// It handles reference counting, load deduplication, and async load/release.
type loadScheduler struct {
	mu sync.Mutex

	// segmentRefs tracks which views hold references to which segments.
	// segmentID → set of view keys.
	segmentRefs map[int64]map[qviews.QueryViewKey]struct{}

	// loadingTasks tracks in-flight segment loads for deduplication.
	// segmentID → loadTask.
	loadingTasks map[int64]*loadTask

	infoProvider SegmentLoadInfoProvider
	loader       sealedSegmentLoader
	segManager   segmentRemover

	pool *conc.Pool[struct{}]
	wg   sync.WaitGroup
}

func newLoadScheduler(
	infoProvider SegmentLoadInfoProvider,
	loader sealedSegmentLoader,
	segManager segmentRemover,
) *loadScheduler {
	poolSize := hardware.GetCPUNum() * 2
	if poolSize < 4 {
		poolSize = 4
	}
	return &loadScheduler{
		segmentRefs:  make(map[int64]map[qviews.QueryViewKey]struct{}),
		loadingTasks: make(map[int64]*loadTask),
		infoProvider: infoProvider,
		loader:       loader,
		segManager:   segManager,
		pool:         conc.NewPool[struct{}](poolSize),
	}
}

// Acquire records segment references and loads segments asynchronously.
// Callbacks (OnReady/OnUnrecoverable) are always invoked from background goroutines.
func (s *loadScheduler) Acquire(req AcquireSegments) {
	s.mu.Lock()

	var toLoad []segPartition
	toWait := make(map[segPartition]*loadTask)
	var readyNow []segPartition

	for partitionID, segIDs := range req.SegmentIDs {
		for _, segID := range segIDs {
			sp := segPartition{segID: segID, partitionID: partitionID}

			refs, exists := s.segmentRefs[segID]
			if !exists {
				refs = make(map[qviews.QueryViewKey]struct{})
				s.segmentRefs[segID] = refs
			}
			refs[req.Key] = struct{}{}

			if task, loading := s.loadingTasks[segID]; loading {
				// Another view is already loading this segment — wait for it.
				toWait[sp] = task
			} else if len(refs) == 1 {
				// First reference, no in-flight load — start loading.
				task := &loadTask{done: make(chan struct{})}
				s.loadingTasks[segID] = task
				toLoad = append(toLoad, sp)
			} else {
				// Already loaded and referenced by another view.
				readyNow = append(readyNow, sp)
			}
		}
	}
	s.mu.Unlock()

	if len(toLoad) == 0 && len(toWait) == 0 && len(readyNow) == 0 {
		// Empty Partitions or all segments handled — report ready asynchronously.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			req.OnReady(nil)
		}()
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.doAcquire(req, toLoad, toWait, readyNow)
	}()
}

func (s *loadScheduler) doAcquire(
	req AcquireSegments,
	toLoad []segPartition,
	toWait map[segPartition]*loadTask,
	readyNow []segPartition,
) {
	ctx := context.Background()

	// Phase 1: Report already-loaded segments.
	if len(readyNow) > 0 {
		req.OnReady(spGroupByPartition(readyNow))
	}

	// Phase 2: Load new segments.
	if len(toLoad) > 0 {
		segIDs := make([]int64, len(toLoad))
		for i, sp := range toLoad {
			segIDs[i] = sp.segID
		}
		infos, err := s.infoProvider.GetSegmentLoadInfos(ctx, req.CollectionID, segIDs)
		if err != nil {
			s.handleLoadFailure(segIDs)
			req.OnUnrecoverable()
			return
		}

		infoMap := make(map[int64]*querypb.SegmentLoadInfo, len(infos))
		for _, info := range infos {
			infoMap[info.GetSegmentID()] = info
		}

		var loadWg sync.WaitGroup
		var failed atomic.Bool
		for _, sp := range toLoad {
			info, ok := infoMap[sp.segID]
			if !ok {
				s.handleLoadFailure([]int64{sp.segID})
				failed.Store(true)
				continue
			}

			loadWg.Add(1)
			sp := sp
			s.pool.Submit(func() (struct{}, error) {
				defer loadWg.Done()
				loadErr := s.loader.LoadSealed(ctx, req.CollectionID, info)

				s.mu.Lock()
				task := s.loadingTasks[sp.segID]
				if loadErr != nil {
					if task != nil {
						task.err = loadErr
					}
					s.cleanupFailedSegment(sp.segID)
				}
				delete(s.loadingTasks, sp.segID)
				s.mu.Unlock()

				if task != nil {
					close(task.done)
				}
				if loadErr != nil {
					failed.Store(true)
				} else {
					req.OnReady(map[int64][]int64{sp.partitionID: {sp.segID}})
				}
				return struct{}{}, nil
			})
		}
		loadWg.Wait()

		if failed.Load() {
			req.OnUnrecoverable()
			return
		}
	}

	// Phase 3: Wait for segments being loaded by other views.
	for sp, task := range toWait {
		<-task.done
		if task.err != nil {
			req.OnUnrecoverable()
			return
		}
		req.OnReady(map[int64][]int64{sp.partitionID: {sp.segID}})
	}
}

// Release decrements segment references and unloads segments with zero refs.
// OnDropped is always invoked from a background goroutine.
func (s *loadScheduler) Release(req ReleaseSegments) {
	s.mu.Lock()
	var toRelease []int64
	toWaitThenRelease := make(map[int64]*loadTask)
	for _, segIDs := range req.SegmentIDs {
		for _, segID := range segIDs {
			refs := s.segmentRefs[segID]
			if refs != nil {
				delete(refs, req.Key)
				if len(refs) == 0 {
					delete(s.segmentRefs, segID)
					if task, ok := s.loadingTasks[segID]; ok {
						toWaitThenRelease[segID] = task
					} else {
						toRelease = append(toRelease, segID)
					}
				}
			}
		}
	}
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for _, segID := range toRelease {
			s.segManager.Remove(context.Background(), segID, querypb.DataScope_Historical)
		}
		for segID, task := range toWaitThenRelease {
			<-task.done
			if task.err == nil {
				s.segManager.Remove(context.Background(), segID, querypb.DataScope_Historical)
			}
		}
		req.OnDropped()
	}()
}

// Close waits for all in-flight goroutines and releases the worker pool.
func (s *loadScheduler) Close() {
	s.wg.Wait()
	s.pool.Release()
}

// spGroupByPartition groups segPartitions by partition ID for OnReady reporting.
func spGroupByPartition(sps []segPartition) map[int64][]int64 {
	result := make(map[int64][]int64)
	for _, sp := range sps {
		result[sp.partitionID] = append(result[sp.partitionID], sp.segID)
	}
	return result
}

// cleanupFailedSegment removes ALL refs for a segment. Caller must hold s.mu.
func (s *loadScheduler) cleanupFailedSegment(segID int64) {
	delete(s.segmentRefs, segID)
}

// handleLoadFailure cleans up state for failed segments and notifies waiters.
// Channels are closed outside the lock to match the pool worker path.
func (s *loadScheduler) handleLoadFailure(segIDs []int64) {
	s.mu.Lock()
	tasksToClose := make([]*loadTask, 0, len(segIDs))
	for _, segID := range segIDs {
		task := s.loadingTasks[segID]
		if task != nil {
			task.err = merr.WrapErrSegmentLoadFailed(segID)
			tasksToClose = append(tasksToClose, task)
		}
		delete(s.loadingTasks, segID)
		s.cleanupFailedSegment(segID)
	}
	s.mu.Unlock()

	for _, task := range tasksToClose {
		close(task.done)
	}
}
