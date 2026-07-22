package qnview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type QueryViewSegmentLoadScheduler struct {
	scheduler nodescheduler.Scheduler
	loader    PhysicalSegmentLoader
	estimator SegmentResourceEstimator

	mu      sync.Mutex
	handles map[int64]nodescheduler.TaskHandle
}

func NewQueryViewSegmentLoadScheduler(meta QueryViewLoadMetadataProvider, loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *QueryViewSegmentLoadScheduler {
	return newQueryViewSegmentLoadScheduler(nodescheduler.Get(), loader, estimators...)
}

func newQueryViewSegmentLoadScheduler(scheduler nodescheduler.Scheduler, loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *QueryViewSegmentLoadScheduler {
	var estimator SegmentResourceEstimator
	if len(estimators) > 0 {
		estimator = estimators[0]
	}
	return &QueryViewSegmentLoadScheduler{
		scheduler: scheduler,
		loader:    loader,
		estimator: estimator,
		handles:   make(map[int64]nodescheduler.TaskHandle),
	}
}

func (s *QueryViewSegmentLoadScheduler) Submit(task SegmentLoadTask) {
	s.mu.Lock()
	handle := s.scheduler.Submit(&segmentLoadSchedulerTask{scheduler: s, task: task})
	s.handles[task.SegmentID] = handle
	s.mu.Unlock()
}

func (s *QueryViewSegmentLoadScheduler) Update(task SegmentUpdateTask) {
	segmentID := task.Snapshot.SegmentID
	if segmentID == 0 {
		segmentID = task.Segment.ID()
	}

	s.mu.Lock()
	handle := s.scheduler.Submit(&segmentUpdateSchedulerTask{scheduler: s, task: task})
	s.handles[segmentID] = handle
	s.mu.Unlock()
}

func (s *QueryViewSegmentLoadScheduler) Cancel(segmentID int64) {
	s.mu.Lock()
	handle := s.handles[segmentID]
	delete(s.handles, segmentID)
	s.mu.Unlock()
	if handle != nil {
		handle.Cancel()
	}
}

func (s *QueryViewSegmentLoadScheduler) load(ctx context.Context, task SegmentLoadTask) error {
	segment, err := s.loadMissing(ctx, task)
	if err != nil {
		if task.OnUnrecoverable != nil {
			task.OnUnrecoverable(err)
		}
		return err
	}
	if task.OnLoaded != nil {
		task.OnLoaded(segment)
	}
	return nil
}

func (s *QueryViewSegmentLoadScheduler) loadMissing(ctx context.Context, task SegmentLoadTask) (TransformSegment, error) {
	loadInfo, indexes, err := s.loadInfo(ctx, task)
	if err != nil {
		return nil, err
	}
	if err := updateCollectionIndexMeta(ctx, task.Collection, indexes); err != nil {
		return nil, err
	}
	reservation, err := s.reserve(ctx, loadInfo, task.Collection)
	if err != nil {
		return nil, err
	}
	if reservation != nil {
		defer reservation.Release()
	}
	segment, err := s.loader.Load(ctx, loadInfo, task.Collection)
	if err != nil {
		return nil, err
	}
	if task.TransformStartAfterTimeTick > 0 {
		segment = &transformStartSegment{
			TransformSegment: segment,
			startAfter:       task.TransformStartAfterTimeTick,
		}
	}
	return segment, nil
}

func (s *QueryViewSegmentLoadScheduler) loadInfo(ctx context.Context, task SegmentLoadTask) (*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	if task.Snapshot.LoadInfo != nil {
		return task.Snapshot.LoadInfo, task.Snapshot.IndexInfos, nil
	}
	return nil, nil, merr.WrapErrServiceInternalMsg("query view segment load requires watch snapshot, segmentID=%d", task.SegmentID)
}

func (s *QueryViewSegmentLoadScheduler) update(ctx context.Context, task SegmentUpdateTask) error {
	action := classifySegmentUpdate(task.Current, task.Snapshot.Revision)
	if action == SegmentUpdateNone {
		if task.OnUpdated != nil {
			task.OnUpdated(task.Current)
		}
		return nil
	}
	if err := updateCollectionIndexMeta(ctx, task.Collection, task.Snapshot.IndexInfos); err != nil {
		if task.OnFailed != nil {
			task.OnFailed(err)
		}
		return err
	}
	if err := s.loader.Update(ctx, task.Segment, task.Collection, task.Snapshot, action); err != nil {
		if task.OnFailed != nil {
			task.OnFailed(err)
		}
		return err
	}
	if task.OnUpdated != nil {
		task.OnUpdated(task.Snapshot.Revision)
	}
	return nil
}

type segmentLoadSchedulerTask struct {
	scheduler *QueryViewSegmentLoadScheduler
	task      SegmentLoadTask
}

func (t *segmentLoadSchedulerTask) Execute(ctx context.Context) error {
	ctx, cancel := mergeTaskContext(ctx, t.task.Context)
	defer cancel()
	return t.scheduler.load(ctx, t.task)
}

type segmentUpdateSchedulerTask struct {
	scheduler *QueryViewSegmentLoadScheduler
	task      SegmentUpdateTask
}

func (t *segmentUpdateSchedulerTask) Execute(ctx context.Context) error {
	ctx, cancel := mergeTaskContext(ctx, t.task.Context)
	defer cancel()
	return t.scheduler.update(ctx, t.task)
}

func mergeTaskContext(schedulerCtx context.Context, taskCtx context.Context) (context.Context, context.CancelFunc) {
	if taskCtx == nil {
		taskCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(taskCtx)
	stop := context.AfterFunc(schedulerCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func classifySegmentUpdate(current, next SegmentLoadInfoRevision) SegmentUpdateAction {
	if next.Empty() || current == next {
		return SegmentUpdateNone
	}
	return SegmentUpdateReopen | SegmentUpdateLoadIndex
}

func (s *QueryViewSegmentLoadScheduler) reserve(ctx context.Context, info *querypb.SegmentLoadInfo, collection CollectionRuntime) (ResourceReservation, error) {
	if s.estimator == nil {
		return nil, nil
	}
	return s.estimator.Reserve(ctx, info, collection)
}

func updateCollectionIndexMeta(ctx context.Context, collection CollectionRuntime, indexes []*indexpb.IndexInfo) error {
	updater, ok := collection.(CollectionIndexMetaUpdater)
	if !ok {
		return nil
	}
	return updater.UpdateIndexMeta(ctx, indexes)
}

type transformStartSegment struct {
	TransformSegment
	startAfter uint64
}

func (s *transformStartSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}

func (s *transformStartSegment) QuerySegment() segments.Segment {
	readable, ok := s.TransformSegment.(ReadableSealedSegment)
	if !ok {
		return nil
	}
	return readable.QuerySegment()
}

func (s *transformStartSegment) Collection() *segments.Collection {
	readable, ok := s.TransformSegment.(ReadableSealedSegment)
	if !ok {
		return nil
	}
	return readable.Collection()
}
