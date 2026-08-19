package qnview

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type ViewScopedPhysicalSegmentManager struct {
	nodeScheduler nodescheduler.Scheduler
	loader        PhysicalSegmentLoader
	estimator     SegmentResourceEstimator
	stream        SegmentLoadInfoStream

	mu                  sync.Mutex
	views               map[qviews.QueryViewKey]*viewRef
	dropping            map[qviews.QueryViewKey]*viewRef
	segments            map[segmentRefKey]*physicalSegmentState
	pendingLoadSegments map[segmentRefKey]struct{}
	cancels             map[qviews.QueryViewKey]context.CancelFunc
}

type viewRef struct {
	segments        map[int64]int64
	pendingLoads    int
	onDropped       func()
	onLoaded        func([]TransformSegment)
	onSegmentFailed func(segmentID int64, err error)
	onUnrecoverable func()
}

type physicalSegmentState struct {
	segment         TransformSegment
	collectionID    int64
	loading         bool
	updating        bool
	updateHandle    nodescheduler.TaskHandle
	updateEpoch     uint64
	loadCancel      context.CancelFunc
	loadDone        func()
	refs            map[qviews.QueryViewKey]struct{}
	requests        map[qviews.QueryViewKey]segmentLoadRequest
	revision        SegmentLoadInfoRevision
	pendingSnapshot *SegmentLoadInfoSnapshot
	subscription    SegmentLoadInfoSubscription
}

type segmentLoadInfoSubscriptionRequest struct {
	key          segmentRefKey
	collectionID int64
	segmentID    int64
	revision     SegmentLoadInfoRevision
	state        *physicalSegmentState
}

type segmentLoadSubmission struct {
	key       segmentRefKey
	segmentID int64
	ctx       context.Context
	request   segmentLoadRequest
	snapshot  SegmentLoadInfoSnapshot
	done      func()
}

type segmentUpdateSubmission struct {
	task  *SegmentUpdateTask
	state *physicalSegmentState
	epoch uint64
}

type segmentLoadRequest struct {
	meta                        *viewpb.QueryViewMeta
	collection                  CollectionRuntime
	walReplicaID                int64
	transformStartAfterTimeTick uint64
}

func NewViewScopedPhysicalSegmentManager(loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *ViewScopedPhysicalSegmentManager {
	return NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodescheduler.Get(), loader, estimators...)
}

func NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodeScheduler nodescheduler.Scheduler, loader PhysicalSegmentLoader, estimators ...SegmentResourceEstimator) *ViewScopedPhysicalSegmentManager {
	return NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndStream(nodeScheduler, loader, nil, estimators...)
}

func NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndStream(nodeScheduler nodescheduler.Scheduler, loader PhysicalSegmentLoader, stream SegmentLoadInfoStream, estimators ...SegmentResourceEstimator) *ViewScopedPhysicalSegmentManager {
	var estimator SegmentResourceEstimator
	if len(estimators) > 0 {
		estimator = estimators[0]
	}
	return &ViewScopedPhysicalSegmentManager{
		nodeScheduler:       nodeScheduler,
		loader:              loader,
		estimator:           estimator,
		stream:              stream,
		views:               make(map[qviews.QueryViewKey]*viewRef),
		dropping:            make(map[qviews.QueryViewKey]*viewRef),
		segments:            make(map[segmentRefKey]*physicalSegmentState),
		pendingLoadSegments: make(map[segmentRefKey]struct{}),
		cancels:             make(map[qviews.QueryViewKey]context.CancelFunc),
	}
}

func (m *ViewScopedPhysicalSegmentManager) Acquire(req AcquirePhysicalSegments) {
	ctx, cancel := context.WithCancel(context.Background())
	toLoad, ok := m.recordView(req, cancel)
	if !ok {
		cancel()
		return
	}
	m.nodeScheduler.Submit(schedulerTaskFunc(func(context.Context) error {
		m.load(ctx, req, toLoad)
		return nil
	}))
}

func (m *ViewScopedPhysicalSegmentManager) Release(req ReleaseSegments) {
	toClose, onDropped := m.removeView(req)
	m.closeSubscriptions(toClose)
	m.submitCallback(onDropped)
}

func (m *ViewScopedPhysicalSegmentManager) ApplyLoadInfoSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot) {
	m.applyLoadInfoSnapshot(ctx, snapshot, nil)
}

func (m *ViewScopedPhysicalSegmentManager) applyLoadInfoSnapshot(ctx context.Context, snapshot SegmentLoadInfoSnapshot, expected *physicalSegmentState) {
	m.applyLoadInfoSnapshotForKey(ctx, segmentRefKey{}, snapshot, expected)
}

func (m *ViewScopedPhysicalSegmentManager) applyLoadInfoSnapshotForKey(ctx context.Context, segmentKey segmentRefKey, snapshot SegmentLoadInfoSnapshot, expected *physicalSegmentState) {
	if ctx == nil {
		ctx = context.Background()
	}
	if snapshot.SegmentID == 0 && snapshot.LoadInfo != nil {
		snapshot.SegmentID = snapshot.LoadInfo.GetSegmentID()
	}
	load, update, ok := m.recordSegmentSnapshot(ctx, segmentKey, snapshot, expected)
	if !ok {
		return
	}
	if load.segmentID != 0 {
		m.submitSegmentLoad(load, nil)
		return
	}
	m.submitSegmentUpdate(update)
}

func (m *ViewScopedPhysicalSegmentManager) recordView(req AcquirePhysicalSegments, cancel context.CancelFunc) ([]segmentLoadSubmission, bool) {
	segmentPartitions := segmentPartitionMap(req.View)
	toLoad := make([]segmentLoadSubmission, 0, len(segmentPartitions))
	toSubscribe := make([]segmentLoadInfoSubscriptionRequest, 0, len(segmentPartitions))
	m.mu.Lock()
	if m.views[req.Key] != nil {
		m.mu.Unlock()
		return nil, false
	}
	m.cancels[req.Key] = cancel
	walReplicaID := req.View.GetWalReplicaId()
	ref := &viewRef{
		segments:        segmentPartitions,
		onLoaded:        req.OnLoaded,
		onSegmentFailed: req.OnSegmentUnrecoverable,
		onUnrecoverable: req.OnUnrecoverable,
	}
	m.views[req.Key] = ref
	for segmentID := range segmentPartitions {
		segmentKey := newSegmentRefKey(segmentID, walReplicaID)
		state, ok := m.segments[segmentKey]
		if !ok {
			state = &physicalSegmentState{
				collectionID: req.Meta.GetCollectionId(),
				refs:         make(map[qviews.QueryViewKey]struct{}),
				requests:     make(map[qviews.QueryViewKey]segmentLoadRequest),
			}
			if m.stream == nil {
				loadCtx, loadCancel := context.WithCancel(context.Background())
				ref.pendingLoads++
				loadDone := onceLoadDone(func() { m.completeLoadAttempts([]qviews.QueryViewKey{req.Key}) })
				state.loading = true
				state.loadCancel = loadCancel
				state.loadDone = loadDone
				toLoad = append(toLoad, segmentLoadSubmission{
					key:       segmentKey,
					segmentID: segmentID,
					ctx:       loadCtx,
					request:   newSegmentLoadRequest(req),
					done:      chainLoadDone(loadDone, loadCancel),
				})
			} else {
				toSubscribe = append(toSubscribe, segmentLoadInfoSubscriptionRequest{
					key:          segmentKey,
					collectionID: req.Meta.GetCollectionId(),
					segmentID:    segmentID,
					revision:     state.revision,
					state:        state,
				})
			}
			m.segments[segmentKey] = state
		} else if _, pending := m.pendingLoadSegments[segmentKey]; state.segment == nil && !state.loading && !pending && m.stream == nil {
			loadCtx, loadCancel := context.WithCancel(context.Background())
			ref.pendingLoads++
			loadDone := onceLoadDone(func() { m.completeLoadAttempts([]qviews.QueryViewKey{req.Key}) })
			state.loading = true
			state.loadCancel = loadCancel
			state.loadDone = loadDone
			toLoad = append(toLoad, segmentLoadSubmission{
				key:       segmentKey,
				segmentID: segmentID,
				ctx:       loadCtx,
				request:   newSegmentLoadRequest(req),
				done:      chainLoadDone(loadDone, loadCancel),
			})
		}
		state.refs[req.Key] = struct{}{}
		state.requests[req.Key] = newSegmentLoadRequest(req)
	}
	m.mu.Unlock()
	m.subscribeSegments(toSubscribe)

	return toLoad, true
}

func (m *ViewScopedPhysicalSegmentManager) load(ctx context.Context, req AcquirePhysicalSegments, toLoad []segmentLoadSubmission) {
	if len(toLoad) > 0 {
		for _, submission := range toLoad {
			if ctx.Err() != nil {
				submission.done()
				continue
			}
			m.submitSegmentLoad(submission, nil)
		}
		return
	}

	loaded, complete := m.collectLoaded(req.View)
	if ctx.Err() != nil {
		return
	}
	if complete && req.OnLoaded != nil {
		req.OnLoaded(loaded)
	}
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentLoad(submission segmentLoadSubmission, done func()) {
	done = chainLoadDone(done, submission.done)
	if done == nil {
		done = func() {}
	}
	task := newSegmentLoadTask(m.loader, m.estimator, SegmentLoadTask{
		Context:                     submission.ctx,
		SegmentID:                   submission.segmentID,
		WALReplicaID:                submission.request.walReplicaID,
		Collection:                  submission.request.collection,
		TransformStartAfterTimeTick: submission.request.transformStartAfterTimeTick,
		Snapshot:                    submission.snapshot,
		OnFinished:                  done,
		OnLoaded: func(segment TransformSegment) {
			defer done()
			if segment == nil {
				notifications, retries, subscriptions := m.failPhysicalSegmentLoad(submission.key, submission.snapshot, nil)
				m.submitSegmentLoadSubmissions(retries)
				m.closeSubscriptions(subscriptions)
				for _, notify := range notifications {
					notify()
				}
				return
			}
			notifications, retries, kept := m.completePhysicalSegmentLoad(submission.key, segment, submission.snapshot.Revision)
			m.submitSegmentLoadSubmissions(retries)
			if !kept {
				_ = segment.Release(context.Background())
				return
			}
			m.submitPendingSegmentUpdate(submission.key)
			for _, notify := range notifications {
				notify()
			}
		},
		OnUnrecoverable: func(err error) {
			defer done()
			notifications, retries, subscriptions := m.failPhysicalSegmentLoad(submission.key, submission.snapshot, err)
			m.submitSegmentLoadSubmissions(retries)
			m.closeSubscriptions(subscriptions)
			for _, notify := range notifications {
				notify()
			}
		},
	})
	m.nodeScheduler.Submit(task)
}

func (m *ViewScopedPhysicalSegmentManager) completePhysicalSegmentLoad(segmentKey segmentRefKey, segment TransformSegment, revision SegmentLoadInfoRevision) ([]func(), []segmentLoadSubmission, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.segments[segmentKey]
	if state == nil {
		return nil, nil, false
	}
	state.segment = segment
	state.loading = false
	delete(m.pendingLoadSegments, segmentKey)
	state.loadCancel = nil
	state.loadDone = nil
	if !revision.Empty() {
		state.revision = revision
	}
	if len(state.refs) == 0 {
		delete(m.segments, segmentKey)
		return nil, nil, false
	}

	notifications := make([]func(), 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil || ref.onLoaded == nil {
			continue
		}
		loaded := []TransformSegment{segment}
		cb := ref.onLoaded
		notifications = append(notifications, func() {
			cb(loaded)
		})
	}
	return notifications, m.collectPendingLoadSubmissionsLocked(), true
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentSnapshot(ctx context.Context, segmentKey segmentRefKey, snapshot SegmentLoadInfoSnapshot, expected *physicalSegmentState) (segmentLoadSubmission, segmentUpdateSubmission, bool) {
	if snapshot.Revision.Empty() {
		return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var state *physicalSegmentState
	if segmentKey.segmentID == 0 {
		segmentKey, state = m.findSegmentStateLocked(snapshot.SegmentID, expected)
	} else {
		state = m.segments[segmentKey]
	}
	if state == nil || len(state.refs) == 0 || (expected != nil && state != expected) {
		return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
	}
	if state.segment == nil {
		if state.loading {
			snapshotCopy := snapshot
			state.pendingSnapshot = &snapshotCopy
			return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
		}
		request, ok := state.loadRequest()
		if !ok {
			return segmentLoadSubmission{}, segmentUpdateSubmission{}, false
		}
		loadCtx, loadCancel := context.WithCancel(context.Background())
		loadDone := onceLoadDone(m.trackPendingLoadAttemptLocked(state))
		delete(m.pendingLoadSegments, segmentKey)
		state.pendingSnapshot = nil
		state.loading = true
		state.loadCancel = loadCancel
		state.loadDone = loadDone
		return segmentLoadSubmission{
			key:       segmentKey,
			segmentID: snapshot.SegmentID,
			ctx:       loadCtx,
			request:   request,
			snapshot:  snapshot,
			done:      chainLoadDone(loadDone, loadCancel),
		}, segmentUpdateSubmission{}, true
	}
	task, ok := m.recordSegmentUpdateLocked(ctx, segmentKey, snapshot, state)
	return segmentLoadSubmission{}, task, ok
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentUpdate(ctx context.Context, snapshot SegmentLoadInfoSnapshot) (segmentUpdateSubmission, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	segmentKey, state := m.findSegmentStateLocked(snapshot.SegmentID, nil)
	return m.recordSegmentUpdateLocked(ctx, segmentKey, snapshot, state)
}

func (m *ViewScopedPhysicalSegmentManager) findSegmentStateLocked(segmentID int64, expected *physicalSegmentState) (segmentRefKey, *physicalSegmentState) {
	for key, state := range m.segments {
		if key.segmentID != segmentID {
			continue
		}
		if expected != nil && state != expected {
			continue
		}
		return key, state
	}
	return segmentRefKey{}, nil
}

func (m *ViewScopedPhysicalSegmentManager) recordSegmentUpdateLocked(ctx context.Context, segmentKey segmentRefKey, snapshot SegmentLoadInfoSnapshot, state *physicalSegmentState) (segmentUpdateSubmission, bool) {
	if state == nil || state.segment == nil || len(state.refs) == 0 || snapshot.Revision.Empty() {
		return segmentUpdateSubmission{}, false
	}
	if !state.updating && state.revision == snapshot.Revision {
		return segmentUpdateSubmission{}, false
	}
	snapshotCopy := snapshot
	state.pendingSnapshot = &snapshotCopy
	if state.updating {
		return segmentUpdateSubmission{}, false
	}
	return m.nextSegmentUpdateLocked(ctx, segmentKey)
}

func (m *ViewScopedPhysicalSegmentManager) nextSegmentUpdateLocked(ctx context.Context, segmentKey segmentRefKey) (segmentUpdateSubmission, bool) {
	state := m.segments[segmentKey]
	if state == nil || state.segment == nil || state.pendingSnapshot == nil || len(state.refs) == 0 {
		if state != nil {
			state.updating = false
		}
		return segmentUpdateSubmission{}, false
	}
	if state.pendingSnapshot.Revision == state.revision {
		state.pendingSnapshot = nil
		state.updating = false
		return segmentUpdateSubmission{}, false
	}
	request, ok := state.loadRequest()
	if !ok || request.collection == nil {
		state.updating = false
		return segmentUpdateSubmission{}, false
	}
	snapshot := *state.pendingSnapshot
	state.pendingSnapshot = nil
	state.updating = true
	state.updateEpoch++
	epoch := state.updateEpoch
	task := newSegmentUpdateTask(m.loader, SegmentUpdateTask{
		Context:    ctx,
		Segment:    state.segment,
		Collection: request.collection,
		Snapshot:   snapshot,
		Current:    state.revision,
		OnUpdated: func(revision SegmentLoadInfoRevision) {
			m.completeSegmentUpdate(segmentKey, state, epoch, revision)
		},
		OnFailed: func(error) {
			m.failSegmentUpdate(segmentKey, state, epoch)
		},
	})
	return segmentUpdateSubmission{task: task, state: state, epoch: epoch}, true
}

func (m *ViewScopedPhysicalSegmentManager) submitPendingSegmentUpdate(segmentKey segmentRefKey) {
	m.mu.Lock()
	submission, ok := m.nextSegmentUpdateLocked(context.Background(), segmentKey)
	m.mu.Unlock()
	if ok {
		m.submitSegmentUpdate(submission)
	}
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentUpdate(submission segmentUpdateSubmission) {
	handle := m.nodeScheduler.Submit(submission.task)
	m.mu.Lock()
	segmentKey := segmentRefKeyFromSegment(submission.task.Segment)
	state := m.segments[segmentKey]
	if state != submission.state || !state.updating || state.updateEpoch != submission.epoch {
		m.mu.Unlock()
		handle.Cancel()
		return
	}
	state.updateHandle = handle
	m.mu.Unlock()
}

func (m *ViewScopedPhysicalSegmentManager) completeSegmentUpdate(segmentKey segmentRefKey, expected *physicalSegmentState, epoch uint64, revision SegmentLoadInfoRevision) {
	var next segmentUpdateSubmission
	var ok bool
	m.mu.Lock()
	state := m.segments[segmentKey]
	if state == expected && state.updateEpoch == epoch {
		state.revision = revision
		state.updating = false
		state.updateHandle = nil
		next, ok = m.nextSegmentUpdateLocked(context.Background(), segmentKey)
	}
	m.mu.Unlock()
	if ok {
		m.submitSegmentUpdate(next)
	}
}

func (m *ViewScopedPhysicalSegmentManager) failSegmentUpdate(segmentKey segmentRefKey, expected *physicalSegmentState, epoch uint64) {
	m.mu.Lock()
	if state := m.segments[segmentKey]; state == expected && state.updateEpoch == epoch {
		state.updating = false
		state.updateHandle = nil
	}
	m.mu.Unlock()
}

func (m *ViewScopedPhysicalSegmentManager) cancelSegmentUpdateLocked(state *physicalSegmentState) {
	if state == nil {
		return
	}
	state.updateEpoch++
	state.updating = false
	state.pendingSnapshot = nil
	if state.updateHandle != nil {
		state.updateHandle.Cancel()
		state.updateHandle = nil
	}
}

func (m *ViewScopedPhysicalSegmentManager) failPhysicalSegmentLoad(segmentKey segmentRefKey, snapshot SegmentLoadInfoSnapshot, err error) ([]func(), []segmentLoadSubmission, []SegmentLoadInfoSubscription) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.segments[segmentKey]
	if state == nil {
		return nil, nil, nil
	}
	state.loading = false
	state.loadCancel = nil
	state.loadDone = nil
	if state.segment == nil && len(state.refs) == 0 {
		subscription := m.detachSubscriptionLocked(state)
		delete(m.segments, segmentKey)
		return nil, nil, subscription
	}
	if isSegmentResourceInsufficient(err) && m.hasOtherLoadingSegmentLocked(segmentKey) {
		m.pendingLoadSegments[segmentKey] = struct{}{}
		if state.pendingSnapshot == nil {
			snapshotCopy := snapshot
			state.pendingSnapshot = &snapshotCopy
		}
		return nil, nil, nil
	}

	notifications := make([]func(), 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil {
			continue
		}
		if ref.onSegmentFailed != nil {
			cb := ref.onSegmentFailed
			notifications = append(notifications, func() {
				cb(segmentKey.segmentID, err)
			})
			continue
		}
		if ref.onUnrecoverable != nil {
			cb := ref.onUnrecoverable
			notifications = append(notifications, func() {
				cb()
			})
		}
	}
	state.refs = make(map[qviews.QueryViewKey]struct{})
	state.requests = make(map[qviews.QueryViewKey]segmentLoadRequest)
	state.loading = false
	delete(m.pendingLoadSegments, segmentKey)
	state.pendingSnapshot = nil
	state.loadCancel = nil
	state.loadDone = nil
	subscription := m.detachSubscriptionLocked(state)
	if state.segment == nil {
		delete(m.segments, segmentKey)
	}
	return notifications, m.collectPendingLoadSubmissionsLocked(), subscription
}

func (m *ViewScopedPhysicalSegmentManager) ResetSegment(segmentID int64, walReplicaID int64) {
	segmentKey := newSegmentRefKey(segmentID, walReplicaID)
	var subscriptions []SegmentLoadInfoSubscription
	m.mu.Lock()
	if state := m.segments[segmentKey]; state != nil {
		if state.loadCancel != nil {
			state.loadCancel()
		}
		m.cancelSegmentUpdateLocked(state)
		subscriptions = m.detachSubscriptionLocked(state)
		delete(m.pendingLoadSegments, segmentKey)
		delete(m.segments, segmentKey)
	}
	for key, ref := range m.views {
		if key.WALReplicaID == walReplicaID {
			delete(ref.segments, segmentID)
		}
	}
	m.mu.Unlock()
	m.closeSubscriptions(subscriptions)
}

func (m *ViewScopedPhysicalSegmentManager) collectLoaded(view *viewpb.QueryViewOfQueryNode) ([]TransformSegment, bool) {
	segments := make([]TransformSegment, 0, len(segmentPartitionMap(view)))
	walReplicaID := view.GetWalReplicaId()

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, partition := range view.GetPartitions() {
		for _, segmentID := range partition.GetSegmentIds() {
			state := m.segments[newSegmentRefKey(segmentID, walReplicaID)]
			if state == nil || state.segment == nil {
				return nil, false
			}
			segments = append(segments, state.segment)
		}
	}
	return segments, true
}

func (m *ViewScopedPhysicalSegmentManager) removeView(req ReleaseSegments) ([]SegmentLoadInfoSubscription, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := req.Key
	ref := m.views[key]
	if ref == nil {
		if cancel, ok := m.cancels[key]; ok {
			cancel()
			delete(m.cancels, key)
		}
		return nil, req.OnDropped
	}
	delete(m.views, key)
	ref.onDropped = req.OnDropped

	toClose := make([]SegmentLoadInfoSubscription, 0, len(ref.segments))
	for segmentID := range ref.segments {
		segmentKey := newSegmentRefKey(segmentID, key.WALReplicaID)
		state := m.segments[segmentKey]
		if state == nil {
			continue
		}
		delete(state.refs, key)
		delete(state.requests, key)
		if len(state.refs) == 0 {
			m.cancelSegmentUpdateLocked(state)
			if state.segment == nil && state.loading {
				if state.loadCancel != nil {
					state.loadCancel()
				}
				state.loadCancel = nil
				state.loadDone = nil
			}
			toClose = append(toClose, m.detachSubscriptionLocked(state)...)
			delete(m.pendingLoadSegments, segmentKey)
			delete(m.segments, segmentKey)
		}
	}
	if cancel, ok := m.cancels[key]; ok {
		cancel()
		delete(m.cancels, key)
	}
	if ref.pendingLoads == 0 {
		return toClose, ref.onDropped
	}
	m.dropping[key] = ref
	return toClose, nil
}

func newSegmentLoadRequest(req AcquirePhysicalSegments) segmentLoadRequest {
	return segmentLoadRequest{
		meta:                        req.Meta,
		collection:                  req.Collection,
		walReplicaID:                req.View.GetWalReplicaId(),
		transformStartAfterTimeTick: req.Meta.GetTransformStartAfterTimetick(),
	}
}

func isSegmentResourceInsufficient(err error) bool {
	return errors.Is(err, merr.ErrSegmentRequestResourceFailed)
}

func (m *ViewScopedPhysicalSegmentManager) hasOtherLoadingSegmentLocked(segmentKey segmentRefKey) bool {
	for key, state := range m.segments {
		if key != segmentKey && state.loading {
			return true
		}
	}
	return false
}

func (m *ViewScopedPhysicalSegmentManager) collectPendingLoadSubmissionsLocked() []segmentLoadSubmission {
	submissions := make([]segmentLoadSubmission, 0, len(m.pendingLoadSegments))
	for segmentKey := range m.pendingLoadSegments {
		state := m.segments[segmentKey]
		if state == nil {
			delete(m.pendingLoadSegments, segmentKey)
			continue
		}
		if state.loading || state.segment != nil || len(state.refs) == 0 {
			continue
		}
		request, ok := state.loadRequest()
		if !ok {
			continue
		}
		loadCtx, loadCancel := context.WithCancel(context.Background())
		loadDone := onceLoadDone(m.trackPendingLoadAttemptLocked(state))
		delete(m.pendingLoadSegments, segmentKey)
		state.loading = true
		snapshot := *state.pendingSnapshot
		state.pendingSnapshot = nil
		state.loadCancel = loadCancel
		state.loadDone = loadDone
		submissions = append(submissions, segmentLoadSubmission{
			key:       segmentKey,
			segmentID: segmentKey.segmentID,
			ctx:       loadCtx,
			request:   request,
			snapshot:  snapshot,
			done:      chainLoadDone(loadDone, loadCancel),
		})
	}
	return submissions
}

func (m *ViewScopedPhysicalSegmentManager) trackPendingLoadAttemptLocked(state *physicalSegmentState) func() {
	keys := make([]qviews.QueryViewKey, 0, len(state.refs))
	for key := range state.refs {
		ref := m.views[key]
		if ref == nil {
			continue
		}
		ref.pendingLoads++
		keys = append(keys, key)
	}
	return func() {
		m.completeLoadAttempts(keys)
	}
}

func (m *ViewScopedPhysicalSegmentManager) completeLoadAttempts(keys []qviews.QueryViewKey) {
	callbacks := make([]func(), 0)
	m.mu.Lock()
	for _, key := range keys {
		ref := m.views[key]
		if ref == nil {
			ref = m.dropping[key]
		}
		if ref == nil || ref.pendingLoads == 0 {
			continue
		}
		ref.pendingLoads--
		if ref.pendingLoads == 0 && m.dropping[key] == ref {
			delete(m.dropping, key)
			callbacks = append(callbacks, ref.onDropped)
		}
	}
	m.mu.Unlock()
	for _, callback := range callbacks {
		m.submitCallback(callback)
	}
}

func (m *ViewScopedPhysicalSegmentManager) submitCallback(callback func()) {
	if callback == nil {
		return
	}
	m.nodeScheduler.Submit(schedulerTaskFunc(func(context.Context) error {
		callback()
		return nil
	}))
}

func (s *physicalSegmentState) loadRequest() (segmentLoadRequest, bool) {
	for key := range s.refs {
		request, ok := s.requests[key]
		if ok {
			return request, true
		}
	}
	return segmentLoadRequest{}, false
}

func (m *ViewScopedPhysicalSegmentManager) submitSegmentLoadSubmissions(submissions []segmentLoadSubmission) {
	for _, submission := range submissions {
		m.submitSegmentLoad(submission, nil)
	}
}

func (m *ViewScopedPhysicalSegmentManager) detachSubscriptionLocked(state *physicalSegmentState) []SegmentLoadInfoSubscription {
	if state == nil || state.subscription == nil {
		return nil
	}
	subscription := state.subscription
	state.subscription = nil
	return []SegmentLoadInfoSubscription{subscription}
}

func (m *ViewScopedPhysicalSegmentManager) subscribeSegments(requests []segmentLoadInfoSubscriptionRequest) {
	if m.stream == nil {
		return
	}
	for _, request := range requests {
		subscription := m.stream.Subscribe(SegmentLoadInfoSubscriptionOption{
			CollectionID: request.collectionID,
			SegmentID:    request.segmentID,
			Revision:     request.revision,
			Handler: physicalSegmentLoadInfoHandler{
				manager: m,
				key:     request.key,
				state:   request.state,
			},
		})
		if subscription == nil {
			continue
		}
		keep := false
		m.mu.Lock()
		if state := m.segments[request.key]; state == request.state && len(state.refs) > 0 && state.subscription == nil {
			state.subscription = subscription
			keep = true
		}
		m.mu.Unlock()
		if !keep {
			subscription.Close()
		}
	}
}

func (m *ViewScopedPhysicalSegmentManager) closeSubscriptions(subscriptions []SegmentLoadInfoSubscription) {
	for _, subscription := range subscriptions {
		subscription.Close()
	}
}

type physicalSegmentLoadInfoHandler struct {
	manager *ViewScopedPhysicalSegmentManager
	key     segmentRefKey
	state   *physicalSegmentState
}

func (h physicalSegmentLoadInfoHandler) Handle(snapshot SegmentLoadInfoSnapshot) error {
	h.manager.applyLoadInfoSnapshotForKey(context.Background(), h.key, snapshot, h.state)
	return nil
}

func (physicalSegmentLoadInfoHandler) Close() {}

func chainLoadDone(first func(), second func()) func() {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	return func() {
		defer first()
		second()
	}
}

func onceLoadDone(done func()) func() {
	var once sync.Once
	return func() {
		once.Do(done)
	}
}
