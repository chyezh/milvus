package viewresource

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// GrowingSegmentRuntimeBuilder converts WAL-side growing state into queryable
// csegment-backed resources for the requested latest DataVersion.
type GrowingSegmentRuntimeBuilder interface {
	Build(ctx context.Context, desc LoadResourceDescriptor) (*GrowingRuntime, error)
}

// GrowingRuntimeApplier is the narrow boundary between WALView resource
// preparation and the concrete growing segment implementation.
type GrowingRuntimeApplier interface {
	LoadPersistedSegment(context.Context, walview.VisibleSegment) error
	ApplySnapshotInsert(context.Context, walview.VisibleSegment, message.ImmutableMessage) error
	ApplyDeleteReplay(context.Context, *streamingpb.TransformLogEntry) error
	ApplyLiveMessage(context.Context, message.ImmutableMessage) error
	Close()
}

type growingSegmentReleaser interface {
	releaseSegment(segmentID int64)
}

type growingSegmentFlushMarker interface {
	markSegmentFlushed(segmentID int64)
}

type GrowingRuntimeApplierFactory func(context.Context, LoadResourceDescriptor) (GrowingRuntimeApplier, error)

// IDFOracleRuntimeBuilder prepares DataVersion-level BM25 IDF resources.
type IDFOracleRuntimeBuilder interface {
	BuildInitial(ctx context.Context, desc LoadResourceDescriptor) (*BM25Runtime, error)
}

// Manager prepares and owns StreamingNode query resources for one PChannel runtime.
type Manager interface {
	walview.LoadConfigListener
	snview.StreamingNodeResourceManager

	Close()
}

type runtimeKey struct {
	vchannel string
	version  qviews.DataVersion
}

type loadKey struct {
	collectionID int64
	vchannel     string
}

type runtimeState struct {
	collectionID int64
	runtime      *ViewRuntime
	loading      bool
	err          error
	cancel       context.CancelFunc
	task         BuildTask
	observer     *liveObserver
}

type referenceState struct {
	initRef       bool
	queryViewRefs map[qviews.QueryViewKey]*viewpb.QueryViewMeta
}

// DefaultManager is the concrete PChannel-local query resource manager.
type DefaultManager struct {
	mu        sync.Mutex
	growing   GrowingSegmentRuntimeBuilder
	bm25      IDFOracleRuntimeBuilder
	scheduler Scheduler

	runtimes map[runtimeKey]*runtimeState
	refs     map[loadKey]*referenceState
	refIndex map[qviews.QueryViewKey]loadKey
	refEpoch map[qviews.QueryViewKey]uint64
	notify   chan struct{}
	closed   bool
}

func NewManager(growing GrowingSegmentRuntimeBuilder, bm25 IDFOracleRuntimeBuilder) *DefaultManager {
	if growing == nil {
		growing = SnapshotGrowingSegmentRuntimeBuilder{}
	}
	if bm25 == nil {
		bm25 = NoopIDFOracleRuntimeBuilder{}
	}
	return &DefaultManager{
		growing:   growing,
		bm25:      bm25,
		scheduler: NewScheduler(4),
		runtimes:  make(map[runtimeKey]*runtimeState),
		refs:      make(map[loadKey]*referenceState),
		refIndex:  make(map[qviews.QueryViewKey]loadKey),
		refEpoch:  make(map[qviews.QueryViewKey]uint64),
		notify:    make(chan struct{}, 1),
	}
}

func (r *DefaultManager) OnAlterLoadConfig(view walview.VChannelWALView) walview.VChannelLiveObserver {
	observer := newLiveObserver()
	desc := LoadResourceDescriptor{
		WALView:    view,
		LiveEvents: observer.Events(),
		LiveDone:   observer.Done(),
		OnApplied:  r.notifyReady,
	}
	if !r.prepareLatestFromAlterLoadConfig(context.Background(), desc, observer) {
		observer.Close()
		return nil
	}
	return observer
}

func (r *DefaultManager) OnDropLoadConfig(event walview.DropLoadConfigEvent) {
	lk := loadKey{collectionID: event.CollectionID, vchannel: event.VChannel}
	r.mu.Lock()
	if refs := r.refs[lk]; refs != nil {
		refs.initRef = false
	}
	cancels, observers, runtimes := r.cleanupIfUnreferencedLocked(lk)
	r.mu.Unlock()

	for _, observer := range observers {
		observer.Close()
	}
	for _, runtime := range runtimes {
		runtime.Close()
	}
	for _, cancel := range cancels {
		cancel()
	}
	r.notifyReady()
}

func (r *DefaultManager) prepareLatestFromAlterLoadConfig(ctx context.Context, desc LoadResourceDescriptor, observer *liveObserver) bool {
	key := runtimeKey{vchannel: desc.VChannel(), version: desc.DataVersion()}
	lk := loadKey{collectionID: desc.CollectionID(), vchannel: desc.VChannel()}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false
	}
	for existingKey, existingState := range r.runtimes {
		if existingKey.vchannel == key.vchannel && existingState.collectionID == desc.CollectionID() && existingState.loading {
			r.mu.Unlock()
			return false
		}
	}
	state, ok := r.runtimes[key]
	if ok && (state.loading || state.runtime != nil) {
		r.mu.Unlock()
		return false
	}
	if !ok {
		state = &runtimeState{}
		r.runtimes[key] = state
	}
	refs := r.refs[lk]
	if refs == nil {
		refs = &referenceState{queryViewRefs: make(map[qviews.QueryViewKey]*viewpb.QueryViewMeta)}
		r.refs[lk] = refs
	}
	refs.initRef = len(refs.queryViewRefs) == 0
	task := newResourceBuildTask(ctx, BuildKey{
		CollectionID: desc.CollectionID(),
		VChannel:     desc.VChannel(),
		DataVersion:  desc.DataVersion(),
	}, func(ctx context.Context) (*ViewRuntime, error) {
		return r.buildRuntime(ctx, desc)
	})
	state.collectionID = desc.CollectionID()
	state.loading = true
	state.err = nil
	state.cancel = task.Cancel
	state.task = task
	state.observer = observer
	r.mu.Unlock()

	r.scheduler.Submit(task)
	go r.finishBuild(key, task)
	return true
}

func (r *DefaultManager) buildRuntime(ctx context.Context, desc LoadResourceDescriptor) (*ViewRuntime, error) {
	bm25, err := r.bm25.BuildInitial(ctx, desc)
	if err != nil {
		return nil, err
	}
	desc.BM25 = bm25
	growing, err := r.growing.Build(ctx, desc)
	if err != nil {
		bm25.Close()
		return nil, err
	}
	growing.setBM25Runtime(bm25)
	if bm25 != nil {
		bm25.DataVersion = desc.DataVersion()
		bm25.MarkCatchupDone()
	}
	return &ViewRuntime{
		CollectionID: desc.CollectionID(),
		VChannel:     desc.VChannel(),
		DataVersion:  desc.DataVersion(),
		Schema:       desc.Schema(),
		Growing:      growing,
		BM25:         bm25,
	}, nil
}

func (r *DefaultManager) finishBuild(key runtimeKey, task BuildTask) {
	runtime, err := task.Result()
	r.mu.Lock()
	state, ok := r.runtimes[key]
	if !ok {
		r.mu.Unlock()
		if runtime != nil {
			runtime.Close()
		}
		r.notifyReady()
		return
	}
	if state.task != task {
		r.mu.Unlock()
		if runtime != nil {
			runtime.Close()
		}
		r.notifyReady()
		return
	}
	state.loading = false
	state.cancel = nil
	state.task = nil
	if err != nil {
		if runtime != nil {
			runtime.Close()
		}
		state.err = err
		if state.observer != nil {
			state.observer.Close()
			state.observer = nil
		}
	} else {
		state.runtime = runtime
	}
	r.mu.Unlock()
	r.notifyReady()
}

func (r *DefaultManager) GetViewRuntime(desc ViewResourceDescriptor) (*ViewRuntime, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key, state, ok := r.findRuntimeForRequestLocked(desc.CollectionID, desc.VChannel, desc.Version.DataVersion)
	_ = key
	if !ok {
		return nil, false, nil
	}
	if key.version.GT(desc.Version.DataVersion) {
		return nil, false, errors.Errorf(
			"view runtime data version %s is behind loaded data version %s",
			desc.Version.DataVersion.String(),
			key.version.String(),
		)
	}
	if state.loading {
		return nil, false, nil
	}
	if state.err != nil {
		return nil, false, state.err
	}
	if state.runtime == nil {
		return nil, false, nil
	}
	if desc.DeleteApplyStartAfterTimeTick > state.runtime.Growing.AppliedTransformTimeTick() {
		return nil, false, nil
	}
	if state.runtime.CollectionID != desc.CollectionID {
		return nil, false, errors.Errorf(
			"view runtime collection mismatch: runtime collection %d, requested collection %d",
			state.runtime.CollectionID,
			desc.CollectionID,
		)
	}
	return state.runtime, true, nil
}

func (r *DefaultManager) findRuntimeForRequestLocked(collectionID int64, vchannel string, requested qviews.DataVersion) (runtimeKey, *runtimeState, bool) {
	var selectedKey runtimeKey
	var selected *runtimeState
	ok := false
	hasServing := false
	for key, state := range r.runtimes {
		if key.vchannel != vchannel || state.collectionID != collectionID {
			continue
		}
		if !key.version.GT(requested) {
			if !hasServing || key.version.GT(selectedKey.version) {
				selectedKey = key
				selected = state
				hasServing = true
				ok = true
			}
			continue
		}
		if !hasServing && (!ok || selectedKey.version.GT(key.version)) {
			selectedKey = key
			selected = state
			ok = true
		}
	}
	return selectedKey, selected, ok
}

func (r *DefaultManager) Acquire(req snview.AcquireResource) {
	epoch, err := r.registerQueryViewRef(req)
	if err != nil {
		go func() {
			if req.OnUnrecoverable != nil {
				req.OnUnrecoverable()
			}
		}()
		return
	}
	go r.waitRuntimeLoop(req.Key, epoch, req.Meta, req.OnReady, req.OnUnrecoverable)
}

func (r *DefaultManager) Release(req snview.ReleaseResource) {
	var truncates []*GrowingRuntime
	var truncateTo qviews.DataVersion
	var hasTruncate bool
	var cancels []context.CancelFunc
	var observers []*liveObserver
	var runtimes []*ViewRuntime

	r.mu.Lock()
	lk, ok := r.refIndex[req.Key]
	if !ok {
		r.mu.Unlock()
		go func() {
			if req.OnDropped != nil {
				req.OnDropped()
			}
		}()
		return
	}
	delete(r.refIndex, req.Key)
	if refs := r.refs[lk]; refs != nil {
		delete(refs.queryViewRefs, req.Key)
		truncateTo, hasTruncate = refs.minQueryViewDataVersion()
	}
	if hasTruncate {
		truncates = r.growingRuntimesLocked(lk)
	}
	cancels, observers, runtimes = r.cleanupIfUnreferencedLocked(lk)
	r.mu.Unlock()

	for _, runtime := range truncates {
		runtime.Truncate(truncateTo)
	}
	for _, observer := range observers {
		observer.Close()
	}
	for _, runtime := range runtimes {
		runtime.Close()
	}
	for _, cancel := range cancels {
		cancel()
	}
	r.notifyReady()
	go func() {
		if req.OnDropped != nil {
			req.OnDropped()
		}
	}()
}

func (r *DefaultManager) Close() {
	var cancels []context.CancelFunc
	var observers []*liveObserver
	var runtimes []*ViewRuntime

	r.mu.Lock()
	remainingQueryViewRefs := r.queryViewRefCountLocked()
	r.closed = true
	for key, state := range r.runtimes {
		if state.cancel != nil {
			cancels = append(cancels, state.cancel)
		}
		if state.observer != nil {
			observers = append(observers, state.observer)
		}
		if state.runtime != nil {
			runtimes = append(runtimes, state.runtime)
		}
		delete(r.runtimes, key)
	}
	r.refs = make(map[loadKey]*referenceState)
	r.refIndex = make(map[qviews.QueryViewKey]loadKey)
	r.refEpoch = make(map[qviews.QueryViewKey]uint64)
	r.mu.Unlock()

	for _, observer := range observers {
		observer.Close()
	}
	for _, runtime := range runtimes {
		runtime.Close()
	}
	for _, cancel := range cancels {
		cancel()
	}
	if r.scheduler != nil {
		r.scheduler.Close()
	}
	r.notifyReady()
	if remainingQueryViewRefs > 0 {
		panic(errors.Errorf("query resource manager closed with %d query view references", remainingQueryViewRefs))
	}
}

func (r *DefaultManager) registerQueryViewRef(req snview.AcquireResource) (uint64, error) {
	if req.Meta == nil || req.Meta.GetVersion() == nil || req.Meta.GetVersion().GetDataVersion() == nil {
		return 0, errors.New("query view meta version is nil")
	}
	lk := loadKey{collectionID: req.Meta.GetCollectionId(), vchannel: req.Meta.GetVchannel()}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, errors.New("query resource manager is closed")
	}
	refs := r.refs[lk]
	if refs == nil {
		return 0, errors.Errorf("query resource for vchannel %s is not initialized", req.Meta.GetVchannel())
	}
	if refs.queryViewRefs == nil {
		refs.queryViewRefs = make(map[qviews.QueryViewKey]*viewpb.QueryViewMeta)
	}
	if existingLK, ok := r.refIndex[req.Key]; ok {
		if existingLK != lk {
			return 0, errors.Errorf("query view %s already references a different resource", req.Key.String())
		}
		if _, ok := refs.queryViewRefs[req.Key]; ok {
			return r.refEpoch[req.Key], nil
		}
	}
	refs.queryViewRefs[req.Key] = req.Meta
	r.refIndex[req.Key] = lk
	r.refEpoch[req.Key]++
	epoch := r.refEpoch[req.Key]
	refs.initRef = false
	return epoch, nil
}

func (r *DefaultManager) waitRuntimeLoop(
	key qviews.QueryViewKey,
	epoch uint64,
	meta *viewpb.QueryViewMeta,
	onReady func(),
	onUnrecoverable func(),
) {
	desc := ViewResourceDescriptor{
		CollectionID:                  meta.GetCollectionId(),
		ReplicaID:                     meta.GetReplicaId(),
		VChannel:                      meta.GetVchannel(),
		Version:                       qviews.FromProtoQueryViewVersion(meta.GetVersion()),
		Settings:                      meta.GetSettings(),
		DeleteApplyStartAfterTimeTick: meta.GetDeleteApplyStartAfterTimetick(),
	}
	for {
		if !r.hasQueryViewRef(key, epoch) {
			if onUnrecoverable != nil {
				onUnrecoverable()
			}
			return
		}
		runtime, ready, err := r.GetViewRuntime(desc)
		if err != nil {
			if onUnrecoverable != nil {
				onUnrecoverable()
			}
			return
		}
		if ready {
			if !r.hasQueryViewRef(key, epoch) {
				if onUnrecoverable != nil {
					onUnrecoverable()
				}
				return
			}
			if runtime != nil && runtime.BM25 != nil {
				select {
				case <-runtime.BM25.CatchupDone():
				case <-r.NotifyReady():
					continue
				}
				runtime.BM25.MaybeAdvance(desc.Version.DataVersion)
			}
			if !r.hasQueryViewRef(key, epoch) {
				if onUnrecoverable != nil {
					onUnrecoverable()
				}
				return
			}
			if onReady != nil {
				onReady()
			}
			return
		}
		select {
		case <-r.NotifyReady():
		}
	}
}

func (r *DefaultManager) hasQueryViewRef(key qviews.QueryViewKey, epoch uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refEpoch[key] != epoch {
		return false
	}
	lk, ok := r.refIndex[key]
	if !ok {
		return false
	}
	refs := r.refs[lk]
	if refs == nil {
		return false
	}
	_, ok = refs.queryViewRefs[key]
	return ok
}

func (r *DefaultManager) queryViewRefCountLocked() int {
	count := 0
	for _, refs := range r.refs {
		count += len(refs.queryViewRefs)
	}
	return count
}

func (s *referenceState) minQueryViewDataVersion() (qviews.DataVersion, bool) {
	var min qviews.DataVersion
	ok := false
	for key := range s.queryViewRefs {
		version := key.QueryViewVersion.DataVersion
		if !ok || min.GT(version) {
			min = version
			ok = true
		}
	}
	return min, ok
}

func (r *DefaultManager) growingRuntimesLocked(lk loadKey) []*GrowingRuntime {
	runtimes := make([]*GrowingRuntime, 0)
	for key, state := range r.runtimes {
		if key.vchannel != lk.vchannel || (lk.collectionID != 0 && state.collectionID != lk.collectionID) {
			continue
		}
		if state.runtime != nil && state.runtime.Growing != nil {
			runtimes = append(runtimes, state.runtime.Growing)
		}
	}
	return runtimes
}

func (r *DefaultManager) cleanupIfUnreferencedLocked(lk loadKey) ([]context.CancelFunc, []*liveObserver, []*ViewRuntime) {
	refs := r.refs[lk]
	if refs != nil && (refs.initRef || len(refs.queryViewRefs) > 0) {
		return nil, nil, nil
	}
	if refs == nil && lk.collectionID != 0 {
		return nil, nil, nil
	}
	delete(r.refs, lk)

	var cancels []context.CancelFunc
	var observers []*liveObserver
	var runtimes []*ViewRuntime
	for key, state := range r.runtimes {
		if key.vchannel != lk.vchannel || (lk.collectionID != 0 && state.collectionID != lk.collectionID) {
			continue
		}
		if state.cancel != nil {
			cancels = append(cancels, state.cancel)
		}
		if state.observer != nil {
			observers = append(observers, state.observer)
		}
		if state.runtime != nil {
			runtimes = append(runtimes, state.runtime)
		}
		delete(r.runtimes, key)
	}
	return cancels, observers, runtimes
}

func (r *DefaultManager) NotifyReady() <-chan struct{} {
	return r.notify
}

func (r *DefaultManager) notifyReady() {
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

type NoopGrowingSegmentRuntimeBuilder struct{}

func (NoopGrowingSegmentRuntimeBuilder) Build(context.Context, LoadResourceDescriptor) (*GrowingRuntime, error) {
	return &GrowingRuntime{}, nil
}

type SnapshotGrowingSegmentRuntimeBuilder struct {
	NewApplier GrowingRuntimeApplierFactory
}

func (p SnapshotGrowingSegmentRuntimeBuilder) Build(ctx context.Context, desc LoadResourceDescriptor) (*GrowingRuntime, error) {
	if err := validateWALViewSnapshot(desc); err != nil {
		return nil, err
	}
	applierFactory := p.NewApplier
	if applierFactory == nil {
		applierFactory = newSegcoreGrowingRuntimeApplier
	}
	applier, err := applierFactory(ctx, desc)
	if err != nil {
		return nil, err
	}
	applierPrepared := false
	defer func() {
		if !applierPrepared {
			applier.Close()
		}
	}()
	segments := desc.WALView.SegmentSnapshot.Segments
	segmentIDs := make([]int64, 0, len(segments))
	sealedAtDataVersions := make(map[int64]qviews.DataVersion)
	flushedSegments := make(map[int64]struct{})
	for _, segment := range segments {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		segmentIDs = append(segmentIDs, segment.SegmentID)
		if segment.Data.PersistedStorage != nil {
			if err := applier.LoadPersistedSegment(ctx, segment); err != nil {
				return nil, err
			}
		}
		for _, msg := range segment.Data.InsertMessages {
			if err := applier.ApplySnapshotInsert(ctx, segment, msg); err != nil {
				return nil, err
			}
		}
		if segment.SealedAtDataVersion != nil {
			sealedAtDataVersions[segment.SegmentID] = qviews.FromProtoDataVersion(segment.SealedAtDataVersion)
			flushedSegments[segment.SegmentID] = struct{}{}
			if marker, ok := applier.(growingSegmentFlushMarker); ok {
				marker.markSegmentFlushed(segment.SegmentID)
			}
		}
	}
	deleteEntries, err := drainDeleteReplay(ctx, desc.WALView.DeleteReplay)
	if err != nil {
		return nil, err
	}
	for _, entry := range deleteEntries {
		if err := applier.ApplyDeleteReplay(ctx, entry); err != nil {
			return nil, err
		}
	}
	runtime := &GrowingRuntime{
		SegmentIDs:           segmentIDs,
		Segments:             segmentsFromApplier(applier),
		DeleteReplayEntries:  deleteEntries,
		LiveEvents:           desc.LiveEvents,
		applier:              applier,
		flushedSegments:      flushedSegments,
		sealedAtDataVersions: sealedAtDataVersions,
	}
	runtime.setBM25Runtime(desc.BM25)
	runtime.appliedGrowingTimeTick.Store(desc.WALView.BaseGrowingTimeTick)
	runtime.appliedTransformTimeTick.Store(desc.WALView.BaseTransformTimeTick)
	runtime.startLiveApply(ctx, desc.LiveDone, desc.OnApplied)
	applierPrepared = true
	return runtime, nil
}

func validateWALViewSnapshot(desc LoadResourceDescriptor) error {
	snapshot := desc.WALView.SegmentSnapshot
	if snapshot.CollectionID != 0 && snapshot.CollectionID != desc.CollectionID() {
		return errors.Errorf(
			"wal view snapshot mismatch: view collection %d, snapshot collection %d",
			desc.CollectionID(),
			snapshot.CollectionID,
		)
	}
	if snapshot.VChannel != "" && snapshot.VChannel != desc.VChannel() {
		return errors.Errorf(
			"wal view snapshot mismatch: view vchannel %s, snapshot vchannel %s",
			desc.VChannel(),
			snapshot.VChannel,
		)
	}
	return nil
}

func drainDeleteReplay(ctx context.Context, scanner transformlogapi.Scanner) ([]*streamingpb.TransformLogEntry, error) {
	if scanner == nil {
		return nil, nil
	}
	entries := make([]*streamingpb.TransformLogEntry, 0)
	for {
		select {
		case event, ok := <-scanner.Chan():
			if !ok {
				return entries, scanner.Close()
			}
			if event.Entry != nil {
				entries = append(entries, event.Entry)
			}
			if event.CaughtUp != nil {
				return entries, scanner.Close()
			}
		case <-scanner.Done():
			return entries, scanner.Close()
		case <-ctx.Done():
			_ = scanner.Close()
			return nil, ctx.Err()
		}
	}
}

func (r *GrowingRuntime) startLiveApply(ctx context.Context, done <-chan struct{}, onApplied func()) {
	if r == nil || r.LiveEvents == nil {
		return
	}
	if done == nil {
		done = neverDone()
	}
	r.mu.Lock()
	if r.liveStopCh == nil {
		r.liveStopCh = make(chan struct{})
	}
	if r.liveDoneCh == nil {
		r.liveDoneCh = make(chan struct{})
	}
	stopCh := r.liveStopCh
	doneCh := r.liveDoneCh
	r.mu.Unlock()
	go func() {
		defer close(doneCh)
		for {
			select {
			case event, ok := <-r.LiveEvents:
				if !ok {
					return
				}
				if r.applyLiveEvent(ctx, event) && onApplied != nil {
					onApplied()
				}
			case <-done:
				return
			case <-stopCh:
				return
			}
		}
	}()
}

func (r *GrowingRuntime) applyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent) bool {
	if event.Message != nil {
		return r.applyLiveMessage(ctx, event.Message)
	}
	if event.SegmentSealed != nil {
		r.markSegmentSealed(event.SegmentSealed.SegmentID, event.SegmentSealed.SealedAtDataVersion)
		if bm25 := r.bm25.Load(); bm25 != nil {
			bm25.ApplySegmentSealed(event.SegmentSealed.SegmentID, event.SegmentSealed.SealedAtDataVersion)
		}
		return true
	}
	return false
}

func (r *GrowingRuntime) applyLiveMessage(ctx context.Context, msg message.ImmutableMessage) bool {
	if r == nil || msg == nil {
		return false
	}
	if r.applier != nil {
		if err := r.applier.ApplyLiveMessage(ctx, msg); err != nil {
			panic(errors.Wrap(err, "failed to apply live message to growing runtime"))
		}
	}
	if bm25 := r.bm25.Load(); bm25 != nil {
		if err := bm25.ApplyLiveMessage(ctx, msg); err != nil {
			panic(errors.Wrap(err, "failed to apply live message to BM25 runtime"))
		}
	}
	timeTick := msg.TimeTick()
	advanced := advanceTimeTick(&r.appliedGrowingTimeTick, timeTick)
	if messageAdvancesTransformFrontier(msg) {
		advanced = advanceTimeTick(&r.appliedTransformTimeTick, timeTick) || advanced
	}
	switch msg.MessageType() {
	case message.MessageTypeCreateSegment:
		r.registerSegment(message.MustAsImmutableCreateSegmentMessageV2(msg).Header().GetSegmentId())
	case message.MessageTypeFlush:
		r.markSegmentFlushed(message.MustAsImmutableFlushMessageV2(msg).Header().GetSegmentId())
	}
	return advanced
}

func messageAdvancesTransformFrontier(msg message.ImmutableMessage) bool {
	if msg == nil {
		return false
	}
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		return true
	case message.MessageTypeTxn:
		txn := message.AsImmutableTxnMessage(msg)
		if txn == nil {
			return false
		}
		containsDelete := false
		_ = txn.RangeOver(func(inner message.ImmutableMessage) error {
			if inner.MessageType() == message.MessageTypeDelete {
				containsDelete = true
			}
			return nil
		})
		return containsDelete
	default:
		return false
	}
}

func advanceTimeTick(value interface {
	Load() uint64
	CompareAndSwap(old uint64, new uint64) bool
}, next uint64) bool {
	for {
		current := value.Load()
		if next <= current {
			return false
		}
		if value.CompareAndSwap(current, next) {
			return true
		}
	}
}

func neverDone() <-chan struct{} {
	return make(chan struct{})
}

type noopGrowingRuntimeApplier struct{}

func noopGrowingRuntimeApplierFactory(context.Context, LoadResourceDescriptor) (GrowingRuntimeApplier, error) {
	return noopGrowingRuntimeApplier{}, nil
}

func segmentsFromApplier(applier GrowingRuntimeApplier) map[int64]segcore.CSegment {
	if concrete, ok := applier.(*segcoreGrowingRuntimeApplier); ok {
		return concrete.snapshotSegments()
	}
	return nil
}

func (noopGrowingRuntimeApplier) LoadPersistedSegment(context.Context, walview.VisibleSegment) error {
	return nil
}

func (noopGrowingRuntimeApplier) ApplySnapshotInsert(context.Context, walview.VisibleSegment, message.ImmutableMessage) error {
	return nil
}

func (noopGrowingRuntimeApplier) ApplyDeleteReplay(context.Context, *streamingpb.TransformLogEntry) error {
	return nil
}

func (noopGrowingRuntimeApplier) ApplyLiveMessage(context.Context, message.ImmutableMessage) error {
	return nil
}

func (noopGrowingRuntimeApplier) Close() {}

type NoopIDFOracleRuntimeBuilder struct{}

func (NoopIDFOracleRuntimeBuilder) BuildInitial(context.Context, LoadResourceDescriptor) (*BM25Runtime, error) {
	runtime := &BM25Runtime{}
	runtime.MarkCatchupDone()
	return runtime, nil
}

const defaultLiveObserverBufferSize = 1024

type liveObserver struct {
	closeOnce sync.Once
	ch        chan walview.VChannelResourceEvent
	closed    chan struct{}
}

func newLiveObserver() *liveObserver {
	return &liveObserver{
		ch:     make(chan walview.VChannelResourceEvent, defaultLiveObserverBufferSize),
		closed: make(chan struct{}),
	}
}

func (o *liveObserver) ObserveEvent(ctx context.Context, event walview.VChannelResourceEvent) bool {
	select {
	case <-o.closed:
		return false
	default:
	}
	select {
	case o.ch <- event:
		return true
	case <-o.closed:
		return false
	case <-ctx.Done():
		return false
	}
}

func (o *liveObserver) Close() {
	o.closeOnce.Do(func() {
		close(o.closed)
	})
}

func (o *liveObserver) Events() <-chan walview.VChannelResourceEvent {
	return o.ch
}

func (o *liveObserver) Done() <-chan struct{} {
	return o.closed
}
