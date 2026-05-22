package viewresource

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// GrowingSegmentPreparer converts WAL-side growing state into queryable
// csegment-backed resources for the requested latest DataVersion.
type GrowingSegmentPreparer interface {
	PrepareLatest(ctx context.Context, desc LoadResourceDescriptor) (*GrowingRuntime, error)
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

type GrowingRuntimeApplierFactory func(context.Context, LoadResourceDescriptor) (GrowingRuntimeApplier, error)

// BM25Provider prepares DataVersion-level BM25 IDF resources.
type BM25Provider interface {
	PrepareLatestFromAlterLoadConfig(ctx context.Context, desc LoadResourceDescriptor) (*BM25Runtime, error)
}

// Registry prepares and evicts vchannel-level view resources.
type Registry interface {
	walview.LoadConfigListener

	GetViewRuntime(desc ViewResourceDescriptor) (*ViewRuntime, bool, error)
	EvictBefore(collectionID int64, vchannel string, min qviews.DataVersion)
	ReleaseLoad(collectionID int64, vchannel string)
	NotifyReady() <-chan struct{}
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
	observer     *liveObserver
}

// DefaultRegistry is the concrete vchannel-level resource registry.
type DefaultRegistry struct {
	mu      sync.Mutex
	growing GrowingSegmentPreparer
	bm25    BM25Provider

	runtimes   map[runtimeKey]*runtimeState
	watermarks map[loadKey]qviews.DataVersion
	dropped    map[loadKey]struct{}
	notify     chan struct{}
}

func NewRegistry(growing GrowingSegmentPreparer, bm25 BM25Provider) *DefaultRegistry {
	if growing == nil {
		growing = SnapshotGrowingSegmentPreparer{}
	}
	if bm25 == nil {
		bm25 = NoopBM25Provider{}
	}
	return &DefaultRegistry{
		growing:    growing,
		bm25:       bm25,
		runtimes:   make(map[runtimeKey]*runtimeState),
		watermarks: make(map[loadKey]qviews.DataVersion),
		dropped:    make(map[loadKey]struct{}),
		notify:     make(chan struct{}, 1),
	}
}

func (r *DefaultRegistry) OnAlterLoadConfig(view walview.VChannelWALView) walview.VChannelLiveObserver {
	observer := newLiveObserver()
	desc := LoadResourceDescriptor{
		WALView:      view,
		LiveMessages: observer.Messages(),
		LiveDone:     observer.Done(),
		OnApplied:    r.notifyReady,
	}
	if !r.prepareLatestFromAlterLoadConfig(context.Background(), desc, observer) {
		observer.Close()
		return nil
	}
	return observer
}

func (r *DefaultRegistry) OnDropLoadConfig(event walview.DropLoadConfigEvent) {
	lk := loadKey{collectionID: event.CollectionID, vchannel: event.VChannel}
	var cancels []context.CancelFunc
	var observers []*liveObserver

	r.mu.Lock()
	r.dropped[lk] = struct{}{}
	for key, state := range r.runtimes {
		if key.vchannel != event.VChannel || state.collectionID != event.CollectionID {
			continue
		}
		if state.loading && state.cancel != nil {
			cancels = append(cancels, state.cancel)
		}
		if state.observer != nil {
			observers = append(observers, state.observer)
			state.observer = nil
		}
		if state.loading {
			delete(r.runtimes, key)
		}
	}
	r.mu.Unlock()

	for _, observer := range observers {
		observer.Close()
	}
	for _, cancel := range cancels {
		cancel()
	}
	r.notifyReady()
}

func (r *DefaultRegistry) prepareLatestFromAlterLoadConfig(ctx context.Context, desc LoadResourceDescriptor, observer *liveObserver) bool {
	key := runtimeKey{vchannel: desc.VChannel(), version: desc.DataVersion()}
	lk := loadKey{collectionID: desc.CollectionID(), vchannel: desc.VChannel()}

	r.mu.Lock()
	delete(r.dropped, lk)
	if watermark, ok := r.watermarks[lk]; ok &&
		watermark.GT(desc.DataVersion()) {
		r.mu.Unlock()
		return false
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
	loadCtx, cancel := context.WithCancel(ctx)
	state.collectionID = desc.CollectionID()
	state.loading = true
	state.err = nil
	state.cancel = cancel
	state.observer = observer
	r.mu.Unlock()

	go r.prepareLatest(loadCtx, key, desc)
	return true
}

func (r *DefaultRegistry) prepareLatest(ctx context.Context, key runtimeKey, desc LoadResourceDescriptor) {
	growing, growingErr := r.growing.PrepareLatest(ctx, desc)
	bm25, bm25Err := r.bm25.PrepareLatestFromAlterLoadConfig(ctx, desc)
	err := errors.CombineErrors(growingErr, bm25Err)

	r.mu.Lock()
	state, ok := r.runtimes[key]
	if !ok {
		r.mu.Unlock()
		growing.Close()
		r.notifyReady()
		return
	}
	state.loading = false
	state.cancel = nil
	if err != nil {
		growing.Close()
		state.err = err
		if state.observer != nil {
			state.observer.Close()
			state.observer = nil
		}
	} else {
		growing.setBM25Runtime(bm25)
		state.runtime = &ViewRuntime{
			CollectionID: desc.CollectionID(),
			VChannel:     desc.VChannel(),
			DataVersion:  desc.DataVersion(),
			Schema:       desc.Schema(),
			Growing:      growing,
			BM25:         bm25,
		}
	}
	r.mu.Unlock()
	r.notifyReady()
}

func (r *DefaultRegistry) GetViewRuntime(desc ViewResourceDescriptor) (*ViewRuntime, bool, error) {
	key := runtimeKey{vchannel: desc.VChannel, version: desc.Version.DataVersion}

	r.mu.Lock()
	defer r.mu.Unlock()

	if watermark, ok := r.watermarks[loadKey{collectionID: desc.CollectionID, vchannel: desc.VChannel}]; ok &&
		watermark.GT(desc.Version.DataVersion) {
		return nil, false, errors.Errorf(
			"view runtime data version %s is lower than vchannel watermark %s",
			desc.Version.DataVersion.String(),
			watermark.String(),
		)
	}
	if _, dropped := r.dropped[loadKey{collectionID: desc.CollectionID, vchannel: desc.VChannel}]; dropped {
		return nil, false, errors.Errorf(
			"view runtime data version %s is unavailable because vchannel load config was dropped",
			desc.Version.DataVersion.String(),
		)
	}

	state, ok := r.runtimes[key]
	if !ok {
		if loadedVersion, ok := r.forwardLoadedDataVersionLocked(desc.CollectionID, desc.VChannel, desc.Version.DataVersion); ok {
			return nil, false, errors.Errorf(
				"view runtime data version %s is behind loaded data version %s",
				desc.Version.DataVersion.String(),
				loadedVersion.String(),
			)
		}
		return nil, false, nil
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

func (r *DefaultRegistry) forwardLoadedDataVersionLocked(collectionID int64, vchannel string, requested qviews.DataVersion) (qviews.DataVersion, bool) {
	var forward qviews.DataVersion
	ok := false
	for key, state := range r.runtimes {
		if key.vchannel != vchannel || state.collectionID != collectionID || !key.version.GT(requested) {
			continue
		}
		if !ok || forward.GT(key.version) {
			forward = key.version
			ok = true
		}
	}
	return forward, ok
}

func (r *DefaultRegistry) EvictBefore(collectionID int64, vchannel string, min qviews.DataVersion) {
	var observers []*liveObserver
	var runtimes []*ViewRuntime
	r.mu.Lock()

	lk := loadKey{collectionID: collectionID, vchannel: vchannel}
	watermarkChanged := false
	if current, ok := r.watermarks[lk]; !ok || min.GT(current) {
		r.watermarks[lk] = min
		watermarkChanged = true
	}
	for key, state := range r.runtimes {
		if key.vchannel != vchannel || state.collectionID != collectionID {
			continue
		}
		if min.GT(key.version) {
			if state.observer != nil {
				observers = append(observers, state.observer)
			}
			if state.runtime != nil {
				runtimes = append(runtimes, state.runtime)
			}
			delete(r.runtimes, key)
		}
	}
	r.mu.Unlock()
	for _, observer := range observers {
		observer.Close()
	}
	for _, runtime := range runtimes {
		runtime.Close()
	}
	if watermarkChanged {
		r.notifyReady()
	}
}

func (r *DefaultRegistry) ReleaseLoad(collectionID int64, vchannel string) {
	var cancels []context.CancelFunc
	var observers []*liveObserver
	var runtimes []*ViewRuntime
	r.mu.Lock()

	for key, state := range r.runtimes {
		if key.vchannel == vchannel && state.collectionID == collectionID {
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
	}
	delete(r.watermarks, loadKey{collectionID: collectionID, vchannel: vchannel})
	delete(r.dropped, loadKey{collectionID: collectionID, vchannel: vchannel})
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
}

func (r *DefaultRegistry) NotifyReady() <-chan struct{} {
	return r.notify
}

func (r *DefaultRegistry) notifyReady() {
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

type NoopGrowingSegmentPreparer struct{}

func (NoopGrowingSegmentPreparer) PrepareLatest(context.Context, LoadResourceDescriptor) (*GrowingRuntime, error) {
	return &GrowingRuntime{}, nil
}

type SnapshotGrowingSegmentPreparer struct {
	NewApplier GrowingRuntimeApplierFactory
}

func (p SnapshotGrowingSegmentPreparer) PrepareLatest(ctx context.Context, desc LoadResourceDescriptor) (*GrowingRuntime, error) {
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
		SegmentIDs:          segmentIDs,
		Segments:            segmentsFromApplier(applier),
		DeleteReplayEntries: deleteEntries,
		LiveMessages:        desc.LiveMessages,
		applier:             applier,
	}
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
	if r == nil || r.LiveMessages == nil {
		return
	}
	if done == nil {
		done = neverDone()
	}
	go func() {
		for {
			select {
			case msg := <-r.LiveMessages:
				if r.applyLiveMessage(ctx, msg) && onApplied != nil {
					onApplied()
				}
			case <-done:
				return
			}
		}
	}()
}

func (r *GrowingRuntime) applyLiveMessage(ctx context.Context, msg message.ImmutableMessage) bool {
	if r == nil || msg == nil {
		return false
	}
	if r.applier != nil {
		if err := r.applier.ApplyLiveMessage(ctx, msg); err != nil {
			return false
		}
	}
	if bm25 := r.bm25.Load(); bm25 != nil {
		if err := bm25.ApplyLiveMessage(ctx, msg); err != nil {
			return false
		}
	}
	timeTick := msg.TimeTick()
	advanced := advanceTimeTick(&r.appliedGrowingTimeTick, timeTick)
	switch msg.MessageType() {
	case message.MessageTypeDelete, message.MessageTypeTxn:
		advanced = advanceTimeTick(&r.appliedTransformTimeTick, timeTick) || advanced
	case message.MessageTypeFlush:
		r.markSegmentFlushed(message.MustAsImmutableFlushMessageV2(msg).Header().GetSegmentId())
	}
	return advanced
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

type NoopBM25Provider struct{}

func (NoopBM25Provider) PrepareLatestFromAlterLoadConfig(context.Context, LoadResourceDescriptor) (*BM25Runtime, error) {
	return &BM25Runtime{}, nil
}

const defaultLiveObserverBufferSize = 1024

type liveObserver struct {
	closeOnce sync.Once
	ch        chan message.ImmutableMessage
	closed    chan struct{}
}

func newLiveObserver() *liveObserver {
	return &liveObserver{
		ch:     make(chan message.ImmutableMessage, defaultLiveObserverBufferSize),
		closed: make(chan struct{}),
	}
}

func (o *liveObserver) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) bool {
	select {
	case <-o.closed:
		return false
	default:
	}
	select {
	case o.ch <- msg:
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

func (o *liveObserver) Messages() <-chan message.ImmutableMessage {
	return o.ch
}

func (o *liveObserver) Done() <-chan struct{} {
	return o.closed
}
