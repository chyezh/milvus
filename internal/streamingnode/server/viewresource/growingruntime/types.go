package growingruntime

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Descriptor describes the latest vchannel runtime that must be prepared after
// WAL observes AlterLoadConfig.
type Descriptor struct {
	WALView    walview.VChannelWALView
	LiveEvents <-chan walview.VChannelResourceEvent
	LiveDone   <-chan struct{}
	OnApplied  func()
	BM25       BM25Runtime
}

func (d Descriptor) CollectionID() int64 {
	return d.WALView.CollectionID
}

func (d Descriptor) VChannel() string {
	return d.WALView.VChannel
}

func (d Descriptor) DataVersion() qviews.DataVersion {
	return d.WALView.SegmentSnapshot.DataVersion
}

func (d Descriptor) Settings() *viewpb.QueryViewSettings {
	return SettingsFromAlterLoadConfig(d.WALView.LoadConfig.GetHeader())
}

func (d Descriptor) Schema() *schemapb.CollectionSchema {
	return d.WALView.Schema
}

func SettingsFromAlterLoadConfig(header *messagespb.AlterLoadConfigMessageHeader) *viewpb.QueryViewSettings {
	if header == nil {
		return &viewpb.QueryViewSettings{}
	}
	fields := make([]int64, 0, len(header.GetLoadFields()))
	for _, field := range header.GetLoadFields() {
		fields = append(fields, field.GetFieldId())
	}
	return &viewpb.QueryViewSettings{
		RequiredPartitions: append([]int64{}, header.GetPartitionIds()...),
		RequiredFields:     fields,
	}
}

// Builder converts WAL-side growing state into queryable csegment-backed
// resources for the requested latest DataVersion.
type Builder interface {
	Build(ctx context.Context, desc Descriptor) (*Runtime, error)
}

// Applier is the narrow boundary between WALView resource preparation and the
// concrete growing segment implementation.
type Applier interface {
	LoadPersistedSegment(context.Context, walview.VisibleSegment) error
	ApplySnapshotInsert(context.Context, walview.VisibleSegment, message.ImmutableMessage) error
	ApplyDeleteReplay(context.Context, *streamingpb.TransformLogEntry) error
	ApplyLiveMessage(context.Context, message.ImmutableMessage) error
	Close()
}

type segmentReleaser interface {
	ReleaseSegment(segmentID int64)
}

type segmentFlushMarker interface {
	markSegmentFlushed(segmentID int64)
}

type ApplierFactory func(context.Context, Descriptor) (Applier, error)

type BM25Runtime interface {
	ApplyLiveMessage(context.Context, message.ImmutableMessage) error
	ApplySegmentSealed(segmentID int64, sealedAt qviews.DataVersion)
}

// Runtime is the csegment-backed growing side prepared for one DataVersion.
type Runtime struct {
	SegmentIDs          []int64
	Segments            map[int64]segcore.CSegment
	DeleteReplayEntries []*streamingpb.TransformLogEntry
	LiveEvents          <-chan walview.VChannelResourceEvent

	applier                  Applier
	mu                       sync.RWMutex
	flushedSegments          map[int64]struct{}
	sealedAtDataVersions     map[int64]qviews.DataVersion
	truncateDataVersion      qviews.DataVersion
	hasTruncateDataVersion   bool
	closeOnce                sync.Once
	liveStopCh               chan struct{}
	liveDoneCh               chan struct{}
	appliedGrowingTimeTick   atomic.Uint64
	appliedTransformTimeTick atomic.Uint64
	bm25                     atomic.Value
}

func NewRuntime(applier Applier) *Runtime {
	return &Runtime{
		applier:              applier,
		flushedSegments:      make(map[int64]struct{}),
		sealedAtDataVersions: make(map[int64]qviews.DataVersion),
	}
}

func (r *Runtime) AppliedGrowingTimeTick() uint64 {
	if r == nil {
		return 0
	}
	return r.appliedGrowingTimeTick.Load()
}

func (r *Runtime) AppliedTransformTimeTick() uint64 {
	if r == nil {
		return 0
	}
	return r.appliedTransformTimeTick.Load()
}

func (r *Runtime) Segment(segmentID int64) (segcore.CSegment, bool) {
	if r == nil {
		return nil, false
	}
	if concrete, ok := r.applier.(*segcoreApplier); ok {
		return concrete.segment(segmentID)
	}
	segment, ok := r.Segments[segmentID]
	return segment, ok
}

func (r *Runtime) SegmentFlushed(segmentID int64) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.flushedSegments[segmentID]
	return ok
}

func (r *Runtime) registerSegment(segmentID int64) {
	if r == nil || segmentID == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.SegmentIDs {
		if existing == segmentID {
			return
		}
	}
	r.SegmentIDs = append(r.SegmentIDs, segmentID)
}

func (r *Runtime) markSegmentFlushed(segmentID int64) {
	if r == nil || segmentID == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.flushedSegments == nil {
		r.flushedSegments = make(map[int64]struct{})
	}
	r.flushedSegments[segmentID] = struct{}{}
}

func (r *Runtime) markSegmentSealed(segmentID int64, sealedAt qviews.DataVersion) {
	if r == nil || segmentID == 0 {
		return
	}
	release := false
	r.mu.Lock()
	if r.sealedAtDataVersions == nil {
		r.sealedAtDataVersions = make(map[int64]qviews.DataVersion)
	}
	if existing, ok := r.sealedAtDataVersions[segmentID]; ok && !existing.EQ(sealedAt) {
		r.mu.Unlock()
		panic("conflicting sealed data version for growing segment")
	}
	r.sealedAtDataVersions[segmentID] = sealedAt
	if r.hasTruncateDataVersion && r.truncateDataVersion.GTE(sealedAt) {
		r.removeSegmentMetadataLocked(segmentID)
		release = true
	}
	r.mu.Unlock()
	if release {
		r.releaseSegment(segmentID)
	}
}

func (r *Runtime) Truncate(minDataVersion qviews.DataVersion) {
	if r == nil || r.applier == nil {
		return
	}
	r.mu.Lock()
	if !r.hasTruncateDataVersion || minDataVersion.GT(r.truncateDataVersion) {
		r.truncateDataVersion = minDataVersion
		r.hasTruncateDataVersion = true
	}
	segmentsToRelease := make([]int64, 0)
	for segmentID, sealedAt := range r.sealedAtDataVersions {
		if r.truncateDataVersion.GTE(sealedAt) {
			segmentsToRelease = append(segmentsToRelease, segmentID)
			r.removeSegmentMetadataLocked(segmentID)
		}
	}
	r.mu.Unlock()
	for _, segmentID := range segmentsToRelease {
		r.releaseSegment(segmentID)
	}
}

func (r *Runtime) removeSegmentMetadataLocked(segmentID int64) {
	delete(r.sealedAtDataVersions, segmentID)
	delete(r.flushedSegments, segmentID)
	delete(r.Segments, segmentID)
	for i, id := range r.SegmentIDs {
		if id == segmentID {
			r.SegmentIDs = append(r.SegmentIDs[:i], r.SegmentIDs[i+1:]...)
			return
		}
	}
}

func (r *Runtime) releaseSegment(segmentID int64) {
	if releaser, ok := r.applier.(segmentReleaser); ok {
		releaser.ReleaseSegment(segmentID)
	}
}

func (r *Runtime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		r.mu.Lock()
		liveStopCh := r.liveStopCh
		liveDoneCh := r.liveDoneCh
		r.liveStopCh = nil
		r.liveDoneCh = nil
		r.mu.Unlock()
		if liveStopCh != nil {
			close(liveStopCh)
		}
		if liveDoneCh != nil {
			<-liveDoneCh
		}
		if r.applier != nil {
			r.applier.Close()
		}
	})
}

func (r *Runtime) SetBM25Runtime(bm25 BM25Runtime) {
	if r == nil || bm25 == nil {
		return
	}
	r.bm25.Store(bm25)
}

func (r *Runtime) bm25Runtime() BM25Runtime {
	if r == nil {
		return nil
	}
	bm25 := r.bm25.Load()
	if bm25 == nil {
		return nil
	}
	return bm25.(BM25Runtime)
}
