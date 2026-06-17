package viewresource

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// LoadResourceDescriptor describes the latest vchannel runtime that must be
// prepared after WAL observes AlterLoadConfig.
type LoadResourceDescriptor struct {
	WALView    walview.VChannelWALView
	LiveEvents <-chan walview.VChannelResourceEvent
	LiveDone   <-chan struct{}
	OnApplied  func()
	BM25       *BM25Runtime
}

func (d LoadResourceDescriptor) CollectionID() int64 {
	return d.WALView.CollectionID
}

func (d LoadResourceDescriptor) VChannel() string {
	return d.WALView.VChannel
}

func (d LoadResourceDescriptor) DataVersion() qviews.DataVersion {
	return d.WALView.SegmentSnapshot.DataVersion
}

func (d LoadResourceDescriptor) Settings() *viewpb.QueryViewSettings {
	return SettingsFromAlterLoadConfig(d.WALView.LoadConfig.GetHeader())
}

func (d LoadResourceDescriptor) Schema() *schemapb.CollectionSchema {
	return d.WALView.Schema
}

// ViewResourceDescriptor describes a QueryView runtime readiness request.
type ViewResourceDescriptor struct {
	CollectionID                  int64
	ReplicaID                     int64
	VChannel                      string
	Version                       qviews.QueryViewVersion
	Settings                      *viewpb.QueryViewSettings
	DeleteApplyStartAfterTimeTick uint64
}

// ViewRuntime is the SN-side prepared runtime for one DataVersion.
type ViewRuntime struct {
	CollectionID int64
	VChannel     string
	DataVersion  qviews.DataVersion
	Schema       *schemapb.CollectionSchema
	Growing      *GrowingRuntime
	BM25         *BM25Runtime
}

func (r *ViewRuntime) Close() {
	if r == nil {
		return
	}
	r.Growing.Close()
	r.BM25.Close()
}

// GrowingRuntime is the csegment-backed growing side prepared for one DataVersion.
type GrowingRuntime struct {
	SegmentIDs          []int64
	Segments            map[int64]segcore.CSegment
	DeleteReplayEntries []*streamingpb.TransformLogEntry
	LiveEvents          <-chan walview.VChannelResourceEvent

	applier                  GrowingRuntimeApplier
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
	bm25                     atomic.Pointer[BM25Runtime]
}

func (r *GrowingRuntime) AppliedGrowingTimeTick() uint64 {
	if r == nil {
		return 0
	}
	return r.appliedGrowingTimeTick.Load()
}

func (r *GrowingRuntime) AppliedTransformTimeTick() uint64 {
	if r == nil {
		return 0
	}
	return r.appliedTransformTimeTick.Load()
}

func (r *GrowingRuntime) Segment(segmentID int64) (segcore.CSegment, bool) {
	if r == nil {
		return nil, false
	}
	if concrete, ok := r.applier.(*segcoreGrowingRuntimeApplier); ok {
		return concrete.segment(segmentID)
	}
	segment, ok := r.Segments[segmentID]
	return segment, ok
}

func (r *GrowingRuntime) SegmentFlushed(segmentID int64) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.flushedSegments[segmentID]
	return ok
}

func (r *GrowingRuntime) registerSegment(segmentID int64) {
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

func (r *GrowingRuntime) markSegmentFlushed(segmentID int64) {
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

func (r *GrowingRuntime) markSegmentSealed(segmentID int64, sealedAt qviews.DataVersion) {
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

func (r *GrowingRuntime) Truncate(minDataVersion qviews.DataVersion) {
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

func (r *GrowingRuntime) removeSegmentMetadataLocked(segmentID int64) {
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

func (r *GrowingRuntime) releaseSegment(segmentID int64) {
	if releaser, ok := r.applier.(growingSegmentReleaser); ok {
		releaser.releaseSegment(segmentID)
	}
}

func (r *GrowingRuntime) Close() {
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

func (r *GrowingRuntime) setBM25Runtime(bm25 *BM25Runtime) {
	if r == nil {
		return
	}
	r.bm25.Store(bm25)
}

// BM25Runtime records the BM25 resources loaded for one DataVersion.
type BM25Runtime struct {
	Resources         []*BM25SegmentResource
	GrowingSegmentIDs []int64
	Oracle            BM25Oracle
	LiveUpdater       BM25LiveUpdater
	Advancer          BM25Advancer
	DataVersion       qviews.DataVersion

	closeOnce   sync.Once
	catchupMu   sync.Mutex
	catchupOnce sync.Once
	catchupDone chan struct{}
	catchupErr  error
	OnClose     func()
}

type BM25Oracle interface {
	BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error)
}

type BM25LiveUpdater interface {
	ApplyLiveMessage(context.Context, message.ImmutableMessage) error
}

type BM25SegmentSealedUpdater interface {
	ApplySegmentSealed(segmentID int64, sealedAt qviews.DataVersion)
}

type BM25Advancer interface {
	MaybeAdvance(qviews.DataVersion)
}

func (r *BM25Runtime) ApplyLiveMessage(ctx context.Context, msg message.ImmutableMessage) error {
	if r == nil || r.LiveUpdater == nil {
		return nil
	}
	return r.LiveUpdater.ApplyLiveMessage(ctx, msg)
}

func (r *BM25Runtime) ApplySegmentSealed(segmentID int64, sealedAt qviews.DataVersion) {
	if r == nil || r.LiveUpdater == nil {
		return
	}
	if updater, ok := r.LiveUpdater.(BM25SegmentSealedUpdater); ok {
		updater.ApplySegmentSealed(segmentID, sealedAt)
	}
}

func (r *BM25Runtime) CatchupDone() <-chan struct{} {
	if r == nil {
		return closedChannel()
	}
	r.catchupMu.Lock()
	defer r.catchupMu.Unlock()
	r.ensureCatchupDoneLocked()
	return r.catchupDone
}

func (r *BM25Runtime) CatchupError() error {
	if r == nil {
		return nil
	}
	r.catchupMu.Lock()
	defer r.catchupMu.Unlock()
	return r.catchupErr
}

func (r *BM25Runtime) MaybeAdvance(target qviews.DataVersion) {
	if r == nil || r.Advancer == nil || !target.GT(r.DataVersion) {
		return
	}
	r.Advancer.MaybeAdvance(target)
}

func (r *BM25Runtime) MarkCatchupDone() {
	r.markCatchupDone(nil)
}

func (r *BM25Runtime) MarkCatchupFailed(err error) {
	r.markCatchupDone(err)
}

func (r *BM25Runtime) markCatchupDone(err error) {
	if r == nil {
		return
	}
	r.catchupMu.Lock()
	defer r.catchupMu.Unlock()
	r.ensureCatchupDoneLocked()
	r.catchupOnce.Do(func() {
		r.catchupErr = err
		close(r.catchupDone)
	})
}

func (r *BM25Runtime) ensureCatchupDoneLocked() {
	if r.catchupDone == nil {
		r.catchupDone = make(chan struct{})
	}
}

func (r *BM25Runtime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		r.MarkCatchupDone()
		if r.OnClose != nil {
			r.OnClose()
		}
	})
}

// BM25SegmentResource is the SN-side shape of QueryCoord's BM25 resource proto.
type BM25SegmentResource struct {
	SegmentID      int64
	PartitionID    int64
	BM25Binlogs    []*datapb.FieldBinlog
	StorageVersion int64
	ManifestPath   string
}

func closedChannel() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
