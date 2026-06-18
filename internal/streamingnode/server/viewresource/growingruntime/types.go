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
	return settingsFromAlterLoadConfig(d.WALView.LoadConfig.GetHeader())
}

func (d Descriptor) Schema() *schemapb.CollectionSchema {
	return d.WALView.Schema
}

func settingsFromAlterLoadConfig(header *messagespb.AlterLoadConfigMessageHeader) *viewpb.QueryViewSettings {
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

type BM25Runtime interface {
	ApplyLiveMessage(context.Context, message.ImmutableMessage) error
	ApplySegmentSealed(segmentID int64, sealedAt qviews.DataVersion)
}

// Runtime is the csegment-backed growing side prepared for one DataVersion.
type Runtime struct {
	LiveEvents <-chan walview.VChannelResourceEvent

	mu                       sync.RWMutex
	collection               *segcore.CCollection
	segments                 map[int64]*growingSegment
	segmentIDs               []int64
	deleteReplayEntries      []*streamingpb.TransformLogEntry
	truncateDataVersion      qviews.DataVersion
	hasTruncateDataVersion   bool
	closeOnce                sync.Once
	liveStopCh               chan struct{}
	liveDoneCh               chan struct{}
	appliedGrowingTimeTick   atomic.Uint64
	appliedTransformTimeTick atomic.Uint64
	bm25                     atomic.Value
}

func newRuntime(collection *segcore.CCollection) *Runtime {
	return &Runtime{
		collection: collection,
		segments:   make(map[int64]*growingSegment),
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
	r.mu.RLock()
	segment := r.segments[segmentID]
	r.mu.RUnlock()
	if segment == nil {
		return nil, false
	}
	return segment.csegment()
}

func (r *Runtime) SegmentFlushed(segmentID int64) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	segment := r.segments[segmentID]
	r.mu.RUnlock()
	return segment != nil && segment.isFlushed()
}

func (r *Runtime) SegmentIDs() []int64 {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]int64(nil), r.segmentIDs...)
}

func (r *Runtime) DeleteReplayEntries() []*streamingpb.TransformLogEntry {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]*streamingpb.TransformLogEntry(nil), r.deleteReplayEntries...)
}

func (r *Runtime) Truncate(minDataVersion qviews.DataVersion) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if !r.hasTruncateDataVersion || minDataVersion.GT(r.truncateDataVersion) {
		r.truncateDataVersion = minDataVersion
		r.hasTruncateDataVersion = true
	}
	segmentsToRelease := make([]*growingSegment, 0)
	for segmentID, segment := range r.segments {
		if segment.shouldRelease(r.truncateDataVersion) {
			segmentsToRelease = append(segmentsToRelease, segment)
			r.removeSegmentMetadataLocked(segmentID)
		}
	}
	r.mu.Unlock()
	for _, segment := range segmentsToRelease {
		segment.release()
	}
}

func (r *Runtime) removeSegmentMetadataLocked(segmentID int64) {
	delete(r.segments, segmentID)
	for i, id := range r.segmentIDs {
		if id == segmentID {
			r.segmentIDs = append(r.segmentIDs[:i], r.segmentIDs[i+1:]...)
			return
		}
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
		r.mu.Lock()
		segments := make([]*growingSegment, 0, len(r.segments))
		for _, segment := range r.segments {
			segments = append(segments, segment)
		}
		r.segments = nil
		r.segmentIDs = nil
		collection := r.collection
		r.collection = nil
		r.mu.Unlock()
		for _, segment := range segments {
			segment.release()
		}
		if collection != nil {
			collection.Release()
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
