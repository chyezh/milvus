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
	WALView      walview.VChannelWALView
	LiveMessages <-chan message.ImmutableMessage
	LiveDone     <-chan struct{}
	OnApplied    func()
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
	LiveMessages        <-chan message.ImmutableMessage

	applier                  GrowingRuntimeApplier
	mu                       sync.RWMutex
	flushedSegments          map[int64]struct{}
	closeOnce                sync.Once
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

func (r *GrowingRuntime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
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

	closeOnce sync.Once
	OnClose   func()
}

type BM25Oracle interface {
	BuildIDF(fieldID int64, tfs *schemapb.SparseFloatArray) ([][]byte, float64, error)
}

type BM25LiveUpdater interface {
	ApplyLiveMessage(context.Context, message.ImmutableMessage) error
}

func (r *BM25Runtime) ApplyLiveMessage(ctx context.Context, msg message.ImmutableMessage) error {
	if r == nil || r.LiveUpdater == nil {
		return nil
	}
	return r.LiveUpdater.ApplyLiveMessage(ctx, msg)
}

func (r *BM25Runtime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
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
