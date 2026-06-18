package viewresource

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource/growingruntime"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type LoadResourceDescriptor = growingruntime.Descriptor
type GrowingRuntime = growingruntime.Runtime
type GrowingSegmentRuntimeBuilder = growingruntime.Builder
type NoopGrowingSegmentRuntimeBuilder = growingruntime.NoopBuilder
type SnapshotGrowingSegmentRuntimeBuilder = growingruntime.SnapshotBuilder

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
	Growing      *growingruntime.Runtime
	BM25         *BM25Runtime
}

func (r *ViewRuntime) Close() {
	if r == nil {
		return
	}
	r.Growing.Close()
	r.BM25.Close()
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
