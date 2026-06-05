package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// SegmentLoadInfoProvider fetches SegmentLoadInfo for given segment IDs.
// Implementation calls DataCoord/MixCoord APIs.
type SegmentLoadInfoProvider interface {
	GetSegmentLoadInfos(ctx context.Context, collectionID int64, segmentIDs []int64) ([]*querypb.SegmentLoadInfo, error)
}

// sealedLoaderAdapter adapts segments.Loader to the sealedSegmentLoader interface
// used by loadScheduler, hardcoding SegmentTypeSealed and version=0.
type sealedLoaderAdapter struct {
	inner segments.Loader
}

func (a *sealedLoaderAdapter) LoadSealed(ctx context.Context, collectionID int64, infos ...*querypb.SegmentLoadInfo) error {
	_, err := a.inner.Load(ctx, collectionID, segments.SegmentTypeSealed, 0, infos...)
	return err
}

type segmentManagerImpl struct {
	loadScheduler *loadScheduler
}

var _ SegmentManager = (*segmentManagerImpl)(nil)

// NewSegmentManagerImpl creates a SegmentManager backed by a loadScheduler.
func NewSegmentManagerImpl(
	infoProvider SegmentLoadInfoProvider,
	loader segments.Loader,
	segManager segments.SegmentManager,
) *segmentManagerImpl {
	return &segmentManagerImpl{
		loadScheduler: newLoadScheduler(infoProvider, &sealedLoaderAdapter{inner: loader}, segManager),
	}
}

func (m *segmentManagerImpl) Acquire(req AcquireSegments) { m.loadScheduler.Acquire(req) }
func (m *segmentManagerImpl) Release(req ReleaseSegments) { m.loadScheduler.Release(req) }

// Close waits for all in-flight goroutines and releases the worker pool.
func (m *segmentManagerImpl) Close() { m.loadScheduler.Close() }
