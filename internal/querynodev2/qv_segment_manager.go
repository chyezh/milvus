package querynodev2

import (
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	qvtransformlogbuffer "github.com/milvus-io/milvus/internal/querynodev2/transformlogbuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/worknode/qnview"
)

type queryViewPhysicalSegmentLoader struct {
	collections qvCollectionManager
	segments    qvSegmentManager
	loader      qvSegmentLoader
}

func NewQueryViewPhysicalSegmentLoader(manager *segments.Manager, loader segments.Loader) qnview.PhysicalSegmentLoader {
	return newQueryViewPhysicalSegmentLoader(manager.Collection, manager.Segment, realQVSegmentLoader{
		loader:         loader,
		collections:    manager.Collection,
		segmentManager: manager.Segment,
	})
}

func (node *QueryNode) NewQueryViewSegmentManager(meta qnview.MetadataProvider, accesser wal.TransformLogAccesser) qnview.SegmentManager {
	if node.manager == nil || node.loader == nil || meta == nil || accesser == nil {
		return nil
	}
	physicalLoader := NewQueryViewPhysicalSegmentLoader(node.manager, node.loader)
	physicalManager := qnview.NewViewAwareSealedSegmentManager(meta, physicalLoader, newQueryViewSegmentResourceEstimator(node.loader))
	collectionRuntime := newQueryViewCollectionRuntimeManager(meta, node.manager.Collection)
	return qnview.NewTransformAwareSegmentManager(physicalManager, qvtransformlogbuffer.New(accesser), collectionRuntime)
}

func newQueryViewPhysicalSegmentLoader(collections qvCollectionManager, segments qvSegmentManager, loader qvSegmentLoader) *queryViewPhysicalSegmentLoader {
	return &queryViewPhysicalSegmentLoader{
		collections: collections,
		segments:    segments,
		loader:      loader,
	}
}
