package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type SealedSegmentHandle interface {
	ID() int64
	PartitionID() int64
	Segment() TransformSegment
	Release()
}

type sealedSegmentHandle struct {
	manager    *QueryViewSegmentReadinessManager
	segmentKey segmentRefKey
	segment    TransformSegment
}

func (h *sealedSegmentHandle) ID() int64 {
	return h.segment.ID()
}

func (h *sealedSegmentHandle) PartitionID() int64 {
	return h.segment.PartitionID()
}

func (h *sealedSegmentHandle) Segment() TransformSegment {
	return h.segment
}

func (h *sealedSegmentHandle) Release() {
	if h.manager == nil {
		return
	}
	manager := h.manager
	h.manager = nil
	manager.releaseSealedSegmentHandle(h.segmentKey)
}

func (m *QueryViewSegmentReadinessManager) AcquireSealedSegmentHandles(ctx context.Context, key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode) ([]SealedSegmentHandle, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	segmentPartitions := segmentPartitionMap(view)
	handles := make([]SealedSegmentHandle, 0, len(segmentPartitions))
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[key] == nil {
		return nil, viewerror.NewViewNotFound("query view %s is not found", key.String())
	}
	for segmentID := range segmentPartitions {
		segmentKey := newSegmentRefKey(segmentID, key.WALReplicaID)
		state := m.segments[segmentKey]
		if state == nil || state.state != transformSegmentLoaded || state.segment == nil {
			for _, handle := range handles {
				rollback := m.segments[handle.(*sealedSegmentHandle).segmentKey]
				if rollback != nil && rollback.queryRefs > 0 {
					rollback.queryRefs--
				}
			}
			return nil, viewerror.NewViewInvalidated("query view %s segment %d is not ready", key.String(), segmentID)
		}
		state.queryRefs++
		handles = append(handles, &sealedSegmentHandle{
			manager:    m,
			segmentKey: segmentKey,
			segment:    state.segment,
		})
	}
	return handles, nil
}

func (m *QueryViewSegmentReadinessManager) releaseSealedSegmentHandle(segmentKey segmentRefKey) {
	var segment TransformSegment
	m.mu.Lock()
	state := m.segments[segmentKey]
	if state != nil && state.queryRefs > 0 {
		state.queryRefs--
		if state.queryRefs == 0 && len(state.refs) == 0 {
			segment = state.segment
			delete(m.segments, segmentKey)
		}
	}
	m.mu.Unlock()
	m.releaseDetachedSegment(segment)
}
