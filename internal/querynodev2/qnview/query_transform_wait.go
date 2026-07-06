package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (m *QueryViewSegmentReadinessManager) WaitTransformVisible(ctx context.Context, key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode, timetick uint64) error {
	if timetick == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	segments, err := m.transformSegmentsForView(key, view)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		if err := segment.WaitTransformApplied(ctx, timetick); err != nil {
			return err
		}
	}
	return nil
}

func (m *QueryViewSegmentReadinessManager) transformSegmentsForView(key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode) ([]TransformSegment, error) {
	segmentPartitions := segmentPartitionMap(view)
	segments := make([]TransformSegment, 0, len(segmentPartitions))

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.views[key] == nil {
		return nil, viewerror.NewViewNotFound("query view %s is not found", key.String())
	}
	for segmentID := range segmentPartitions {
		state := m.segments[segmentID]
		if state == nil || state.state != transformSegmentLoaded || state.segment == nil {
			return nil, viewerror.NewViewInvalidated("query view %s segment %d is not ready", key.String(), segmentID)
		}
		segments = append(segments, state.segment)
	}
	return segments, nil
}
