package qnview

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
)

func (m *QueryViewSegmentReadinessManager) WaitTransformVisible(ctx context.Context, key qviews.QueryViewKey, timetick uint64) error {
	if timetick == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	view := m.views[key]
	if view == nil {
		m.mu.Unlock()
		return viewerror.NewViewNotFound("query view %s is not found", key.String())
	}
	guard := view.transformGuard
	m.mu.Unlock()
	if guard == nil {
		return viewerror.NewViewInvalidated("query view %s transform guard is not ready", key.String())
	}
	if err := guard.WaitTransformVisible(ctx, timetick); err != nil {
		if isTransformLogUnavailableError(err) {
			return viewerror.NewViewInvalidated("query view %s transform log is unavailable: %s", key.String(), err.Error())
		}
		return err
	}
	return nil
}

func isTransformLogUnavailableError(err error) bool {
	if errors.Is(err, wal.ErrTransformLogVChannelUnavailable) {
		return true
	}
	streamingErr := streamingstatus.AsStreamingError(err)
	return streamingErr != nil && streamingErr.IsWrongStreamingNode()
}
