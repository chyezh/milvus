package viewquery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func toRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return err
	}
	var viewErr *viewerror.ViewError
	if errors.As(err, &viewErr) {
		return viewerror.NewGRPCStatusFromViewError(viewErr).Err()
	}
	if errors.Is(err, merr.ErrServiceUnavailable) {
		return viewerror.NewGRPCStatusFromViewError(viewerror.NewOnShutdownError("%s", err.Error())).Err()
	}
	return viewerror.NewGRPCStatusFromViewError(viewerror.AsViewError(err)).Err()
}
