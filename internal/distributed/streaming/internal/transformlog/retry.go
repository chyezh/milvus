package transformlog

import (
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

func isRetryable(err error) bool {
	if err == nil {
		return true
	}
	if errors.IsAny(err, wal.ErrTransformLogInvalidReadOption, wal.ErrTransformLogStartPointTruncated, wal.ErrTransformLogVChannelUnavailable) {
		return false
	}
	var streamingErr *streamingstatus.StreamingError
	if errors.As(err, &streamingErr) {
		return false
	}
	grpcStatus, ok := grpcstatus.FromError(err)
	if !ok {
		return false
	}
	switch grpcStatus.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Canceled:
		return true
	default:
		return false
	}
}
