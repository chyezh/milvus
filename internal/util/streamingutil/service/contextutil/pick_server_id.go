package contextutil

import (
	"context"

	grpccontextutil "github.com/milvus-io/milvus/internal/util/grpcutil/contextutil"
)

// WithPickServerID returns a new context with the pick result.
// Delegates to grpcutil/contextutil.
func WithPickServerID(ctx context.Context, serverID int64) context.Context {
	return grpccontextutil.WithPickServerID(ctx, serverID)
}

// GetPickServerID must get the pick result from context.
// Delegates to grpcutil/contextutil.
func GetPickServerID(ctx context.Context) (int64, bool) {
	return grpccontextutil.GetPickServerID(ctx)
}
