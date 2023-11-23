package interceptor

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal/wal"
)

// Builder is the interface to build a interceptor.
// 1. Builder is concurrent safe.
// 2. Builder can used to build a interceptor with cross-wal shared resources.
type Builder interface {
	// Build build a interceptor with wal that interceptor will work on.
	Build(wal wal.WAL) AppendInterceptor
}
