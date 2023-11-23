package interceptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
)

// Append is the common function to append a msg to the wal.
type Append func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

// AppendInterceptorCall is the common function to execute the interceptor.
type AppendInterceptorCall func(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error)

// AppendInterceptor is the interceptor for Append functions.
// All wal extra operations should be done by these function, such as
// 1. time tick setup.
// 2. unique primary key filter and build.
// 3. index builder.
// AppendInterceptor should be lazy initialized, fast execution.
type AppendInterceptor interface {
	// Execute the append operation with interceptor.
	Do(ctx context.Context, msg message.MutableMessage, append Append) (message.MessageID, error)

	// Close the interceptor release the resources.
	Close()
}

// Some interceptor may need to wait for some resource to be ready or recovery process.
type AppendInterceptorWithReady interface {
	AppendInterceptor

	// Ready check if interceptor is ready.
	// Some append interceptor may be stateful, such as index builder and unique primary key filter,
	// so it need to implement the recovery logic from crash by itself before notifying ready.
	// Append operation will block until ready or canceled.
	// Consumer do not blocked by it.
	Ready() <-chan struct{}
}
