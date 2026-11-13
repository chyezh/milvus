package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

func (w *walAdaptorImpl) SubscribeWALEvents(ctx context.Context, after walevent.Cursor) (*walevent.Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !w.IsAvailable() {
		return nil, status.NewOnShutdownError("WAL replica event source is unavailable")
	}
	return w.param.RecoveryStorage.Events().Subscribe(after)
}

var _ wal.ReplicaEventProvider = (*walAdaptorImpl)(nil)
