package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
)

// ReplicaEventProvider exports the applied event prefix of a primary WAL.
type ReplicaEventProvider interface {
	SubscribeWALEvents(context.Context, walevent.Cursor) (*walevent.Reader, error)
}
