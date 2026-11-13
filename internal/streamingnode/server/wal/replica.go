package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// ReplicaEventProvider exports the applied event prefix of a primary WAL.
type ReplicaEventProvider interface {
	SubscribeWALEvents(context.Context, walevent.Cursor) (*walevent.Reader, error)
}

// ReplicaSourceUpdater changes a read-only runtime's upstream without closing
// its local QueryViews. Updating a source does not grant write authority.
type ReplicaSourceUpdater interface {
	UpdateReplicaSource(context.Context, types.PChannelInfoAssigned) error
}

// ReplicaRefreshProvider asks the lifecycle owner to prepare a replacement
// baseline. A rejected resume does not invalidate queries on the old prefix.
type ReplicaRefreshProvider interface {
	ReplicaRefreshRequired() bool
}
