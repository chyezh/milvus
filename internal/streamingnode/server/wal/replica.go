package wal

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// ReplicaEventProvider exports the applied event prefix of a primary WAL.
type ReplicaEventProvider interface {
	SubscribeWALEvents(context.Context, walevent.Cursor) (*walevent.Reader, error)
}

// ReplicaBootstrap owns protection for one immutable metadata cut, its objects,
// physical replay range and S+1 reader. Next returns assembled, ordered WAL
// messages strictly after Snapshot().Checkpoint. Close releases all protection.
// Implementations must honor cancellation and never expose an unprotected cut.
type ReplicaBootstrap interface {
	Snapshot() *streamingpb.WALReplicaSnapshot
	Next(context.Context) (message.ImmutableMessage, error)
	Close()
}

type ReplicaBootstrapProvider interface {
	OpenReplicaBootstrap(context.Context, *streamingpb.WALCheckpoint) (ReplicaBootstrap, error)
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

// QueryReplicaManager binds a query plan to the local instance that prepared
// it, independently of later primary assignments or local baseline replacement.
type QueryReplicaManager interface {
	QueryReplicaIdentity(WAL) (*viewpb.StreamingWorkNode, error)
	GetAvailableQueryReplica(*viewpb.StreamingWorkNode) (WAL, error)
}

// QueryReplicaEndpointProvider resolves a stable member placement to its local
// query runtime, without waiting for the primary's current source term.
type QueryReplicaEndpointProvider interface {
	QueryReplicaEndpoint(*streamingpb.WALReplicaEndpointRequest) (*viewpb.StreamingWorkNode, error)
}
