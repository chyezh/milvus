package metastore

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// WALReplicaRuntimeCatalog transfers object obligations from a private
// candidate to its installed runtime. Closing a transport never releases them.
// Release fences late acquisition before cleaning up the runtime's roots.
type WALReplicaRuntimeCatalog interface {
	CreateWALReplicaRuntime(context.Context, *viewpb.StreamingWorkNode) error
	PrepareWALReplicaRuntime(context.Context, *viewpb.StreamingWorkNode, *streamingpb.WALReplicaCursor) error
	InstallWALReplicaRuntime(context.Context, *viewpb.StreamingWorkNode, *streamingpb.WALReplicaCursor) error
	ReleaseWALReplicaRuntime(context.Context, *viewpb.StreamingWorkNode) error
	ListWALReplicaRuntimes(context.Context, string) ([]*streamingpb.WALReplicaRuntimeRetention, error)
	SweepReleasedWALReplicaRoots(context.Context, string, int) error
}
