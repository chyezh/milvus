package metastore

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// WALSharedObjectRetentionCatalog protects immutable storage objects shared
// across PChannels. It is separate from the channel-owned Summary namespace:
// deletion must see every runtime owner, not just the current channel's roots.
// Every physical deleter of this namespace must authorize its exact key here.
type WALSharedObjectRetentionCatalog interface {
	RetainSharedWALObjects(context.Context, *streamingpb.WALObjectRetentionRoot) error
	ReleaseSharedWALObjects(context.Context, *viewpb.StreamingWorkNode, string) error
	ListSharedWALObjectRoots(context.Context) ([]*streamingpb.WALObjectRetentionRoot, error)
	SweepReleasedSharedWALRoots(context.Context, int) error
	BeginSharedWALObjectDeletion(context.Context, string) (*streamingpb.WALObjectDeletion, error)
	CompleteSharedWALObjectDeletion(context.Context, *streamingpb.WALObjectDeletion) error
	ListPendingSharedWALObjectDeletions(context.Context, int) ([]*streamingpb.WALObjectDeletion, error)
}
