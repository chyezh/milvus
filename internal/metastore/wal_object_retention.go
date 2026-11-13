package metastore

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// WALObjectRetentionCatalog serializes durable object obligations with deletion
// authorization. Callers register all transitive immutable dependencies before
// publishing a root and release only after its recovery/serving users drain.
// Roots have no implicit timeout: losing a transport is not proof of release.
// A root ID and an object key must never be reused for a different lifetime.
//
// This protocol must cover every publisher and deleter of the protected object
// namespace. It is not safe to enable GC by counting only online subscribers.
type WALObjectRetentionCatalog interface {
	RetainWALObjects(context.Context, string, *streamingpb.WALObjectRetentionRoot) error
	ReleaseWALObjects(context.Context, string, string) error
	ListWALObjectRetentionRoots(context.Context, string) ([]*streamingpb.WALObjectRetentionRoot, error)
	// Begin returns nil when a live root still references the object. New
	// authorization requires the current checkpoint term; an existing exact
	// operation can be resumed by any owner, including after a crash.
	BeginWALObjectDeletion(context.Context, string, int64, string) (*streamingpb.WALObjectDeletion, error)
	ListPendingWALObjectDeletions(context.Context, string, int) ([]*streamingpb.WALObjectDeletion, error)
	CompleteWALObjectDeletion(context.Context, string, *streamingpb.WALObjectDeletion) error
}
