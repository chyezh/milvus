package metastore

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

const (
	WALObjectRetentionRootMaxObjects = 4096
	WALObjectRetentionRootMaxBytes   = 512 * 1024
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
	RetainWALRecoveryObjects(context.Context, string, int64, *streamingpb.WALObjectRetentionRoot) error
	ReleaseWALObjects(context.Context, string, string) error
	ListWALObjectRetentionRoots(context.Context, string) ([]*streamingpb.WALObjectRetentionRoot, error)
	// CommitWALRecoveryRoot replaces one authoritative recovery slot after its
	// metadata is durable. The new root must already be retained; the previous
	// root is released atomically with the slot update and writer term check.
	// A root belongs to exactly one slot and cannot be released independently.
	CommitWALRecoveryRoot(context.Context, string, int64, string, string) error
	// Begin returns nil when a live root still references the object. New
	// authorization requires the current checkpoint term; an existing exact
	// operation can be resumed by any owner, including after a crash.
	BeginWALObjectDeletion(context.Context, string, int64, string) (*streamingpb.WALObjectDeletion, error)
	ListPendingWALObjectDeletions(context.Context, string, int) ([]*streamingpb.WALObjectDeletion, error)
	CompleteWALObjectDeletion(context.Context, string, *streamingpb.WALObjectDeletion) error
}
