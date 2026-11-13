package gc

import (
	"context"

	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// removeObject authorizes an immutable key before issuing its physical delete.
// The durable intent prevents a concurrent or delayed replica capture from
// acquiring the key after deletion has already been sent to object storage.
func (gc *GarbageCollector) removeObject(ctx context.Context, key string) error {
	catalog, ok := gc.Option.StreamingNodeCatalog.(metastore.WALSharedObjectRetentionCatalog)
	if !ok {
		return gc.Option.Cli.Remove(ctx, key)
	}
	operation, err := catalog.BeginSharedWALObjectDeletion(ctx, key)
	if err != nil {
		return err
	}
	if operation == nil {
		// Never report success for a retained file: callers retire segment/index
		// metadata only after every physical deletion has succeeded.
		return merr.WrapErrServiceUnavailable("object %q is retained by a WAL replica", key)
	}
	return gc.completeSharedWALDeletion(ctx, catalog, operation)
}

func (gc *GarbageCollector) completeSharedWALDeletion(ctx context.Context, catalog metastore.WALSharedObjectRetentionCatalog, operation *streamingpb.WALObjectDeletion) error {
	// A completed intent still fences new readers, but a delayed PUT may have
	// recreated the immutable key. Re-delete it when a later GC walk finds it.
	if err := gc.Option.Cli.Remove(ctx, operation.ObjectKey); err != nil {
		return err
	}
	return catalog.CompleteSharedWALObjectDeletion(ctx, operation)
}

// Prefix deletion must pass through the same exact-key authorization as the
// orphan, index and LOB paths. Do not issue an unguarded backend prefix delete
// after this walk: a new object/root may have appeared in the meantime.
func (gc *GarbageCollector) removeObjectsWithPrefix(ctx context.Context, prefix string) error {
	if _, ok := gc.Option.StreamingNodeCatalog.(metastore.WALSharedObjectRetentionCatalog); !ok {
		return gc.Option.Cli.RemoveWithPrefix(ctx, prefix)
	}
	var removeErr error
	err := gc.Option.Cli.WalkWithPrefix(ctx, prefix, true, func(info *storage.ChunkObjectInfo) bool {
		removeErr = gc.removeObject(ctx, info.FilePath)
		return removeErr == nil
	})
	if removeErr != nil {
		return removeErr
	}
	if copysegment.IsAbsentLocalPrefix(gc.Option.Cli, prefix, err) {
		return nil
	}
	return err
}

// A restarted coordinator finishes previously authorized deletes even if a
// lost response made the object disappear from subsequent prefix listings.
func (gc *GarbageCollector) maintainSharedWALRetention(ctx context.Context) error {
	catalog, ok := gc.Option.StreamingNodeCatalog.(metastore.WALSharedObjectRetentionCatalog)
	if !ok {
		return nil
	}
	if err := catalog.SweepReleasedSharedWALRoots(ctx, 128); err != nil {
		return err
	}
	operations, err := catalog.ListPendingSharedWALObjectDeletions(ctx, 128)
	if err != nil {
		return err
	}
	for _, operation := range operations {
		if err := gc.completeSharedWALDeletion(ctx, catalog, operation); err != nil {
			return err
		}
	}
	return nil
}
