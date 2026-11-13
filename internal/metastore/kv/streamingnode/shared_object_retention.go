package streamingnode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ metastore.WALSharedObjectRetentionCatalog = (*catalog)(nil)

const (
	sharedRetentionPrefix = "streamingnode-meta/shared-object-retention/"
	maxSharedRoots        = 8192
	maxSharedRootBytes    = 128 * 1024 * 1024
)

func sharedRetentionKey(kind, identity string) string {
	digest := sha256.Sum256([]byte(identity))
	return sharedRetentionPrefix + kind + "/" + hex.EncodeToString(digest[:])
}

func sharedRootKey(kind string, runtime *viewpb.StreamingWorkNode, id string) string {
	return sharedRetentionKey(kind, replicaRuntimeKey(runtime)+"/"+id)
}

func (c *catalog) RetainSharedWALObjects(ctx context.Context, root *streamingpb.WALObjectRetentionRoot) error {
	if root.GetRootId() == "" || root.GetReleased() || len(root.GetObjectKeys()) == 0 ||
		len(root.GetObjectKeys()) > maxRetentionObjects || proto.Size(root) > maxRetentionRootBytes {
		return merr.WrapErrServiceInternalMsg("invalid shared WAL object root")
	}
	if err := validateRetentionRuntime(root.Runtime); err != nil {
		return err
	}
	root = proto.CloneOf(root)
	slices.Sort(root.ObjectKeys)
	root.ObjectKeys = slices.Compact(root.ObjectKeys)
	if root.ObjectKeys[0] == "" {
		return merr.WrapErrServiceInternalMsg("shared WAL root contains an empty object key")
	}
	epoch, err := c.loadRetentionValue(ctx, sharedRetentionPrefix+"epoch")
	if err != nil {
		return err
	}
	// The receiver's runtime fence lives in its own PChannel. Guard its exact
	// value in the global transaction, so a concurrent Close cannot be missed.
	runtimeKey := replicaRuntimeKey(root.Runtime)
	runtimeValue, err := c.metaKV.Load(ctx, runtimeKey)
	if err != nil {
		return err
	}
	runtime, err := decodeReplicaRuntime(runtimeValue, root.Runtime)
	if err != nil {
		return err
	}
	if runtime.State == streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
		return merr.WrapErrServiceUnavailable("shared WAL object owner has drained")
	}
	released, err := c.loadRetentionValue(ctx, sharedRootKey("released", root.Runtime, root.RootId))
	if err != nil {
		return err
	}
	if released != "" {
		return merr.WrapErrServiceUnavailable("shared WAL object root was released")
	}
	key := sharedRootKey("roots", root.Runtime, root.RootId)
	value, err := c.loadRetentionValue(ctx, key)
	if err != nil {
		return err
	}
	if value != "" {
		stored := &streamingpb.WALObjectRetentionRoot{}
		if err := proto.Unmarshal([]byte(value), stored); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode shared WAL object root")
		}
		if !proto.Equal(stored, root) {
			return merr.WrapErrDataIntegrityMsg("shared WAL root identity was reused")
		}
	} else {
		roots, err := c.ListSharedWALObjectRoots(ctx)
		if err != nil {
			return err
		}
		bytes := proto.Size(root)
		for _, stored := range roots {
			bytes += proto.Size(stored)
		}
		if len(roots) >= maxSharedRoots || bytes > maxSharedRootBytes {
			return merr.WrapErrServiceUnavailable("shared WAL object retention budget exhausted")
		}
	}
	keys := make([]string, len(root.ObjectKeys))
	for i, object := range root.ObjectKeys {
		keys[i] = sharedRetentionKey("deletions", object)
	}
	batchSize := min(128, c.metaKV.MaxTxnOps())
	if batchSize <= 0 {
		return merr.WrapErrServiceUnavailable("shared WAL retention catalog has no transaction capacity")
	}
	for start := 0; start < len(keys); start += batchSize {
		batch := keys[start:min(start+batchSize, len(keys))]
		values, err := c.metaKV.MultiLoad(ctx, batch)
		if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
			return err
		}
		if len(values) != len(batch) {
			return merr.WrapErrDataIntegrityMsg("incomplete shared WAL deletion lookup")
		}
		for _, value := range values {
			if value != "" {
				return merr.WrapErrServiceUnavailable("shared WAL object is already authorized for deletion")
			}
		}
	}
	encoded, err := proto.Marshal(root)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "encode shared WAL object root")
	}
	return c.commitRetentionAt(ctx, sharedRetentionPrefix+"epoch", epoch, map[string]string{key: string(encoded)}, nil, predicates.ValueEqual(runtimeKey, runtimeValue))
}

func (c *catalog) ReleaseSharedWALObjects(ctx context.Context, runtime *viewpb.StreamingWorkNode, rootID string) error {
	if err := validateRetentionRuntime(runtime); err != nil {
		return err
	}
	if rootID == "" {
		return merr.WrapErrServiceInternalMsg("shared WAL root release requires its identity")
	}
	epoch, err := c.loadRetentionValue(ctx, sharedRetentionPrefix+"epoch")
	if err != nil {
		return err
	}
	return c.commitRetentionAt(ctx, sharedRetentionPrefix+"epoch", epoch,
		map[string]string{sharedRootKey("released", runtime, rootID): rootID}, []string{sharedRootKey("roots", runtime, rootID)})
}

func (c *catalog) ListSharedWALObjectRoots(ctx context.Context) ([]*streamingpb.WALObjectRetentionRoot, error) {
	var roots []*streamingpb.WALObjectRetentionRoot
	bytes := 0
	err := c.metaKV.WalkWithPrefix(ctx, sharedRetentionPrefix+"roots/", 128, func(_, value []byte) error {
		bytes += len(value)
		if len(roots) >= maxSharedRoots || bytes > maxSharedRootBytes || len(value) > maxRetentionRootBytes {
			return merr.WrapErrServiceUnavailable("shared WAL object roots exceed the read budget")
		}
		root := &streamingpb.WALObjectRetentionRoot{}
		if err := proto.Unmarshal(value, root); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode shared WAL root")
		}
		if root.RootId == "" || root.Released || len(root.ObjectKeys) == 0 || len(root.ObjectKeys) > maxRetentionObjects {
			return merr.WrapErrDataIntegrityMsg("invalid shared WAL object root")
		}
		if err := validateRetentionRuntime(root.Runtime); err != nil {
			return merr.WrapErrDataIntegrity(err, "invalid shared WAL object owner")
		}
		roots = append(roots, root)
		return nil
	})
	return roots, err
}

func (c *catalog) SweepReleasedSharedWALRoots(ctx context.Context, limit int) error {
	if limit <= 0 || limit > maxSharedRoots {
		return merr.WrapErrServiceInternalMsg("invalid shared WAL root sweep budget")
	}
	roots, err := c.ListSharedWALObjectRoots(ctx)
	if err != nil {
		return err
	}
	for _, root := range roots {
		runtime, err := c.loadReplicaRuntime(ctx, root.Runtime)
		if err != nil {
			return err
		}
		if runtime == nil || runtime.State != streamingpb.WALReplicaRuntimeRetentionState_WAL_REPLICA_RUNTIME_RELEASED {
			continue
		}
		if err := c.ReleaseSharedWALObjects(ctx, root.Runtime, root.RootId); err != nil {
			return err
		}
		limit--
		if limit == 0 {
			break
		}
	}
	return nil
}

func (c *catalog) loadSharedObjectDeletion(ctx context.Context, object string) (*streamingpb.WALObjectDeletion, error) {
	value, err := c.loadRetentionValue(ctx, sharedRetentionKey("deletions", object))
	if err != nil || value == "" {
		return nil, err
	}
	operation := &streamingpb.WALObjectDeletion{}
	if err := proto.Unmarshal([]byte(value), operation); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode shared WAL object deletion")
	}
	if operation.ObjectKey != object || operation.OperationId == "" {
		return nil, merr.WrapErrDataIntegrityMsg("shared WAL object deletion identity mismatch")
	}
	return operation, nil
}

func (c *catalog) BeginSharedWALObjectDeletion(ctx context.Context, object string) (*streamingpb.WALObjectDeletion, error) {
	if object == "" {
		return nil, merr.WrapErrServiceInternalMsg("shared WAL deletion requires an object key")
	}
	epoch, err := c.loadRetentionValue(ctx, sharedRetentionPrefix+"epoch")
	if err != nil {
		return nil, err
	}
	if operation, err := c.loadSharedObjectDeletion(ctx, object); operation != nil || err != nil {
		return operation, err
	}
	roots, err := c.ListSharedWALObjectRoots(ctx)
	if err != nil {
		return nil, err
	}
	for _, root := range roots {
		if slices.Contains(root.ObjectKeys, object) {
			return nil, nil
		}
	}
	operation := &streamingpb.WALObjectDeletion{ObjectKey: object, OperationId: uuid.NewString()}
	encoded, err := proto.Marshal(operation)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "encode shared WAL object deletion")
	}
	if err := c.commitRetentionAt(ctx, sharedRetentionPrefix+"epoch", epoch, map[string]string{
		sharedRetentionKey("deletions", object): string(encoded), sharedRetentionKey("pending", object): string(encoded),
	}, nil); err != nil {
		return nil, err
	}
	return operation, nil
}

func (c *catalog) CompleteSharedWALObjectDeletion(ctx context.Context, operation *streamingpb.WALObjectDeletion) error {
	if operation.GetObjectKey() == "" || operation.GetOperationId() == "" {
		return merr.WrapErrServiceInternalMsg("invalid shared WAL deletion completion")
	}
	epoch, err := c.loadRetentionValue(ctx, sharedRetentionPrefix+"epoch")
	if err != nil {
		return err
	}
	stored, err := c.loadSharedObjectDeletion(ctx, operation.ObjectKey)
	if err != nil {
		return err
	}
	if stored == nil || stored.OperationId != operation.OperationId {
		return merr.WrapErrDataIntegrityMsg("shared WAL deletion completion has no matching authorization")
	}
	if stored.Done {
		return nil
	}
	stored.Done = true
	encoded, err := proto.Marshal(stored)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "encode shared WAL deletion completion")
	}
	return c.commitRetentionAt(ctx, sharedRetentionPrefix+"epoch", epoch,
		map[string]string{sharedRetentionKey("deletions", operation.ObjectKey): string(encoded)}, []string{sharedRetentionKey("pending", operation.ObjectKey)})
}

func (c *catalog) ListPendingSharedWALObjectDeletions(ctx context.Context, limit int) ([]*streamingpb.WALObjectDeletion, error) {
	if limit <= 0 || limit > maxRetentionObjects {
		return nil, merr.WrapErrServiceInternalMsg("invalid shared WAL deletion page budget")
	}
	operations := make([]*streamingpb.WALObjectDeletion, 0, limit)
	err := c.metaKV.WalkWithPrefix(ctx, sharedRetentionPrefix+"pending/", min(128, limit), func(_, value []byte) error {
		if len(operations) == limit {
			return errRetentionPageFull
		}
		operation := &streamingpb.WALObjectDeletion{}
		if err := proto.Unmarshal(value, operation); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode pending shared WAL deletion")
		}
		if operation.ObjectKey == "" || operation.OperationId == "" || operation.Done {
			return merr.WrapErrDataIntegrityMsg("invalid pending shared WAL deletion")
		}
		operations = append(operations, operation)
		return nil
	})
	if errors.Is(err, errRetentionPageFull) {
		err = nil
	}
	return operations, err
}
