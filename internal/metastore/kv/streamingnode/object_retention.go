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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var _ metastore.WALObjectRetentionCatalog = (*catalog)(nil)

// errRetentionPageFull stops a bounded catalog walk without reporting failure.
var errRetentionPageFull = errors.New("WAL object retention page is full")

const (
	maxRetentionRootBytes = metastore.WALObjectRetentionRootMaxBytes
	maxRetentionObjects   = metastore.WALObjectRetentionRootMaxObjects
	maxRetentionRoots     = 1024
	maxRetentionBytes     = 64 * 1024 * 1024
)

func retentionPrefix(channel string) string { return buildWALPrefix(channel) + "object-retention/" }

func retentionKey(channel, kind, identity string) string {
	digest := sha256.Sum256([]byte(identity))
	return retentionPrefix(channel) + kind + "/" + hex.EncodeToString(digest[:])
}

func (c *catalog) loadRetentionValue(ctx context.Context, key string) (string, error) {
	value, err := c.metaKV.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return "", nil
	}
	return value, err
}

// Every root/deletion mutation changes epoch in the same transaction. Reads
// outside that transaction are valid only if its captured epoch still matches.
// Epoch is never reset, including after the last root is released (no ABA).
func (c *catalog) commitRetention(ctx context.Context, channel, epoch string, saves map[string]string, removals []string, guards ...predicates.Predicate) error {
	epochKey := retentionPrefix(channel) + "epoch"
	next := uuid.NewString()
	saves[epochKey] = next
	if len(saves)+len(removals) > c.metaKV.MaxTxnOps() {
		return merr.WrapErrServiceUnavailable("WAL object retention transaction exceeds catalog limit")
	}
	if epoch == "" {
		guards = append(guards, predicates.KeyNotExist(epochKey))
	} else {
		guards = append(guards, predicates.ValueEqual(epochKey, epoch))
	}
	err := c.metaKV.MultiSaveAndRemove(ctx, saves, removals, guards...)
	// A failed response may follow a successful commit. Conversely some KV
	// implementations can return nil on a failed predicate: verify the nonce.
	actual, readErr := c.metaKV.Load(ctx, epochKey)
	if readErr == nil && actual == next {
		return nil
	}
	if err != nil {
		return merr.Wrap(err, "commit WAL object retention")
	}
	if readErr != nil {
		return merr.Wrap(readErr, "verify WAL object retention")
	}
	return merr.WrapErrServiceUnavailable("WAL object retention changed concurrently; retry with fresh metadata")
}

func (c *catalog) RetainWALObjects(ctx context.Context, channel string, root *streamingpb.WALObjectRetentionRoot) error {
	return c.retainWALObjects(ctx, channel, 0, root)
}

func (c *catalog) RetainWALRecoveryObjects(ctx context.Context, channel string, term int64, root *streamingpb.WALObjectRetentionRoot) error {
	if term <= 0 {
		return merr.WrapErrServiceInternalMsg("WAL recovery root requires a writer term")
	}
	return c.retainWALObjects(ctx, channel, term, root)
}

func (c *catalog) retainWALObjects(ctx context.Context, channel string, term int64, root *streamingpb.WALObjectRetentionRoot) error {
	if root.GetRootId() == "" || root.GetReleased() || len(root.GetObjectKeys()) == 0 ||
		len(root.GetObjectKeys()) > maxRetentionObjects || proto.Size(root) > maxRetentionRootBytes {
		return merr.WrapErrServiceInternalMsg("invalid or oversized WAL object retention root")
	}
	root = proto.CloneOf(root)
	slices.Sort(root.ObjectKeys)
	root.ObjectKeys = slices.Compact(root.ObjectKeys)
	if root.ObjectKeys[0] == "" {
		return merr.WrapErrServiceInternalMsg("WAL object retention root contains an empty object key")
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return err
	}
	var guards []predicates.Predicate
	if err := c.checkRuntimeRetention(ctx, channel, root.Runtime); err != nil {
		return err
	}
	if term > 0 {
		owner, err := c.metaKV.Load(ctx, buildConsumeCheckpointKey(channel))
		if err != nil {
			return err
		}
		checkpoint := &streamingpb.WALCheckpoint{}
		if err := proto.Unmarshal([]byte(owner), checkpoint); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode WAL recovery object owner")
		}
		if checkpoint.Term != term {
			return merr.WrapErrServiceUnavailable("WAL recovery object publisher was fenced")
		}
		guards = append(guards, predicates.ValueEqual(buildConsumeCheckpointKey(channel), owner))
	}
	released, err := c.loadRetentionValue(ctx, retentionKey(channel, "released", root.RootId))
	if err != nil {
		return err
	}
	if released != "" {
		return merr.WrapErrServiceUnavailable("WAL object retention root was already released")
	}
	key := retentionKey(channel, "roots", root.RootId)
	existing, err := c.loadRetentionValue(ctx, key)
	if err != nil {
		return err
	}
	if existing != "" {
		stored := &streamingpb.WALObjectRetentionRoot{}
		if err := proto.Unmarshal([]byte(existing), stored); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode WAL object retention root")
		}
		if !proto.Equal(root, stored) {
			return merr.WrapErrDataIntegrityMsg("WAL object retention root ID was reused")
		}
		// Serialize even an idempotent acquisition with concurrent release.
	} else {
		roots, err := c.ListWALObjectRetentionRoots(ctx, channel)
		if err != nil {
			return err
		}
		size := proto.Size(root)
		for _, stored := range roots {
			size += proto.Size(stored)
		}
		if len(roots) >= maxRetentionRoots || size > maxRetentionBytes {
			return merr.WrapErrServiceUnavailable("WAL object retention root budget exhausted")
		}
	}
	if err := c.checkObjectsRetainable(ctx, channel, root.ObjectKeys); err != nil {
		return err
	}
	encoded, err := proto.Marshal(root)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "encode WAL object retention root")
	}
	return c.commitRetention(ctx, channel, epoch, map[string]string{key: string(encoded)}, nil, guards...)
}

func (c *catalog) checkObjectsRetainable(ctx context.Context, channel string, objects []string) error {
	// MultiLoad preserves positions for absent keys. Bound request count to the
	// store's atomic operation budget, rather than one RPC per object.
	batchSize := min(128, c.metaKV.MaxTxnOps())
	if batchSize <= 0 {
		return merr.WrapErrServiceUnavailable("WAL object retention catalog has no transaction capacity")
	}
	for start := 0; start < len(objects); start += batchSize {
		batch := objects[start:min(start+batchSize, len(objects))]
		keys := make([]string, len(batch))
		for i, object := range batch {
			keys[i] = retentionKey(channel, "deletions", object)
		}
		values, err := c.metaKV.MultiLoad(ctx, keys)
		if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
			return err
		}
		if len(values) != len(keys) {
			return merr.WrapErrDataIntegrityMsg("WAL object deletion lookup returned an incomplete result")
		}
		for _, value := range values {
			if value != "" {
				return merr.WrapErrServiceUnavailable("WAL object is already authorized for deletion; recapture the baseline")
			}
		}
	}
	return nil
}

// Released IDs and completed deletion intents are permanent fences. They must
// not be removed by an age-based sweep: a paused old publisher/delete request
// can still arrive. Compaction requires a separately proven generation fence.
func (c *catalog) ReleaseWALObjects(ctx context.Context, channel, rootID string) error {
	if rootID == "" {
		return merr.WrapErrServiceInternalMsg("empty WAL object retention root ID")
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return err
	}
	// Record release even when acquisition was canceled before its response.
	// A delayed acquisition then either loses epoch CAS or sees this fence.
	return c.commitRetention(ctx, channel, epoch, map[string]string{retentionKey(channel, "released", rootID): rootID}, []string{retentionKey(channel, "roots", rootID)})
}

func (c *catalog) ListWALObjectRetentionRoots(ctx context.Context, channel string) ([]*streamingpb.WALObjectRetentionRoot, error) {
	roots := make([]*streamingpb.WALObjectRetentionRoot, 0)
	size := 0
	err := c.metaKV.WalkWithPrefix(ctx, retentionPrefix(channel)+"roots/", 128, func(_, value []byte) error {
		size += len(value)
		if len(value) > maxRetentionRootBytes || len(roots) >= maxRetentionRoots || size > maxRetentionBytes {
			return merr.WrapErrServiceUnavailable("WAL object retention catalog exceeds its read budget")
		}
		root := &streamingpb.WALObjectRetentionRoot{}
		if err := proto.Unmarshal(value, root); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode WAL object retention root")
		}
		if root.GetRootId() == "" || root.Released || len(root.ObjectKeys) == 0 || len(root.ObjectKeys) > maxRetentionObjects {
			return merr.WrapErrDataIntegrityMsg("invalid active WAL object retention root")
		}
		roots = append(roots, root)
		return nil
	})
	return roots, err
}

func (c *catalog) loadObjectDeletion(ctx context.Context, channel, object string) (*streamingpb.WALObjectDeletion, error) {
	value, err := c.loadRetentionValue(ctx, retentionKey(channel, "deletions", object))
	if err != nil || value == "" {
		return nil, err
	}
	intent := &streamingpb.WALObjectDeletion{}
	if err := proto.Unmarshal([]byte(value), intent); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode WAL object deletion")
	}
	if intent.ObjectKey != object || intent.OperationId == "" {
		return nil, merr.WrapErrDataIntegrityMsg("WAL object deletion identity mismatch")
	}
	return intent, nil
}

func (c *catalog) BeginWALObjectDeletion(ctx context.Context, channel string, term int64, object string) (*streamingpb.WALObjectDeletion, error) {
	if object == "" || term <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("WAL object deletion requires an object and writer term")
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return nil, err
	}
	if intent, err := c.loadObjectDeletion(ctx, channel, object); intent != nil || err != nil {
		return intent, err
	}
	checkpointKey := buildConsumeCheckpointKey(channel)
	checkpoint, err := c.metaKV.Load(ctx, checkpointKey)
	if err != nil {
		return nil, err
	}
	cp := &streamingpb.WALCheckpoint{}
	if err := proto.Unmarshal([]byte(checkpoint), cp); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode WAL deletion owner checkpoint")
	}
	if cp.Term != term {
		return nil, merr.WrapErrServiceUnavailable("WAL object deletion writer was fenced")
	}
	roots, err := c.ListWALObjectRetentionRoots(ctx, channel)
	if err != nil {
		return nil, err
	}
	for _, root := range roots {
		if slices.Contains(root.ObjectKeys, object) {
			return nil, nil
		}
	}
	intent := &streamingpb.WALObjectDeletion{ObjectKey: object, OperationId: uuid.NewString()}
	encoded, err := proto.Marshal(intent)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "encode WAL object deletion")
	}
	if err := c.commitRetention(ctx, channel, epoch, map[string]string{
		retentionKey(channel, "deletions", object): string(encoded),
		retentionKey(channel, "pending", object):   string(encoded),
	}, nil, predicates.ValueEqual(checkpointKey, checkpoint)); err != nil {
		return nil, err
	}
	return intent, nil
}

// Pending operations have their own index so crash recovery does not scan the
// permanent Done fences. Callers complete a page before asking for the next.
func (c *catalog) ListPendingWALObjectDeletions(ctx context.Context, channel string, limit int) ([]*streamingpb.WALObjectDeletion, error) {
	if limit <= 0 || limit > maxRetentionObjects {
		return nil, merr.WrapErrServiceInternalMsg("invalid WAL object deletion page limit")
	}
	operations := make([]*streamingpb.WALObjectDeletion, 0, limit)
	err := c.metaKV.WalkWithPrefix(ctx, retentionPrefix(channel)+"pending/", min(128, limit), func(_, value []byte) error {
		if len(operations) == limit {
			return errRetentionPageFull
		}
		intent := &streamingpb.WALObjectDeletion{}
		if err := proto.Unmarshal(value, intent); err != nil {
			return merr.WrapErrDataIntegrity(err, "decode pending WAL object deletion")
		}
		if intent.ObjectKey == "" || intent.OperationId == "" || intent.Done {
			return merr.WrapErrDataIntegrityMsg("invalid pending WAL object deletion")
		}
		operations = append(operations, intent)
		return nil
	})
	if errors.Is(err, errRetentionPageFull) {
		err = nil
	}
	return operations, err
}

func (c *catalog) CompleteWALObjectDeletion(ctx context.Context, channel string, operation *streamingpb.WALObjectDeletion) error {
	if operation.GetObjectKey() == "" || operation.GetOperationId() == "" {
		return merr.WrapErrServiceInternalMsg("invalid WAL object deletion completion")
	}
	epoch, err := c.loadRetentionValue(ctx, retentionPrefix(channel)+"epoch")
	if err != nil {
		return err
	}
	stored, err := c.loadObjectDeletion(ctx, channel, operation.ObjectKey)
	if err != nil {
		return err
	}
	if stored == nil || stored.OperationId != operation.OperationId {
		return merr.WrapErrDataIntegrityMsg("WAL object deletion completion has no matching authorization")
	}
	if stored.Done {
		return nil
	}
	stored.Done = true
	encoded, err := proto.Marshal(stored)
	if err != nil {
		return merr.WrapErrDataIntegrity(err, "encode WAL object deletion completion")
	}
	return c.commitRetention(ctx, channel, epoch, map[string]string{retentionKey(channel, "deletions", operation.ObjectKey): string(encoded)}, []string{retentionKey(channel, "pending", operation.ObjectKey)})
}
