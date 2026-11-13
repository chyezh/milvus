package walsummary

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"strconv"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const summaryRecoveryRootPrefix = "summary-recovery/"

// Retain the complete recovery image before publishing its manifest. The
// digest makes retries of one immutable image idempotent; generations/coverage
// never regress, so a retired image is never legitimately published again.
func (s *Store) retainManifest(ctx context.Context, manifest *streamingpb.PChannelSummaryManifest, encoded []byte) ([]*streamingpb.WALObjectRetentionRoot, error) {
	if s.retention == nil {
		return nil, nil
	}
	roots, err := s.manifestRoots(manifest, encoded)
	if err != nil {
		return nil, err
	}
	for _, root := range roots {
		if err := s.retention.RetainWALRecoveryObjects(ctx, s.pchannel, s.term, root); err != nil {
			return nil, err
		}
	}
	return roots, nil
}

// Split large manifests into bounded catalog records. All parts are acquired
// before the object write or any slot replacement, so moving an object between
// parts never creates a gap in its protection.
func (s *Store) manifestRoots(manifest *streamingpb.PChannelSummaryManifest, encoded []byte) ([]*streamingpb.WALObjectRetentionRoot, error) {
	baseID := fmt.Sprintf("%s%d/%x/", summaryRecoveryRootPrefix, s.term, sha256.Sum256(encoded))
	keys := append(s.ObjectKeys(manifest), s.manifestImageKey(encoded))
	var roots []*streamingpb.WALObjectRetentionRoot
	root := &streamingpb.WALObjectRetentionRoot{RootId: baseID + "0"}
	size := proto.Size(root)
	for _, key := range keys {
		entrySize := 1 + protowire.SizeBytes(len(key))
		if len(root.ObjectKeys) == metastore.WALObjectRetentionRootMaxObjects || size+entrySize > metastore.WALObjectRetentionRootMaxBytes {
			roots = append(roots, root)
			root = &streamingpb.WALObjectRetentionRoot{RootId: baseID + strconv.Itoa(len(roots))}
			size = proto.Size(root)
		}
		if size+entrySize > metastore.WALObjectRetentionRootMaxBytes {
			return nil, merr.WrapErrServiceInternalMsg("summary object key exceeds retention record budget")
		}
		root.ObjectKeys = append(root.ObjectKeys, key)
		size += entrySize
	}
	return append(roots, root), nil
}

func (s *Store) protectRecoveredManifest(ctx context.Context, manifest *streamingpb.PChannelSummaryManifest) error {
	encoded, err := marshalManifest(manifest)
	if err != nil {
		return err
	}
	_, err = s.retainManifest(ctx, manifest, encoded)
	return err
}

// Each publication has a content-addressed immutable image. Unit stores without
// a catalog retain their standalone manifest path; runtime stores always use
// the authoritative catalog pointer and never recover by listing these images.
func (s *Store) manifestImageKey(encoded []byte) string {
	if s.retention == nil {
		return s.ManifestKey()
	}
	return fmt.Sprintf("%s_%x", s.ManifestKey(), sha256.Sum256(encoded))
}

// ObjectKeys lists the immutable transitive dependencies of an in-memory
// manifest. Its encoded metadata travels in the replica snapshot, so the
// immutable manifest image itself is not a dependency of that cut.
func (s *Store) ObjectKeys(manifest *streamingpb.PChannelSummaryManifest) []string {
	keys := make([]string, 0, len(manifest.GetChunks()))
	for _, chunk := range manifest.GetChunks() {
		keys = append(keys, buildChunkKey(s.chunkManager, s.pchannel, chunk.GetGeneration(), chunk.GetTerm()))
	}
	return keys
}

func (s *Store) deleteObject(ctx context.Context, key string) (bool, error) {
	var intent *streamingpb.WALObjectDeletion
	if s.retention != nil {
		var err error
		intent, err = s.retention.BeginWALObjectDeletion(ctx, s.pchannel, s.term, key)
		if err != nil {
			return false, err
		}
		if intent == nil {
			return false, nil
		}
	}
	if err := s.chunkManager.Remove(ctx, key); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, merr.Wrapf(err, "failed to delete summary object %s", key)
	}
	if intent != nil {
		return true, s.retention.CompleteWALObjectDeletion(ctx, s.pchannel, intent)
	}
	return true, nil
}

// ResumeDeletions finishes exact operations whose physical delete or completion
// response was lost. No current-term reauthorization is needed: the durable
// operation already excludes every future root from referencing its key.
func (s *Store) ResumeDeletions(ctx context.Context, limit int) error {
	if s.retention == nil {
		return nil
	}
	if runtimes, ok := s.retention.(metastore.WALReplicaRuntimeCatalog); ok {
		if err := runtimes.FenceLostWALReplicaRuntimes(ctx, s.pchannel, limit); err != nil {
			return err
		}
		if err := runtimes.SweepReleasedWALReplicaRoots(ctx, s.pchannel, limit); err != nil {
			return err
		}
	}
	intents, err := s.retention.ListPendingWALObjectDeletions(ctx, s.pchannel, limit)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		if _, err := s.deleteObject(ctx, intent.ObjectKey); err != nil {
			return err
		}
	}
	return nil
}
