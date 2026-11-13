package walsummary

import (
	"context"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/storage"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func retainedTestStore(t *testing.T) (*Store, metastore.StreamingNodeCataLog, metastore.WALObjectRetentionCatalog) {
	t.Helper()
	paramtable.Init()
	client, _ := kvfactory.GetEtcdAndPath()
	kv := etcdkv.NewEtcdKV(client, "summary-retention-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, kv.RemoveWithPrefix(context.Background(), "")) })
	catalog := catalogkv.NewCataLog(kv)
	s := newTestStore(t)
	require.NoError(t, catalog.SaveRecoverySnapshot(t.Context(), s.pchannel, &metastore.WALRecoverySnapshot{ConsumeCheckpoint: &streamingpb.WALCheckpoint{Term: 1}}))
	retention := catalog.(metastore.WALObjectRetentionCatalog)
	return NewStore(s.chunkManager, s.pchannel, 1, WithRetentionCatalog(retention)), catalog, retention
}

func TestSummaryDeletionHonorsRemoteRootsAndOwnerFence(t *testing.T) {
	s, catalog, retention := retainedTestStore(t)
	ctx := t.Context()
	key := s.ChunkKey(1)
	require.NoError(t, s.chunkManager.Write(ctx, key, []byte("retained")))
	root := &streamingpb.WALObjectRetentionRoot{RootId: "ro-runtime", ObjectKeys: []string{key}}
	require.NoError(t, retention.RetainWALObjects(ctx, s.pchannel, root))
	require.NoError(t, s.DeleteChunk(ctx, 1, 1))
	require.NoError(t, s.RemoveAllObjects(ctx), "prefix cleanup must also honor exact object pins")
	payload, err := s.chunkManager.Read(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "retained", string(payload))
	require.Equal(t, []string{key}, s.ObjectKeys(&streamingpb.PChannelSummaryManifest{Chunks: []*streamingpb.PChannelSummaryChunkIndexEntry{{Generation: 1, Term: 1}}}))
	require.Empty(t, s.ObjectKeys(nil))
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, s.pchannel, &metastore.WALRecoverySnapshot{ConsumeCheckpoint: &streamingpb.WALCheckpoint{Term: 2}}))
	require.NoError(t, retention.ReleaseWALObjects(ctx, s.pchannel, root.RootId))
	require.Error(t, s.DeleteChunk(ctx, 1, 1), "a stale owner cannot start deletion")
	next := NewStore(s.chunkManager, s.pchannel, 2, WithRetentionCatalog(retention))
	require.NoError(t, next.DeleteChunk(ctx, 1, 1))
	_, err = s.chunkManager.Read(ctx, key)
	require.Error(t, err)
	require.NoError(t, next.DeleteChunk(ctx, 1, 1), "completed deletion is idempotent")
	require.Error(t, retention.RetainWALObjects(ctx, s.pchannel, &streamingpb.WALObjectRetentionRoot{RootId: "late", ObjectKeys: []string{key}}))
}

func TestSummaryDeletionResumesAfterPhysicalOrCompletionFailure(t *testing.T) {
	for _, failure := range []string{"delete", "complete"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("a replacement owner resumes the same durable deletion", t, func() {
				s, catalog, retention := retainedTestStore(t)
				ctx := t.Context()
				key := s.ChunkKey(1)
				require.NoError(t, s.chunkManager.Write(ctx, key, []byte("unreferenced")))
				var patch *mockey.Mocker
				if failure == "delete" {
					patch = mockey.Mock((*storage.LocalChunkManager).Remove).Return(merr.ErrServiceUnavailable).Build()
				} else {
					patch = mockey.Mock(mockey.GetMethod(retention, "CompleteWALObjectDeletion")).Return(merr.ErrServiceUnavailable).Build()
				}
				require.ErrorIs(t, s.DeleteChunk(ctx, 1, 1), merr.ErrServiceUnavailable)
				patch.UnPatch()
				pending, err := retention.ListPendingWALObjectDeletions(ctx, s.pchannel, 1)
				require.NoError(t, err)
				require.Len(t, pending, 1)
				require.NoError(t, catalog.SaveRecoverySnapshot(ctx, s.pchannel, &metastore.WALRecoverySnapshot{ConsumeCheckpoint: &streamingpb.WALCheckpoint{Term: 2}}))
				next := NewStore(s.chunkManager, s.pchannel, 2, WithRetentionCatalog(retention))
				require.NoError(t, next.ResumeDeletions(ctx, 1))
				pending, err = retention.ListPendingWALObjectDeletions(ctx, s.pchannel, 1)
				require.NoError(t, err)
				require.Empty(t, pending)
				_, err = s.chunkManager.Read(ctx, key)
				require.Error(t, err)
			})
		})
	}
}

func TestSummaryRetentionFailuresDoNotDelete(t *testing.T) {
	mockey.PatchConvey("catalog failures preserve objects", t, func() {
		s, _, retention := retainedTestStore(t)
		key := s.ChunkKey(1)
		require.NoError(t, s.chunkManager.Write(t.Context(), key, []byte("retain")))
		patch := mockey.Mock(mockey.GetMethod(retention, "ListPendingWALObjectDeletions")).Return(nil, merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, s.ResumeDeletions(t.Context(), 1), merr.ErrServiceUnavailable)
		patch.UnPatch()
		for _, method := range []string{"FenceLostWALReplicaRuntimes", "SweepReleasedWALReplicaRoots"} {
			patch = mockey.Mock(mockey.GetMethod(retention, method)).Return(merr.ErrServiceUnavailable).Build()
			require.ErrorIs(t, s.ResumeDeletions(t.Context(), 1), merr.ErrServiceUnavailable)
			patch.UnPatch()
		}
		intent, err := retention.BeginWALObjectDeletion(t.Context(), s.pchannel, 1, key)
		require.NoError(t, err)
		require.NotNil(t, intent)
		patch = mockey.Mock((*storage.LocalChunkManager).Remove).Return(merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, s.ResumeDeletions(t.Context(), 1), merr.ErrServiceUnavailable)
		require.ErrorIs(t, s.RemoveAllObjects(t.Context()), merr.ErrServiceUnavailable)
		patch.UnPatch()
		patch = mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).Return(merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, s.RemoveAllObjects(t.Context()), merr.ErrServiceUnavailable)
		patch.UnPatch()
	})
}

func TestSummaryRecoveryRootsSurviveFailedPublicationAndTakeover(t *testing.T) {
	mockey.PatchConvey("a new owner adopts all dependencies before retiring recovery roots", t, func() {
		s, catalog, retention := retainedTestStore(t)
		ctx := t.Context()
		m := newTestManager(t, s, 1<<30)
		require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
		require.NoError(t, drainSummary(ctx, m))
		remote := &streamingpb.WALObjectRetentionRoot{RootId: "ro-runtime", ObjectKeys: []string{s.ChunkKey(0)}}
		require.NoError(t, retention.RetainWALObjects(ctx, s.pchannel, remote))
		require.NoError(t, stageChunk(t, m, 200).Execute(ctx))
		patch := mockey.Mock((*storage.LocalChunkManager).Write).Return(merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, s.WriteManifest(ctx, m.Manifest()), merr.ErrServiceUnavailable)
		patch.UnPatch()
		roots, err := retention.ListWALObjectRetentionRoots(ctx, s.pchannel)
		require.NoError(t, err)
		require.Len(t, roots, 3, "previous image, failed preparation, and remote runtime remain protected")
		require.NoError(t, catalog.SaveRecoverySnapshot(ctx, s.pchannel, &metastore.WALRecoverySnapshot{ConsumeCheckpoint: &streamingpb.WALCheckpoint{Term: 2}}))
		next := NewStore(s.chunkManager, s.pchannel, 2, WithRetentionCatalog(retention))
		recovered := newTestManager(t, next, 1<<30)
		require.NoError(t, recovered.Restore(ctx))
		require.NoError(t, drainSummary(ctx, recovered))
		roots, err = retention.ListWALObjectRetentionRoots(ctx, s.pchannel)
		require.NoError(t, err)
		require.Len(t, roots, 2, "only the new recovery image and remote runtime remain")
		recovered.cfg.MaxRetainedChunks = 1
		require.NoError(t, gcSummary(ctx, recovered))
		exists, err := s.chunkManager.Exist(ctx, s.ChunkKey(0))
		require.NoError(t, err)
		require.True(t, exists, "writer retirement cannot override a remote runtime")
		require.NoError(t, retention.ReleaseWALObjects(ctx, s.pchannel, remote.RootId))
		require.NoError(t, gcSummary(ctx, recovered))
		exists, err = s.chunkManager.Exist(ctx, s.ChunkKey(0))
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestSummaryPinnedObjectsDoNotConsumeDeletionBudget(t *testing.T) {
	s, _, retention := retainedTestStore(t)
	ctx := t.Context()
	for _, generation := range []uint64{0, 1} {
		require.NoError(t, s.chunkManager.Write(ctx, s.ChunkKey(generation), []byte("object")))
	}
	require.NoError(t, retention.RetainWALObjects(ctx, s.pchannel, &streamingpb.WALObjectRetentionRoot{RootId: "reader", ObjectKeys: []string{s.ChunkKey(0)}}))
	deleted, _, err := s.sweepGarbage(ctx, 1, &streamingpb.SummaryCoverage{Generation: 1}, nil, 1)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	exists, err := s.chunkManager.Exist(ctx, s.ChunkKey(1))
	require.NoError(t, err)
	require.False(t, exists, "retained objects cannot starve later garbage")
}

func TestSummaryRecoveryRootFailureIsFailClosed(t *testing.T) {
	mockey.PatchConvey("no manifest is exposed without its durable dependencies", t, func() {
		s, _, retention := retainedTestStore(t)
		manifest := &streamingpb.PChannelSummaryManifest{}
		patch := mockey.Mock(mockey.GetMethod(retention, "RetainWALRecoveryObjects")).Return(merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, s.WriteManifest(t.Context(), manifest), merr.ErrServiceUnavailable)
		require.ErrorIs(t, newTestManager(t, s, 1<<30).Restore(t.Context()), merr.ErrServiceUnavailable)
		patch.UnPatch()
		patch = mockey.Mock(mockey.GetMethod(retention, "PublishWALSummary")).Return(merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, s.WriteManifest(t.Context(), manifest), merr.ErrServiceUnavailable)
		patch.UnPatch()
		patch = mockey.Mock(mockey.GetMethod(retention, "ListWALObjectRetentionRoots")).Return(nil, merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, s.WriteManifest(t.Context(), manifest), merr.ErrServiceUnavailable)
		patch.UnPatch()
		orphan := &streamingpb.WALObjectRetentionRoot{RootId: "summary-recovery/1/orphan", ObjectKeys: []string{s.ManifestKey()}}
		require.NoError(t, retention.RetainWALObjects(t.Context(), s.pchannel, orphan))
		for _, rootID := range []string{"summary-recovery/invalid", "summary-recovery/9/future"} {
			require.NoError(t, retention.RetainWALObjects(t.Context(), s.pchannel, &streamingpb.WALObjectRetentionRoot{RootId: rootID, ObjectKeys: []string{s.ManifestKey()}}))
		}
		require.NoError(t, s.WriteManifest(t.Context(), manifest))
	})
}

func TestSummaryRecoveryRootsBoundEachCatalogRecord(t *testing.T) {
	manifest := &streamingpb.PChannelSummaryManifest{}
	for i := range metastore.WALObjectRetentionRootMaxObjects + 1 {
		manifest.Chunks = append(manifest.Chunks, &streamingpb.PChannelSummaryChunkIndexEntry{Generation: uint64(i), Term: 1})
	}
	for _, prefix := range []string{"short", strings.Repeat("long", 500)} {
		s := NewStore(storage.NewLocalChunkManager(objectstorage.RootPath(prefix)), "p", 1)
		roots, err := s.manifestRoots(manifest, []byte("manifest"))
		require.NoError(t, err)
		require.Greater(t, len(roots), 1)
		seen := make(map[string]bool)
		for _, root := range roots {
			require.NotEmpty(t, root.ObjectKeys)
			require.LessOrEqual(t, len(root.ObjectKeys), metastore.WALObjectRetentionRootMaxObjects)
			require.LessOrEqual(t, proto.Size(root), metastore.WALObjectRetentionRootMaxBytes)
			for _, key := range root.ObjectKeys {
				require.False(t, seen[key])
				seen[key] = true
			}
		}
		require.Len(t, seen, len(manifest.Chunks)+1)
	}
	s := NewStore(storage.NewLocalChunkManager(objectstorage.RootPath(strings.Repeat("x", metastore.WALObjectRetentionRootMaxBytes))), "p", 1)
	_, err := s.manifestRoots(&streamingpb.PChannelSummaryManifest{}, nil)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
