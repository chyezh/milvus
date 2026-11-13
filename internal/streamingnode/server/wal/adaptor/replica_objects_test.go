package adaptor

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	kvstreamingnode "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newReplicaObjectRetentionForTest(t *testing.T) *replicaObjectRetention {
	t.Helper()
	client, _ := kvfactory.GetEtcdAndPath()
	store := etcdkv.NewEtcdKV(client, "replica-objects-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")) })
	catalog := kvstreamingnode.NewCataLog(store)
	runtime := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 1, RuntimeId: 2, WalReplicaId: 3, WalReplicaRevision: 4, WalTerm: 5, ReadOnly: true}
	require.NoError(t, catalog.(metastore.WALReplicaRuntimeCatalog).CreateWALReplicaRuntime(t.Context(), runtime))
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	r := newReplicaObjectRetention(catalog.(replicaObjectCatalog), runtime, cm, &indexpb.StorageConfig{StorageType: "local", RootPath: root})
	t.Cleanup(r.Close)
	return r
}

func TestReplicaObjectRetentionTransfersSnapshotOwnership(t *testing.T) {
	r := newReplicaObjectRetentionForTest(t)
	ctx := t.Context()
	view := replicaRetentionView(1)
	view.StreamingNode.PartitionStats = []*viewpb.ClusteredPartitionStats{{PartitionId: 10, Version: 7}, {PartitionId: 11}}
	segment := &streamingpb.SegmentAssignmentMeta{
		CollectionId: 1, PartitionId: 10, SegmentId: 100,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			Binlogs: []*streamingpb.L1SegmentBinLogs{{
				FieldBinlog: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 11}}}},
				StatsBinlog: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "stats"}}}},
				Bm25Binlog:  []*datapb.FieldBinlog{{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: "bm25"}}}},
			}}, DeltaBinlog: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "delta"}}}},
			MergedStatsBinlog: &datapb.FieldBinlog{Binlogs: []*datapb.Binlog{{LogPath: "merged"}}},
		},
	}
	image := &streamingpb.WALReplicaSnapshot{Segments: []*streamingpb.SegmentAssignmentMeta{segment}, QueryViews: []*viewpb.QueryViewOfShard{view}}
	require.NoError(t, r.RetainSnapshot(ctx, image))
	insert, err := binlog.BuildLogPathWithRootPath(r.rootPath, storage.InsertBinlog, 1, 10, 100, 101, 11)
	require.NoError(t, err)
	stats := path.Join(r.rootPath, common.PartitionStatsPath, "1/10/p_1v0/7")
	for _, key := range []string{insert, "stats", "bm25", "delta", "merged", stats} {
		operation, err := r.catalog.BeginSharedWALObjectDeletion(ctx, key)
		require.NoError(t, err)
		require.Nil(t, operation, key)
	}
	roots, err := r.catalog.ListSharedWALObjectRoots(ctx)
	require.NoError(t, err)
	require.Len(t, roots, 2)
	// Source-side Close ends transport ownership only. Receiver retries use
	// the same immutable root identities and take no additional catalog space.
	r.Close()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(r.rootPath))
	receiver := newReplicaObjectRetention(r.catalog, r.runtime, cm, r.config)
	defer receiver.Close()
	require.NoError(t, receiver.RetainSnapshot(ctx, image))
	require.NoError(t, receiver.RetainSnapshot(ctx, image))
	again, err := r.catalog.ListSharedWALObjectRoots(ctx)
	require.NoError(t, err)
	require.Len(t, again, len(roots))
	require.Equal(t, 2, receiver.roots)
	require.NoError(t, r.catalog.(metastore.WALReplicaRuntimeCatalog).ReleaseWALReplicaRuntime(ctx, r.runtime))
	require.NoError(t, r.catalog.SweepReleasedSharedWALRoots(ctx, 10))
	operation, err := r.catalog.BeginSharedWALObjectDeletion(ctx, insert)
	require.NoError(t, err)
	require.NotNil(t, operation)
	require.Error(t, r.RetainSnapshot(ctx, image))
	require.Error(t, r.RetainViewStats(ctx, nil))
}

func TestReplicaObjectRetentionProtectsManifestAndLivePublications(t *testing.T) {
	r := newReplicaObjectRetentionForTest(t)
	ctx := t.Context()
	base := path.Join(r.rootPath, "insert_log/1/2/3")
	file := path.Join(base, "_data/100.parquet")
	manifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, packed.ManifestEarliest), r.config, &packed.ManifestUpdates{
		ColumnGroups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: file, EndIndex: 5}}}},
	})
	require.NoError(t, err)
	segment := &streamingpb.SegmentAssignmentMeta{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{ManifestPath: manifest}}
	image := &streamingpb.WALReplicaSnapshot{Segments: []*streamingpb.SegmentAssignmentMeta{segment}, Storage: &streamingpb.WALReplicaStoragePublication{
		Summary: &streamingpb.PChannelSummaryManifest{Chunks: []*streamingpb.PChannelSummaryChunkIndexEntry{{Generation: 1, Term: 5}}},
	}}
	require.NoError(t, r.RetainSnapshot(ctx, image))
	require.NoError(t, r.PrepareEvent(ctx, walevent.Event{Payload: walevent.StoragePublished{Catalog: &streamingpb.WALReplicaCatalogPublication{Segments: []*streamingpb.SegmentAssignmentMeta{segment}}}}))
	require.Len(t, r.manifests, 1)
	operation, err := r.catalog.BeginSharedWALObjectDeletion(ctx, file)
	require.NoError(t, err)
	require.Nil(t, operation)
	roots, err := r.catalog.ListWALObjectRetentionRoots(ctx, "p")
	require.NoError(t, err)
	require.Len(t, roots, 1)
	require.Equal(t, []string{r.store.ChunkKey(1)}, roots[0].ObjectKeys)
	view := replicaRetentionView(2)
	view.Meta.State = viewpb.QueryViewState_QueryViewStateDown
	require.NoError(t, r.PrepareEvent(ctx, walevent.Event{Payload: walevent.QueryViewIntentBatch{Views: []*viewpb.QueryViewOfShard{view}}}))
	require.NoError(t, r.PrepareEvent(ctx, walevent.Event{Payload: walevent.CheckpointPublished{}}))
	segment.PersistedStorage.ManifestPath = "invalid"
	require.Error(t, r.RetainStorage(ctx, &streamingpb.WALReplicaStoragePublication{Segments: []*streamingpb.SegmentAssignmentMeta{segment}}))
	_, err = r.catalog.BeginSharedWALObjectDeletion(ctx, "already-retired")
	require.NoError(t, err)
	segment.PersistedStorage = &streamingpb.L1SegmentPersistedStorage{DeltaBinlog: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: "already-retired"}}}}}
	require.Error(t, r.RetainStorage(ctx, &streamingpb.WALReplicaStoragePublication{Segments: []*streamingpb.SegmentAssignmentMeta{segment}}))
	require.NotContains(t, r.shared, "already-retired", "failed acquisition never grants local ownership")
}

func TestReplicaObjectRetentionBoundedRecordsAndLifetime(t *testing.T) {
	r := newReplicaObjectRetentionForTest(t)
	keys := make([]string, metastore.WALObjectRetentionRootMaxObjects+1)
	for i := range keys {
		keys[i] = fmt.Sprint(i)
	}
	roots, err := replicaObjectRoots(r.runtime, "shared", keys)
	require.NoError(t, err)
	require.Len(t, roots, 2)
	for _, root := range roots {
		require.LessOrEqual(t, proto.Size(root), metastore.WALObjectRetentionRootMaxBytes)
	}
	large := strings.Repeat("x", 300000)
	roots, err = replicaObjectRoots(r.runtime, "shared", []string{large, large + "y"})
	require.NoError(t, err)
	require.Len(t, roots, 2)
	_, err = replicaObjectRoots(r.runtime, "shared", []string{strings.Repeat("x", metastore.WALObjectRetentionRootMaxBytes)})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	_, err = replicaObjectRoots(r.runtime, "shared", []string{"\xff"})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorIs(t, r.retain(t.Context(), "shared", r.shared, []string{""}), merr.ErrDataIntegrity)
	r.bytes = 64 << 20
	require.ErrorIs(t, r.retain(t.Context(), "shared", r.shared, []string{"new"}), merr.ErrServiceUnavailable)
	r.bytes = 0
	r.roots = maxReplicaObjectRoots
	require.ErrorIs(t, r.retain(t.Context(), "shared", r.shared, []string{"new"}), merr.ErrServiceUnavailable)
	require.Empty(t, r.shared)
}

func TestReplicaObjectRetentionProtectsLazyIDFResources(t *testing.T) {
	r := newReplicaObjectRetentionForTest(t)
	response := &datapb.GetStreamingNodeQueryViewResourcesResponse{
		CollectionId: 1,
		Bm25Resources: []*datapb.StreamingNodeBM25Resource{{
			SegmentId: 3, PartitionId: 2,
			Bm25Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{LogPath: "sealed-bm25"}}}},
		}},
	}
	require.NoError(t, r.RetainQueryResources(t.Context(), response))
	operation, err := r.catalog.BeginSharedWALObjectDeletion(t.Context(), "sealed-bm25")
	require.NoError(t, err)
	require.Nil(t, operation)
	replica := &readOnlyReplica{dependencyRefresh: r.refresh.Load}
	require.False(t, replica.ReplicaRefreshRequired())
	r.roots = maxReplicaObjectRoots
	response.Bm25Resources[0].Bm25Binlogs[0].Binlogs[0].LogPath = "new-sealed-bm25"
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, r.RetainQueryResources(ctx, response))
	require.False(t, replica.ReplicaRefreshRequired(), "query cancellation does not replace a healthy runtime")
	require.Error(t, r.RetainQueryResources(t.Context(), response))
	require.True(t, replica.ReplicaRefreshRequired())
	operation, err = r.catalog.BeginSharedWALObjectDeletion(t.Context(), "sealed-bm25")
	require.NoError(t, err)
	require.Nil(t, operation, "replacement must preserve the old query dependencies")
}

func TestReplicaObjectRetentionCompactsWithoutReleasingObjects(t *testing.T) {
	r := newReplicaObjectRetentionForTest(t)
	ctx := t.Context()
	for i := range maxReplicaObjectRoots * 2 {
		namespace, retained := "summary", r.summary
		if i%2 == 0 {
			namespace, retained = "shared", r.shared
		}
		require.NoError(t, r.retain(ctx, namespace, retained, []string{fmt.Sprint(i)}))
		require.LessOrEqual(t, r.roots, maxReplicaObjectRoots)
	}
	require.Positive(t, r.compaction)
	require.False(t, r.refresh.Load())
	for _, namespace := range []string{"summary", "shared"} {
		var roots []*streamingpb.WALObjectRetentionRoot
		var err error
		if namespace == "summary" {
			roots, err = r.catalog.ListWALObjectRetentionRoots(ctx, "p")
		} else {
			roots, err = r.catalog.ListSharedWALObjectRoots(ctx)
		}
		require.NoError(t, err)
		all := make(map[string]bool)
		for _, root := range roots {
			require.True(t, proto.Equal(r.runtime, root.Runtime))
			for _, key := range root.ObjectKeys {
				all[key] = true
			}
		}
		for i := range maxReplicaObjectRoots * 2 {
			require.Equal(t, (i%2 == 0) == (namespace == "shared"), all[fmt.Sprint(i)])
		}
	}
}

func TestReplicaObjectCompactionFailurePreservesOwnership(t *testing.T) {
	for _, namespace := range []string{"summary", "shared"} {
		for _, failAcquire := range []bool{false, true} {
			mockey.PatchConvey("replacement roots precede retirement and retry preserves every key", t, func() {
				r := newReplicaObjectRetentionForTest(t)
				ctx := t.Context()
				retained := r.summary
				acquire, release := "RetainWALObjects", "ReleaseWALObjects"
				if namespace == "shared" {
					retained = r.shared
					acquire, release = "RetainSharedWALObjects", "ReleaseSharedWALObjects"
				}
				require.NoError(t, r.retain(ctx, namespace, retained, []string{"a"}))
				require.NoError(t, r.retain(ctx, namespace, retained, []string{"b"}))
				method := release
				if failAcquire {
					method = acquire
				}
				patch := mockey.Mock(mockey.GetMethod(r.catalog, method)).Return(merr.ErrServiceUnavailable).Build()
				require.ErrorIs(t, r.compactRoots(ctx), merr.ErrServiceUnavailable)
				patch.UnPatch()
				require.NoError(t, r.compactRoots(ctx))
				require.Equal(t, 1, r.roots)
				require.Len(t, retained, 2)
				if namespace == "shared" {
					for _, key := range []string{"a", "b"} {
						intent, err := r.catalog.BeginSharedWALObjectDeletion(ctx, key)
						require.NoError(t, err)
						require.Nil(t, intent)
					}
				}
			})
		}
	}
}
