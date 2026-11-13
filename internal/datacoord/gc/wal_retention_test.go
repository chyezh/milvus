package gc

import (
	"context"
	"path"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	kvstreamingnode "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func sharedWALGC(t *testing.T) (*GarbageCollector, *viewpb.StreamingWorkNode) {
	t.Helper()
	client, _ := kvfactory.GetEtcdAndPath()
	root := "wal-shared-gc-" + uuid.NewString()
	kv := etcdkv.NewEtcdKV(client, root)
	t.Cleanup(func() { require.NoError(t, kv.RemoveWithPrefix(context.Background(), "")) })
	objects := t.TempDir()
	cli := storage.NewLocalChunkManager(objectstorage.RootPath(objects))
	catalog := kvdatacoord.NewCatalog(kv, objects, root)
	store := metacache.NewMetaStore(catalog)
	m := &meta.Meta{Ctx: t.Context(), Catalog: catalog, Segments: meta.NewSegmentsInfo(store), MetaStore: store}
	gc := NewGarbageCollector(m, nil, GcOption{Cli: cli, StreamingNodeCatalog: kvstreamingnode.NewCataLog(kv)})
	t.Cleanup(gc.Close)
	runtime := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 1, RuntimeId: 2, WalReplicaId: 3, WalReplicaRevision: 4, WalTerm: 5, ReadOnly: true}
	require.NoError(t, gc.Option.StreamingNodeCatalog.(metastore.WALReplicaRuntimeCatalog).CreateWALReplicaRuntime(t.Context(), runtime))
	return gc, runtime
}

func TestSharedWALRetentionGuardsDroppedSegmentPrefix(t *testing.T) {
	gc, runtime := sharedWALGC(t)
	ctx := t.Context()
	base := path.Join(gc.Option.Cli.RootPath(), "insert_log/1/2/3")
	file := path.Join(base, "_data/data.parquet")
	require.NoError(t, gc.Option.Cli.Write(ctx, file, []byte("lazy growing base")))
	segment := meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Dropped,
		StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath(base, 1),
	})
	require.NoError(t, gc.Meta.AddSegment(ctx, segment))
	catalog := gc.Option.StreamingNodeCatalog.(metastore.WALSharedObjectRetentionCatalog)
	require.NoError(t, catalog.RetainSharedWALObjects(ctx, &streamingpb.WALObjectRetentionRoot{RootId: "baseline", Runtime: runtime, ObjectKeys: []string{file}}))
	gc.RecycleDroppedSegment(ctx, nil, 3)
	require.NotNil(t, gc.Meta.GetSegment(ctx, 3), "retained file must prevent metadata retirement")
	data, err := gc.Option.Cli.Read(ctx, file)
	require.NoError(t, err)
	require.Equal(t, "lazy growing base", string(data))
	require.NoError(t, gc.Option.StreamingNodeCatalog.(metastore.WALReplicaRuntimeCatalog).ReleaseWALReplicaRuntime(ctx, runtime))
	require.NoError(t, gc.maintainSharedWALRetention(ctx))
	gc.RecycleDroppedSegment(ctx, nil, 3)
	require.Nil(t, gc.Meta.GetSegment(ctx, 3))
	exists, err := gc.Option.Cli.Exist(ctx, file)
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, gc.removeObjectsWithPrefix(ctx, path.Join(base, "absent")))
}

func TestSharedWALGCResumesAmbiguousDeletion(t *testing.T) {
	gc, runtime := sharedWALGC(t)
	ctx := t.Context()
	file := path.Join(gc.Option.Cli.RootPath(), "stats/old")
	require.NoError(t, gc.Option.Cli.Write(ctx, file, []byte("stats")))
	catalog := gc.Option.StreamingNodeCatalog.(metastore.WALSharedObjectRetentionCatalog)
	fail := mockey.Mock(mockey.GetMethod(catalog, "CompleteSharedWALObjectDeletion")).Return(merr.ErrServiceUnavailable).Build()
	require.ErrorIs(t, gc.removeObject(ctx, file), merr.ErrServiceUnavailable)
	fail.UnPatch()
	exists, err := gc.Option.Cli.Exist(ctx, file)
	require.NoError(t, err)
	require.False(t, exists)
	pending, err := catalog.ListPendingSharedWALObjectDeletions(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1, "physical deletion can succeed before its metadata response")
	require.Error(t, catalog.RetainSharedWALObjects(ctx, &streamingpb.WALObjectRetentionRoot{RootId: "late", Runtime: runtime, ObjectKeys: []string{file}}))
	restarted := &GarbageCollector{Option: gc.Option}
	require.NoError(t, restarted.maintainSharedWALRetention(ctx))
	pending, err = catalog.ListPendingSharedWALObjectDeletions(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, pending)
	fail = mockey.Mock((*storage.LocalChunkManager).Remove).Return(merr.ErrServiceUnavailable).Build()
	require.NoError(t, restarted.removeObject(ctx, file), "Done must not send another physical delete")
	fail.UnPatch()
}

func TestSharedWALGCFailsClosed(t *testing.T) {
	gc, _ := sharedWALGC(t)
	ctx := t.Context()
	catalog := gc.Option.StreamingNodeCatalog.(metastore.WALSharedObjectRetentionCatalog)
	file := path.Join(gc.Option.Cli.RootPath(), "data/file")
	require.NoError(t, gc.Option.Cli.Write(ctx, file, []byte("data")))
	fail := mockey.Mock(mockey.GetMethod(catalog, "BeginSharedWALObjectDeletion")).Return(nil, merr.ErrServiceUnavailable).Build()
	require.ErrorIs(t, gc.removeObjectsWithPrefix(ctx, path.Dir(file)), merr.ErrServiceUnavailable)
	fail.UnPatch()
	exists, err := gc.Option.Cli.Exist(ctx, file)
	require.NoError(t, err)
	require.True(t, exists)
	fail = mockey.Mock((*storage.LocalChunkManager).Remove).Return(merr.ErrServiceUnavailable).Build()
	require.ErrorIs(t, gc.removeObject(ctx, file), merr.ErrServiceUnavailable)
	require.ErrorIs(t, gc.maintainSharedWALRetention(ctx), merr.ErrServiceUnavailable)
	fail.UnPatch()
	for _, method := range []string{"SweepReleasedSharedWALRoots", "ListPendingSharedWALObjectDeletions"} {
		var patch *mockey.Mocker
		if method == "SweepReleasedSharedWALRoots" {
			patch = mockey.Mock(mockey.GetMethod(catalog, method)).Return(merr.ErrServiceUnavailable).Build()
		} else {
			patch = mockey.Mock(mockey.GetMethod(catalog, method)).Return(nil, merr.ErrServiceUnavailable).Build()
		}
		require.ErrorIs(t, gc.maintainSharedWALRetention(ctx), merr.ErrServiceUnavailable)
		patch.UnPatch()
	}
	fail = mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).Return(merr.ErrServiceUnavailable).Build()
	require.ErrorIs(t, gc.removeObjectsWithPrefix(ctx, path.Dir(file)), merr.ErrServiceUnavailable)
	fail.UnPatch()
	require.NoError(t, gc.maintainSharedWALRetention(ctx))
	gc.Option.StreamingNodeCatalog = nil
	require.NoError(t, gc.removeObject(ctx, file))
	require.NoError(t, gc.removeObjectsWithPrefix(ctx, path.Dir(file)))
	require.NoError(t, gc.maintainSharedWALRetention(ctx))
}
