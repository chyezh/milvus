package adaptor

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

const (
	maxReplicaObjectKeys  = 100000
	maxReplicaObjectRoots = 128
)

type replicaObjectCatalog interface {
	metastore.WALObjectRetentionCatalog
	metastore.WALSharedObjectRetentionCatalog
}

// replicaObjectRetention protects all lazily loaded versions admitted by one
// local runtime. Incremental roots are compacted without releasing their objects.
// The owner replaces the runtime when its key/byte budget is reached; old roots are released only after its queries/loaders drain.
// Close releases the native executor, never the durable object obligations.
type replicaObjectRetention struct {
	refresh    atomic.Bool
	mu         sync.Mutex
	closed     bool
	catalog    replicaObjectCatalog
	runtime    *viewpb.StreamingWorkNode
	store      *walsummary.Store
	rootPath   string
	config     *indexpb.StorageConfig
	io         *packed.ManifestIOContext
	summary    map[string]struct{}
	shared     map[string]struct{}
	manifests  map[string]struct{}
	rootIDs    map[string]map[string]struct{}
	compaction uint64
	bytes      int
}

func (r *replicaObjectRetention) RetainQueryResources(ctx context.Context, response *datapb.GetStreamingNodeQueryViewResourcesResponse) error {
	publication := &streamingpb.WALReplicaStoragePublication{}
	for _, stats := range response.Bm25Resources {
		publication.Segments = append(publication.Segments, &streamingpb.SegmentAssignmentMeta{
			CollectionId: response.CollectionId, PartitionId: stats.PartitionId, SegmentId: stats.SegmentId,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
				ManifestPath: stats.ManifestPath, Binlogs: []*streamingpb.L1SegmentBinLogs{{Bm25Binlog: stats.Bm25Binlogs}},
			},
		})
	}
	err := r.RetainStorage(ctx, publication)
	if err != nil && ctx.Err() == nil {
		// Retain the old query prefix while the controller prepares a fresh
		// baseline, just as it does for event dependency admission failures.
		r.refresh.Store(true)
	}
	return err
}

func newReplicaObjectRetention(catalog replicaObjectCatalog, runtime *viewpb.StreamingWorkNode, cm storage.ChunkManager, config *indexpb.StorageConfig) *replicaObjectRetention {
	return &replicaObjectRetention{
		catalog: catalog, runtime: proto.CloneOf(runtime), rootPath: cm.RootPath(), config: config,
		store: walsummary.NewStore(cm, runtime.Pchannel, runtime.WalTerm), io: packed.NewManifestIOContext(2),
		summary: make(map[string]struct{}), shared: make(map[string]struct{}), manifests: make(map[string]struct{}),
		rootIDs: map[string]map[string]struct{}{"summary": {}, "shared": {}},
	}
}

func (r *replicaObjectRetention) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.io.Close()
}

func (r *replicaObjectRetention) RetainSnapshot(ctx context.Context, image *streamingpb.WALReplicaSnapshot) error {
	publication := proto.CloneOf(image.GetStorage())
	if publication == nil {
		publication = &streamingpb.WALReplicaStoragePublication{}
	}
	publication.Segments = append(publication.Segments, image.Segments...)
	if err := r.RetainStorage(ctx, publication); err != nil {
		return err
	}
	return r.RetainViewStats(ctx, image.QueryViews)
}

func (r *replicaObjectRetention) RetainStorage(ctx context.Context, publication *streamingpb.WALReplicaStoragePublication) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return merr.WrapErrServiceUnavailable("WAL replica object retention is closed")
	}
	var shared, manifests []string
	segments := append(slices.Clone(publication.GetSegments()), publication.GetCatalog().GetSegments()...)
	for _, segment := range segments {
		stored := segment.GetPersistedStorage()
		manifest := stored.GetManifestPath()
		if manifest != "" {
			if _, retained := r.manifests[manifest]; !retained && !slices.Contains(manifests, manifest) {
				keys, err := packed.GetManifestObjectKeysAsync(ctx, r.io, manifest, r.config, maxReplicaObjectKeys)
				if err != nil {
					return err
				}
				shared = append(shared, keys...)
				manifests = append(manifests, manifest)
			}
		} else {
			keys, err := r.segmentBinlogKeys(segment)
			if err != nil {
				return err
			}
			shared = append(shared, keys...)
		}
		if len(shared) > maxReplicaObjectKeys {
			return merr.WrapErrServiceUnavailable("WAL replica storage dependency budget exhausted")
		}
	}
	if err := r.retain(ctx, "summary", r.summary, r.store.ObjectKeys(publication.GetSummary())); err != nil {
		return err
	}
	if err := r.retain(ctx, "shared", r.shared, shared); err != nil {
		return err
	}
	for _, manifest := range manifests {
		r.manifests[manifest] = struct{}{}
	}
	return nil
}

func (r *replicaObjectRetention) RetainViewStats(ctx context.Context, views []*viewpb.QueryViewOfShard) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var keys []string
	for _, view := range views {
		if view.Meta.State != viewpb.QueryViewState_QueryViewStatePreparing && view.Meta.State != viewpb.QueryViewState_QueryViewStateUp {
			continue
		}
		for _, stats := range view.GetStreamingNode().GetPartitionStats() {
			if stats.Version > 0 {
				keys = append(keys, path.Join(r.rootPath, common.PartitionStatsPath, strconv.FormatInt(view.Meta.CollectionId, 10),
					strconv.FormatInt(stats.PartitionId, 10), view.Meta.Vchannel, strconv.FormatInt(stats.Version, 10)))
			}
		}
	}
	return r.retain(ctx, "shared", r.shared, keys)
}

// Caller holds mu. Stable roots include the receiver identity, making source
// protection and receiver retries idempotent without sharing process handles.
func (r *replicaObjectRetention) retain(ctx context.Context, namespace string, retained map[string]struct{}, keys []string) error {
	if r.closed {
		return merr.WrapErrServiceUnavailable("WAL replica object retention is closed")
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	keys = slices.DeleteFunc(keys, func(key string) bool { _, found := retained[key]; return found })
	if len(keys) == 0 {
		return ctx.Err()
	}
	bytes := 0
	for _, key := range keys {
		if key == "" {
			return merr.WrapErrDataIntegrityMsg("WAL replica dependency has an empty object key")
		}
		bytes += len(key)
	}
	if len(r.summary)+len(r.shared)+len(keys) > maxReplicaObjectKeys || r.bytes+bytes > 64<<20 {
		return merr.WrapErrServiceUnavailable("WAL replica object lifetime budget exhausted")
	}
	roots, err := replicaObjectRoots(r.runtime, namespace, keys)
	if err != nil {
		return err
	}
	if r.rootCount()+len(roots) > maxReplicaObjectRoots {
		if err := r.compactRoots(ctx); err != nil {
			return err
		}
		if r.rootCount()+len(roots) > maxReplicaObjectRoots {
			return merr.WrapErrServiceUnavailable("WAL replica requires a fresh object baseline")
		}
	}
	if err := r.retainRoots(ctx, namespace, roots); err != nil {
		return err
	}
	for _, key := range keys {
		retained[key] = struct{}{}
	}
	r.bytes += bytes
	return nil
}

func (r *replicaObjectRetention) retainRoots(ctx context.Context, namespace string, roots []*streamingpb.WALObjectRetentionRoot) error {
	for _, root := range roots {
		if err := retry.Do(ctx, func() error {
			if namespace == "summary" {
				return r.catalog.RetainWALObjects(ctx, r.runtime.Pchannel, root)
			}
			return r.catalog.RetainSharedWALObjects(ctx, root)
		}, retry.Attempts(3), retry.Sleep(10*time.Millisecond)); err != nil {
			return err
		}
		r.rootIDs[namespace][root.RootId] = struct{}{}
	}
	return nil
}

func (r *replicaObjectRetention) rootCount() int {
	return len(r.rootIDs["summary"]) + len(r.rootIDs["shared"])
}

// Merge incremental obligations without changing the set of retained objects.
// Acquire every replacement before releasing any predecessor, so queries and
// concurrent GC never see an ownership gap. Runtime Close remains the only
// operation that releases the objects themselves.
func (r *replicaObjectRetention) compactRoots(ctx context.Context) error {
	r.compaction++
	for _, namespace := range []string{"summary", "shared"} {
		keys := r.summary
		if namespace == "shared" {
			keys = r.shared
		}
		if len(r.rootIDs[namespace]) < 2 {
			continue
		}
		// Use a new identity even if sorted chunk boundaries revisit an old
		// set: released root identities are permanent retry fences.
		roots, err := replicaObjectRoots(r.runtime, namespace+"/compact/"+strconv.FormatUint(r.compaction, 10), slices.Sorted(maps.Keys(keys)))
		if err != nil {
			return err
		}
		old := maps.Clone(r.rootIDs[namespace])
		if err := r.retainRoots(ctx, namespace, roots); err != nil {
			return err
		}
		for id := range old {
			if err := retry.Do(ctx, func() error {
				if namespace == "summary" {
					return r.catalog.ReleaseWALObjects(ctx, r.runtime.Pchannel, id)
				}
				return r.catalog.ReleaseSharedWALObjects(ctx, r.runtime, id)
			}, retry.Attempts(3), retry.Sleep(10*time.Millisecond)); err != nil {
				return err
			}
			delete(r.rootIDs[namespace], id)
		}
	}
	return nil
}

func replicaObjectRoots(runtime *viewpb.StreamingWorkNode, namespace string, keys []string) ([]*streamingpb.WALObjectRetentionRoot, error) {
	var roots []*streamingpb.WALObjectRetentionRoot
	for len(keys) != 0 {
		root := &streamingpb.WALObjectRetentionRoot{Runtime: runtime}
		// Reserve the complete fixed-length digest identity before splitting.
		size := proto.Size(root) + 128
		for len(keys) != 0 && len(root.ObjectKeys) < metastore.WALObjectRetentionRootMaxObjects {
			next := 1 + protowire.SizeBytes(len(keys[0]))
			if size+next > metastore.WALObjectRetentionRootMaxBytes {
				break
			}
			root.ObjectKeys = append(root.ObjectKeys, keys[0])
			keys = keys[1:]
			size += next
		}
		if len(root.ObjectKeys) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("WAL replica object key exceeds catalog record budget")
		}
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(root)
		if err != nil {
			return nil, merr.WrapErrDataIntegrity(err, "encode WAL replica object root")
		}
		root.RootId = fmt.Sprintf("replica/%s/%x", namespace, sha256.Sum256(encoded))
		roots = append(roots, root)
	}
	return roots, nil
}

func (r *replicaObjectRetention) segmentBinlogKeys(segment *streamingpb.SegmentAssignmentMeta) ([]string, error) {
	var keys []string
	add := func(kind storage.BinlogType, fields []*datapb.FieldBinlog) error {
		for _, field := range fields {
			for _, log := range field.GetBinlogs() {
				key := log.LogPath
				if key == "" {
					var err error
					key, err = binlog.BuildLogPathWithRootPath(r.rootPath, kind, segment.CollectionId, segment.PartitionId, segment.SegmentId, field.FieldID, log.LogID)
					if err != nil {
						return err
					}
				}
				keys = append(keys, key)
			}
		}
		return nil
	}
	stored := segment.GetPersistedStorage()
	for _, batch := range stored.GetBinlogs() {
		for kind, fields := range map[storage.BinlogType][]*datapb.FieldBinlog{
			storage.InsertBinlog: batch.FieldBinlog, storage.StatsBinlog: batch.StatsBinlog, storage.BM25Binlog: batch.Bm25Binlog,
		} {
			if err := add(kind, fields); err != nil {
				return nil, err
			}
		}
	}
	if err := add(storage.DeleteBinlog, stored.GetDeltaBinlog()); err != nil {
		return nil, err
	}
	if err := add(storage.StatsBinlog, []*datapb.FieldBinlog{stored.GetMergedStatsBinlog()}); err != nil {
		return nil, err
	}
	return keys, nil
}

func (r *replicaObjectRetention) PrepareEvent(ctx context.Context, event walevent.Event) error {
	switch payload := event.Payload.(type) {
	case walevent.StoragePublished:
		return r.RetainStorage(ctx, &streamingpb.WALReplicaStoragePublication{
			Summary: payload.Summary, Segments: payload.Segments, Catalog: payload.Catalog,
		})
	case walevent.QueryViewIntentBatch:
		return r.RetainViewStats(ctx, payload.Views)
	default:
		return nil
	}
}
