package qvresource

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type queryViewCollectionRuntimeManager struct {
	meta        qnview.QueryViewLoadMetadataProvider
	collections qvCollectionManager

	loadMetadataMu     sync.Mutex
	loadMetadataStates map[int64]*collectionLoadMetadataState
}

type collectionLoadMetadataState struct {
	mu              sync.Mutex
	deliveryVersion uint64
	refs            uint32
}

func newQueryViewCollectionRuntimeManager(meta qnview.QueryViewLoadMetadataProvider, collections qvCollectionManager) *queryViewCollectionRuntimeManager {
	return &queryViewCollectionRuntimeManager{
		meta:               meta,
		collections:        collections,
		loadMetadataStates: make(map[int64]*collectionLoadMetadataState),
	}
}

func (m *queryViewCollectionRuntimeManager) acquireLoadMetadataState(collectionID int64) *collectionLoadMetadataState {
	m.loadMetadataMu.Lock()
	defer m.loadMetadataMu.Unlock()
	state := m.loadMetadataStates[collectionID]
	if state == nil {
		state = &collectionLoadMetadataState{}
		m.loadMetadataStates[collectionID] = state
	}
	state.refs++
	return state
}

func (m *queryViewCollectionRuntimeManager) releaseLoadMetadataState(collectionID int64, expected *collectionLoadMetadataState) {
	m.loadMetadataMu.Lock()
	defer m.loadMetadataMu.Unlock()
	if m.loadMetadataStates[collectionID] != expected {
		return
	}
	expected.refs--
	if expected.refs == 0 {
		delete(m.loadMetadataStates, collectionID)
	}
}

func (m *queryViewCollectionRuntimeManager) Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (qnview.CollectionRuntimeGuard, bool, error) {
	if view == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("query view is nil")
	}
	pb := view.IntoProto()
	meta := pb.GetMeta()
	collection, err := m.meta.DescribeCollection(ctx, meta.GetCollectionId())
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	if collection == nil || collection.GetSchema() == nil {
		return nil, false, merr.WrapErrServiceInternalMsg("collection metadata is incomplete")
	}
	loadInfo, err := m.loadInfo(ctx, meta)
	if err != nil {
		return nil, isRetryableCollectionRuntimeError(err), err
	}
	if err := m.collections.PutOrRef(
		meta.GetCollectionId(),
		collection.GetSchema(),
		segments.ComposeIndexMeta(ctx, loadInfo.IndexInfos, collection.GetSchema()),
		&querypb.LoadMetaInfo{
			LoadType:        querypb.LoadType_LoadCollection,
			CollectionID:    meta.GetCollectionId(),
			PartitionIDs:    loadInfoPartitionIDs(loadInfo, view.ViewOfQueryNode()),
			DbName:          collection.GetDbName(),
			LoadFields:      loadInfoFieldIDs(loadInfo),
			SchemaBarrierTs: collection.GetUpdateTimestamp(),
		},
	); err != nil {
		return nil, false, err
	}
	localCollection := m.collections.Get(meta.GetCollectionId())
	var ccollection *segcore.CCollection
	if localCollection != nil {
		ccollection = localCollection.GetCCollection()
	}
	return &queryViewCollectionRuntimeGuard{
		manager:       m,
		collections:   m.collections,
		collection:    localCollection,
		loadMetadata:  m.acquireLoadMetadataState(meta.GetCollectionId()),
		collectionID:  meta.GetCollectionId(),
		databaseName:  collection.GetDbName(),
		schema:        collection.GetSchema(),
		schemaVersion: int64(collection.GetUpdateTimestamp()),
		ccollection:   ccollection,
	}, false, nil
}

func isRetryableCollectionRuntimeError(err error) bool {
	if err == nil || merr.GetErrorType(err) == merr.InputError {
		return false
	}
	return !errors.Is(err, merr.ErrCollectionNotFound) &&
		!errors.Is(err, merr.ErrDatabaseNotFound) &&
		!errors.Is(err, merr.ErrPartitionNotFound) &&
		!errors.Is(err, merr.ErrSegmentNotFound) &&
		!errors.Is(err, merr.ErrIndexNotFound)
}

func (m *queryViewCollectionRuntimeManager) loadInfo(ctx context.Context, meta *viewpb.QueryViewMeta) (qnview.QueryViewLoadInfo, error) {
	return m.meta.GetQueryViewLoadInfo(ctx, meta.GetCollectionId(), qnview.QueryViewLoadInfoVersionFromProto(meta.GetLoadInfoVersion()))
}

type queryViewCollectionRuntimeGuard struct {
	manager       *queryViewCollectionRuntimeManager
	collections   qvCollectionManager
	collection    *segments.Collection
	loadMetadata  *collectionLoadMetadataState
	collectionID  int64
	databaseName  string
	schema        *schemapb.CollectionSchema
	schemaVersion int64
	ccollection   *segcore.CCollection
}

func (g *queryViewCollectionRuntimeGuard) CollectionID() int64 {
	return g.collectionID
}

func (g *queryViewCollectionRuntimeGuard) DatabaseName() string {
	return g.databaseName
}

func (g *queryViewCollectionRuntimeGuard) Schema() *schemapb.CollectionSchema {
	if g.collection != nil {
		return g.collection.Schema()
	}
	return g.schema
}

func (g *queryViewCollectionRuntimeGuard) SchemaVersion() int64 {
	return g.schemaVersion
}

func (g *queryViewCollectionRuntimeGuard) CCollection() *segcore.CCollection {
	return g.ccollection
}

func (g *queryViewCollectionRuntimeGuard) PinnedCollection() *segments.Collection {
	return g.collection
}

func (g *queryViewCollectionRuntimeGuard) UpdateIndexMeta(ctx context.Context, indexes []*indexpb.IndexInfo) error {
	indexMeta := segments.ComposeIndexMeta(ctx, indexes, g.collection.Schema())
	return g.collection.UpdateIndexMeta(indexMeta)
}

func (g *queryViewCollectionRuntimeGuard) UpdateSchema(_ context.Context, schema *schemapb.CollectionSchema, schemaBarrierTs uint64) error {
	return g.collections.UpdateSchema(g.collectionID, schema, schemaBarrierTs)
}

func (g *queryViewCollectionRuntimeGuard) UpdateLoadMetadata(ctx context.Context, deliveryVersion uint64, schema *schemapb.CollectionSchema, schemaBarrierTs uint64, indexes []*indexpb.IndexInfo) error {
	if deliveryVersion == 0 || g.loadMetadata == nil {
		if schema != nil {
			if err := g.UpdateSchema(ctx, schema, schemaBarrierTs); err != nil {
				return err
			}
		}
		return g.UpdateIndexMeta(ctx, indexes)
	}

	g.loadMetadata.mu.Lock()
	defer g.loadMetadata.mu.Unlock()
	if deliveryVersion <= g.loadMetadata.deliveryVersion {
		return nil
	}
	if schema != nil {
		if err := g.UpdateSchema(ctx, schema, schemaBarrierTs); err != nil {
			return err
		}
	}
	if err := g.UpdateIndexMeta(ctx, indexes); err != nil {
		return err
	}
	g.loadMetadata.deliveryVersion = deliveryVersion
	return nil
}

func (g *queryViewCollectionRuntimeGuard) Release() {
	g.collections.Unref(g.collectionID, 1)
	if g.manager != nil {
		g.manager.releaseLoadMetadataState(g.collectionID, g.loadMetadata)
	}
}

func loadInfoPartitionIDs(info qnview.QueryViewLoadInfo, fallback *viewpb.QueryViewOfQueryNode) []int64 {
	if len(info.PartitionIDs) > 0 {
		return append([]int64(nil), info.PartitionIDs...)
	}
	return qvViewPartitionIDs(fallback)
}

func loadInfoFieldIDs(info qnview.QueryViewLoadInfo) []int64 {
	fields := make([]int64, 0, len(info.LoadFields))
	for _, field := range info.LoadFields {
		fields = append(fields, field.GetFieldId())
	}
	return fields
}

func qvViewPartitionIDs(view *viewpb.QueryViewOfQueryNode) []int64 {
	partitions := make([]int64, 0, len(view.GetPartitions()))
	for _, partition := range view.GetPartitions() {
		partitions = append(partitions, partition.GetPartitionId())
	}
	return partitions
}
