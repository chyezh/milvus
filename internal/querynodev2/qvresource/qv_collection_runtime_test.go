//go:build test && dynamic

package qvresource

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewCollectionRuntimeManager_AcquireRefsCollectionAndReleaseUnrefs(t *testing.T) {
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, &schemapb.CollectionSchema{Name: "coll"})
	collection := &fakeQVCollectionManager{collection: localCollection}
	provider := &fakeQVLoadMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{
			CollectionID:    1,
			DbName:          "db",
			UpdateTimestamp: 9,
			Properties:      []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
			Schema:          &schemapb.CollectionSchema{Name: "coll"},
		},
		partitionIDs: []int64{10, 20},
		loadFields:   []int64{100, 101},
	}
	manager := newQueryViewCollectionRuntimeManager(provider, collection)

	guard, retryable, err := manager.Acquire(context.Background(), qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{
			CollectionId:    1,
			LoadInfoVersion: 7,
		},
		&viewpb.QueryViewOfQueryNode{
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{1}},
				{PartitionId: 20, SegmentIds: []int64{2}},
			},
		},
	).(*qviews.QueryViewAtQueryNode))
	require.NoError(t, err)
	assert.False(t, retryable)
	require.NotNil(t, guard)
	assert.Same(t, localCollection, guard.(*queryViewCollectionRuntimeGuard).collection)

	assert.Equal(t, int64(1), guard.CollectionID())
	assert.Equal(t, "db", guard.DatabaseName())
	assert.Equal(t, "coll", guard.Schema().GetName())
	assert.Equal(t, int64(9), guard.SchemaVersion())
	assert.Equal(t, int64(1), collection.putCollectionID)
	assert.Equal(t, "coll", collection.putSchema.GetName())
	require.NotNil(t, collection.putLoadMeta)
	assert.Equal(t, querypb.LoadType_LoadCollection, collection.putLoadMeta.GetLoadType())
	assert.Equal(t, int64(1), collection.putLoadMeta.GetCollectionID())
	assert.Equal(t, "db", collection.putLoadMeta.GetDbName())
	assert.Equal(t, uint64(9), collection.putLoadMeta.GetSchemaBarrierTs())
	assert.Equal(t, []int64{10, 20}, collection.putLoadMeta.GetPartitionIDs())
	assert.Equal(t, []int64{100, 101}, collection.putLoadMeta.GetLoadFields())

	guard.Release()
	assert.Equal(t, int64(1), collection.unrefCollection)
	assert.Equal(t, uint32(1), collection.unrefCount)
}

func TestQueryViewCollectionRuntimeManager_AcquireUsesLoadInfoVersion(t *testing.T) {
	collection := &fakeQVCollectionManager{}
	provider := &fakeQVLoadMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{
			CollectionID:    1,
			DbName:          "db",
			UpdateTimestamp: 9,
			Schema:          &schemapb.CollectionSchema{Name: "coll"},
		},
		partitionIDs: []int64{10, 20},
		loadFields:   []int64{100, 101, 102},
	}
	manager := newQueryViewCollectionRuntimeManager(provider, collection)

	guard, retryable, err := manager.Acquire(context.Background(), qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{
			CollectionId:    1,
			LoadInfoVersion: 7,
		},
		&viewpb.QueryViewOfQueryNode{
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, SegmentIds: []int64{1}},
			},
		},
	).(*qviews.QueryViewAtQueryNode))
	require.NoError(t, err)
	assert.False(t, retryable)
	require.NotNil(t, guard)

	assert.Equal(t, []int64{10, 20}, collection.putLoadMeta.GetPartitionIDs())
	assert.Equal(t, []int64{100, 101, 102}, collection.putLoadMeta.GetLoadFields())
}

func TestQueryViewCollectionRuntimeManager_AcquireClassifiesRetryability(t *testing.T) {
	view := qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{CollectionId: 1, LoadInfoVersion: 7},
		&viewpb.QueryViewOfQueryNode{},
	).(*qviews.QueryViewAtQueryNode)

	t.Run("transient", func(t *testing.T) {
		manager := newQueryViewCollectionRuntimeManager(
			&fakeQVLoadMetadataProvider{err: merr.WrapErrNodeNotMatch(1, 2)},
			&fakeQVCollectionManager{},
		)

		guard, retryable, err := manager.Acquire(context.Background(), view)

		assert.Nil(t, guard)
		assert.True(t, retryable)
		require.ErrorIs(t, err, merr.ErrNodeNotMatch)
	})

	t.Run("not found", func(t *testing.T) {
		manager := newQueryViewCollectionRuntimeManager(
			&fakeQVLoadMetadataProvider{err: merr.WrapErrCollectionNotFound(1)},
			&fakeQVCollectionManager{},
		)

		guard, retryable, err := manager.Acquire(context.Background(), view)

		assert.Nil(t, guard)
		assert.False(t, retryable)
		require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})
}

func TestQueryViewCollectionRuntimeGuard_UpdateIndexMetaUsesPinnedCollection(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "coll"}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	collection := &fakeQVCollectionManager{collection: localCollection}
	guard := &queryViewCollectionRuntimeGuard{
		collections:  collection,
		collection:   localCollection,
		collectionID: 1,
		schema:       schema,
	}
	indexes := []*indexpb.IndexInfo{{CollectionID: 1, FieldID: 100, IndexName: "vec_idx"}}

	var updatedCollection *segments.Collection
	var updatedMeta *segcorepb.CollectionIndexMeta
	patch := mockey.Mock((*segments.Collection).UpdateIndexMeta).
		To(func(collection *segments.Collection, meta *segcorepb.CollectionIndexMeta) error {
			updatedCollection = collection
			updatedMeta = meta
			return nil
		}).
		Build()
	t.Cleanup(func() {
		patch.UnPatch()
	})

	err := guard.UpdateIndexMeta(context.Background(), indexes)
	require.NoError(t, err)

	assert.Same(t, localCollection, updatedCollection)
	require.Len(t, updatedMeta.GetIndexMetas(), 1)
	assert.Equal(t, int64(100), updatedMeta.GetIndexMetas()[0].GetFieldID())
	assert.Equal(t, "vec_idx", updatedMeta.GetIndexMetas()[0].GetIndexName())
	assert.Zero(t, collection.putCount)
	assert.Zero(t, collection.unrefCount)
}

func TestQueryViewCollectionRuntimeGuardReadsLatestPinnedSchema(t *testing.T) {
	initialSchema := &schemapb.CollectionSchema{Name: "coll", Version: 1}
	latestSchema := &schemapb.CollectionSchema{Name: "coll", Version: 2}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, initialSchema)
	guard := &queryViewCollectionRuntimeGuard{
		collection:   localCollection,
		collectionID: 1,
		schema:       initialSchema,
	}
	patch := mockey.Mock((*segments.Collection).Schema).
		To(func(collection *segments.Collection) *schemapb.CollectionSchema {
			assert.Same(t, localCollection, collection)
			return latestSchema
		}).
		Build()
	t.Cleanup(func() { patch.UnPatch() })

	assert.Same(t, latestSchema, guard.Schema())
}

func TestQueryViewCollectionRuntimeGuardRejectsStaleLoadMetadataVersion(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "coll", Version: 1}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	collections := &fakeQVCollectionManager{collection: localCollection}
	provider := &fakeQVLoadMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{
			CollectionID: 1,
			DbName:       "db",
			Schema:       schema,
		},
	}
	manager := newQueryViewCollectionRuntimeManager(provider, collections)
	view := qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{CollectionId: 1},
		&viewpb.QueryViewOfQueryNode{},
	).(*qviews.QueryViewAtQueryNode)
	first, _, err := manager.Acquire(context.Background(), view)
	require.NoError(t, err)
	second, _, err := manager.Acquire(context.Background(), view)
	require.NoError(t, err)

	type loadMetadataUpdater interface {
		UpdateLoadMetadata(context.Context, uint64, *schemapb.CollectionSchema, uint64, []*indexpb.IndexInfo) error
	}
	newer, ok := first.(loadMetadataUpdater)
	require.True(t, ok)
	older, ok := second.(loadMetadataUpdater)
	require.True(t, ok)

	var applied []string
	patch := mockey.Mock((*segments.Collection).UpdateIndexMeta).
		To(func(_ *segments.Collection, meta *segcorepb.CollectionIndexMeta) error {
			require.Len(t, meta.GetIndexMetas(), 1)
			applied = append(applied, meta.GetIndexMetas()[0].GetIndexName())
			return nil
		}).
		Build()
	t.Cleanup(func() { patch.UnPatch() })

	require.NoError(t, newer.UpdateLoadMetadata(context.Background(), 2, schema, 20, []*indexpb.IndexInfo{{
		CollectionID: 1,
		FieldID:      100,
		IndexID:      200,
		IndexName:    "new-index",
	}}))
	require.NoError(t, older.UpdateLoadMetadata(context.Background(), 1, schema, 10, []*indexpb.IndexInfo{{
		CollectionID: 1,
		FieldID:      100,
		IndexID:      200,
		IndexName:    "old-index",
	}}))

	assert.Equal(t, []string{"new-index"}, applied)
}

func TestQueryViewCollectionRuntimeGuardDoesNotAdvanceDeliveryVersionOnUpdateFailure(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "coll", Version: 1}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	collections := &fakeQVCollectionManager{collection: localCollection}
	manager := newQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{
			CollectionID: 1,
			DbName:       "db",
			Schema:       schema,
		},
	}, collections)
	view := qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{CollectionId: 1},
		&viewpb.QueryViewOfQueryNode{},
	).(*qviews.QueryViewAtQueryNode)
	guard, _, err := manager.Acquire(context.Background(), view)
	require.NoError(t, err)
	defer guard.Release()
	updater := guard.(interface {
		UpdateLoadMetadata(context.Context, uint64, *schemapb.CollectionSchema, uint64, []*indexpb.IndexInfo) error
	})

	var calls atomic.Int32
	patch := mockey.Mock((*segments.Collection).UpdateIndexMeta).
		To(func(_ *segments.Collection, _ *segcorepb.CollectionIndexMeta) error {
			if calls.Add(1) == 1 {
				return assert.AnError
			}
			return nil
		}).
		Build()
	t.Cleanup(func() { patch.UnPatch() })
	indexes := []*indexpb.IndexInfo{{CollectionID: 1, FieldID: 100, IndexID: 200, IndexName: "new-index"}}

	require.ErrorIs(t, updater.UpdateLoadMetadata(context.Background(), 2, schema, 20, indexes), assert.AnError)
	require.NoError(t, updater.UpdateLoadMetadata(context.Background(), 2, schema, 20, indexes),
		"the same delivery must retry after a failed application")
	require.NoError(t, updater.UpdateLoadMetadata(context.Background(), 2, schema, 20, indexes))
	assert.Equal(t, int32(2), calls.Load(), "successful application must advance the delivery gate exactly once")
}

func TestQueryViewCollectionRuntimeGuardSerializesConcurrentDeliveryVersions(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "coll", Version: 1}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	collections := &fakeQVCollectionManager{collection: localCollection}
	manager := newQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{
			CollectionID: 1,
			DbName:       "db",
			Schema:       schema,
		},
	}, collections)
	view := qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{CollectionId: 1},
		&viewpb.QueryViewOfQueryNode{},
	).(*qviews.QueryViewAtQueryNode)
	newerGuard, _, err := manager.Acquire(context.Background(), view)
	require.NoError(t, err)
	defer newerGuard.Release()
	olderGuard, _, err := manager.Acquire(context.Background(), view)
	require.NoError(t, err)
	defer olderGuard.Release()
	type loadMetadataUpdater interface {
		UpdateLoadMetadata(context.Context, uint64, *schemapb.CollectionSchema, uint64, []*indexpb.IndexInfo) error
	}
	newer := newerGuard.(loadMetadataUpdater)
	older := olderGuard.(loadMetadataUpdater)

	newerEntered := make(chan struct{})
	allowNewer := make(chan struct{})
	var appliedMu sync.Mutex
	var applied []string
	patch := mockey.Mock((*segments.Collection).UpdateIndexMeta).
		To(func(_ *segments.Collection, meta *segcorepb.CollectionIndexMeta) error {
			name := meta.GetIndexMetas()[0].GetIndexName()
			appliedMu.Lock()
			applied = append(applied, name)
			appliedMu.Unlock()
			if name == "new-index" {
				close(newerEntered)
				<-allowNewer
			}
			return nil
		}).
		Build()
	t.Cleanup(func() { patch.UnPatch() })

	newerDone := make(chan error, 1)
	go func() {
		newerDone <- newer.UpdateLoadMetadata(context.Background(), 2, schema, 20, []*indexpb.IndexInfo{{
			CollectionID: 1,
			FieldID:      100,
			IndexID:      200,
			IndexName:    "new-index",
		}})
	}()
	<-newerEntered
	olderDone := make(chan error, 1)
	go func() {
		olderDone <- older.UpdateLoadMetadata(context.Background(), 1, schema, 10, []*indexpb.IndexInfo{{
			CollectionID: 1,
			FieldID:      100,
			IndexID:      200,
			IndexName:    "old-index",
		}})
	}()
	select {
	case err := <-olderDone:
		t.Fatalf("older delivery completed before the newer delivery committed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(allowNewer)
	require.NoError(t, <-newerDone)
	require.NoError(t, <-olderDone)

	appliedMu.Lock()
	defer appliedMu.Unlock()
	assert.Equal(t, []string{"new-index"}, applied)
}

func TestQueryViewCollectionRuntimeManagerRemovesLoadMetadataStateAfterLastGuardRelease(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "coll", Version: 1}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	manager := newQueryViewCollectionRuntimeManager(&fakeQVLoadMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{
			CollectionID: 1,
			DbName:       "db",
			Schema:       schema,
		},
	}, &fakeQVCollectionManager{collection: localCollection})
	view := qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{CollectionId: 1},
		&viewpb.QueryViewOfQueryNode{},
	).(*qviews.QueryViewAtQueryNode)

	guard, _, err := manager.Acquire(context.Background(), view)
	require.NoError(t, err)
	require.Contains(t, manager.loadMetadataStates, int64(1))
	guard.Release()
	assert.NotContains(t, manager.loadMetadataStates, int64(1))
}

func TestQueryViewCollectionRuntimeManagerKeepsOneLoadMetadataStateAcrossAcquireReleaseRace(t *testing.T) {
	schema := &schemapb.CollectionSchema{Name: "coll", Version: 1}
	localCollection := segments.NewCollectionWithoutSegcoreForTest(1, schema)
	collections := &fakeQVCollectionManager{collection: localCollection}
	provider := &fakeQVLoadMetadataProvider{
		collection: &milvuspb.DescribeCollectionResponse{
			CollectionID: 1,
			DbName:       "db",
			Schema:       schema,
		},
	}
	manager := newQueryViewCollectionRuntimeManager(provider, collections)
	view := qviews.NewQueryViewAtQueryNode(
		&viewpb.QueryViewMeta{CollectionId: 1},
		&viewpb.QueryViewOfQueryNode{},
	).(*qviews.QueryViewAtQueryNode)

	first, _, err := manager.Acquire(context.Background(), view)
	require.NoError(t, err)
	firstGuard := first.(*queryViewCollectionRuntimeGuard)

	unrefStarted := make(chan struct{})
	allowUnref := make(chan struct{})
	var once sync.Once
	patch := mockey.Mock((*fakeQVCollectionManager).Unref).
		To(func(_ *fakeQVCollectionManager, _ int64, _ uint32) bool {
			once.Do(func() { close(unrefStarted) })
			<-allowUnref
			return true
		}).
		Build()
	t.Cleanup(func() { patch.UnPatch() })

	released := make(chan struct{})
	go func() {
		first.Release()
		close(released)
	}()
	<-unrefStarted

	second, _, err := manager.Acquire(context.Background(), view)
	require.NoError(t, err)
	secondGuard := second.(*queryViewCollectionRuntimeGuard)
	assert.Same(t, firstGuard.loadMetadata, secondGuard.loadMetadata)

	close(allowUnref)
	<-released

	third, _, err := manager.Acquire(context.Background(), view)
	require.NoError(t, err)
	thirdGuard := third.(*queryViewCollectionRuntimeGuard)
	assert.Same(t, secondGuard.loadMetadata, thirdGuard.loadMetadata,
		"concurrent acquire must not retain a state that release removes from the manager")
}
