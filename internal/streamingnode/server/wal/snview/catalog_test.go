//go:build test && dynamic

package snview

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/mocks/mock_kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func newTestStreamingNodeCatalog(t *testing.T) (StreamingNodeCatalog, map[string]string) {
	metaKV := mock_kv.NewMockMetaKv(t)
	storage := make(map[string]string)

	metaKV.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, prefix string) ([]string, []string, error) {
			keys := make([]string, 0)
			values := make([]string, 0)
			for key, value := range storage {
				if strings.HasPrefix(key, prefix) {
					keys = append(keys, key)
					values = append(values, value)
				}
			}
			return keys, values, nil
		}).Maybe()

	metaKV.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			for key, value := range saves {
				storage[key] = value
			}
			for _, key := range removals {
				delete(storage, key)
			}
			return nil
		}).Maybe()

	return NewKVStreamingNodeCatalog(metaKV, 1001), storage
}

func makeCatalogTestView(collectionID, replicaID int64, vchannel string, sv, cv, qv int64, state viewpb.QueryViewState) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: collectionID,
			ReplicaId:    replicaID,
			Vchannel:     vchannel,
			Version: &viewpb.QueryViewVersion{
				DataVersion: &viewpb.DataVersion{
					StreamingVersion: sv,
					CompactVersion:   cv,
				},
				QueryVersion: qv,
			},
			State: state,
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
}

func TestKVStreamingNodeCatalogPersistsOnlyUpViews(t *testing.T) {
	catalog, storage := newTestStreamingNodeCatalog(t)
	ctx := context.Background()
	view := makeCatalogTestView(1, 10, "v1", 20, 0, 30, viewpb.QueryViewState_QueryViewStateUp)

	require.NoError(t, catalog.SaveQueryView(view))
	require.Len(t, storage, 1)

	views, err := catalog.ListQueryViews(ctx)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, viewpb.QueryViewState_QueryViewStateUp, views[0].GetMeta().GetState())

	view.Meta.State = viewpb.QueryViewState_QueryViewStateDown
	require.NoError(t, catalog.SaveQueryView(view))

	views, err = catalog.ListQueryViews(ctx)
	require.NoError(t, err)
	require.Empty(t, views)
	require.Empty(t, storage)
}
