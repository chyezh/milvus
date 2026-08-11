// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type dataViewMemoryMetaKV struct {
	*memkv.MemoryKV
}

func newDataViewMemoryMetaKV() *dataViewMemoryMetaKV {
	return &dataViewMemoryMetaKV{MemoryKV: memkv.NewMemoryKV()}
}

func (kv *dataViewMemoryMetaKV) GetPath(key string) string {
	return key
}

func (kv *dataViewMemoryMetaKV) CompareVersionAndSwap(context.Context, string, int64, string) (bool, error) {
	panic("unused")
}

func (kv *dataViewMemoryMetaKV) WalkWithPrefix(ctx context.Context, prefix string, _ int, fn func([]byte, []byte) error) error {
	keys, values, err := kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	for i := range keys {
		if err := fn([]byte(keys[i]), []byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

func TestDataViewVersionStateSaveAndLoad(t *testing.T) {
	ctx := context.Background()
	kv := newDataViewMemoryMetaKV()
	catalog := NewCatalog(kv, rootPath, "")
	state := &viewpb.CollectionDataVersionState{
		CollectionId:              100,
		AllocatedStreamingVersion: 3,
		PublishedDataVersion: &viewpb.DataVersion{
			StreamingVersion: 2,
			CompactVersion:   1,
		},
	}

	require.NoError(t, catalog.SaveDataViewVersionState(ctx, state))
	stored, err := catalog.GetDataViewVersionState(ctx, state.GetCollectionId())
	require.NoError(t, err)
	require.True(t, proto.Equal(state, stored))

	missing, err := catalog.GetDataViewVersionState(ctx, 101)
	require.NoError(t, err)
	require.Nil(t, missing)
}

func TestDataViewVersionStatePublishedAtomically(t *testing.T) {
	ctx := context.Background()
	kv := newDataViewMemoryMetaKV()
	catalog := NewCatalog(kv, rootPath, "")
	state, view := publishedDataViewFixture()

	var origin func(*memkv.MemoryKV, context.Context, map[string]string) error
	multiSaveCalls := 0
	var saved map[string]string
	patch := mockey.Mock((*memkv.MemoryKV).MultiSave).
		To(func(store *memkv.MemoryKV, ctx context.Context, kvs map[string]string) error {
			multiSaveCalls++
			saved = make(map[string]string, len(kvs))
			for key, value := range kvs {
				saved[key] = value
			}
			return origin(store, ctx, kvs)
		}).
		Origin(&origin).
		Build()
	defer patch.UnPatch()

	require.NoError(t, catalog.SavePublishedDataView(ctx, state, view))
	require.Equal(t, 1, multiSaveCalls)
	require.Len(t, saved, 2)

	stateValue, ok := saved["coord/dv/100/state"]
	require.True(t, ok)
	storedState := &viewpb.CollectionDataVersionState{}
	require.NoError(t, proto.Unmarshal([]byte(stateValue), storedState))
	require.True(t, proto.Equal(state, storedState))

	viewValue, ok := saved["coord/dv/100/versions/2/1"]
	require.True(t, ok)
	storedView := &viewpb.DataViewOfCollection{}
	require.NoError(t, proto.Unmarshal([]byte(viewValue), storedView))
	require.True(t, proto.Equal(view, storedView))
}

func TestDataViewVersionStateRepeatedPublicationIsIdempotent(t *testing.T) {
	ctx := context.Background()
	kv := newDataViewMemoryMetaKV()
	catalog := NewCatalog(kv, rootPath, "")
	state, view := publishedDataViewFixture()

	require.NoError(t, catalog.SavePublishedDataView(ctx, state, view))
	require.NoError(t, catalog.SavePublishedDataView(ctx, state, view))

	storedState, err := catalog.GetDataViewVersionState(ctx, state.GetCollectionId())
	require.NoError(t, err)
	require.True(t, proto.Equal(state, storedState))
	views, err := catalog.ListDataViews(ctx, state.GetCollectionId())
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.True(t, proto.Equal(view, views[0]))
}

func TestDataViewVersionStatePublicationRejectsMismatchedCollectionIDs(t *testing.T) {
	ctx := context.Background()
	kv := newDataViewMemoryMetaKV()
	catalog := NewCatalog(kv, rootPath, "")
	state, view := publishedDataViewFixture()
	state.CollectionId = 101

	multiSaveCalls := countDataViewMultiSaveCalls(t)
	err := catalog.SavePublishedDataView(ctx, state, view)
	require.Zero(t, *multiSaveCalls)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestDataViewVersionStatePublicationRejectsMismatchedPublishedVersion(t *testing.T) {
	ctx := context.Background()
	kv := newDataViewMemoryMetaKV()
	catalog := NewCatalog(kv, rootPath, "")
	state, view := publishedDataViewFixture()
	state.PublishedDataVersion = &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 2}

	multiSaveCalls := countDataViewMultiSaveCalls(t)
	err := catalog.SavePublishedDataView(ctx, state, view)
	require.Zero(t, *multiSaveCalls)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestListAllDataViewsIgnoresVersionStateKeys(t *testing.T) {
	ctx := context.Background()
	kv := newDataViewMemoryMetaKV()
	catalog := NewCatalog(kv, rootPath, "")
	state, view := publishedDataViewFixture()

	require.NoError(t, catalog.SavePublishedDataView(ctx, state, view))
	require.NoError(t, catalog.MarkDataViewCollectionDropped(ctx, 200))

	views, err := catalog.ListAllDataViews(ctx)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.True(t, proto.Equal(view, views[0]))
}

func publishedDataViewFixture() (*viewpb.CollectionDataVersionState, *viewpb.DataViewOfCollection) {
	version := &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1}
	return &viewpb.CollectionDataVersionState{
			CollectionId:              100,
			AllocatedStreamingVersion: 3,
			PublishedDataVersion:      proto.Clone(version).(*viewpb.DataVersion),
		}, &viewpb.DataViewOfCollection{
			CollectionId: 100,
			DataVersion:  version,
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{101, 102}},
					},
				},
			},
		}
}

func countDataViewMultiSaveCalls(t *testing.T) *int {
	var origin func(*memkv.MemoryKV, context.Context, map[string]string) error
	calls := new(int)
	patch := mockey.Mock((*memkv.MemoryKV).MultiSave).
		To(func(store *memkv.MemoryKV, ctx context.Context, kvs map[string]string) error {
			(*calls)++
			return origin(store, ctx, kvs)
		}).
		Origin(&origin).
		Build()
	t.Cleanup(func() { patch.UnPatch() })
	return calls
}
