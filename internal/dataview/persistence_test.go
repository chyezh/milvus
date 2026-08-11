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

package dataview

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type publishedDataViewRecoveryCatalogStub struct {
	state     *viewpb.CollectionDataVersionState
	views     []*viewpb.DataViewOfCollection
	listCalls int
}

func (c *publishedDataViewRecoveryCatalogStub) GetDataViewVersionState(context.Context, int64) (*viewpb.CollectionDataVersionState, error) {
	return c.state, nil
}

func (c *publishedDataViewRecoveryCatalogStub) ListDataViews(context.Context, int64) ([]*viewpb.DataViewOfCollection, error) {
	c.listCalls++
	return c.views, nil
}

func TestRecoverPublishedDataViewMissingStateSkipsSnapshots(t *testing.T) {
	catalog := &publishedDataViewRecoveryCatalogStub{
		views: []*viewpb.DataViewOfCollection{dataViewWithVersion(100, 1, 0)},
	}

	state, view, err := recoverPublishedDataView(context.Background(), catalog, 100)
	require.NoError(t, err)
	require.Nil(t, state)
	require.Nil(t, view)
	require.Zero(t, catalog.listCalls)
}

func TestRecoverPublishedDataViewUnpublishedStateSkipsSnapshots(t *testing.T) {
	state := &viewpb.CollectionDataVersionState{
		CollectionId:              100,
		AllocatedStreamingVersion: 1,
	}
	catalog := &publishedDataViewRecoveryCatalogStub{
		state: state,
		views: []*viewpb.DataViewOfCollection{dataViewWithVersion(100, 1, 0)},
	}

	recoveredState, view, err := recoverPublishedDataView(context.Background(), catalog, 100)
	require.NoError(t, err)
	require.True(t, proto.Equal(state, recoveredState))
	require.Nil(t, view)
	require.Zero(t, catalog.listCalls)
}

func TestRecoverPublishedDataViewSelectsDurableHeadAndIgnoresOrphans(t *testing.T) {
	state := &viewpb.CollectionDataVersionState{
		CollectionId:              100,
		AllocatedStreamingVersion: 4,
		PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1},
	}
	headed := dataViewWithVersion(100, 2, 1)
	catalog := &publishedDataViewRecoveryCatalogStub{
		state: state,
		views: []*viewpb.DataViewOfCollection{
			dataViewWithVersion(100, 1, 0),
			headed,
			dataViewWithVersion(100, 3, 0),
		},
	}

	recoveredState, recoveredView, err := recoverPublishedDataView(context.Background(), catalog, 100)
	require.NoError(t, err)
	require.True(t, proto.Equal(state, recoveredState))
	require.True(t, proto.Equal(headed, recoveredView))
	require.Equal(t, 1, catalog.listCalls)
}

func TestRecoverPublishedDataViewMissingHeadIsDataIntegrityError(t *testing.T) {
	state := &viewpb.CollectionDataVersionState{
		CollectionId:              100,
		AllocatedStreamingVersion: 4,
		PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1},
	}
	catalog := &publishedDataViewRecoveryCatalogStub{
		state: state,
		views: []*viewpb.DataViewOfCollection{
			dataViewWithVersion(100, 1, 0),
			dataViewWithVersion(100, 3, 0),
		},
	}

	_, _, err := recoverPublishedDataView(context.Background(), catalog, 100)
	require.Error(t, err)
	require.True(t, errors.Is(err, merr.ErrDataIntegrity))
	require.Equal(t, 1, catalog.listCalls)
}

func dataViewWithVersion(collectionID, streamingVersion, compactVersion int64) *viewpb.DataViewOfCollection {
	return &viewpb.DataViewOfCollection{
		CollectionId: collectionID,
		DataVersion: &viewpb.DataVersion{
			StreamingVersion: streamingVersion,
			CompactVersion:   compactVersion,
		},
	}
}
