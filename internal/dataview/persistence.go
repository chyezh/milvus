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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type publishedDataViewRecoveryCatalog interface {
	GetDataViewVersionState(ctx context.Context, collectionID int64) (*viewpb.CollectionDataVersionState, error)
	ListDataViews(ctx context.Context, collectionID int64) ([]*viewpb.DataViewOfCollection, error)
}

func recoverPublishedDataView(
	ctx context.Context,
	catalog publishedDataViewRecoveryCatalog,
	collectionID int64,
) (*viewpb.CollectionDataVersionState, *viewpb.DataViewOfCollection, error) {
	state, err := catalog.GetDataViewVersionState(ctx, collectionID)
	if err != nil {
		return nil, nil, merr.Wrapf(err, "recover data view version state for collection %d", collectionID)
	}
	if state == nil || state.GetPublishedDataVersion() == nil {
		return state, nil, nil
	}

	views, err := catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return nil, nil, merr.Wrapf(err, "list data view snapshots for collection %d", collectionID)
	}
	for _, view := range views {
		if proto.Equal(view.GetDataVersion(), state.GetPublishedDataVersion()) {
			return state, view, nil
		}
	}

	version := state.GetPublishedDataVersion()
	return nil, nil, merr.WrapErrDataIntegrityMsg(
		"published data view snapshot is missing for collection %d at version %d/%d",
		collectionID,
		version.GetStreamingVersion(),
		version.GetCompactVersion(),
	)
}
