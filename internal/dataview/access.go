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
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (m *dataViewManager) Get(
	ctx context.Context,
	collectionID int64,
	version qviews.DataVersion,
) (DataViewRef, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.dropped {
		return nil, unavailableDataViewError(collectionID, version)
	}
	if err := m.recoverPublicationStateLocked(ctx, state, m.catalog); err != nil {
		return nil, err
	}
	versionProto := version.IntoProto()
	if state.published == nil || compareDataVersion(versionProto, state.published.GetDataVersion()) > 0 {
		return nil, unavailableDataViewError(collectionID, version)
	}
	if compareDataVersion(state.published.GetDataVersion(), versionProto) == 0 {
		return newDataViewRef(state, newDataView(state.published)), nil
	}

	views, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), versionProto) == 0 {
			return newDataViewRef(state, newDataView(view)), nil
		}
	}
	return nil, unavailableDataViewError(collectionID, version)
}

func (m *dataViewManager) LatestPublished(ctx context.Context, collectionID int64) (DataViewRef, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.dropped {
		return nil, unavailableLatestDataViewError(collectionID)
	}
	if err := m.recoverPublicationStateLocked(ctx, state, m.catalog); err != nil {
		return nil, err
	}
	if state.published == nil {
		return nil, unavailableLatestDataViewError(collectionID)
	}
	return newDataViewRef(state, newDataView(state.published)), nil
}

func unavailableDataViewError(collectionID int64, version qviews.DataVersion) error {
	return &dataViewUnavailableError{collectionID: collectionID, version: version}
}

type dataViewUnavailableError struct {
	collectionID int64
	version      qviews.DataVersion
	latest       bool
}

func (e *dataViewUnavailableError) Error() string {
	if e.latest {
		return fmt.Sprintf("latest published data view of collection %d is no longer available", e.collectionID)
	}
	return fmt.Sprintf("data view %s of collection %d is no longer available", e.version.String(), e.collectionID)
}

func (*dataViewUnavailableError) Unwrap() error { return merr.ErrServiceNotReady }

// IsUnavailableDataViewError reports whether err means the exact requested
// DataView is absent or the collection has already become terminal. Other
// service-not-ready failures, such as a catalog outage, are not matched.
func IsUnavailableDataViewError(err error) bool {
	var target *dataViewUnavailableError
	return errors.As(err, &target)
}

// NewUnavailableDataViewError reports that an exact immutable DataView cannot
// be referenced. It is exposed for reference-manager implementations and tests;
// callers should branch with IsUnavailableDataViewError.
func NewUnavailableDataViewError(collectionID int64, version qviews.DataVersion) error {
	return unavailableDataViewError(collectionID, version)
}

func unavailableLatestDataViewError(collectionID int64) error {
	return &dataViewUnavailableError{collectionID: collectionID, latest: true}
}
