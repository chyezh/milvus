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

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (m *dataViewManager) InitializeCollection(ctx context.Context, initialization CollectionInitialization) (*viewpb.DataVersion, error) {
	state := m.getOrCreateState(initialization.CollectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped {
		return nil, nil
	}

	if state.published != nil {
		state.published = m.withDeleteTimetick(ctx, state.published)
		return dataVersionFromView(state.published), nil
	}

	if err := m.recoverPublicationStateLocked(ctx, state, m.catalog); err != nil {
		return nil, err
	}
	if state.published != nil {
		return dataVersionFromView(state.published), nil
	}
	persistedViews, err := m.catalog.ListDataViews(ctx, initialization.CollectionID)
	if err != nil {
		return nil, err
	}
	if len(persistedViews) > 0 {
		return nil, merr.WrapErrServiceNotReadyMsg(
			"collection %d has DataView snapshots but no durable published head",
			initialization.CollectionID,
		)
	}
	if state.versionState.GetAllocatedStreamingVersion() > 0 || len(state.pendingAssigned) > 0 {
		return nil, merr.WrapErrServiceNotReadyMsg(
			"collection %d has assigned Streaming epochs but no durable published head",
			initialization.CollectionID,
		)
	}

	view := buildEmptyDataView(initialization.CollectionID, initialization.VChannels)
	view.DataVersion = &viewpb.DataVersion{StreamingVersion: 1}
	toPersist := cloneDataViewWithoutDeleteTimetick(view)
	if state.versionState == nil {
		state.versionState = &viewpb.CollectionDataVersionState{CollectionId: initialization.CollectionID}
	}
	if err := m.persistPublishedLocked(ctx, state, m.catalog, toPersist); err != nil {
		return nil, err
	}
	state.publicationRecovered = true
	return dataVersionFromView(state.published), nil
}

func (m *dataViewManager) MarkCollectionTerminal(ctx context.Context, collectionID int64) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.published = nil
	state.dropped = true
	return nil
}

func (m *dataViewManager) FinalizeDropCollection(ctx context.Context, collectionID int64) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.dropped {
		return nil
	}
	if len(state.refs) != 0 {
		return merr.WrapErrServiceNotReadyMsg("data view references for terminal collection %d have not drained", collectionID)
	}
	if err := m.catalog.DropDataViews(ctx, collectionID); err != nil {
		return err
	}
	m.updateRetainedMembership(collectionID, nil)
	return nil
}
