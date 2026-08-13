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
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func RecoverManager(ctx context.Context, catalog RecoveryCatalog, segments SegmentStore) (Manager, error) {
	manager := NewManager(catalog, segments).(*dataViewManager)
	dataViews, err := catalog.ListAllDataViews(ctx)
	if err != nil {
		return nil, err
	}
	manager.recoverFromDataViews(dataViews)
	viewsByCollection := make(map[int64][]*viewpb.DataViewOfCollection)
	collectionIDs := make(map[int64]struct{})
	for _, view := range dataViews {
		if view != nil {
			viewsByCollection[view.GetCollectionId()] = append(viewsByCollection[view.GetCollectionId()], view)
			collectionIDs[view.GetCollectionId()] = struct{}{}
		}
	}
	versionStates, err := catalog.ListAllDataViewVersionStates(ctx)
	if err != nil {
		return nil, err
	}
	for _, versionState := range versionStates {
		if versionState != nil {
			collectionIDs[versionState.GetCollectionId()] = struct{}{}
		}
	}
	orderedCollectionIDs := make([]int64, 0, len(collectionIDs))
	for collectionID := range collectionIDs {
		orderedCollectionIDs = append(orderedCollectionIDs, collectionID)
	}
	sort.Slice(orderedCollectionIDs, func(i, j int) bool {
		return orderedCollectionIDs[i] < orderedCollectionIDs[j]
	})
	for _, collectionID := range orderedCollectionIDs {
		persistedViews := viewsByCollection[collectionID]
		durable, published, err := recoverPublishedDataView(ctx, catalog, collectionID)
		if err != nil {
			return nil, err
		}
		versionStatePersisted := durable != nil
		if durable == nil {
			durable = &viewpb.CollectionDataVersionState{CollectionId: collectionID}
		}
		if !versionStatePersisted && published == nil && len(persistedViews) > 0 {
			// Legacy catalogs predate the durable published head and may contain
			// a newer snapshot for an invisible sort/compaction handoff. During
			// this one-time migration, select the newest snapshot whose added
			// membership is loadable; after the head is backfilled, recovery never
			// performs this SegmentMeta-based visibility selection again.
			published = manager.latestLegacyLoadablePersistedView(ctx, persistedViews)
			if published == nil {
				return nil, merr.WrapErrDataIntegrityMsg(
					"legacy DataView snapshots for collection %d have no loadable migration head",
					collectionID,
				)
			}
			if published.GetCollectionId() != collectionID ||
				published.GetDataVersion() == nil ||
				published.GetDataVersion().GetStreamingVersion() <= 0 ||
				published.GetDataVersion().GetCompactVersion() < 0 {
				return nil, merr.WrapErrDataIntegrityMsg(
					"invalid legacy DataView migration snapshot for collection %d",
					collectionID,
				)
			}
			durable = proto.Clone(durable).(*viewpb.CollectionDataVersionState)
			durable.PublishedDataVersion = cloneDataVersion(published.GetDataVersion())
			if published.GetDataVersion().GetStreamingVersion() > durable.GetAllocatedStreamingVersion() {
				durable.AllocatedStreamingVersion = published.GetDataVersion().GetStreamingVersion()
			}
			if err := catalog.SavePublishedDataView(ctx, durable, cloneDataViewWithoutDeleteTimetick(published)); err != nil {
				return nil, merr.Wrap(
					publishedDataViewPersistenceError(collectionID, err),
					"backfill legacy DataView head",
				)
			}
			versionStatePersisted = true
		}
		if published != nil {
			manager.retainRecoveredDataViewsThrough(collectionID, published.GetDataVersion())
			manager.recoverCollectionFromDataViews(collectionID, []*viewpb.DataViewOfCollection{published})
			retained, _ := manager.recoveredDataViews(collectionID)
			manager.updateRetainedMembership(collectionID, retained)
		} else if versionStatePersisted {
			manager.retainRecoveredDataViewsThrough(collectionID, nil)
			manager.updateRetainedMembership(collectionID, nil)
			manager.recoverCollectionFromDataViews(collectionID, nil)
		}
		state := manager.getOrCreateState(collectionID)
		state.mu.Lock()
		state.versionState = proto.Clone(durable).(*viewpb.CollectionDataVersionState)
		state.versionStatePersisted = versionStatePersisted
		state.persistedAllocated = durable.GetAllocatedStreamingVersion()
		state.mu.Unlock()
	}
	return manager, nil
}

func (m *dataViewManager) recoverFromDataViews(dataViews []*viewpb.DataViewOfCollection) {
	viewsByCollection := make(map[int64][]*viewpb.DataViewOfCollection)
	recoveredViews := make(map[int64][]*viewpb.DataViewOfCollection)
	for _, view := range dataViews {
		if view == nil {
			continue
		}
		collectionID := view.GetCollectionId()
		viewsByCollection[collectionID] = append(viewsByCollection[collectionID], view)
		recoveredViews[collectionID] = append(recoveredViews[collectionID], canonicalDataViewClone(view))
	}
	m.mu.Lock()
	m.recoveredAll = true
	m.recoveredViews = recoveredViews
	m.mu.Unlock()

	collectionIDs := make([]int64, 0, len(viewsByCollection))
	for collectionID := range viewsByCollection {
		collectionIDs = append(collectionIDs, collectionID)
	}
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })
	for _, collectionID := range collectionIDs {
		m.recoverCollectionFromDataViews(collectionID, viewsByCollection[collectionID])
		m.updateRetainedMembership(collectionID, viewsByCollection[collectionID])
	}
}

func (m *dataViewManager) recoveredDataViews(collectionID int64) ([]*viewpb.DataViewOfCollection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.recoveredAll {
		return nil, false
	}
	return cloneDataViews(m.recoveredViews[collectionID]), true
}

func (m *dataViewManager) rememberRecoveredDataView(view *viewpb.DataViewOfCollection) {
	if view == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.recoveredAll {
		return
	}
	collectionID := view.GetCollectionId()
	version := view.GetDataVersion()
	views := m.recoveredViews[collectionID]
	for idx, recovered := range views {
		if compareDataVersion(recovered.GetDataVersion(), version) == 0 {
			views[idx] = canonicalDataViewClone(view)
			m.recoveredViews[collectionID] = views
			return
		}
	}
	m.recoveredViews[collectionID] = append(views, canonicalDataViewClone(view))
}

func (m *dataViewManager) retainRecoveredDataViewsThrough(collectionID int64, head *viewpb.DataVersion) {
	m.mu.Lock()
	defer m.mu.Unlock()
	views := m.recoveredViews[collectionID]
	retained := views[:0]
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), head) <= 0 {
			retained = append(retained, view)
		}
	}
	m.recoveredViews[collectionID] = retained
}

func (m *dataViewManager) recoverCollectionFromDataViews(collectionID int64, persistedViews []*viewpb.DataViewOfCollection) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.dropped = false
	state.published = canonicalDataViewClone(latestDataView(persistedViews))
}

func (m *dataViewManager) latestLegacyLoadablePersistedView(
	ctx context.Context,
	views []*viewpb.DataViewOfCollection,
) *viewpb.DataViewOfCollection {
	ordered := cloneDataViews(views)
	sort.Slice(ordered, func(i, j int) bool {
		return compareDataVersion(ordered[i].GetDataVersion(), ordered[j].GetDataVersion()) < 0
	})
	var latest *viewpb.DataViewOfCollection
	historicalSegments := make(map[int64]struct{})
	for _, view := range ordered {
		loadable := true
		for segmentID := range dataViewSegmentIDSet(view) {
			segment := m.segments.GetSegment(ctx, segmentID)
			_, appearedBefore := historicalSegments[segmentID]
			if !isLegacyMigratableMembership(segment, appearedBefore) {
				loadable = false
				break
			}
		}
		if loadable {
			latest = view
		}
		for segmentID := range dataViewSegmentIDSet(view) {
			historicalSegments[segmentID] = struct{}{}
		}
	}
	return m.withDeleteTimetick(ctx, latest)
}

func isLegacyMigratableMembership(segment *Segment, appearedBefore bool) bool {
	if segment == nil || segment.GetIsInvisible() || segment.GetIsImporting() || segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if segment.GetState() == commonpb.SegmentState_Flushed {
		return true
	}
	return appearedBefore && segment.GetState() == commonpb.SegmentState_Dropped
}
