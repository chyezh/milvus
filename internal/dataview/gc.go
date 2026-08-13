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

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (m *dataViewManager) IsSegmentReferenced(ctx context.Context, collectionID int64, segmentID int64) (bool, error) {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	m.retainedMu.RLock()
	segments := m.retained[collectionID]
	_, referenced := segments[segmentID]
	m.retainedMu.RUnlock()
	if segments != nil {
		return referenced, nil
	}
	if err := m.recoverPublicationStateLocked(ctx, state, m.catalog); err != nil {
		return true, err
	}
	publishedVersion := cloneDataVersion(state.versionState.GetPublishedDataVersion())
	views, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return true, err
	}
	retained := views[:0]
	for _, view := range views {
		version := view.GetDataVersion()
		_, hasLiveRef := state.refs[qviews.FromProtoDataVersion(version)]
		isPublishedHistory := !state.versionStatePersisted ||
			(publishedVersion != nil && compareDataVersion(version, publishedVersion) <= 0)
		if isPublishedHistory || hasLiveRef {
			retained = append(retained, view)
		}
	}
	m.updateRetainedMembership(collectionID, retained)
	m.retainedMu.RLock()
	_, referenced = m.retained[collectionID][segmentID]
	m.retainedMu.RUnlock()
	return referenced, nil
}

func (m *dataViewManager) GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error {
	state := m.getOrCreateState(collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	if retainLatest < 1 {
		retainLatest = 1
	}
	views, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return err
	}
	versionState, err := m.catalog.GetDataViewVersionState(ctx, collectionID)
	if err != nil {
		return err
	}
	versionStatePersisted := versionState != nil
	var publishedVersion *viewpb.DataVersion
	if versionState != nil && versionState.GetPublishedDataVersion() != nil {
		publishedVersion = cloneDataVersion(versionState.GetPublishedDataVersion())
	}
	sort.Slice(views, func(i, j int) bool {
		return compareDataVersion(views[i].GetDataVersion(), views[j].GetDataVersion()) > 0
	})
	protectedSet := make(map[string]struct{})
	for version, count := range state.refs {
		if count > 0 {
			protectedSet[dataVersionKey(version.IntoProto())] = struct{}{}
		}
	}
	if publishedVersion != nil {
		// The version-state record is the authority after recovery. A newer
		// snapshot can only be an unpublished orphan and must not displace the
		// published head in the retention window.
		protectedSet[dataVersionKey(publishedVersion)] = struct{}{}
	}
	foundPublished := publishedVersion == nil
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), publishedVersion) == 0 {
			foundPublished = true
			break
		}
	}
	if !foundPublished {
		return merr.WrapErrDataIntegrityMsg(
			"published data view snapshot is missing for collection %d at version %d/%d",
			collectionID,
			publishedVersion.GetStreamingVersion(),
			publishedVersion.GetCompactVersion(),
		)
	}
	authoritativeCount := 0
	for _, view := range views {
		version := view.GetDataVersion()
		isPublishedHistory := (!versionStatePersisted && publishedVersion == nil) ||
			(publishedVersion != nil && compareDataVersion(version, publishedVersion) <= 0)
		if isPublishedHistory && authoritativeCount < retainLatest {
			authoritativeCount++
			continue
		}
		if _, ok := protectedSet[dataVersionKey(version)]; ok {
			continue
		}
		if err := m.catalog.DropDataView(ctx, collectionID, version); err != nil {
			return err
		}
	}
	remaining, err := m.catalog.ListDataViews(ctx, collectionID)
	if err != nil {
		return err
	}
	m.updateRetainedMembership(collectionID, remaining)
	return nil
}

func (m *dataViewManager) updateRetainedMembership(collectionID int64, views []*viewpb.DataViewOfCollection) {
	segments := make(map[int64]int)
	for _, view := range views {
		for _, partition := range dataViewPartitions(view) {
			for _, segmentID := range partition.GetSegmentIds() {
				segments[segmentID]++
			}
		}
	}
	m.retainedMu.Lock()
	m.retained[collectionID] = segments
	m.retainedMu.Unlock()
}

func (m *dataViewManager) invalidateRetainedMembership(collectionID int64) {
	m.retainedMu.Lock()
	delete(m.retained, collectionID)
	m.retainedMu.Unlock()
}
