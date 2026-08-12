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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The legacy helpers below keep old unit-test scenarios readable while the
// production manager exposes only explicit publication operations.
type FlushDataViewEvent struct {
	CollectionID         int64
	SegmentIDs           []int64
	TemporaryUnavailable bool
	AssignedVersion      *viewpb.DataVersion
}

type ImportDataViewEvent struct {
	CollectionID int64
	SegmentIDs   []int64
}

type CopySegmentCompleteDataViewEvent struct {
	CollectionID int64
	SegmentIDs   []int64
}

type CompactDataViewEvent struct {
	CollectionID     int64
	CompactFrom      []int64
	CompactTo        []int64
	AllowInvisibleTo bool
}

type L0CompactDataViewEvent struct {
	CollectionID int64
}

type ExternalRefreshDataViewEvent struct {
	CollectionID int64
	AddSegments  []int64
	DropSegments []int64
}

type DropPartitionDataViewEvent struct {
	CollectionID int64
	PartitionIDs []int64
}

type TruncateDataViewEvent struct {
	CollectionID int64
	VChannel     string
	FlushTs      uint64
}

type dataViewAdvance int

const (
	dataViewAdvanceNone dataViewAdvance = iota
	dataViewAdvanceStreaming
	dataViewAdvanceCompact
)

type dataViewMembershipMutation struct {
	collectionID    int64
	addSegmentIDs   []int64
	dropSegmentIDs  []int64
	advance         dataViewAdvance
	allowInvisible  bool
	assignedVersion *viewpb.DataVersion
	dropPredicate   func(segmentID int64, partitionID int64, vchannel string) bool
	classifyAdvance func(removed bool, added bool) dataViewAdvance
}

func (m *dataViewManager) OnFlush(ctx context.Context, event FlushDataViewEvent) (*viewpb.DataVersion, error) {
	if event.TemporaryUnavailable {
		if event.AssignedVersion != nil {
			return cloneDataVersion(event.AssignedVersion), nil
		}
		state := m.getState(event.CollectionID)
		if state == nil {
			return nil, nil
		}
		state.mu.RLock()
		defer state.mu.RUnlock()
		return dataVersionFromView(state.published), nil
	}
	return m.applyLegacyTestMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID:    event.CollectionID,
		addSegmentIDs:   event.SegmentIDs,
		advance:         dataViewAdvanceStreaming,
		assignedVersion: event.AssignedVersion,
	})
}

func (m *dataViewManager) OnImport(ctx context.Context, event ImportDataViewEvent) (*viewpb.DataVersion, error) {
	return m.applyLegacyTestMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID: event.CollectionID, addSegmentIDs: event.SegmentIDs, advance: dataViewAdvanceStreaming,
	})
}

func (m *dataViewManager) OnCopySegmentComplete(ctx context.Context, event CopySegmentCompleteDataViewEvent) (*viewpb.DataVersion, error) {
	return m.applyLegacyTestMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID: event.CollectionID, addSegmentIDs: event.SegmentIDs, advance: dataViewAdvanceStreaming,
	})
}

func (m *dataViewManager) OnCompact(ctx context.Context, event CompactDataViewEvent) (*viewpb.DataVersion, error) {
	if m.hasPendingLegacyTestCompactOutput(ctx, event.CompactTo, event.AllowInvisibleTo) {
		return m.legacyTestPublishedVersion(event.CollectionID), nil
	}
	return m.applyLegacyTestMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID: event.CollectionID, addSegmentIDs: event.CompactTo,
		dropSegmentIDs: event.CompactFrom, advance: dataViewAdvanceCompact,
	})
}

func (m *dataViewManager) OnL0Compact(ctx context.Context, event L0CompactDataViewEvent) (*viewpb.DataVersion, error) {
	state := m.getState(event.CollectionID)
	if state == nil {
		return nil, nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped {
		return nil, nil
	}
	if state.published != nil {
		state.published = m.withDeleteTimetick(ctx, state.published)
	}
	return dataVersionFromView(state.published), nil
}

func (m *dataViewManager) OnExternalRefresh(ctx context.Context, event ExternalRefreshDataViewEvent) (*viewpb.DataVersion, error) {
	return m.applyLegacyTestMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID: event.CollectionID, addSegmentIDs: event.AddSegments, dropSegmentIDs: event.DropSegments,
		classifyAdvance: func(removed, added bool) dataViewAdvance {
			if removed {
				return dataViewAdvanceCompact
			}
			if added {
				return dataViewAdvanceStreaming
			}
			return dataViewAdvanceNone
		},
	})
}

func (m *dataViewManager) OnDropPartition(ctx context.Context, event DropPartitionDataViewEvent) (*viewpb.DataVersion, error) {
	partitions := make(map[int64]struct{}, len(event.PartitionIDs))
	for _, partitionID := range event.PartitionIDs {
		partitions[partitionID] = struct{}{}
	}
	return m.applyLegacyTestMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID: event.CollectionID, advance: dataViewAdvanceCompact,
		dropPredicate: func(_ int64, partitionID int64, _ string) bool {
			_, ok := partitions[partitionID]
			return ok
		},
	})
}

func (m *dataViewManager) OnTruncate(ctx context.Context, event TruncateDataViewEvent) (*viewpb.DataVersion, error) {
	return m.applyLegacyTestMembershipMutation(ctx, dataViewMembershipMutation{
		collectionID: event.CollectionID, advance: dataViewAdvanceCompact,
		dropPredicate: func(segmentID int64, _ int64, vchannel string) bool {
			segment := m.segments.GetSegment(ctx, segmentID)
			return vchannel == event.VChannel && segment != nil && legacyTestSegmentEffectiveDmlTs(segment) <= event.FlushTs
		},
	})
}

func (m *dataViewManager) applyLegacyTestMembershipMutation(ctx context.Context, mutation dataViewMembershipMutation) (*viewpb.DataVersion, error) {
	state := m.getOrCreateState(mutation.collectionID)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dropped {
		return nil, nil
	}
	next := canonicalDataViewClone(state.published)
	if next == nil {
		next = &viewpb.DataViewOfCollection{CollectionId: mutation.collectionID, DataVersion: &viewpb.DataVersion{}}
	}
	removed, added := false, false
	for _, segmentID := range mutation.dropSegmentIDs {
		removed = removeSegmentFromDataView(next, segmentID) || removed
	}
	if mutation.dropPredicate != nil {
		removed = removeSegmentsByPredicate(next, mutation.dropPredicate) || removed
	}
	for _, segmentID := range mutation.addSegmentIDs {
		segment := m.segments.GetSegment(ctx, segmentID)
		if !isLegacyTestJoinableSegment(segment, mutation.allowInvisible) {
			continue
		}
		added = addLegacyTestSegment(next, segment) || added
	}
	canonicalizeDataView(next)
	membershipEqual := isDataViewMembershipEqual(state.published, next)
	if mutation.assignedVersion != nil &&
		(membershipEqual || compareDataVersion(dataVersionFromView(state.published), mutation.assignedVersion) >= 0) {
		return m.verifyLegacyTestAssignedFlushVersion(ctx, mutation)
	}
	if membershipEqual {
		return dataVersionFromView(state.published), nil
	}
	advance := mutation.advance
	if mutation.classifyAdvance != nil {
		advance = mutation.classifyAdvance(removed, added)
	}
	if mutation.assignedVersion != nil {
		next.DataVersion = cloneDataVersion(mutation.assignedVersion)
	} else {
		next.DataVersion = nextLegacyTestDataVersion(state.published, advance)
	}
	toPersist := cloneDataViewWithoutDeleteTimetick(next)
	if err := m.catalog.SaveDataView(ctx, toPersist); err != nil {
		return nil, err
	}
	state.published = m.withDeleteTimetick(ctx, toPersist)
	m.invalidateRetainedMembership(state.collectionID)
	return dataVersionFromView(state.published), nil
}

func (m *dataViewManager) verifyLegacyTestAssignedFlushVersion(ctx context.Context, mutation dataViewMembershipMutation) (*viewpb.DataVersion, error) {
	views, err := m.catalog.ListDataViews(ctx, mutation.collectionID)
	if err != nil {
		return nil, merr.Wrapf(err, "list DataView snapshots for assigned flush of collection %d", mutation.collectionID)
	}
	for _, view := range views {
		if !proto.Equal(view.GetDataVersion(), mutation.assignedVersion) {
			continue
		}
		for _, segmentID := range mutation.addSegmentIDs {
			if !dataViewContainsSegment(view, segmentID) {
				return nil, merr.WrapErrDataIntegrityMsg("assigned flush DataView for collection %d does not contain segment %d", mutation.collectionID, segmentID)
			}
		}
		return cloneDataVersion(view.GetDataVersion()), nil
	}
	return nil, merr.WrapErrDataIntegrityMsg("assigned flush DataView is missing for collection %d", mutation.collectionID)
}

func (m *dataViewManager) legacyTestPublishedVersion(collectionID int64) *viewpb.DataVersion {
	state := m.getState(collectionID)
	if state == nil {
		return nil
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return dataVersionFromView(state.published)
}

func (m *dataViewManager) hasPendingLegacyTestCompactOutput(ctx context.Context, segmentIDs []int64, allowInvisible bool) bool {
	for _, segmentID := range segmentIDs {
		segment := m.segments.GetSegment(ctx, segmentID)
		if segment != nil && segment.GetState() == commonpb.SegmentState_Flushed && segment.GetLevel() != datapb.SegmentLevel_L0 &&
			(segment.GetIsImporting() || segment.GetIsInvisible() && !allowInvisible) {
			return true
		}
	}
	return false
}

func isLegacyTestJoinableSegment(segment *Segment, allowInvisible bool) bool {
	return segment != nil && segment.GetState() == commonpb.SegmentState_Flushed &&
		segment.GetLevel() != datapb.SegmentLevel_L0 && !segment.GetIsImporting() &&
		(allowInvisible || !segment.GetIsInvisible())
}

func addLegacyTestSegment(view *viewpb.DataViewOfCollection, segment *Segment) bool {
	return addPublishedMembership(view, SegmentMembership{
		SegmentID: segment.GetID(), CollectionID: segment.GetCollectionID(), PartitionID: segment.GetPartitionID(),
		VChannel: segment.GetInsertChannel(), State: segment.GetState(), Level: segment.GetLevel(),
		IsImporting: segment.GetIsImporting(), IsInvisible: segment.GetIsInvisible(),
	})
}

func legacyTestSegmentEffectiveDmlTs(segment *Segment) uint64 {
	if ts := segment.GetCommitTimestamp(); ts != 0 {
		return ts
	}
	return segment.GetDmlPosition().GetTimestamp()
}

func nextLegacyTestDataVersion(base *viewpb.DataViewOfCollection, advance dataViewAdvance) *viewpb.DataVersion {
	if base == nil || base.GetDataVersion().GetStreamingVersion() == 0 {
		return &viewpb.DataVersion{StreamingVersion: 1}
	}
	current := base.GetDataVersion()
	switch advance {
	case dataViewAdvanceStreaming:
		return &viewpb.DataVersion{StreamingVersion: current.GetStreamingVersion() + 1}
	case dataViewAdvanceCompact:
		return &viewpb.DataVersion{StreamingVersion: current.GetStreamingVersion(), CompactVersion: current.GetCompactVersion() + 1}
	default:
		return cloneDataVersion(current)
	}
}
