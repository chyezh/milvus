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
	"math"
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (m *dataViewManager) snapshot(ctx context.Context, collectionIDs []int64) []*viewpb.DataViewOfCollection {
	states := make([]*collectionDataViewState, 0, len(collectionIDs))
	if len(collectionIDs) == 0 {
		states = m.listStates()
	} else {
		for _, collectionID := range collectionIDs {
			state := m.getState(collectionID)
			if state != nil {
				states = append(states, state)
			}
		}
	}
	sort.Slice(states, func(i, j int) bool { return states[i].collectionID < states[j].collectionID })

	views := make([]*viewpb.DataViewOfCollection, 0, len(states))
	for _, state := range states {
		state.mu.RLock()
		if state.dropped || state.published == nil {
			state.mu.RUnlock()
			continue
		}
		views = append(views, m.withDeleteTimetick(ctx, state.published))
		state.mu.RUnlock()
	}
	return views
}

type segmentSnapshot map[int64]*SegmentInfo

func (s segmentSnapshot) Get(segmentID int64) (*SegmentInfo, bool) {
	info, ok := s[segmentID]
	return info, ok
}

func (s segmentSnapshot) Range(yield func(segmentID int64, info *SegmentInfo) bool) {
	for segmentID, info := range s {
		if !yield(segmentID, info) {
			return
		}
	}
}

// SegmentSnapshot looks up arbitrary segment metadata without requiring the
// segments to belong to the latest visible DataViews.
func (m *dataViewManager) SegmentSnapshot(ctx context.Context, segmentIDs []int64) SegmentSnapshot {
	return newSegmentSnapshot(segmentIDs, m.getSegments(ctx, segmentIDs))
}

func (m *dataViewManager) getSegments(ctx context.Context, segmentIDs []int64) map[int64]*Segment {
	segments := make(map[int64]*Segment, len(segmentIDs))
	if len(segmentIDs) == 0 {
		return segments
	}
	for _, segment := range m.segments.GetSegments(ctx, segmentIDs) {
		if segment != nil {
			segments[segment.GetID()] = segment
		}
	}
	return segments
}

func newSegmentSnapshot(segmentIDs []int64, segmentsByID map[int64]*Segment) SegmentSnapshot {
	segments := make(segmentSnapshot, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segment := segmentsByID[segmentID]
		if segment == nil {
			continue
		}
		segments[segmentID] = &SegmentInfo{
			SegmentID:   segment.GetID(),
			PartitionID: segment.GetPartitionID(),
			MemSize:     segment.GetMemSize(),
			RowNum:      segment.GetNumOfRows(),
		}
	}
	return segments
}

// setDataViewDeleteTimeticks derives each shard's minimum transform-start
// timetick from the prefetched segments. Empty shards or any missing member use
// zero so consumers do not advance beyond unknown metadata.
func setDataViewDeleteTimeticks(views []*viewpb.DataViewOfCollection, segments map[int64]*Segment) {
	for _, view := range views {
		for _, shard := range view.GetShards() {
			minTs := uint64(math.MaxUint64)
			hasSegment := false
			missingSegment := false
			for _, partition := range shard.GetPartitions() {
				for _, segmentID := range partition.GetSegmentIds() {
					hasSegment = true
					segment := segments[segmentID]
					if segment == nil {
						missingSegment = true
						continue
					}
					if ts := segmentTransformStartAfterTimetick(segment); ts < minTs {
						minTs = ts
					}
				}
			}
			if !hasSegment || missingSegment {
				shard.TransformStartAfterTimetick = 0
			} else {
				shard.TransformStartAfterTimetick = minTs
			}
		}
	}
}

func (m *dataViewManager) ShardTimeTicks(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewShardTimeTick, error) {
	views := m.snapshot(ctx, collectionIDs)
	timeticks := make([]*viewpb.DataViewShardTimeTick, 0)
	for _, view := range views {
		timeticks = append(timeticks, dataViewTimeTicks(view)...)
	}
	return timeticks, nil
}

func (m *dataViewManager) withDeleteTimetick(ctx context.Context, view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	clone := canonicalDataViewClone(view)
	if clone == nil {
		return nil
	}
	for _, shard := range clone.GetShards() {
		minTs := uint64(math.MaxUint64)
		hasSegment := false
		for _, partition := range shard.GetPartitions() {
			for _, segmentID := range partition.GetSegmentIds() {
				hasSegment = true
				segment := m.segments.GetSegment(ctx, segmentID)
				ts := segmentTransformStartAfterTimetick(segment)
				if ts < minTs {
					minTs = ts
				}
			}
		}
		if hasSegment {
			shard.TransformStartAfterTimetick = minTs
		} else {
			shard.TransformStartAfterTimetick = 0
		}
	}
	return clone
}

func dataViewTimeTicks(view *viewpb.DataViewOfCollection) []*viewpb.DataViewShardTimeTick {
	if view == nil {
		return nil
	}
	timeticks := make([]*viewpb.DataViewShardTimeTick, 0, len(view.GetShards()))
	for _, shard := range view.GetShards() {
		timeticks = append(timeticks, &viewpb.DataViewShardTimeTick{
			Vchannel:                    shard.GetVchannel(),
			TransformStartAfterTimetick: shard.GetTransformStartAfterTimetick(),
		})
	}
	return timeticks
}

func segmentTransformStartAfterTimetick(segment *Segment) uint64 {
	if segment == nil {
		return 0
	}
	if ts := segment.GetTransformStartAfterTimetick(); ts != 0 {
		return ts
	}
	if ts := segment.GetCommitTimestamp(); ts != 0 {
		return ts
	}
	if segment.GetStartPosition() != nil {
		return segment.GetStartPosition().GetTimestamp()
	}
	return 0
}
