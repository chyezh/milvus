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
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// SegmentMembership is the caller-validated final location and lifecycle state
// of one segment submitted for publication. It deliberately contains no
// SegmentMeta handle so publication never needs to rediscover membership.
type SegmentMembership struct {
	SegmentID    int64
	CollectionID int64
	PartitionID  int64
	VChannel     string
	State        commonpb.SegmentState
	Level        datapb.SegmentLevel
	IsImporting  bool
	IsInvisible  bool
}

// PublishedMutation is an explicit mutation of published, loadable membership.
type PublishedMutation struct {
	Add    []SegmentMembership
	Remove []int64
}

func (membership SegmentMembership) loadable() bool {
	return membership.State == commonpb.SegmentState_Flushed &&
		!membership.IsImporting &&
		!membership.IsInvisible &&
		membership.Level != datapb.SegmentLevel_L0
}

func applyPublishedMutation(view *viewpb.DataViewOfCollection, mutation PublishedMutation) bool {
	changed := false
	for _, segmentID := range mutation.Remove {
		changed = removeSegmentFromDataView(view, segmentID) || changed
	}
	for _, membership := range mutation.Add {
		changed = addPublishedMembership(view, membership) || changed
	}
	canonicalizeDataView(view)
	return changed
}

func addPublishedMembership(view *viewpb.DataViewOfCollection, membership SegmentMembership) bool {
	if view == nil || membership.CollectionID != view.GetCollectionId() || dataViewContainsSegment(view, membership.SegmentID) {
		return false
	}
	shard := findOrCreateDataViewShard(view, membership.VChannel)
	partition := findOrCreateDataViewPartition(shard, membership.PartitionID)
	partition.SegmentIds = append(partition.SegmentIds, membership.SegmentID)
	return true
}

func dataViewContainsMembership(view *viewpb.DataViewOfCollection, membership SegmentMembership) bool {
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() != membership.VChannel {
			continue
		}
		for _, partition := range shard.GetPartitions() {
			if partition.GetPartitionId() != membership.PartitionID {
				continue
			}
			for _, segmentID := range partition.GetSegmentIds() {
				if segmentID == membership.SegmentID {
					return true
				}
			}
		}
	}
	return false
}
