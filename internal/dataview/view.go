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
	"sort"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type dataViewPartitionKey struct {
	vchannel    string
	partitionID int64
}

// DataView is an immutable collection data view.
type DataView struct {
	collectionID int64
	version      qviews.DataVersion
	vchannels    map[string]struct{}
	segmentIDs   map[dataViewPartitionKey][]int64
}

func newDataView(view *viewpb.DataViewOfCollection) *DataView {
	dataView := &DataView{
		collectionID: view.GetCollectionId(),
		version:      qviews.FromProtoDataVersion(view.GetDataVersion()),
		vchannels:    make(map[string]struct{}),
		segmentIDs:   make(map[dataViewPartitionKey][]int64),
	}
	for _, shard := range view.GetShards() {
		dataView.vchannels[shard.GetVchannel()] = struct{}{}
		for _, partition := range shard.GetPartitions() {
			key := dataViewPartitionKey{
				vchannel:    shard.GetVchannel(),
				partitionID: partition.GetPartitionId(),
			}
			dataView.segmentIDs[key] = append([]int64(nil), partition.GetSegmentIds()...)
		}
	}
	return dataView
}

// HasVChannel reports whether the immutable view contains the shard.
func (v *DataView) HasVChannel(vchannel string) bool {
	if v == nil {
		return false
	}
	_, ok := v.vchannels[vchannel]
	return ok
}

// VChannels returns the immutable view's shard names in deterministic order.
func (v *DataView) VChannels() []string {
	if v == nil {
		return nil
	}
	vchannels := make([]string, 0, len(v.vchannels))
	for vchannel := range v.vchannels {
		vchannels = append(vchannels, vchannel)
	}
	sort.Strings(vchannels)
	return vchannels
}

// RangePartitions visits immutable membership in deterministic shard and
// partition order. Segment IDs are copied before they are passed to yield.
func (v *DataView) RangePartitions(yield func(vchannel string, partitionID int64, segmentIDs []int64) bool) {
	if v == nil || yield == nil {
		return
	}
	keys := make([]dataViewPartitionKey, 0, len(v.segmentIDs))
	for key := range v.segmentIDs {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].vchannel != keys[j].vchannel {
			return keys[i].vchannel < keys[j].vchannel
		}
		return keys[i].partitionID < keys[j].partitionID
	})
	for _, key := range keys {
		if !yield(key.vchannel, key.partitionID, append([]int64(nil), v.segmentIDs[key]...)) {
			return
		}
	}
}

// CollectionID returns the collection ID of the view.
func (v *DataView) CollectionID() int64 {
	return v.collectionID
}

// Version returns the immutable version of the view.
func (v *DataView) Version() qviews.DataVersion {
	return v.version
}

// SegmentIDs returns a copy of the segment IDs for a partition in a shard.
func (v *DataView) SegmentIDs(vchannel string, partitionID int64) []int64 {
	key := dataViewPartitionKey{vchannel: vchannel, partitionID: partitionID}
	return append([]int64(nil), v.segmentIDs[key]...)
}

// SegmentIDsForVChannel returns a copy of segment IDs in a shard, optionally
// restricted to the requested partitions. An empty partition list selects all
// partitions in the shard.
func (v *DataView) SegmentIDsForVChannel(vchannel string, partitionIDs []int64) []int64 {
	if v == nil {
		return nil
	}
	partitionSet := make(map[int64]struct{}, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionSet[partitionID] = struct{}{}
	}
	keys := make([]dataViewPartitionKey, 0)
	for key := range v.segmentIDs {
		if key.vchannel != vchannel {
			continue
		}
		if len(partitionSet) > 0 {
			if _, ok := partitionSet[key.partitionID]; !ok {
				continue
			}
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].partitionID < keys[j].partitionID })
	segmentIDs := make([]int64, 0)
	seen := make(map[int64]struct{})
	for _, key := range keys {
		ids := append([]int64(nil), v.segmentIDs[key]...)
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, segmentID := range ids {
			if _, ok := seen[segmentID]; ok {
				continue
			}
			seen[segmentID] = struct{}{}
			segmentIDs = append(segmentIDs, segmentID)
		}
	}
	return segmentIDs
}
