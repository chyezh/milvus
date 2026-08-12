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
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

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

func latestDataView(views []*viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	var latest *viewpb.DataViewOfCollection
	for _, view := range views {
		if compareDataVersion(view.GetDataVersion(), latest.GetDataVersion()) > 0 {
			latest = view
		}
	}
	return canonicalDataViewClone(latest)
}

func compareDataVersion(left, right *viewpb.DataVersion) int {
	leftStreaming, leftCompact := int64(0), int64(0)
	if left != nil {
		leftStreaming = left.GetStreamingVersion()
		leftCompact = left.GetCompactVersion()
	}
	rightStreaming, rightCompact := int64(0), int64(0)
	if right != nil {
		rightStreaming = right.GetStreamingVersion()
		rightCompact = right.GetCompactVersion()
	}
	if leftStreaming != rightStreaming {
		if leftStreaming > rightStreaming {
			return 1
		}
		return -1
	}
	if leftCompact != rightCompact {
		if leftCompact > rightCompact {
			return 1
		}
		return -1
	}
	return 0
}

func removeSegmentFromDataView(view *viewpb.DataViewOfCollection, segmentID int64) bool {
	return removeSegmentsByPredicate(view, func(id int64, partitionID int64, vchannel string) bool {
		return id == segmentID
	})
}

func removeSegmentsByPredicate(view *viewpb.DataViewOfCollection, predicate func(segmentID int64, partitionID int64, vchannel string) bool) bool {
	changed := false
	for _, shard := range view.GetShards() {
		partitions := shard.Partitions[:0]
		for _, partition := range shard.GetPartitions() {
			segmentIDs := partition.SegmentIds[:0]
			for _, segmentID := range partition.GetSegmentIds() {
				if predicate(segmentID, partition.GetPartitionId(), shard.GetVchannel()) {
					changed = true
					continue
				}
				segmentIDs = append(segmentIDs, segmentID)
			}
			partition.SegmentIds = segmentIDs
			if len(partition.GetSegmentIds()) > 0 {
				partitions = append(partitions, partition)
			}
		}
		shard.Partitions = partitions
	}
	shards := view.Shards[:0]
	for _, shard := range view.GetShards() {
		if len(shard.GetPartitions()) > 0 {
			shards = append(shards, shard)
		}
	}
	view.Shards = shards
	return changed
}

func findOrCreateDataViewShard(view *viewpb.DataViewOfCollection, vchannel string) *viewpb.DataViewOfShard {
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard
		}
	}
	shard := &viewpb.DataViewOfShard{Vchannel: vchannel}
	view.Shards = append(view.Shards, shard)
	return shard
}

func findOrCreateDataViewPartition(shard *viewpb.DataViewOfShard, partitionID int64) *viewpb.DataViewOfPartition {
	for _, partition := range shard.GetPartitions() {
		if partition.GetPartitionId() == partitionID {
			return partition
		}
	}
	partition := &viewpb.DataViewOfPartition{PartitionId: partitionID}
	shard.Partitions = append(shard.Partitions, partition)
	return partition
}

func dataViewPartitions(view *viewpb.DataViewOfCollection) []*viewpb.DataViewOfPartition {
	if view == nil {
		return nil
	}
	partitions := make([]*viewpb.DataViewOfPartition, 0)
	for _, shard := range view.GetShards() {
		partitions = append(partitions, shard.GetPartitions()...)
	}
	return partitions
}

func canonicalDataViewClone(view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	if view == nil {
		return nil
	}
	clone := proto.Clone(view).(*viewpb.DataViewOfCollection)
	canonicalizeDataView(clone)
	return clone
}

func cloneDataViews(views []*viewpb.DataViewOfCollection) []*viewpb.DataViewOfCollection {
	if len(views) == 0 {
		return nil
	}
	clones := make([]*viewpb.DataViewOfCollection, 0, len(views))
	for _, view := range views {
		if view == nil {
			continue
		}
		clones = append(clones, canonicalDataViewClone(view))
	}
	return clones
}

func cloneDataVersion(version *viewpb.DataVersion) *viewpb.DataVersion {
	if version == nil {
		return nil
	}
	return proto.Clone(version).(*viewpb.DataVersion)
}

func dataVersionFromView(view *viewpb.DataViewOfCollection) *viewpb.DataVersion {
	if view == nil {
		return nil
	}
	return cloneDataVersion(view.GetDataVersion())
}

func cloneDataViewWithoutDeleteTimetick(view *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	clone := canonicalDataViewClone(view)
	if clone == nil {
		return nil
	}
	for _, shard := range clone.GetShards() {
		shard.TransformStartAfterTimetick = 0
	}
	return clone
}

func canonicalizeDataView(view *viewpb.DataViewOfCollection) {
	if view == nil {
		return
	}
	sort.Slice(view.Shards, func(i, j int) bool {
		return view.Shards[i].GetVchannel() < view.Shards[j].GetVchannel()
	})
	for _, shard := range view.GetShards() {
		sort.Slice(shard.Partitions, func(i, j int) bool {
			return shard.Partitions[i].GetPartitionId() < shard.Partitions[j].GetPartitionId()
		})
		for _, partition := range shard.GetPartitions() {
			sort.Slice(partition.SegmentIds, func(i, j int) bool {
				return partition.SegmentIds[i] < partition.SegmentIds[j]
			})
			partition.SegmentIds = dedupSortedInt64s(partition.SegmentIds)
		}
	}
}

func buildEmptyDataView(collectionID int64, vchannels []string) *viewpb.DataViewOfCollection {
	view := &viewpb.DataViewOfCollection{
		CollectionId: collectionID,
		DataVersion:  &viewpb.DataVersion{},
	}
	if len(vchannels) == 0 {
		return view
	}
	seen := make(map[string]struct{}, len(vchannels))
	for _, vchannel := range vchannels {
		if _, ok := seen[vchannel]; ok {
			continue
		}
		seen[vchannel] = struct{}{}
		view.Shards = append(view.Shards, &viewpb.DataViewOfShard{Vchannel: vchannel})
	}
	canonicalizeDataView(view)
	return view
}

func dedupSortedInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return values
	}
	write := 1
	for read := 1; read < len(values); read++ {
		if values[read] == values[write-1] {
			continue
		}
		values[write] = values[read]
		write++
	}
	return values[:write]
}

func isDataViewMembershipEqual(left, right *viewpb.DataViewOfCollection) bool {
	leftClone := cloneDataViewWithoutDeleteTimetick(left)
	rightClone := cloneDataViewWithoutDeleteTimetick(right)
	if leftClone != nil {
		leftClone.DataVersion = nil
	}
	if rightClone != nil {
		rightClone.DataVersion = nil
	}
	if isDataViewEmpty(leftClone) && isDataViewEmpty(rightClone) {
		return true
	}
	return proto.Equal(leftClone, rightClone)
}

func isDataViewEmpty(view *viewpb.DataViewOfCollection) bool {
	return view == nil || len(view.GetShards()) == 0
}

func dataViewSegmentIDSet(view *viewpb.DataViewOfCollection) map[int64]struct{} {
	segments := make(map[int64]struct{})
	for _, partition := range dataViewPartitions(view) {
		for _, segmentID := range partition.GetSegmentIds() {
			segments[segmentID] = struct{}{}
		}
	}
	return segments
}

func dataViewContainsSegment(view *viewpb.DataViewOfCollection, target int64) bool {
	for _, partition := range dataViewPartitions(view) {
		for _, segmentID := range partition.GetSegmentIds() {
			if segmentID == target {
				return true
			}
		}
	}
	return false
}

func dataVersionKey(version *viewpb.DataVersion) string {
	return fmt.Sprintf("%d/%d", version.GetStreamingVersion(), version.GetCompactVersion())
}
