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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestDataViewImmutableMembershipAccessors(t *testing.T) {
	view := newDataView(&viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1},
		Shards: []*viewpb.DataViewOfShard{
			{
				Vchannel: "ch-b",
				Partitions: []*viewpb.DataViewOfPartition{
					{PartitionId: 20, SegmentIds: []int64{200}},
					{PartitionId: 10, SegmentIds: []int64{101, 100}},
				},
			},
			{Vchannel: "ch-a"},
		},
	})

	require.Equal(t, []string{"ch-a", "ch-b"}, view.VChannels())
	require.True(t, view.HasVChannel("ch-a"))
	require.False(t, view.HasVChannel("ch-missing"))
	require.Equal(t, []int64{100, 101, 200}, view.SegmentIDsForVChannel("ch-b", nil))
	require.Equal(t, []int64{100, 101}, view.SegmentIDsForVChannel("ch-b", []int64{10}))

	type visitedPartition struct {
		vchannel    string
		partitionID int64
		segmentIDs  []int64
	}
	visited := make([]visitedPartition, 0)
	view.RangePartitions(func(vchannel string, partitionID int64, segmentIDs []int64) bool {
		visited = append(visited, visitedPartition{vchannel, partitionID, segmentIDs})
		segmentIDs[0] = -1
		return true
	})
	require.Equal(t, []visitedPartition{
		{"ch-b", 10, []int64{-1, 100}},
		{"ch-b", 20, []int64{-1}},
	}, visited)
	require.Equal(t, []int64{101, 100}, view.SegmentIDs("ch-b", 10))
}

func TestNilDataViewMembershipAccessors(t *testing.T) {
	var view *DataView
	require.Nil(t, view.VChannels())
	require.False(t, view.HasVChannel("ch-a"))
	require.Nil(t, view.SegmentIDsForVChannel("ch-a", nil))
	view.RangePartitions(func(string, int64, []int64) bool {
		require.FailNow(t, "nil view must not yield membership")
		return false
	})
}
