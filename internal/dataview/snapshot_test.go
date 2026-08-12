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

func TestSnapshotOwnsImmutableMembershipAndSegmentInfo(t *testing.T) {
	collection := &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 2},
		Shards: []*viewpb.DataViewOfShard{{
			Vchannel: "v1",
			Partitions: []*viewpb.DataViewOfPartition{{
				PartitionId: 10,
				SegmentIds:  []int64{100},
			}},
		}},
	}
	segments := segmentSnapshot{100: {SegmentID: 100, PartitionID: 10, RowNum: 1000}}
	snapshot := NewSnapshot(1, []*viewpb.DataViewOfCollection{collection}, segments)

	collection.Shards[0].Partitions[0].SegmentIds[0] = 999
	segments[100].RowNum = 9999

	shard, ok := snapshot.ShardView(1, "v1")
	require.True(t, ok)
	require.Equal(t, []int64{100}, shard.GetPartitions()[0].GetSegmentIds())
	info, ok := snapshot.SegmentInfo(100)
	require.True(t, ok)
	require.Equal(t, int64(1000), info.RowNum)

	shard.Partitions[0].SegmentIds[0] = 888
	info.RowNum = 8888
	snapshot.RangeShards(1, func(shard *viewpb.DataViewOfShard) bool {
		shard.Partitions[0].SegmentIds[0] = 777
		return true
	})

	shard, ok = snapshot.ShardView(1, "v1")
	require.True(t, ok)
	require.Equal(t, []int64{100}, shard.GetPartitions()[0].GetSegmentIds())
	info, ok = snapshot.SegmentInfo(100)
	require.True(t, ok)
	require.Equal(t, int64(1000), info.RowNum)
}
