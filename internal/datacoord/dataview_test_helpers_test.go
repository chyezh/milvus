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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func latestPublishedDataView(
	ctx context.Context,
	manager interface {
		LatestPublished(context.Context, int64) (dataview.DataViewRef, error)
	},
	collectionID int64,
) (*viewpb.DataViewOfCollection, error) {
	ref, err := manager.LatestPublished(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	defer ref.Deref()
	dataView := ref.DataView()
	result := &viewpb.DataViewOfCollection{
		CollectionId: dataView.CollectionID(),
		DataVersion:  dataView.Version().IntoProto(),
	}
	shards := make(map[string]*viewpb.DataViewOfShard)
	for _, vchannel := range dataView.VChannels() {
		shard := &viewpb.DataViewOfShard{Vchannel: vchannel}
		result.Shards = append(result.Shards, shard)
		shards[vchannel] = shard
	}
	dataView.RangePartitions(func(vchannel string, partitionID int64, segmentIDs []int64) bool {
		shards[vchannel].Partitions = append(shards[vchannel].Partitions, &viewpb.DataViewOfPartition{
			PartitionId: partitionID,
			SegmentIds:  segmentIDs,
		})
		return true
	})
	return result, nil
}
