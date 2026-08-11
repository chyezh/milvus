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
	segmentIDs   map[dataViewPartitionKey][]int64
}

func newDataView(view *viewpb.DataViewOfCollection) *DataView {
	dataView := &DataView{
		collectionID: view.GetCollectionId(),
		version:      qviews.FromProtoDataVersion(view.GetDataVersion()),
		segmentIDs:   make(map[dataViewPartitionKey][]int64),
	}
	for _, shard := range view.GetShards() {
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
