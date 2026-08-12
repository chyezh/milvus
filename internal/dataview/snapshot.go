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
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Snapshot is an immutable multi-collection DataView projection.
type Snapshot struct {
	version           uint64
	collectionByID    map[int64]*viewpb.DataViewOfCollection
	shardsByCollVCh   map[int64]map[string]*viewpb.DataViewOfShard
	dataVersionByColl map[int64]qviews.DataVersion
	segments          map[int64]SegmentInfo
}

// SnapshotRef owns the manager references represented by one Snapshot.
type SnapshotRef interface {
	Snapshot() *Snapshot
	Release()
}

type snapshotRef struct {
	snapshot *Snapshot
	once     sync.Once
	release  func()
}

func (r *snapshotRef) Snapshot() *Snapshot {
	if r == nil {
		return nil
	}
	return r.snapshot
}

func (r *snapshotRef) Release() {
	if r == nil {
		return
	}
	r.once.Do(r.release)
}

func NewSnapshot(
	version uint64,
	collections []*viewpb.DataViewOfCollection,
	segments SegmentSnapshot,
) *Snapshot {
	snapshot := &Snapshot{
		version:           version,
		collectionByID:    make(map[int64]*viewpb.DataViewOfCollection, len(collections)),
		shardsByCollVCh:   make(map[int64]map[string]*viewpb.DataViewOfShard, len(collections)),
		dataVersionByColl: make(map[int64]qviews.DataVersion, len(collections)),
		segments:          make(map[int64]SegmentInfo),
	}
	for _, collection := range collections {
		if collection == nil {
			continue
		}
		owned := cloneSnapshotCollection(collection)
		collectionID := owned.GetCollectionId()
		snapshot.collectionByID[collectionID] = owned
		snapshot.dataVersionByColl[collectionID] = qviews.FromProtoDataVersion(owned.GetDataVersion())
		shards := make(map[string]*viewpb.DataViewOfShard, len(owned.GetShards()))
		for _, shard := range owned.GetShards() {
			if shard != nil {
				shards[shard.GetVchannel()] = shard
			}
		}
		snapshot.shardsByCollVCh[collectionID] = shards
	}
	if segments != nil {
		segments.Range(func(segmentID int64, info *SegmentInfo) bool {
			if info != nil {
				snapshot.segments[segmentID] = *info
			}
			return true
		})
	}
	return snapshot
}

func cloneSnapshotCollection(collection *viewpb.DataViewOfCollection) *viewpb.DataViewOfCollection {
	owned := proto.Clone(collection).(*viewpb.DataViewOfCollection)
	owned.Shards = nil
	for _, shard := range collection.GetShards() {
		if shard != nil {
			owned.Shards = append(owned.Shards, cloneDataViewShard(shard))
		}
	}
	return owned
}

func (s *Snapshot) Version() uint64 {
	if s == nil {
		return 0
	}
	return s.version
}

func (s *Snapshot) DataVersion(collectionID int64) (qviews.DataVersion, bool) {
	if s == nil {
		return qviews.DataVersion{}, false
	}
	version, ok := s.dataVersionByColl[collectionID]
	return version, ok
}

func (s *Snapshot) ShardView(collectionID int64, vchannel string) (*viewpb.DataViewOfShard, bool) {
	if s == nil {
		return nil, false
	}
	shards := s.shardsByCollVCh[collectionID]
	if shards == nil {
		return nil, false
	}
	shard, ok := shards[vchannel]
	if !ok {
		return nil, false
	}
	return cloneDataViewShard(shard), true
}

func (s *Snapshot) RangeShards(collectionID int64, yield func(*viewpb.DataViewOfShard) bool) {
	if s == nil || yield == nil {
		return
	}
	collection := s.collectionByID[collectionID]
	if collection == nil {
		return
	}
	for _, shard := range collection.GetShards() {
		if !yield(cloneDataViewShard(shard)) {
			return
		}
	}
}

func (s *Snapshot) SegmentInfo(segmentID int64) (*SegmentInfo, bool) {
	if s == nil {
		return nil, false
	}
	info, ok := s.segments[segmentID]
	if !ok {
		return nil, false
	}
	return &info, true
}

func cloneDataViewShard(shard *viewpb.DataViewOfShard) *viewpb.DataViewOfShard {
	if shard == nil {
		return nil
	}
	return proto.Clone(shard).(*viewpb.DataViewOfShard)
}

// SegmentSnapshot is an immutable segment metadata lookup associated with a
// DataView snapshot.
type SegmentSnapshot interface {
	Get(segmentID int64) (*SegmentInfo, bool)
	Range(yield func(segmentID int64, info *SegmentInfo) bool)
}

// SegmentInfo carries the minimum per-segment metadata needed by DataView
// consumers when planning placement.
type SegmentInfo struct {
	SegmentID   int64
	PartitionID int64
	// MemSize is retained for compatibility and diagnostics. The row-count
	// balance policy does not consume it.
	MemSize int64
	// RowNum is the segment row count and the sole balance load metric.
	RowNum int64
}

func (m *dataViewManager) DataViewSnapshotRefForCollections(
	ctx context.Context,
	collectionIDs map[int64]struct{},
) (SnapshotRef, error) {
	states := make([]*collectionDataViewState, 0)
	if collectionIDs == nil {
		states = m.listStates()
	} else {
		for collectionID := range collectionIDs {
			states = append(states, m.getOrCreateState(collectionID))
		}
	}

	views := make([]*viewpb.DataViewOfCollection, 0, len(states))
	segmentIDs := make([]int64, 0)
	seenSegments := make(map[int64]struct{})
	references := make([]DataViewRef, 0, len(states))
	for _, state := range states {
		state.mu.Lock()
		if err := m.recoverPublicationStateLocked(ctx, state, m.catalog); err != nil {
			state.mu.Unlock()
			releaseDataViewRefs(references)
			return nil, err
		}
		if state.dropped || state.published == nil {
			state.mu.Unlock()
			continue
		}
		view := canonicalDataViewClone(state.published)
		reference := newDataViewRef(state, newDataView(view))
		state.mu.Unlock()
		references = append(references, reference)
		views = append(views, view)
		for _, partition := range dataViewPartitions(view) {
			for _, segmentID := range partition.GetSegmentIds() {
				if _, ok := seenSegments[segmentID]; ok {
					continue
				}
				seenSegments[segmentID] = struct{}{}
				segmentIDs = append(segmentIDs, segmentID)
			}
		}
	}
	segments := m.getSegments(ctx, segmentIDs)
	setDataViewDeleteTimeticks(views, segments)
	return &snapshotRef{
		snapshot: NewSnapshot(0, views, newSegmentSnapshot(segmentIDs, segments)),
		release: func() {
			releaseDataViewRefs(references)
		},
	}, nil
}

func releaseDataViewRefs(references []DataViewRef) {
	for _, reference := range references {
		reference.Deref()
	}
}
