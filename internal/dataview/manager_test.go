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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeDataViewCatalog struct {
	metastore.DataCoordCatalog

	mu                  sync.Mutex
	views               []*viewpb.DataViewOfCollection
	versionStates       map[int64]*viewpb.CollectionDataVersionState
	listCalls           int
	listAllCalls        int
	saveErrOnce         error
	saveVersionErrOnce  error
	saveVersionCalls    int
	blockCollection     int64
	saveStarted         chan struct{}
	saveBlock           chan struct{}
	blockDropCollection int64
	dropStarted         chan struct{}
	dropBlock           chan struct{}
}

func (c *fakeDataViewCatalog) SaveDataViewVersionState(ctx context.Context, state *viewpb.CollectionDataVersionState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saveVersionCalls++
	if c.saveVersionErrOnce != nil {
		err := c.saveVersionErrOnce
		c.saveVersionErrOnce = nil
		return err
	}
	if c.versionStates == nil {
		c.versionStates = make(map[int64]*viewpb.CollectionDataVersionState)
	}
	c.versionStates[state.GetCollectionId()] = proto.Clone(state).(*viewpb.CollectionDataVersionState)
	return nil
}

func (c *fakeDataViewCatalog) GetDataViewVersionState(ctx context.Context, collectionID int64) (*viewpb.CollectionDataVersionState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.versionStates[collectionID]
	if state == nil {
		return nil, nil
	}
	return proto.Clone(state).(*viewpb.CollectionDataVersionState), nil
}

func (c *fakeDataViewCatalog) SaveDataView(ctx context.Context, dataView *viewpb.DataViewOfCollection) error {
	if dataView.GetCollectionId() == c.blockCollection && c.saveBlock != nil {
		if c.saveStarted != nil {
			select {
			case <-c.saveStarted:
			default:
				close(c.saveStarted)
			}
		}
		<-c.saveBlock
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.saveErrOnce != nil {
		err := c.saveErrOnce
		c.saveErrOnce = nil
		return err
	}
	c.views = append(c.views, proto.Clone(dataView).(*viewpb.DataViewOfCollection))
	return nil
}

func (c *fakeDataViewCatalog) ListDataViews(ctx context.Context, collectionID int64) ([]*viewpb.DataViewOfCollection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listCalls++
	views := make([]*viewpb.DataViewOfCollection, 0)
	for _, view := range c.views {
		if view.GetCollectionId() == collectionID {
			views = append(views, proto.Clone(view).(*viewpb.DataViewOfCollection))
		}
	}
	return views, nil
}

func (c *fakeDataViewCatalog) ListAllDataViews(ctx context.Context) ([]*viewpb.DataViewOfCollection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listAllCalls++
	views := make([]*viewpb.DataViewOfCollection, 0, len(c.views))
	for _, view := range c.views {
		views = append(views, proto.Clone(view).(*viewpb.DataViewOfCollection))
	}
	return views, nil
}

func (c *fakeDataViewCatalog) ListAllDataViewVersionStates(ctx context.Context) ([]*viewpb.CollectionDataVersionState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	states := make([]*viewpb.CollectionDataVersionState, 0, len(c.versionStates))
	for _, state := range c.versionStates {
		states = append(states, proto.Clone(state).(*viewpb.CollectionDataVersionState))
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].GetCollectionId() < states[j].GetCollectionId()
	})
	return states, nil
}

func (c *fakeDataViewCatalog) DropDataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) error {
	if collectionID == c.blockDropCollection && c.dropBlock != nil {
		if c.dropStarted != nil {
			select {
			case <-c.dropStarted:
			default:
				close(c.dropStarted)
			}
		}
		<-c.dropBlock
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	views := c.views[:0]
	for _, view := range c.views {
		if view.GetCollectionId() == collectionID && compareDataVersion(view.GetDataVersion(), dataVersion) == 0 {
			continue
		}
		views = append(views, view)
	}
	c.views = views
	return nil
}

func (c *fakeDataViewCatalog) DropDataViews(ctx context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	views := c.views[:0]
	for _, view := range c.views {
		if view.GetCollectionId() != collectionID {
			views = append(views, view)
		}
	}
	c.views = views
	delete(c.versionStates, collectionID)
	return nil
}

type fakeDataViewSegmentStore struct {
	mu              sync.RWMutex
	segments        map[int64]*Segment
	assignmentSaves int
}

func (s *fakeDataViewSegmentStore) GetSegment(ctx context.Context, segID int64) *Segment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.segments[segID]
}

func (s *fakeDataViewSegmentStore) GetSegments(ctx context.Context, segIDs []int64) []*Segment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	segments := make([]*Segment, 0, len(segIDs))
	for _, segmentID := range segIDs {
		if segment := s.segments[segmentID]; segment != nil {
			segments = append(segments, segment)
		}
	}
	return segments
}

func (s *fakeDataViewSegmentStore) SelectSegments(ctx context.Context, collectionID int64) []*Segment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	segments := make([]*Segment, 0, len(s.segments))
	for _, segment := range s.segments {
		if segment.GetCollectionID() == collectionID {
			segments = append(segments, segment)
		}
	}
	return segments
}

func (s *fakeDataViewSegmentStore) ListAllSegmentsForVersionAllocation(ctx context.Context, collectionID int64) []*Segment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	segments := make([]*Segment, 0, len(s.segments))
	for _, segment := range s.segments {
		if segment.GetCollectionID() == collectionID {
			segments = append(segments, segment)
		}
	}
	return segments
}

func (s *fakeDataViewSegmentStore) SaveSealedAtDataVersion(ctx context.Context, segmentID int64, version *viewpb.DataVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	segment := s.segments[segmentID]
	if segment == nil {
		return errors.New("segment missing")
	}
	segment.SealedAtDataVersion = proto.Clone(version).(*viewpb.DataVersion)
	s.assignmentSaves++
	return nil
}

func newTestDataViewManager() (*dataViewManager, *fakeDataViewCatalog, *fakeDataViewSegmentStore) {
	catalog := &fakeDataViewCatalog{}
	store := &fakeDataViewSegmentStore{segments: make(map[int64]*Segment)}
	return NewManager(catalog, store).(*dataViewManager), catalog, store
}

func dataViewSnapshotForTest(
	ctx context.Context,
	t *testing.T,
	manager *dataViewManager,
	collectionIDs map[int64]struct{},
) *balancerapi.DataViewSnapshot {
	t.Helper()
	ref, err := manager.DataViewSnapshotRefForCollections(ctx, collectionIDs)
	require.NoError(t, err)
	t.Cleanup(ref.Release)
	return ref.Snapshot()
}

func noErrorVersion(_ *viewpb.DataVersion, err error) error {
	return err
}

func requireDataVersion(t *testing.T, version *viewpb.DataVersion, streamingVersion, compactVersion int64) {
	require.NotNil(t, version)
	require.Equal(t, streamingVersion, version.GetStreamingVersion())
	require.Equal(t, compactVersion, version.GetCompactVersion())
}

func newDataViewTestSegment(collectionID, partitionID, segmentID int64, channel string, dmlTs uint64) *Segment {
	return &Segment{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channel,
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		StartPosition: &msgpb.MsgPosition{
			ChannelName: channel,
			Timestamp:   dmlTs,
		},
		DmlPosition: &msgpb.MsgPosition{
			ChannelName: channel,
			Timestamp:   dmlTs,
		},
		TransformStartAfterTimetick: dmlTs,
	}
}

func testMembership(segment *Segment) SegmentMembership {
	return SegmentMembership{
		SegmentID:    segment.GetID(),
		CollectionID: segment.GetCollectionID(),
		PartitionID:  segment.GetPartitionID(),
		VChannel:     segment.GetInsertChannel(),
		State:        segment.GetState(),
		Level:        segment.GetLevel(),
		IsImporting:  segment.GetIsImporting(),
		IsInvisible:  segment.GetIsInvisible(),
	}
}

func assignAndPublishTestSegment(
	ctx context.Context,
	t *testing.T,
	manager *dataViewManager,
	segmentID int64,
) *viewpb.DataVersion {
	t.Helper()
	segment := manager.segments.GetSegment(ctx, segmentID)
	require.NotNil(t, segment)
	assigned, err := manager.AssignFlushVersion(ctx, segment.GetCollectionID(), segmentID)
	require.NoError(t, err)
	published, err := manager.CommitPublishedView(ctx, segment.GetCollectionID(), assigned, PublishedMutation{
		Add: []SegmentMembership{testMembership(segment)},
	})
	require.NoError(t, err)
	require.Equal(t, assigned, published)
	return published
}

func commitTestRewrite(
	ctx context.Context,
	t *testing.T,
	manager *dataViewManager,
	collectionID int64,
	remove []int64,
	addSegmentIDs ...int64,
) *viewpb.DataVersion {
	t.Helper()
	mutation := PublishedMutation{Remove: append([]int64(nil), remove...)}
	for _, segmentID := range addSegmentIDs {
		segment := manager.segments.GetSegment(ctx, segmentID)
		require.NotNil(t, segment)
		mutation.Add = append(mutation.Add, testMembership(segment))
	}
	version, err := manager.CommitRewrite(ctx, collectionID, mutation)
	require.NoError(t, err)
	return version
}

func newTestDataView(collectionID, streamingVersion, compactVersion int64, shards ...*viewpb.DataViewOfShard) *viewpb.DataViewOfCollection {
	return &viewpb.DataViewOfCollection{
		CollectionId: collectionID,
		DataVersion: &viewpb.DataVersion{
			StreamingVersion: streamingVersion,
			CompactVersion:   compactVersion,
		},
		Shards: shards,
	}
}

func newTestDataViewShard(vchannel string, partitionID int64, segmentIDs ...int64) *viewpb.DataViewOfShard {
	return &viewpb.DataViewOfShard{
		Vchannel: vchannel,
		Partitions: []*viewpb.DataViewOfPartition{
			{
				PartitionId: partitionID,
				SegmentIds:  append([]int64(nil), segmentIDs...),
			},
		},
	}
}

func findDataViewShard(view *viewpb.DataViewOfCollection, vchannel string) (*viewpb.DataViewOfShard, bool) {
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard, true
		}
	}
	return nil, false
}

func TestDataViewManagerOnCreateCollectionCreatesEmptyVisibleView(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()

	version, err := manager.InitializeCollection(ctx, CollectionInitialization{
		CollectionID: 1,
		VChannels:    []string{"ch-1", "ch-0"},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)

	require.Len(t, catalog.views, 1)
	persisted := catalog.views[0]
	require.Equal(t, int64(1), persisted.GetCollectionId())
	requireDataVersion(t, persisted.GetDataVersion(), 1, 0)
	require.Len(t, persisted.GetShards(), 2)
	require.Equal(t, "ch-0", persisted.GetShards()[0].GetVchannel())
	require.Equal(t, "ch-1", persisted.GetShards()[1].GetVchannel())
	require.Empty(t, persisted.GetShards()[0].GetPartitions())
	require.Empty(t, persisted.GetShards()[1].GetPartitions())

	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	requireDataVersion(t, visible.GetDataVersion(), 1, 0)
	require.Len(t, visible.GetShards(), 2)
	require.Zero(t, visible.GetShards()[0].GetTransformStartAfterTimetick())

	snapshot := dataViewSnapshotForTest(ctx, t, manager, nil)
	_, ok := snapshot.ShardView(1, "ch-0")
	require.True(t, ok)
	_, ok = snapshot.ShardView(1, "ch-1")
	require.True(t, ok)
}

func TestDataViewManagerOnCreateCollectionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	event := CollectionInitialization{
		CollectionID: 1,
		VChannels:    []string{"ch-0"},
	}

	first, err := manager.InitializeCollection(ctx, event)
	require.NoError(t, err)
	second, err := manager.InitializeCollection(ctx, event)
	require.NoError(t, err)

	requireDataVersion(t, first, 1, 0)
	requireDataVersion(t, second, 1, 0)
	require.Len(t, catalog.views, 1)
}

func TestDataViewManagerOnCreateCollectionReusesPersistedView(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = append(catalog.views, &viewpb.DataViewOfCollection{
		CollectionId: 1,
		DataVersion:  &viewpb.DataVersion{StreamingVersion: 1},
		Shards:       []*viewpb.DataViewOfShard{{Vchannel: "ch-0"}},
	})

	version, err := manager.InitializeCollection(ctx, CollectionInitialization{
		CollectionID: 1,
		VChannels:    []string{"ch-0"},
	})

	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)

	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Len(t, visible.GetShards(), 1)
	require.Equal(t, "ch-0", visible.GetShards()[0].GetVchannel())
}

func TestDataViewManagerOnCreateCollectionDoesNotPublishOrphanSnapshot(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = append(catalog.views, newTestDataView(1, 1, 0, newTestDataViewShard("ch-0", 10, 100)))
	catalog.versionStates = map[int64]*viewpb.CollectionDataVersionState{
		1: {
			CollectionId:              1,
			AllocatedStreamingVersion: 1,
		},
	}

	_, err := manager.InitializeCollection(ctx, CollectionInitialization{
		CollectionID: 1,
		VChannels:    []string{"ch-0"},
	})
	require.Error(t, err)
	state, getErr := catalog.GetDataViewVersionState(ctx, 1)
	require.NoError(t, getErr)
	require.Nil(t, state.GetPublishedDataVersion())
}

func TestDataViewManagerOnFlushCreatesVisibleView(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)

	require.Len(t, catalog.views, 1)
	require.Equal(t, int64(1), catalog.views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[0].GetDataVersion().GetCompactVersion())
	require.Zero(t, catalog.views[0].GetShards()[0].GetTransformStartAfterTimetick())

	view, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, uint64(1000), view.GetShards()[0].GetTransformStartAfterTimetick())
	require.Equal(t, []int64{100}, view.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerOnFlushRetryReturnsExactPersistedAssignedVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:    1,
		SegmentIDs:      []int64{100},
		AssignedVersion: &viewpb.DataVersion{StreamingVersion: 1},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)

	version, err = manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:    1,
		SegmentIDs:      []int64{101},
		AssignedVersion: &viewpb.DataVersion{StreamingVersion: 2},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 2, 0)

	version, err = manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:    1,
		SegmentIDs:      []int64{100},
		AssignedVersion: &viewpb.DataVersion{StreamingVersion: 1},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 2)

	latest, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	requireDataVersion(t, latest.GetDataVersion(), 2, 0)
	require.True(t, dataViewContainsSegment(latest, 100))
	require.True(t, dataViewContainsSegment(latest, 101))
}

func TestDataViewManagerOnFlushRetryRejectsMissingAssignedSnapshot(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)

	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:    1,
		SegmentIDs:      []int64{100},
		AssignedVersion: &viewpb.DataVersion{StreamingVersion: 1},
	})))
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:    1,
		SegmentIDs:      []int64{101},
		AssignedVersion: &viewpb.DataVersion{StreamingVersion: 2},
	})))
	catalog.mu.Lock()
	catalog.views = catalog.views[1:]
	catalog.mu.Unlock()

	_, err := manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:    1,
		SegmentIDs:      []int64{100},
		AssignedVersion: &viewpb.DataVersion{StreamingVersion: 1},
	})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Len(t, catalog.views, 1)
}

func TestDataViewManagerOnFlushRetryRejectsAssignedSnapshotWithoutSegment(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)

	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:    1,
		SegmentIDs:      []int64{100},
		AssignedVersion: &viewpb.DataVersion{StreamingVersion: 1},
	})))
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:    1,
		SegmentIDs:      []int64{101},
		AssignedVersion: &viewpb.DataVersion{StreamingVersion: 2},
	})))
	catalog.mu.Lock()
	catalog.views[0] = newTestDataView(1, 1, 0)
	catalog.mu.Unlock()

	_, err := manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:    1,
		SegmentIDs:      []int64{100},
		AssignedVersion: &viewpb.DataVersion{StreamingVersion: 1},
	})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Len(t, catalog.views, 2)
}

func TestDataViewManagerOnFlushSkipsNonLoadableSegments(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[100].State = commonpb.SegmentState_Sealed
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	store.segments[101].IsInvisible = true
	store.segments[102] = newDataViewTestSegment(1, 10, 102, "ch-1", 1200)
	store.segments[102].IsImporting = true
	store.segments[103] = newDataViewTestSegment(1, 10, 103, "ch-1", 1300)
	store.segments[103].Level = datapb.SegmentLevel_L0
	store.segments[104] = newDataViewTestSegment(1, 10, 104, "ch-1", 1400)
	store.segments[104].State = commonpb.SegmentState_Dropped

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101, 102, 103, 104}})
	require.NoError(t, err)
	require.Nil(t, version)

	require.Empty(t, catalog.views)
	view, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.Nil(t, view)
}

func TestDataViewManagerOnFlushExposesVisibleTimeTick(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	_, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)

	timeticks, err := manager.ShardTimeTicks(ctx, []int64{1})
	require.NoError(t, err)
	require.Len(t, timeticks, 1)
	require.Equal(t, "ch-1", timeticks[0].GetVchannel())
	require.Equal(t, uint64(1000), timeticks[0].GetTransformStartAfterTimetick())
}

func TestDataViewManagerFlushTemporaryThenSortHandoff(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	temp := newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	temp.IsInvisible = true
	store.segments[100] = temp
	assigned, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)

	version, err := manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:         1,
		SegmentIDs:           []int64{100},
		TemporaryUnavailable: true,
		AssignedVersion:      assigned,
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Empty(t, catalog.views)
	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.Nil(t, visible)

	temp.State = commonpb.SegmentState_Dropped
	final := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	final.CompactionFrom = []int64{100}
	store.segments[101] = final

	version, err = manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{100},
		CompactTo:    []int64{101},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)
	require.Equal(t, int64(1), catalog.views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[0].GetDataVersion().GetCompactVersion())

	visible, err = latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{101}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
	require.Equal(t, uint64(1100), visible.GetShards()[0].GetTransformStartAfterTimetick())
}

func TestDataViewManagerImportAndCopySegmentCompleteAdvanceStreamingVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)

	require.NoError(t, noErrorVersion(manager.OnImport(ctx, ImportDataViewEvent{
		CollectionID: 1,
		SegmentIDs:   []int64{100},
	})))
	require.NoError(t, noErrorVersion(manager.OnCopySegmentComplete(ctx, CopySegmentCompleteDataViewEvent{
		CollectionID: 1,
		SegmentIDs:   []int64{101},
	})))

	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[0].GetDataVersion().GetCompactVersion())
	require.Equal(t, int64(2), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[1].GetDataVersion().GetCompactVersion())
	view, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, []int64{100, 101}, view.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerTemporaryFlushKeepsPreviousVisibleView(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	temp := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	temp.IsInvisible = true
	store.segments[101] = temp
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:         1,
		SegmentIDs:           []int64{101},
		TemporaryUnavailable: true,
	})))

	require.Len(t, catalog.views, 1)
	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, int64(1), visible.GetDataVersion().GetStreamingVersion())
	require.Equal(t, []int64{100}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerShardTimeTicksUseLatestVisibleView(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	temp := newDataViewTestSegment(1, 10, 101, "ch-1", 800)
	temp.IsInvisible = true
	store.segments[101] = temp
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:         1,
		SegmentIDs:           []int64{101},
		TemporaryUnavailable: true,
	})))

	timeticks, err := manager.ShardTimeTicks(ctx, []int64{1})
	require.NoError(t, err)
	require.Len(t, timeticks, 1)
	require.Equal(t, "ch-1", timeticks[0].GetVchannel())
	require.Equal(t, uint64(1000), timeticks[0].GetTransformStartAfterTimetick())
}

func TestDataViewManagerSnapshotReturnsLatestVisibleClone(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	temp := newDataViewTestSegment(1, 10, 101, "ch-1", 800)
	temp.IsInvisible = true
	store.segments[101] = temp
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:         1,
		SegmentIDs:           []int64{101},
		TemporaryUnavailable: true,
	})))

	views := manager.snapshot(ctx, []int64{1})
	require.Len(t, views, 1)
	require.Equal(t, int64(1), views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, []int64{100}, views[0].GetShards()[0].GetPartitions()[0].GetSegmentIds())
	require.Equal(t, uint64(1000), views[0].GetShards()[0].GetTransformStartAfterTimetick())

	views[0].Shards[0].Partitions[0].SegmentIds[0] = 999
	views = manager.snapshot(ctx, []int64{1})
	require.Len(t, views, 1)
	require.Equal(t, []int64{100}, views[0].GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerDataViewSnapshotForBalancer(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[100].NumOfRows = 11
	store.segments[100].MemSize = 4096
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	snapshot := dataViewSnapshotForTest(ctx, t, manager, nil)
	require.NotNil(t, snapshot)

	version, ok := snapshot.DataVersion(1)
	require.True(t, ok)
	require.Equal(t, int64(1), version.StreamingVersion)

	shard, ok := snapshot.ShardView(1, "ch-1")
	require.True(t, ok)
	require.Equal(t, []int64{100}, shard.GetPartitions()[0].GetSegmentIds())

	segment, ok := snapshot.SegmentInfo(100)
	require.True(t, ok)
	require.Equal(t, int64(10), segment.PartitionID)
	require.Equal(t, int64(4096), segment.MemSize)
	require.Equal(t, int64(11), segment.RowNum)
}

func TestDataViewManagerDataViewSnapshotForCollectionsScope(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	store.segments[101].IsInvisible = true
	store.segments[200] = newDataViewTestSegment(2, 20, 200, "ch-2", 1200)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{
		CollectionID:         1,
		SegmentIDs:           []int64{101},
		TemporaryUnavailable: true,
	})))
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 2, SegmentIDs: []int64{200}})))

	tests := []struct {
		name  string
		scope map[int64]struct{}
		has1  bool
		has2  bool
	}{
		{name: "all", scope: nil, has1: true, has2: true},
		{name: "empty", scope: map[int64]struct{}{}},
		{name: "selected", scope: map[int64]struct{}{1: {}}, has1: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := dataViewSnapshotForTest(ctx, t, manager, test.scope)
			_, has1 := snapshot.DataVersion(1)
			_, has2 := snapshot.DataVersion(2)
			require.Equal(t, test.has1, has1)
			require.Equal(t, test.has2, has2)
		})
	}

	snapshot := dataViewSnapshotForTest(ctx, t, manager, map[int64]struct{}{1: {}})
	shard, ok := snapshot.ShardView(1, "ch-1")
	require.True(t, ok)
	require.Equal(t, []int64{100}, shard.GetPartitions()[0].GetSegmentIds())
	shard.Partitions[0].SegmentIds[0] = 999

	snapshot = dataViewSnapshotForTest(ctx, t, manager, map[int64]struct{}{1: {}})
	shard, ok = snapshot.ShardView(1, "ch-1")
	require.True(t, ok)
	require.Equal(t, []int64{100}, shard.GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerDataViewSnapshotForCollectionsTimeticks(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-shared", 900)
	store.segments[101] = newDataViewTestSegment(1, 11, 101, "ch-shared", 700)
	store.segments[200] = newDataViewTestSegment(1, 12, 200, "ch-missing", 600)

	sharedShard := newTestDataViewShard("ch-shared", 10, 100)
	sharedShard.Partitions = append(sharedShard.Partitions,
		&viewpb.DataViewOfPartition{PartitionId: 11, SegmentIds: []int64{101}})
	manager.states[1] = &collectionDataViewState{
		collectionID: 1,
		refs:         make(map[qviews.DataVersion]int),
		published: newTestDataView(
			1, 1, 0,
			sharedShard,
			newTestDataViewShard("ch-missing", 12, 200, 999),
			&viewpb.DataViewOfShard{Vchannel: "ch-empty"},
		),
	}

	snapshot := dataViewSnapshotForTest(ctx, t, manager, map[int64]struct{}{1: {}})
	shared, ok := snapshot.ShardView(1, "ch-shared")
	require.True(t, ok)
	require.Equal(t, uint64(700), shared.GetTransformStartAfterTimetick())
	missing, ok := snapshot.ShardView(1, "ch-missing")
	require.True(t, ok)
	require.Zero(t, missing.GetTransformStartAfterTimetick())
	empty, ok := snapshot.ShardView(1, "ch-empty")
	require.True(t, ok)
	require.Zero(t, empty.GetTransformStartAfterTimetick())
}

func TestDataViewManagerPublishedStateIsLatestAuthority(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 900)
	published := newTestDataView(1, 2, 1, newTestDataViewShard("ch-1", 10, 100))
	manager.states[1] = &collectionDataViewState{
		collectionID: 1,
		published:    published,
		refs:         make(map[qviews.DataVersion]int),
	}

	latest, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, qviews.DataVersion{StreamingVersion: 2, CompactVersion: 1}, latest.DataView().Version())
	latest.Deref()

	views := manager.snapshot(ctx, []int64{1})
	require.Len(t, views, 1)
	requireDataVersion(t, views[0].GetDataVersion(), 2, 1)
	require.Equal(t, []int64{100}, views[0].GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerSegmentSnapshot(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 900)
	store.segments[100].NumOfRows = 11

	snapshot := manager.SegmentSnapshot(ctx, []int64{100, 999})
	segment, ok := snapshot.Get(100)
	require.True(t, ok)
	require.Equal(t, int64(10), segment.PartitionID)
	require.Equal(t, int64(11), segment.RowNum)
	_, ok = snapshot.Get(999)
	require.False(t, ok)
}

func TestDataViewManagerCompactPendingOutputIsNoopUntilVisible(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	output := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	output.IsInvisible = true
	output.CompactionFrom = []int64{100}
	store.segments[101] = output
	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{100},
		CompactTo:    []int64{101},
	})))

	require.Len(t, catalog.views, 1)
	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{100}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())

	store.segments[100].State = commonpb.SegmentState_Dropped
	output.IsInvisible = false
	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{100},
		CompactTo:    []int64{101},
	})))

	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetCompactVersion())
	visible, err = latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{101}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerL0CompactRefreshesDeleteTimetickWithoutVersionBump(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 900)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101}})))

	store.segments[101].TransformStartAfterTimetick = 800
	version, err := manager.OnL0Compact(ctx, L0CompactDataViewEvent{CollectionID: 1})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)

	view, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, uint64(800), view.GetShards()[0].GetTransformStartAfterTimetick())
	require.Equal(t, int64(1), view.GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), view.GetDataVersion().GetCompactVersion())
}

func TestDataViewManagerDeleteTimetickUsesSegmentFieldBeforeDmlPosition(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[100].DmlPosition.Timestamp = 5000
	store.segments[100].TransformStartAfterTimetick = 900

	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	view, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(900), view.GetShards()[0].GetTransformStartAfterTimetick())
}

func TestDataViewManagerDeleteTimetickFallbackForLegacySegments(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[100].TransformStartAfterTimetick = 0
	store.segments[100].CommitTimestamp = 900
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	store.segments[101].TransformStartAfterTimetick = 0
	store.segments[101].StartPosition.Timestamp = 800
	store.segments[101].DmlPosition.Timestamp = 7000

	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101}})))

	view, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(800), view.GetShards()[0].GetTransformStartAfterTimetick())
}

func TestDataViewManagerDropPartitionAdvancesCompactVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 11, 101, "ch-1", 900)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101}})))

	require.NoError(t, noErrorVersion(manager.OnDropPartition(ctx, DropPartitionDataViewEvent{CollectionID: 1, PartitionIDs: []int64{10}})))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetCompactVersion())

	view, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, int64(11), view.GetShards()[0].GetPartitions()[0].GetPartitionId())
	require.Equal(t, []int64{101}, view.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerTruncateAdvancesCompactVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	store.segments[200] = newDataViewTestSegment(1, 10, 200, "ch-2", 900)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101, 200}})))

	require.NoError(t, noErrorVersion(manager.OnTruncate(ctx, TruncateDataViewEvent{
		CollectionID: 1,
		VChannel:     "ch-1",
		FlushTs:      1000,
	})))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[1].GetDataVersion().GetCompactVersion())

	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	shard1, ok := findDataViewShard(visible, "ch-1")
	require.True(t, ok)
	require.Equal(t, []int64{101}, shard1.GetPartitions()[0].GetSegmentIds())
	shard2, ok := findDataViewShard(visible, "ch-2")
	require.True(t, ok)
	require.Equal(t, []int64{200}, shard2.GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerTruncateUsesCommitTimestamp(t *testing.T) {
	ctx := context.Background()
	manager, _, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[100].CommitTimestamp = 1200
	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100, 101}})))

	require.NoError(t, noErrorVersion(manager.OnTruncate(ctx, TruncateDataViewEvent{
		CollectionID: 1,
		VChannel:     "ch-1",
		FlushTs:      1100,
	})))

	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.NotNil(t, visible)
	require.Equal(t, []int64{100}, visible.GetShards()[0].GetPartitions()[0].GetSegmentIds())
}

func TestDataViewManagerDuplicateEventIsNoop(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	version, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)
}

func TestDataViewManagerCompactSameSegmentIDIsNoop(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)

	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))
	require.NoError(t, noErrorVersion(manager.OnCompact(ctx, CompactDataViewEvent{
		CollectionID: 1,
		CompactFrom:  []int64{100},
		CompactTo:    []int64{100},
	})))
	require.Len(t, catalog.views, 1)
}

func TestDataViewManagerExternalRefreshClassifiesActualMembershipChange(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	store.segments[101] = newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	require.NoError(t, noErrorVersion(manager.OnExternalRefresh(ctx, ExternalRefreshDataViewEvent{
		CollectionID: 1,
		AddSegments:  []int64{101},
		DropSegments: []int64{999},
	})))
	require.Len(t, catalog.views, 2)
	require.Equal(t, int64(2), catalog.views[1].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), catalog.views[1].GetDataVersion().GetCompactVersion())

	store.segments[102] = newDataViewTestSegment(1, 10, 102, "ch-1", 1200)
	require.NoError(t, noErrorVersion(manager.OnExternalRefresh(ctx, ExternalRefreshDataViewEvent{
		CollectionID: 1,
		AddSegments:  []int64{102},
		DropSegments: []int64{100},
	})))
	require.Len(t, catalog.views, 3)
	require.Equal(t, int64(2), catalog.views[2].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(1), catalog.views[2].GetDataVersion().GetCompactVersion())
}

func TestRecoverManagerLoadsAllDataViewsWithoutSegmentMetaRepair(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
			newTestDataView(2, 2, 1, newTestDataViewShard("ch-2", 20, 200)),
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
		200: newDataViewTestSegment(1, 10, 200, "ch-1", 2000),
	}}

	manager, err := RecoverManager(ctx, catalog, store)

	require.NoError(t, err)
	require.Equal(t, 1, catalog.listAllCalls)
	require.Zero(t, catalog.listCalls)
	concrete := manager.(*dataViewManager)
	snapshot := concrete.snapshot(ctx, nil)
	require.Len(t, snapshot, 2)
	require.Equal(t, int64(1), snapshot[0].GetCollectionId())
	require.Equal(t, int64(2), snapshot[1].GetCollectionId())
	require.Len(t, catalog.views, 2)
}

func TestRecoverManagerUsesDurablePublishedHeadInsteadOfNewerOrphan(t *testing.T) {
	ctx := context.Background()
	head := newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100))
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			head,
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 200)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 2,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
		200: newDataViewTestSegment(1, 10, 200, "ch-1", 2000),
	}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	ref, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	require.Equal(t, int64(1), ref.DataView().Version().StreamingVersion)
	require.Equal(t, []int64{100}, ref.DataView().SegmentIDs("ch-1", 10))

	version, err := manager.InitializeCollection(ctx, CollectionInitialization{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
}

func TestRecoverManagerRejectsPublishedHeadWhenCollectionHasNoSnapshots(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 1,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}

	_, err := RecoverManager(ctx, catalog, &fakeDataViewSegmentStore{segments: map[int64]*Segment{}})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestRecoverManagerAllowsAllocatedStateWithoutPublishedHeadOrSnapshots(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 1,
			},
		},
	}

	manager, err := RecoverManager(ctx, catalog, &fakeDataViewSegmentStore{segments: map[int64]*Segment{}})
	require.NoError(t, err)
	_, err = manager.LatestPublished(ctx, 1)
	requireUnavailableDataViewError(t, err)

	state := manager.(*dataViewManager).getState(1)
	require.NotNil(t, state)
	state.mu.RLock()
	defer state.mu.RUnlock()
	require.Equal(t, int64(1), state.versionState.GetAllocatedStreamingVersion())
	require.Nil(t, state.versionState.GetPublishedDataVersion())
}

func TestRecoverManagerDoesNotPublishOrphanWhenDurableHeadIsAbsent(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 1,
			},
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
	}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	_, err = manager.LatestPublished(ctx, 1)
	requireUnavailableDataViewError(t, err)
	state, err := catalog.GetDataViewVersionState(ctx, 1)
	require.NoError(t, err)
	require.Nil(t, state.GetPublishedDataVersion())
}

func TestRecoverManagerRepairDoesNotAdoptNewerOrphanThanDurableHead(t *testing.T) {
	ctx := context.Background()
	head := newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100))
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			head,
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 200)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 2,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
	}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	ref, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	require.Equal(t, int64(1), ref.DataView().Version().StreamingVersion)
	require.Zero(t, ref.DataView().Version().CompactVersion)
	require.Equal(t, []int64{100}, ref.DataView().SegmentIDs("ch-1", 10))
	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	requireDataVersion(t, visible.GetDataVersion(), 1, 0)
	require.Equal(t, []int64{100}, publishedSegmentIDs(t, visible, "ch-1", 10))
	requireDataVersion(t, catalog.versionStates[1].GetPublishedDataVersion(), 1, 0)
	require.Len(t, catalog.views, 2)
}

func TestRecoverManagerDoesNotInferMembershipFromSegmentMeta(t *testing.T) {
	ctx := context.Background()
	head := newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100))
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{head},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 1,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
		// SegmentMeta may be ahead of the published DataView. Generic recovery
		// repair must not infer a new membership or allocate a version.
		101: newDataViewTestSegment(1, 10, 101, "ch-1", 1100),
	}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	require.Len(t, catalog.views, 1)
	ref, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	require.Equal(t, []int64{100}, ref.DataView().SegmentIDs("ch-1", 10))
}

func TestRecoverManagerLegacyMigrationSkipsUnavailableLatestSnapshot(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 100, 101)),
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
		101: func() *Segment {
			segment := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
			segment.IsInvisible = true
			return segment
		}(),
	}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	ref, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	requireDataVersion(t, ref.DataView().Version().IntoProto(), 1, 0)
	require.Equal(t, []int64{100}, ref.DataView().SegmentIDs("ch-1", 10))
	state, err := catalog.GetDataViewVersionState(ctx, 1)
	require.NoError(t, err)
	requireDataVersion(t, state.GetPublishedDataVersion(), 1, 0)
}

func TestRecoverManagerLegacyMigrationRejectsWithoutLoadableCandidate(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 101)),
		},
	}
	segment := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	segment.IsInvisible = true
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{101: segment}}

	_, err := RecoverManager(ctx, catalog, store)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Nil(t, catalog.versionStates)
}

func TestRecoverManagerLegacyMigrationAllowsDroppedRetainedMember(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 100, 101)),
		},
	}
	old := newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	old.State = commonpb.SegmentState_Dropped
	newSegment := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{100: old, 101: newSegment}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	ref, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	requireDataVersion(t, ref.DataView().Version().IntoProto(), 2, 0)
	require.Equal(t, []int64{100, 101}, ref.DataView().SegmentIDs("ch-1", 10))
}

func TestRecoverManagerLegacyMigrationDoesNotCarryInvalidIntermediateMembership(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 100, 101)),
			newTestDataView(1, 3, 0, newTestDataViewShard("ch-1", 10, 100, 101, 102)),
		},
	}
	newSegment := newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	invalid := newDataViewTestSegment(1, 10, 101, "ch-1", 1100)
	invalid.IsInvisible = true
	latest := newDataViewTestSegment(1, 10, 102, "ch-1", 1200)
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{100: newSegment, 101: invalid, 102: latest}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	ref, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	requireDataVersion(t, ref.DataView().Version().IntoProto(), 1, 0)
	require.Equal(t, []int64{100}, ref.DataView().SegmentIDs("ch-1", 10))
}

func TestRecoverManagerRetainsMembershipFromEveryDurableSnapshot(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 200)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 2,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 2},
			},
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
		200: newDataViewTestSegment(1, 10, 200, "ch-1", 2000),
	}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	referenced, err := manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, referenced)
}

func TestDataViewManagerGarbageCollectKeepsDurablePublishedHeadAndDropsNewerOrphan(t *testing.T) {
	ctx := context.Background()
	head := newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100))
	orphan := newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 200))
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{head, orphan},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 2,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}
	store := &fakeDataViewSegmentStore{segments: make(map[int64]*Segment)}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))

	views, err := catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Len(t, views, 1)
	requireDataVersion(t, views[0].GetDataVersion(), 1, 0)
	referenced, err := manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, referenced)
	referenced, err = manager.IsSegmentReferenced(ctx, 1, 200)
	require.NoError(t, err)
	require.False(t, referenced)

	restarted, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	ref, err := restarted.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	requireDataVersion(t, ref.DataView().Version().IntoProto(), 1, 0)
	require.Equal(t, []int64{100}, ref.DataView().SegmentIDs("ch-1", 10))
}

func TestDataViewManagerGarbageCollectRejectsMissingDurablePublishedHead(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 200)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:         1,
				PublishedDataVersion: &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}
	manager := NewManager(catalog, &fakeDataViewSegmentStore{segments: make(map[int64]*Segment)})

	err := manager.GarbageCollect(ctx, 1, 1)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	views, listErr := catalog.ListDataViews(ctx, 1)
	require.NoError(t, listErr)
	require.Len(t, views, 1, "GC must not delete snapshots when the durable head is missing")
}

func TestRecoverManagerLegacyMigrationRejectsNonFlushedFirstSnapshot(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
		},
	}
	segment := newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	segment.State = commonpb.SegmentState_Sealed
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{100: segment}}

	_, err := RecoverManager(ctx, catalog, store)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Nil(t, catalog.versionStates)
}

func TestRecoverManagerLegacyMigrationRejectsDroppedFirstSnapshot(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
		},
	}
	segment := newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	segment.State = commonpb.SegmentState_Dropped
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{100: segment}}

	_, err := RecoverManager(ctx, catalog, store)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Nil(t, catalog.versionStates)
}

func TestRecoverManagerDoesNotBackfillHeadWhenDurableStateAlreadyExists(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 100, 200)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 2,
			},
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
		200: newDataViewTestSegment(1, 10, 200, "ch-1", 2000),
	}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	_, err = manager.InitializeCollection(ctx, CollectionInitialization{CollectionID: 1, VChannels: []string{"ch-1"}})
	require.Error(t, err)
	require.Nil(t, catalog.versionStates[1].GetPublishedDataVersion())
	_, err = manager.LatestPublished(ctx, 1)
	requireUnavailableDataViewError(t, err)
}

func TestRecoverManagerBackfillsStateForSnapshotOnlyLegacyCollection(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
			newTestDataView(1, 2, 0, newTestDataViewShard("ch-1", 10, 100, 200)),
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
		200: newDataViewTestSegment(1, 10, 200, "ch-1", 2000),
	}}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	requireDataVersion(t, catalog.versionStates[1].GetPublishedDataVersion(), 2, 0)
	require.Equal(t, int64(2), catalog.versionStates[1].GetAllocatedStreamingVersion())

	version, err := manager.CommitStreamingView(ctx, 1, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 300, "ch-1")},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 3, 0)
	requireDataVersion(t, catalog.versionStates[1].GetPublishedDataVersion(), 3, 0)
	ref, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	require.Equal(t, []int64{100, 200, 300}, ref.DataView().SegmentIDs("ch-1", 10))
}

func TestRecoverManagerRepairPreservesAssignedStreamingEpochs(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 3,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
		101: newDataViewTestSegment(1, 10, 101, "ch-1", 1100),
		102: newDataViewTestSegment(1, 10, 102, "ch-1", 1200),
	}}
	store.segments[100].SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 1}
	store.segments[101].SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 2}
	store.segments[102].SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 3}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	state, err := catalog.GetDataViewVersionState(ctx, 1)
	require.NoError(t, err)
	requireDataVersion(t, state.GetPublishedDataVersion(), 1, 0)
	require.Equal(t, int64(3), state.GetAllocatedStreamingVersion())
	require.Len(t, catalog.views, 1)
	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	requireDataVersion(t, visible.GetDataVersion(), 1, 0)
	require.Equal(t, []int64{100}, publishedSegmentIDs(t, visible, "ch-1", 10))

	published, err := manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 2}, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 101, "ch-1")},
	})
	require.NoError(t, err)
	requireDataVersion(t, published, 2, 0)
	published, err = manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 3}, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 102, "ch-1")},
	})
	require.NoError(t, err)
	requireDataVersion(t, published, 3, 0)

	later, err := manager.CommitStreamingView(ctx, 1, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 103, "ch-1")},
	})
	require.NoError(t, err)
	requireDataVersion(t, later, 4, 0)
	ref, err := manager.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	require.Equal(t, []int64{100, 101, 102, 103}, ref.DataView().SegmentIDs("ch-1", 10))
}

func TestRecoverManagerRepairDefersStreamingAdvanceBehindAssignedEpoch(t *testing.T) {
	ctx := context.Background()
	catalog := &fakeDataViewCatalog{
		views: []*viewpb.DataViewOfCollection{
			newTestDataView(1, 1, 0, newTestDataViewShard("ch-1", 10, 100)),
		},
		versionStates: map[int64]*viewpb.CollectionDataVersionState{
			1: {
				CollectionId:              1,
				AllocatedStreamingVersion: 1,
				PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
			},
		},
	}
	store := &fakeDataViewSegmentStore{segments: map[int64]*Segment{
		100: newDataViewTestSegment(1, 10, 100, "ch-1", 1000),
		101: newDataViewTestSegment(1, 10, 101, "ch-1", 1100),
		102: newDataViewTestSegment(1, 10, 102, "ch-1", 1200),
	}}
	store.segments[100].SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 1}

	manager, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	assigned, err := manager.AssignFlushVersion(ctx, 1, 101)
	require.NoError(t, err)
	requireDataVersion(t, assigned, 2, 0)
	_, err = manager.CommitStreamingView(ctx, 1, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 102, "ch-1")},
	})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.True(t, merr.IsRetryableErr(err))

	restarted, err := RecoverManager(ctx, catalog, store)
	require.NoError(t, err)
	state, err := catalog.GetDataViewVersionState(ctx, 1)
	require.NoError(t, err)
	requireDataVersion(t, state.GetPublishedDataVersion(), 1, 0)
	require.Equal(t, int64(2), state.GetAllocatedStreamingVersion())
	visible, err := latestPublishedDataView(ctx, restarted, 1)
	require.NoError(t, err)
	requireDataVersion(t, visible.GetDataVersion(), 1, 0)
	require.Equal(t, []int64{100}, publishedSegmentIDs(t, visible, "ch-1", 10))

	published, err := restarted.CommitPublishedView(ctx, 1, assigned, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 101, "ch-1")},
	})
	require.NoError(t, err)
	requireDataVersion(t, published, 2, 0)
	published, err = restarted.CommitStreamingView(ctx, 1, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 102, "ch-1")},
	})
	require.NoError(t, err)
	requireDataVersion(t, published, 3, 0)
	ref, err := restarted.LatestPublished(ctx, 1)
	require.NoError(t, err)
	t.Cleanup(ref.Deref)
	require.Equal(t, []int64{100, 101, 102}, ref.DataView().SegmentIDs("ch-1", 10))
}

func TestDataViewManagerDropCollectionDropsStateAndCatalog(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	require.NoError(t, noErrorVersion(manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})))

	require.NoError(t, manager.MarkCollectionTerminal(ctx, 1))
	require.NotEmpty(t, catalog.views)
	require.NoError(t, manager.FinalizeDropCollection(ctx, 1))
	require.Empty(t, catalog.views)

	visible, err := latestPublishedDataView(ctx, manager, 1)
	require.NoError(t, err)
	require.Nil(t, visible)
}

func TestDataViewManagerSegmentReferenceUsesRetainedViews(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = append(
		catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{100}},
					},
				},
			},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			Shards: []*viewpb.DataViewOfShard{
				{
					Vchannel: "ch-1",
					Partitions: []*viewpb.DataViewOfPartition{
						{PartitionId: 10, SegmentIds: []int64{101}},
					},
				},
			},
		},
	)

	referenced, err := manager.IsSegmentReferenced(ctx, 1, 100)
	require.NoError(t, err)
	require.True(t, referenced)

	referenced, err = manager.IsSegmentReferenced(ctx, 1, 102)
	require.NoError(t, err)
	require.False(t, referenced)
}

func TestDataViewManagerGarbageCollectRetainsLatestViews(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = append(
		catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 2,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		},
	)

	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))

	views, err := catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, int64(2), views[0].GetDataVersion().GetStreamingVersion())
	require.Equal(t, int64(0), views[0].GetDataVersion().GetCompactVersion())

	views, err = catalog.ListDataViews(ctx, 2)
	require.NoError(t, err)
	require.Len(t, views, 1)
}

func TestDataViewManagerGarbageCollectHonorsManagerOwnedRefs(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = append(
		catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 2},
		},
	)
	manager.recoverCollectionFromDataViews(1, catalog.views)

	ref, err := manager.Get(ctx, 1, qviews.DataVersion{StreamingVersion: 1})
	require.NoError(t, err)
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	views, err := catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Len(t, views, 2, "live manager-owned ref must protect the old DataView")

	ref.Deref()
	require.NoError(t, manager.GarbageCollect(ctx, 1, 1))
	views, err = catalog.ListDataViews(ctx, 1)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.EqualValues(t, 2, views[0].GetDataVersion().GetStreamingVersion())
}

func TestDataViewManagerDoesNotBlockOtherCollections(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	catalog.blockCollection = 1
	catalog.saveStarted = make(chan struct{})
	catalog.saveBlock = make(chan struct{})
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-1", 1000)
	store.segments[200] = newDataViewTestSegment(2, 20, 200, "ch-2", 2000)

	blockedErr := make(chan error, 1)
	go func() {
		_, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 1, SegmentIDs: []int64{100}})
		blockedErr <- err
	}()
	<-catalog.saveStarted

	otherErr := make(chan error, 1)
	go func() {
		_, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 2, SegmentIDs: []int64{200}})
		otherErr <- err
	}()
	select {
	case err := <-otherErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("DataView update for another collection was blocked")
	}

	close(catalog.saveBlock)
	require.NoError(t, <-blockedErr)
}

func TestDataViewManagerGarbageCollectDoesNotBlockOtherCollections(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	catalog.views = append(
		catalog.views,
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
		},
		&viewpb.DataViewOfCollection{
			CollectionId: 1,
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
		},
	)
	catalog.blockDropCollection = 1
	catalog.dropStarted = make(chan struct{})
	catalog.dropBlock = make(chan struct{})
	store.segments[200] = newDataViewTestSegment(2, 20, 200, "ch-2", 2000)

	blockedErr := make(chan error, 1)
	go func() {
		blockedErr <- manager.GarbageCollect(ctx, 1, 1)
	}()
	<-catalog.dropStarted

	otherErr := make(chan error, 1)
	go func() {
		_, err := manager.OnFlush(ctx, FlushDataViewEvent{CollectionID: 2, SegmentIDs: []int64{200}})
		otherErr <- err
	}()
	select {
	case err := <-otherErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("DataView update for another collection was blocked by collection GC")
	}

	close(catalog.dropBlock)
	require.NoError(t, <-blockedErr)
}
