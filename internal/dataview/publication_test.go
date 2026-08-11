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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (c *fakeDataViewCatalog) SavePublishedDataView(
	ctx context.Context,
	state *viewpb.CollectionDataVersionState,
	view *viewpb.DataViewOfCollection,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.saveErrOnce != nil {
		err := c.saveErrOnce
		c.saveErrOnce = nil
		return err
	}
	if c.versionStates == nil {
		c.versionStates = make(map[int64]*viewpb.CollectionDataVersionState)
	}
	c.versionStates[state.GetCollectionId()] = proto.Clone(state).(*viewpb.CollectionDataVersionState)
	c.views = append(c.views, proto.Clone(view).(*viewpb.DataViewOfCollection))
	return nil
}

func TestPublicationRejectsNonLoadableMembership(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()

	_, err := manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 1}, PublishedMutation{
		Add: []SegmentMembership{{
			SegmentID:    100,
			CollectionID: 1,
			PartitionID:  10,
			VChannel:     "ch-0",
			State:        commonpb.SegmentState_Growing,
			Level:        datapb.SegmentLevel_L1,
		}},
	})
	require.Error(t, err)
	require.Empty(t, catalog.views)

	version, err := manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 1}, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 100, "ch-0")},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Equal(t, []int64{100}, publishedSegmentIDs(t, catalog.views[0], "ch-0", 10))
}

func TestPublicationSuppressesDuplicateMembership(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	membership := loadableMembership(1, 10, 100, "ch-0")

	version, err := manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 1}, PublishedMutation{
		Add: []SegmentMembership{membership, membership},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Equal(t, []int64{100}, publishedSegmentIDs(t, catalog.views[0], "ch-0", 10))

	version, err = manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 1}, PublishedMutation{
		Add: []SegmentMembership{membership},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)
}

func TestPublicationCompactRewriteAdvancesCompactVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()

	_, err := manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 4}, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 100, "ch-0")},
	})
	require.NoError(t, err)

	version, err := manager.CommitRewrite(ctx, 1, PublishedMutation{
		Add:    []SegmentMembership{loadableMembership(1, 10, 101, "ch-0")},
		Remove: []int64{100},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 4, 1)
	require.Len(t, catalog.views, 2)
	require.Equal(t, []int64{101}, publishedSegmentIDs(t, catalog.views[1], "ch-0", 10))
}

func TestPublicationStreamingMutationAdvancesAndRetriesAfterRestart(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-0"}})
	require.NoError(t, err)
	mutation := PublishedMutation{Add: []SegmentMembership{loadableMembership(1, 10, 100, "ch-0")}}

	version, err := manager.CommitStreamingView(ctx, 1, mutation)
	require.NoError(t, err)
	requireDataVersion(t, version, 2, 0)
	requireDataVersion(t, catalog.versionStates[1].GetPublishedDataVersion(), 2, 0)
	require.Equal(t, int64(2), catalog.versionStates[1].GetAllocatedStreamingVersion())

	restarted := NewManager(catalog, store)
	retried, err := restarted.CommitStreamingView(ctx, 1, mutation)
	require.NoError(t, err)
	requireDataVersion(t, retried, 2, 0)
	require.Len(t, catalog.views, 2)
}

func TestPublicationStreamingMutationWaitsForPendingAssignedFlush(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-0"}})
	require.NoError(t, err)
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-0", 1000)
	assigned, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)
	requireDataVersion(t, assigned, 2, 0)

	published, err := manager.CommitStreamingView(ctx, 1, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 200, "ch-0")},
	})
	require.Error(t, err)
	require.True(t, merr.IsRetryableErr(err))
	require.Nil(t, published)
	require.Len(t, catalog.views, 1)
}

func TestPublicationStreamingMutationRetryRequiresExactAddedMembership(t *testing.T) {
	ctx := context.Background()
	manager, _, _ := newTestDataViewManager()
	_, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{CollectionID: 1, VChannels: []string{"ch-0"}})
	require.NoError(t, err)
	_, err = manager.CommitStreamingView(ctx, 1, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 100, "ch-0")},
	})
	require.NoError(t, err)

	published, err := manager.CommitStreamingView(ctx, 1, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 11, 100, "ch-0")},
	})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Nil(t, published)
}

func TestPublicationMetadataOnlyMutationIsNoOp(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()

	first, err := manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 3}, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 100, "ch-0")},
	})
	require.NoError(t, err)

	version, err := manager.CommitRewrite(ctx, 1, PublishedMutation{
		Add:    []SegmentMembership{loadableMembership(1, 10, 100, "ch-0")},
		Remove: []int64{100, 999},
	})
	require.NoError(t, err)
	require.True(t, proto.Equal(first, version))
	require.Len(t, catalog.views, 1)
}

func TestPublicationDelayedSortOutputInheritsFlushVersion(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-0", 1000)

	assigned, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)
	requireDataVersion(t, assigned, 1, 0)
	require.Empty(t, catalog.views)

	version, err := manager.CommitPublishedView(ctx, 1, assigned, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 101, "ch-0")},
	})
	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)
	require.Equal(t, []int64{101}, publishedSegmentIDs(t, catalog.views[0], "ch-0", 10))
}

func TestPublicationDoesNotOvertakeEarlierAssignedStreamingEpoch(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-0", 1000)
	store.segments[200] = newDataViewTestSegment(1, 10, 200, "ch-0", 2000)

	first, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)
	second, err := manager.AssignFlushVersion(ctx, 1, 200)
	require.NoError(t, err)

	published, err := manager.CommitPublishedView(ctx, 1, second, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 200, "ch-0")},
	})
	require.Error(t, err)
	require.True(t, merr.IsRetryableErr(err))
	require.Nil(t, published)
	require.Empty(t, catalog.views)

	published, err = manager.CommitPublishedView(ctx, 1, first, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 100, "ch-0")},
	})
	require.NoError(t, err)
	requireDataVersion(t, published, 1, 0)
	require.Len(t, catalog.views, 2)
	requireDataVersion(t, catalog.views[0].GetDataVersion(), 1, 0)
	requireDataVersion(t, catalog.views[1].GetDataVersion(), 2, 0)
	require.Equal(t, []int64{100, 200}, publishedSegmentIDs(t, catalog.views[1], "ch-0", 10))
}

func TestPublicationRestartDoesNotOvertakePersistedEarlierAssignment(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-0", 1000)
	store.segments[200] = newDataViewTestSegment(1, 10, 200, "ch-0", 2000)

	_, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.NoError(t, err)
	second, err := manager.AssignFlushVersion(ctx, 1, 200)
	require.NoError(t, err)

	restarted := NewManager(catalog, store)
	published, err := restarted.CommitPublishedView(ctx, 1, second, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 200, "ch-0")},
	})
	require.Error(t, err)
	require.True(t, merr.IsRetryableErr(err))
	require.Nil(t, published)
	require.Empty(t, catalog.views)
}

func TestPublicationAssignedNoChangeDoesNotReportSuccess(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	membership := loadableMembership(1, 10, 100, "ch-0")

	_, err := manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 1}, PublishedMutation{
		Add: []SegmentMembership{membership},
	})
	require.NoError(t, err)

	published, err := manager.CommitPublishedView(ctx, 1, &viewpb.DataVersion{StreamingVersion: 2}, PublishedMutation{
		Add: []SegmentMembership{membership},
	})
	require.Error(t, err)
	require.True(t, merr.IsRetryableErr(err))
	require.Nil(t, published)
	require.Len(t, catalog.views, 1)
	requireDataVersion(t, catalog.versionStates[1].GetPublishedDataVersion(), 1, 0)
}

func TestPublicationRetryProvesDurableAssignedMutation(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	assigned := &viewpb.DataVersion{StreamingVersion: 1}

	_, err := manager.CommitPublishedView(ctx, 1, assigned, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 100, "ch-0")},
	})
	require.NoError(t, err)

	published, err := manager.CommitPublishedView(ctx, 1, assigned, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 101, "ch-0")},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Nil(t, published)
	require.Len(t, catalog.views, 1)
}

func TestPublicationRetryRejectsDurableSnapshotWithExtraMembership(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	assigned := &viewpb.DataVersion{StreamingVersion: 1}

	_, err := manager.CommitPublishedView(ctx, 1, assigned, PublishedMutation{
		Add: []SegmentMembership{
			loadableMembership(1, 10, 100, "ch-0"),
			loadableMembership(1, 10, 200, "ch-0"),
		},
	})
	require.NoError(t, err)

	published, err := manager.CommitPublishedView(ctx, 1, assigned, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 100, "ch-0")},
	})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Nil(t, published)
	require.Len(t, catalog.views, 1)
}

func TestPublicationAssignmentStateFailureStillBlocksLaterEpoch(t *testing.T) {
	ctx := context.Background()
	manager, catalog, store := newTestDataViewManager()
	store.segments[100] = newDataViewTestSegment(1, 10, 100, "ch-0", 1000)
	store.segments[200] = newDataViewTestSegment(1, 10, 200, "ch-0", 2000)
	catalog.saveVersionErrOnce = merr.WrapErrServiceUnavailableMsg("state save failed")

	_, err := manager.AssignFlushVersion(ctx, 1, 100)
	require.Error(t, err)
	requireDataVersion(t, store.segments[100].GetSealedAtDataVersion(), 1, 0)

	second, err := manager.AssignFlushVersion(ctx, 1, 200)
	require.NoError(t, err)
	requireDataVersion(t, second, 2, 0)

	published, err := manager.CommitPublishedView(ctx, 1, second, PublishedMutation{
		Add: []SegmentMembership{loadableMembership(1, 10, 200, "ch-0")},
	})
	require.Error(t, err)
	require.True(t, merr.IsRetryableErr(err))
	require.Nil(t, published)
	require.Empty(t, catalog.views)
}

func TestPublicationCreateCollectionPersistsPublishedHeadAtomically(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()

	version, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1,
		VChannels:    []string{"ch-0"},
	})

	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	require.Len(t, catalog.views, 1)
	requireDataVersion(t, catalog.versionStates[1].GetPublishedDataVersion(), 1, 0)
}

func TestPublicationCreateCollectionRecoversDurableHeadBeforeNewerOrphan(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = []*viewpb.DataViewOfCollection{
		newTestDataView(1, 1, 0, newTestDataViewShard("ch-0", 10, 100)),
		newTestDataView(1, 2, 0, newTestDataViewShard("ch-0", 10, 200)),
	}
	catalog.versionStates = map[int64]*viewpb.CollectionDataVersionState{
		1: {
			CollectionId:              1,
			AllocatedStreamingVersion: 2,
			PublishedDataVersion:      &viewpb.DataVersion{StreamingVersion: 1},
		},
	}

	version, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1,
		VChannels:    []string{"ch-0"},
	})

	require.NoError(t, err)
	requireDataVersion(t, version, 1, 0)
	view, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []int64{100}, publishedSegmentIDs(t, view, "ch-0", 10))
}

func TestPublicationCreateCollectionBackfillsLegacySnapshotWhenDurableHeadIsMissing(t *testing.T) {
	ctx := context.Background()
	manager, catalog, _ := newTestDataViewManager()
	catalog.views = []*viewpb.DataViewOfCollection{
		newTestDataView(1, 1, 0, newTestDataViewShard("ch-0", 10, 100)),
		newTestDataView(1, 2, 0, newTestDataViewShard("ch-0", 10, 100, 200)),
	}
	catalog.versionStates = map[int64]*viewpb.CollectionDataVersionState{
		1: {
			CollectionId:              1,
			AllocatedStreamingVersion: 2,
		},
	}

	version, err := manager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1,
		VChannels:    []string{"ch-0"},
	})

	require.NoError(t, err)
	requireDataVersion(t, version, 2, 0)
	requireDataVersion(t, catalog.versionStates[1].GetPublishedDataVersion(), 2, 0)
	view, err := manager.LatestVisibleDataView(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []int64{100, 200}, publishedSegmentIDs(t, view, "ch-0", 10))
}

func loadableMembership(collectionID, partitionID, segmentID int64, vchannel string) SegmentMembership {
	return SegmentMembership{
		SegmentID:    segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		VChannel:     vchannel,
		State:        commonpb.SegmentState_Flushed,
		Level:        datapb.SegmentLevel_L1,
	}
}

func publishedSegmentIDs(
	t *testing.T,
	view *viewpb.DataViewOfCollection,
	vchannel string,
	partitionID int64,
) []int64 {
	t.Helper()
	for _, shard := range view.GetShards() {
		if shard.GetVchannel() != vchannel {
			continue
		}
		for _, partition := range shard.GetPartitions() {
			if partition.GetPartitionId() == partitionID {
				return partition.GetSegmentIds()
			}
		}
	}
	t.Fatalf("membership %s/%d not found", vchannel, partitionID)
	return nil
}
