package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSegmentSelectionPreservesPhysicalBindings(t *testing.T) {
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 2, SegmentIds: []int64{10, 11, 12}, SegmentManifestVersions: []int64{4, 5, 6}, ReadySegmentIds: []int64{10, 11, 12}}}}
	selection := SelectSegments([]int64{10, 11, 12}, map[int64]struct{}{11: {}})
	require.True(t, selection.Exclude)
	filtered := FilterSelectedSegments(view, selection)
	require.Equal(t, []int64{10, 12}, filtered.Partitions[0].SegmentIds)
	require.Equal(t, []int64{4, 6}, filtered.Partitions[0].SegmentManifestVersions)
	require.Equal(t, []int64{10, 12}, filtered.Partitions[0].ReadySegmentIds)
	require.Len(t, view.Partitions[0].SegmentIds, 3)
	require.Empty(t, FilterSelectedSegments(view, &viewpb.SegmentSelection{}).Partitions)
	require.Same(t, view, FilterSelectedSegments(view, nil))
}

func TestBuilderBindsStatsForAssignedPartitions(t *testing.T) {
	dv := &viewpb.DataViewOfCollection{CollectionId: 1, DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, Shards: []*viewpb.DataViewOfShard{{Vchannel: "p_1v0", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 2, SegmentIds: []int64{10}, PartitionStatsVersion: 7}, {PartitionId: 3, SegmentIds: []int64{11}, PartitionStatsVersion: 8}}}}}
	view := NewQueryViewAtCoordBuilder(1, dv, "p_1v0").SetAssignments(map[int64]map[int64][]int64{100: {2: {10}}}).Build()
	require.Equal(t, []*viewpb.ClusteredPartitionStats{{PartitionId: 2, Version: 7}}, view.StreamingNode.PartitionStats)
}
