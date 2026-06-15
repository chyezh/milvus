package qnview

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"google.golang.org/protobuf/proto"
)

func segmentPartitionMap(view *viewpb.QueryViewOfQueryNode) map[int64]int64 {
	segments := make(map[int64]int64)
	for _, partition := range view.GetPartitions() {
		for _, segmentID := range partition.GetSegmentIds() {
			segments[segmentID] = partition.GetPartitionId()
		}
	}
	return segments
}

func readyByPartition(view *viewpb.QueryViewOfQueryNode) map[int64][]int64 {
	ready := make(map[int64][]int64, len(view.GetPartitions()))
	for _, partition := range view.GetPartitions() {
		if len(partition.GetSegmentIds()) == 0 {
			continue
		}
		ready[partition.GetPartitionId()] = append([]int64(nil), partition.GetSegmentIds()...)
	}
	return ready
}

func hasRequiredAssignedSegments(meta *viewpb.QueryViewMeta, view *viewpb.QueryViewOfQueryNode) bool {
	requiredPartitions := make(map[int64]struct{})
	optionalPartitions := make(map[int64]struct{})
	if meta != nil && meta.GetSettings() != nil {
		for _, partitionID := range meta.GetSettings().GetRequiredPartitions() {
			requiredPartitions[partitionID] = struct{}{}
		}
		for _, partitionID := range meta.GetSettings().GetOptionalPartitions() {
			optionalPartitions[partitionID] = struct{}{}
		}
	}
	for _, partition := range view.GetPartitions() {
		if len(partition.GetSegmentIds()) == 0 {
			continue
		}
		if isRequiredPartition(partition.GetPartitionId(), requiredPartitions, optionalPartitions) {
			return true
		}
	}
	return false
}

func filterViewSegments(view *viewpb.QueryViewOfQueryNode, segmentIDs []int64) *viewpb.QueryViewOfQueryNode {
	if len(segmentIDs) == len(segmentPartitionMap(view)) {
		return proto.Clone(view).(*viewpb.QueryViewOfQueryNode)
	}
	keep := make(map[int64]struct{}, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		keep[segmentID] = struct{}{}
	}
	filtered := &viewpb.QueryViewOfQueryNode{NodeId: view.GetNodeId()}
	for _, partition := range view.GetPartitions() {
		out := &viewpb.QueryViewOfPartition{PartitionId: partition.GetPartitionId()}
		for _, segmentID := range partition.GetSegmentIds() {
			if _, ok := keep[segmentID]; ok {
				out.SegmentIds = append(out.SegmentIds, segmentID)
			}
		}
		if len(out.SegmentIds) > 0 {
			filtered.Partitions = append(filtered.Partitions, out)
		}
	}
	return filtered
}
