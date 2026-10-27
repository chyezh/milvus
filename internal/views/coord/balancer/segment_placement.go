package balancer

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MoveSegments is the segment API adapter to ManualPlace. It only edits the
// supplied builders; admission, lifecycle and exact-version waits are shared.
func (b *DefaultBalancer) MoveSegments(ctx context.Context, scope PlacementScope, destinations, segments []int64) (map[qviews.ShardID]ManualPlaceResult, error) {
	return b.ManualPlace(ctx, scope, func(builders map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error {
		wanted := make(map[int64]bool, len(segments))
		for _, segment := range segments {
			wanted[segment] = false
		}
		placements := make(map[int64]*CollectionPlacement)
		rows := make(map[int64]int64)
		for _, builder := range builders {
			meta := builder.Build().GetMeta()
			pl := placements[meta.GetCollectionId()]
			if pl == nil {
				var err error
				pl, err = b.placementLocked(meta.GetCollectionId())
				if err != nil {
					return err
				}
				placements[meta.GetCollectionId()] = pl
			}
			candidates := destinations
			if len(candidates) == 0 {
				candidates = pl.Nodes[meta.GetReplicaId()]
			}
			candidates = slices.Clone(candidates)
			slices.Sort(candidates)
			candidates = slices.Compact(candidates)
			candidates = slices.DeleteFunc(candidates, func(node int64) bool { return node == scope.SourceNodeID })
			assignments := builder.Assignments()
			// Iterate a snapshot: destinations are inserted into assignments below.
			partitions := assignments[scope.SourceNodeID]
			for partition, ids := range partitions {
				remaining := make([]int64, 0, len(ids))
				for _, segment := range ids {
					if _, requested := wanted[segment]; len(wanted) != 0 && !requested {
						remaining = append(remaining, segment)
						continue
					}
					if len(candidates) == 0 {
						return merr.WrapErrParameterInvalidMsg("no destination query node other than source")
					}
					for _, node := range candidates {
						if !slices.Contains(pl.Nodes[meta.GetReplicaId()], node) {
							return merr.WrapErrNodeNotFound(node, "destination is outside the replica placement")
						}
						if _, ok := rows[node]; !ok {
							entry := b.cache.GetNode(node)
							if entry == nil || !entry.Info().Alive || entry.Info().Stopping {
								return merr.WrapErrServiceUnavailableMsg("destination node %d is unavailable", node)
							}
							rows[node] = entry.Info().TargetLogicalSize
						}
					}
					target := candidates[0]
					for _, node := range candidates[1:] {
						if rows[node] < rows[target] {
							target = node
						}
					}
					if assignments[target] == nil {
						assignments[target] = make(map[int64][]int64)
					}
					assignments[target][partition] = append(assignments[target][partition], segment)
					if data := b.cache.GetCollection(meta.GetCollectionId()).DataView(); data != nil {
						if info, ok := data.Segment(segment); ok {
							rows[target] += info.LogicalSize
						}
					}
					if len(wanted) != 0 {
						wanted[segment] = true
					}
				}
				if len(remaining) == 0 {
					delete(partitions, partition)
				} else {
					partitions[partition] = remaining
				}
			}
			if len(partitions) == 0 {
				delete(assignments, scope.SourceNodeID)
			}
		}
		for segment, found := range wanted {
			if !found {
				return merr.WrapErrSegmentNotFound(segment, "segment not found in source placement")
			}
		}
		return nil
	})
}
