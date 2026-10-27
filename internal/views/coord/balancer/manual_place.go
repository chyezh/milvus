package balancer

import (
	"cmp"
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PlacementScope selects resident shards by collection and/or source node.
// Zero means no restriction for that dimension; at least one must be provided.
type PlacementScope struct {
	CollectionID int64
	SourceNodeID int64
}

type ManualPlaceResult struct {
	Version qviews.QueryViewVersion
	Changed bool
	State   qviews.QueryViewState
	Err     error
}

// ManualPlace serializes planning, not waiting, with automatic reconciliation.
// The callback runs once and must not block, reenter the balancer, or retain its
// builders. Results identify exact accepted versions, including partial failure.
func (b *DefaultBalancer) ManualPlace(ctx context.Context, scope PlacementScope, modify func(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error) (map[qviews.ShardID]ManualPlaceResult, error) {
	if err := b.cache.WaitForReady(ctx); err != nil {
		return nil, err
	}
	results := make(map[qviews.ShardID]ManualPlaceResult)
	pending := make(map[qviews.ShardID]*coordview.Preparation)
	err := func() error {
		b.reconcileMu.Lock()
		defer b.reconcileMu.Unlock()
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		ids, err := b.manualShards(scope)
		if err != nil {
			return err
		}
		builders := make(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder, len(ids))
		originals := make(map[qviews.ShardID]*viewpb.QueryViewOfShard, len(ids))
		managers := make(map[qviews.ShardID]*coordview.ShardViewManager, len(ids))
		placements := make(map[int64]*CollectionPlacement)
		for _, id := range ids {
			manager := b.viewRegistry.Get(id)
			if manager == nil {
				return merr.WrapErrServiceUnavailableMsg("shard %s was released during manual placement", id.String())
			}
			builder, err := manager.PlacementBuilder()
			if err != nil {
				return err
			}
			original := builder.Build()
			collectionID := original.GetMeta().GetCollectionId()
			if _, ok := placements[collectionID]; !ok {
				pl, err := b.placementLocked(collectionID)
				if err != nil {
					return err
				}
				if pl.Config == nil || !pl.LayoutReady {
					return merr.WrapErrServiceUnavailableMsg("collection %d placement is changing", collectionID)
				}
				placements[collectionID] = pl
			}
			managers[id], originals[id] = manager, original
			builders[id] = qviews.NewQueryViewAtCoordBuilderFromView(original)
		}
		if err := modify(builders); err != nil {
			return err
		}
		if len(builders) != len(originals) {
			return merr.WrapErrServiceInternalMsg("manual placement callback changed the shard set")
		}
		changed := make(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder)
		for _, id := range ids {
			builder := builders[id]
			if builder == nil {
				return merr.WrapErrServiceInternalMsg("manual placement callback removed shard %s", id.String())
			}
			before, after := originals[id], builder.Build()
			pl := placements[before.GetMeta().GetCollectionId()]
			if err := b.validateManualPlacement(before, after, pl); err != nil {
				return err
			}
			if proto.Equal(before, after) {
				results[id] = ManualPlaceResult{Version: qviews.FromProtoQueryViewVersion(before.GetMeta().GetVersion())}
				continue
			}
			// Detach all callback-owned memory before publishing to managers.
			changed[id] = qviews.NewQueryViewAtCoordBuilderFromView(after)
		}
		for _, id := range ids {
			builder := changed[id]
			if builder == nil {
				continue
			}
			before := originals[id]
			pl := placements[before.GetMeta().GetCollectionId()]
			current := b.cache.GetCollection(pl.Config.CollectionID)
			if current == nil || current.LoadConfig() != pl.Config || current.ConfigVersion() != pl.ConfigVersion {
				return merr.WrapErrServiceUnavailableMsg("load configuration changed during manual placement")
			}
			p, err := managers[id].AddManualPreparing(ctx, builder, qviews.FromProtoQueryViewVersion(before.GetMeta().GetVersion()))
			if err != nil {
				results[id] = ManualPlaceResult{Err: err}
				return err
			}
			pending[id] = p
			results[id] = ManualPlaceResult{Version: p.Version, Changed: true, State: qviews.QueryViewStatePreparing}
		}
		return nil
	}()
	// No lock is held here. Each completion retains the first outcome even if
	// subsequent handoffs already removed that version from the registry.
	for id, p := range pending {
		outcome := p.Wait(ctx)
		result := results[id]
		result.State, result.Err = outcome.State, outcome.Err
		results[id] = result
		if outcome.Err != nil {
			err = errors.CombineErrors(err, merr.Wrapf(outcome.Err, "manual placement shard %s version %v", id.String(), p.Version))
		}
	}
	return results, err
}

func (b *DefaultBalancer) manualShards(scope PlacementScope) ([]qviews.ShardID, error) {
	if scope.CollectionID != 0 {
		collection := b.cache.GetCollection(scope.CollectionID)
		if collection == nil || collection.LoadConfig() == nil {
			return nil, merr.WrapErrCollectionNotLoaded(scope.CollectionID)
		}
	}
	var ids []qviews.ShardID
	if scope.SourceNodeID != 0 {
		node := b.cache.GetNode(scope.SourceNodeID)
		if node == nil {
			return nil, merr.WrapErrNodeNotFound(scope.SourceNodeID)
		}
		node.RangeShards(func(id qviews.ShardID) bool {
			collectionID, ok := b.cache.CollectionForReplica(id.ReplicaID)
			if ok && (scope.CollectionID == 0 || scope.CollectionID == collectionID) {
				ids = append(ids, id)
			}
			return true
		})
	} else if scope.CollectionID != 0 {
		ids = b.viewRegistry.CollectionShards(scope.CollectionID)
	} else {
		return nil, merr.WrapErrParameterInvalidMsg("manual placement requires a collection or source node")
	}
	slices.SortFunc(ids, func(a, c qviews.ShardID) int {
		if a.ReplicaID != c.ReplicaID {
			return cmp.Compare(a.ReplicaID, c.ReplicaID)
		}
		return cmp.Compare(a.VChannel, c.VChannel)
	})
	return ids, nil
}

func (b *DefaultBalancer) validateManualPlacement(before, after *viewpb.QueryViewOfShard, pl *CollectionPlacement) error {
	if !proto.Equal(before.GetMeta(), after.GetMeta()) || !proto.Equal(before.GetStreamingNode(), after.GetStreamingNode()) {
		return merr.WrapErrServiceInternalMsg("manual placement must preserve view metadata")
	}
	type member struct{ partition, manifest, node int64 }
	members := make(map[int64]member)
	for _, node := range before.GetQueryNode() {
		for _, p := range node.GetPartitions() {
			for i, segment := range p.GetSegmentIds() {
				var manifest int64
				if i < len(p.GetSegmentManifestVersions()) {
					manifest = p.GetSegmentManifestVersions()[i]
				}
				members[segment] = member{p.GetPartitionId(), manifest, node.GetNodeId()}
			}
		}
	}
	for _, node := range after.GetQueryNode() {
		for _, p := range node.GetPartitions() {
			for i, segment := range p.GetSegmentIds() {
				original, ok := members[segment]
				var manifest int64
				if i < len(p.GetSegmentManifestVersions()) {
					manifest = p.GetSegmentManifestVersions()[i]
				}
				if !ok || original.partition != p.GetPartitionId() || original.manifest != manifest {
					return merr.WrapErrServiceInternalMsg("manual placement changed or duplicated segment %d", segment)
				}
				if original.node != node.GetNodeId() {
					info := b.cache.GetNode(node.GetNodeId())
					if info == nil || !info.Info().Alive || info.Info().Stopping {
						return merr.WrapErrServiceUnavailableMsg("destination node %d is unavailable", node.GetNodeId())
					}
					if !slices.Contains(pl.Nodes[after.GetMeta().GetReplicaId()], node.GetNodeId()) {
						return merr.WrapErrNodeNotFound(node.GetNodeId(), "destination is outside the replica placement")
					}
				}
				delete(members, segment)
			}
		}
	}
	if len(members) != 0 {
		return merr.WrapErrServiceInternalMsg("manual placement removed segments")
	}
	return nil
}
