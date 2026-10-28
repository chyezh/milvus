package querycoordv2

import (
	"context"
	"maps"
	"slices"

	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// updateLoadConfig changes placement through the same ordered callback as Load.
// Cluster reconciliation resolves defaults and user intent under the collection
// lock, so a concurrent release cannot be undone by a stale watcher snapshot.
func (s *Server) updateLoadConfig(ctx context.Context, ids []int64, replicas int32, groups []string) error {
	var result error
	for _, id := range ids {
		if err := s.updateCollectionLoadConfig(ctx, id, replicas, groups, false); err != nil {
			result = merr.Combine(result, err)
		}
	}
	return result
}

func (s *Server) updateCollectionLoadConfig(ctx context.Context, id int64, replicas int32, groups []string, clusterDefault bool) error {
	// Most periodic checks are no-ops. Avoid metadata RPCs and a broadcast lock
	// for those collections; a possible change is rechecked after serialization.
	if clusterDefault {
		if _, _, changed := clusterDefaultPlacementChange(s.qviewsRuntime.loadConfigStore.GetConfig(id)); !changed {
			return nil
		}
	}
	broadcaster, err := s.startBroadcastWithCollectionIDLock(ctx, id)
	if err != nil {
		return err
	}
	defer broadcaster.Close()
	current := s.qviewsRuntime.loadConfigStore.GetConfig(id)
	if current == nil {
		return nil
	}
	if clusterDefault {
		var changed bool
		replicas, groups, changed = clusterDefaultPlacementChange(current)
		if !changed {
			return nil
		}
	}
	if replicas == 0 {
		replicas = int32(len(current.Replicas))
	}
	if replicas < 0 {
		return merr.WrapErrParameterInvalidMsg("replica number must be positive")
	}
	actual := make(map[string]int)
	for _, replica := range current.Replicas {
		actual[replica.ResourceGroup]++
	}
	if len(groups) == 0 {
		if int(replicas) == len(current.Replicas) {
			return nil
		}
		groups = slices.Sorted(maps.Keys(actual))
	}
	expected, err := utils.ReplicaNumberByResourceGroup(groups, replicas)
	if err != nil {
		return err
	}
	// Compare multiplicities, not just the set of resource group names.
	if maps.Equal(actual, expected) {
		return nil
	}
	if _, err := utils.AssignReplica(ctx, s.meta, groups, replicas, false); err != nil {
		return err
	}
	header := loadConfigIntoAlterLoadConfigHeader(current)
	priority := current.Replicas[0].Priority
	header.Replicas, err = s.generateQViewsReplicaConfigs(ctx, current, qviewsExpectedLoadConfig{ReplicaNumber: expected, Priority: priority})
	if err != nil {
		return err
	}
	msg := message.NewAlterLoadConfigMessageBuilderV2().WithHeader(header).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).WithControlChannelBroadcast().MustBuildBroadcast()
	// Broadcast waits for durable application, not resource preparation. Each
	// collection can converge independently; loadstatus observes the new version.
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

// clusterDefaultPlacementChange uses only immutable local load metadata. Always
// repeat it under the collection lock before publishing a changed placement.
func clusterDefaultPlacementChange(current *loadmgr.LoadConfig) (int32, []string, bool) {
	if current == nil {
		return 0, nil, false
	}
	cfg := &paramtable.Get().QueryCoordCfg
	if current.UserSpecifiedReplicaMode && !cfg.ClusterLevelLoadForceOverrideUserReplicaMode.GetAsBool() {
		return 0, nil, false
	}
	replicas := cfg.ClusterLevelLoadReplicaNumber.GetAsInt32()
	groups := cfg.ClusterLevelLoadResourceGroups.GetAsStrings()
	// Independently refreshed settings can temporarily be incomplete.
	if replicas <= 0 || len(groups) == 0 || (len(groups) != 1 && len(groups) != int(replicas)) {
		return 0, nil, false
	}
	expected, _ := utils.ReplicaNumberByResourceGroup(groups, replicas)
	actual := make(map[string]int)
	for _, replica := range current.Replicas {
		actual[replica.ResourceGroup]++
	}
	return replicas, groups, !maps.Equal(actual, expected)
}
