package balancer

import (
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// WALReplicaSnapshot is the QueryView balancer's read-only view of serviceable
// WAL replicas published by StreamingCoord assignment discovery.
type WALReplicaSnapshot struct {
	replicasByPChannel map[string][]types.WALReplicaInfo
}

func NewWALReplicaSnapshot(replicas []types.WALReplicaInfo) *WALReplicaSnapshot {
	snapshot := &WALReplicaSnapshot{
		replicasByPChannel: make(map[string][]types.WALReplicaInfo),
	}
	for _, replica := range replicas {
		pchannel := replica.ChannelID.Name
		snapshot.replicasByPChannel[pchannel] = append(snapshot.replicasByPChannel[pchannel], replica)
	}
	for pchannel := range snapshot.replicasByPChannel {
		sort.Slice(snapshot.replicasByPChannel[pchannel], func(i, j int) bool {
			left := snapshot.replicasByPChannel[pchannel][i]
			right := snapshot.replicasByPChannel[pchannel][j]
			if left.AccessMode != right.AccessMode {
				return left.AccessMode == types.AccessModeRW
			}
			return left.ChannelID.WALReplicaID < right.ChannelID.WALReplicaID
		})
	}
	return snapshot
}

func (s *WALReplicaSnapshot) Select(pchannel string, resourceGroup string) (int64, bool) {
	if s == nil {
		return 0, true
	}
	resourceGroup = normalizeWALReplicaResourceGroup(resourceGroup)
	replicas := s.replicasByPChannel[pchannel]
	for _, replica := range replicas {
		if normalizeWALReplicaResourceGroup(replica.ResourceGroup) != resourceGroup {
			continue
		}
		if !isServiceableWALReplica(replica) {
			continue
		}
		return replica.ChannelID.WALReplicaID, true
	}
	return 0, false
}

func (s *WALReplicaSnapshot) SelectByAccessMode(pchannel string, resourceGroup string, accessMode types.AccessMode) (int64, bool) {
	if s == nil {
		return 0, true
	}
	resourceGroup = normalizeWALReplicaResourceGroup(resourceGroup)
	replicas := s.replicasByPChannel[pchannel]
	for _, replica := range replicas {
		if replica.AccessMode != accessMode {
			continue
		}
		if normalizeWALReplicaResourceGroup(replica.ResourceGroup) != resourceGroup {
			continue
		}
		if !isServiceableWALReplica(replica) {
			continue
		}
		return replica.ChannelID.WALReplicaID, true
	}
	return 0, false
}

func (s *WALReplicaSnapshot) HasAccessModeInResourceGroup(pchannel string, resourceGroup string, accessMode types.AccessMode) bool {
	if s == nil {
		return true
	}
	resourceGroup = normalizeWALReplicaResourceGroup(resourceGroup)
	replicas := s.replicasByPChannel[pchannel]
	for _, replica := range replicas {
		if replica.AccessMode != accessMode {
			continue
		}
		if normalizeWALReplicaResourceGroup(replica.ResourceGroup) != resourceGroup {
			continue
		}
		switch replica.State {
		case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING:
			return true
		default:
			continue
		}
	}
	return false
}

func (s *WALReplicaSnapshot) AccessMode(pchannel string, walReplicaID int64) (types.AccessMode, bool) {
	if s == nil {
		if walReplicaID == 0 {
			return types.AccessModeRW, true
		}
		return 0, false
	}
	for _, replica := range s.replicasByPChannel[pchannel] {
		if replica.ChannelID.WALReplicaID != walReplicaID {
			continue
		}
		if !isServiceableWALReplica(replica) {
			return 0, false
		}
		return replica.AccessMode, true
	}
	return 0, false
}

func (s *WALReplicaSnapshot) IsServiceableInResourceGroup(pchannel string, walReplicaID int64, resourceGroup string) bool {
	if s == nil {
		return walReplicaID == 0
	}
	resourceGroup = normalizeWALReplicaResourceGroup(resourceGroup)
	for _, replica := range s.replicasByPChannel[pchannel] {
		if replica.ChannelID.WALReplicaID != walReplicaID {
			continue
		}
		if normalizeWALReplicaResourceGroup(replica.ResourceGroup) != resourceGroup {
			return false
		}
		return isServiceableWALReplica(replica)
	}
	return false
}

func (s *WALReplicaSnapshot) ReadOnlyReplicas() []types.WALReplicaInfo {
	if s == nil {
		return nil
	}
	replicas := make([]types.WALReplicaInfo, 0)
	for _, byPChannel := range s.replicasByPChannel {
		for _, replica := range byPChannel {
			if replica.AccessMode != types.AccessModeRO {
				continue
			}
			if !isServiceableWALReplica(replica) {
				continue
			}
			replicas = append(replicas, replica)
		}
	}
	sort.Slice(replicas, func(i, j int) bool {
		left := replicas[i].ChannelID
		right := replicas[j].ChannelID
		return left.LT(right)
	})
	return replicas
}

func (s *BalancerSnapshot) WALReplicaIDForShard(vchannel string, resourceGroup string) (int64, bool) {
	if s == nil || s.WALReplicaSnapshot == nil {
		return 0, true
	}
	ch, err := metautil.ParseChannel(vchannel, metautil.NewDynChannelMapper())
	if err != nil {
		return 0, true
	}
	return s.WALReplicaSnapshot.Select(ch.PhysicalName(), resourceGroup)
}

func normalizeWALReplicaResourceGroup(resourceGroup string) string {
	if resourceGroup == "" {
		return common.DefaultResourceGroupName
	}
	return resourceGroup
}

func isServiceableWALReplica(replica types.WALReplicaInfo) bool {
	switch replica.State {
	case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED:
		return true
	case streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING:
		return replica.AccessMode == types.AccessModeRO
	default:
		return false
	}
}
