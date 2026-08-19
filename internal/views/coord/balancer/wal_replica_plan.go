package balancer

import (
	"sort"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

type walReplicaPlanState struct {
	snap                    *BalancerSnapshot
	readWriteReplicaByShard map[string]int64
}

func newWALReplicaPlanState(snap *BalancerSnapshot) *walReplicaPlanState {
	state := &walReplicaPlanState{
		snap:                    snap,
		readWriteReplicaByShard: make(map[string]int64),
	}
	if snap == nil {
		return state
	}
	for shardID, stats := range snap.ShardStatsMap() {
		if stats == nil {
			continue
		}
		for walReplicaID := range stats.WALReplicaDependencies {
			if state.isReadWrite(shardID.VChannel, walReplicaID) {
				state.readWriteReplicaByShard[shardID.VChannel] = walReplicaID
				break
			}
		}
	}
	return state
}

func (s *walReplicaPlanState) Select(shardID qviews.ShardID, resourceGroup string) (int64, bool) {
	if s == nil || s.snap == nil || s.snap.WALReplicaSnapshot == nil {
		return 0, true
	}
	pchannel, ok := pchannelForVChannel(shardID.VChannel)
	if !ok {
		return s.snap.WALReplicaIDForShard(shardID.VChannel, resourceGroup)
	}
	if _, occupied := s.readWriteReplicaByShard[shardID.VChannel]; !occupied {
		if walReplicaID, ok := s.snap.WALReplicaSnapshot.SelectByAccessMode(pchannel, resourceGroup, types.AccessModeRW); ok {
			return walReplicaID, true
		}
	}
	if walReplicaID, ok := s.currentServiceableWALReplica(shardID, pchannel, resourceGroup); ok {
		return walReplicaID, true
	}
	if walReplicaID, ok := s.snap.WALReplicaSnapshot.SelectByAccessMode(pchannel, resourceGroup, types.AccessModeRO); ok {
		return walReplicaID, true
	}
	return 0, false
}

func (s *walReplicaPlanState) Accept(shardID qviews.ShardID, walReplicaID int64) {
	if s == nil || s.snap == nil || s.snap.WALReplicaSnapshot == nil {
		return
	}
	if s.isReadWrite(shardID.VChannel, walReplicaID) {
		s.readWriteReplicaByShard[shardID.VChannel] = walReplicaID
	}
}

func (s *walReplicaPlanState) Release(shardID qviews.ShardID) {
	if s == nil || s.snap == nil {
		return
	}
	stats := s.snap.ShardStatsMap()[shardID]
	if stats == nil {
		return
	}
	for walReplicaID := range stats.WALReplicaDependencies {
		if !s.isReadWrite(shardID.VChannel, walReplicaID) {
			continue
		}
		delete(s.readWriteReplicaByShard, shardID.VChannel)
		return
	}
	if stats.UpVersion != nil && s.isReadWrite(shardID.VChannel, stats.UpWALReplicaID) {
		delete(s.readWriteReplicaByShard, shardID.VChannel)
	}
}

func (s *walReplicaPlanState) currentServiceableWALReplica(
	shardID qviews.ShardID,
	pchannel string,
	resourceGroup string,
) (int64, bool) {
	stats := s.snap.ShardStatsMap()[shardID]
	if stats == nil {
		return 0, false
	}
	if stats.UpVersion != nil {
		if s.snap.WALReplicaSnapshot.IsServiceableInResourceGroup(pchannel, stats.UpWALReplicaID, resourceGroup) {
			return stats.UpWALReplicaID, true
		}
	}
	replicaIDs := make([]int64, 0, len(stats.WALReplicaDependencies))
	for walReplicaID := range stats.WALReplicaDependencies {
		if stats.UpVersion != nil && walReplicaID == stats.UpWALReplicaID {
			continue
		}
		replicaIDs = append(replicaIDs, walReplicaID)
	}
	sort.Slice(replicaIDs, func(i, j int) bool {
		return replicaIDs[i] < replicaIDs[j]
	})
	for _, walReplicaID := range replicaIDs {
		if s.snap.WALReplicaSnapshot.IsServiceableInResourceGroup(pchannel, walReplicaID, resourceGroup) {
			return walReplicaID, true
		}
	}
	return 0, false
}

func (s *walReplicaPlanState) isReadWrite(vchannel string, walReplicaID int64) bool {
	if s == nil || s.snap == nil || s.snap.WALReplicaSnapshot == nil {
		return walReplicaID == 0
	}
	pchannel, ok := pchannelForVChannel(vchannel)
	if !ok {
		return walReplicaID == 0
	}
	accessMode, ok := s.snap.WALReplicaSnapshot.AccessMode(pchannel, walReplicaID)
	return ok && accessMode == types.AccessModeRW
}

func pchannelForVChannel(vchannel string) (string, bool) {
	ch, err := metautil.ParseChannel(vchannel, metautil.NewDynChannelMapper())
	if err != nil {
		return "", false
	}
	return ch.PhysicalName(), true
}
