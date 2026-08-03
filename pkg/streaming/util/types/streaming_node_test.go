package types

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestStreamingNodeStatus(t *testing.T) {
	s := StreamingNodeStatus{Err: ErrStopping}
	assert.False(t, s.IsHealthy())

	s = StreamingNodeStatus{Err: ErrNotAlive}
	assert.False(t, s.IsHealthy())

	info := StreamingNodeInfo{
		ServerID: 1,
		Address:  "localhost:8080",
	}
	pb := NewProtoFromStreamingNodeInfo(info)
	info2 := NewStreamingNodeInfoFromProto(pb)
	assert.Equal(t, info.ServerID, info2.ServerID)
	assert.Equal(t, info.Address, info2.Address)
}

func TestShardAssignmentInfoKeepsWALReplicaID(t *testing.T) {
	info := ShardAssignmentInfo{
		PChannelAssignments: []PChannelShardAssignment{
			{
				PChannel: "p0",
				Entries: []ShardAssignmentEntry{
					{CollectionID: 1, ShardIndex: 2, ReplicaID: 3, WALReplicaID: 4},
				},
			},
		},
	}

	pb := NewProtoFromShardAssignmentInfo(info)
	roundTrip := NewShardAssignmentInfoFromProto(pb)

	assert.Equal(t, int64(4), pb.GetPchannelAssignments()[0].GetEntries()[0].GetWalReplicaId())
	assert.Equal(t, info, roundTrip)
}

func TestWALReplicaInfoFromProto(t *testing.T) {
	info := NewWALReplicaInfoFromProto(&streamingpb.WALReplicaInfo{
		Pchannel:          "p0",
		WalReplicaId:      2,
		AccessMode:        streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
		ResourceGroup:     "rg1",
		PchannelWriteTerm: 7,
		AssignmentEpoch:   11,
		State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
	})

	assert.Equal(t, WALReplicaInfo{
		ChannelID:         ChannelID{Name: "p0", WALReplicaID: 2},
		AccessMode:        AccessModeRO,
		ResourceGroup:     "rg1",
		PChannelWriteTerm: 7,
		AssignmentEpoch:   11,
		State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
	}, info)
	assert.Equal(t, int64(11), NewProtoFromWALReplicaInfo(info).GetAssignmentEpoch())
}
