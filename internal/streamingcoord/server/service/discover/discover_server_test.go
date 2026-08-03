package discover

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_manager"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSendFullAssignmentPublishesSecondaryChannelsAndShardAssignments(t *testing.T) {
	mc := mock_manager.NewMockManagerClient(t)
	mc.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg1"},
		2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"}, ResourceGroup: "rg1"},
	}, nil)
	resource.InitForTest(resource.OptStreamingManagerClient(mc))

	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	streamServer.EXPECT().Send(mock.Anything).RunAndReturn(func(resp *streamingpb.AssignmentDiscoverResponse) error {
		fullAssignment := resp.GetFullAssignment()
		assignments := make(map[int64]*streamingpb.StreamingNodeAssignment, len(fullAssignment.Assignments))
		for _, assignment := range fullAssignment.Assignments {
			assignments[assignment.GetNode().GetServerId()] = assignment
		}

		node1Assignment := assignments[1]
		assert.NotNil(t, node1Assignment)
		assert.Equal(t, []string{"rw-channel"}, pchannelNames(node1Assignment.Channels))
		assert.Equal(t, []string{"ro-channel"}, pchannelNames(node1Assignment.SecondaryChannels))
		assert.Equal(t, []types.WALReplicaInfo{
			{
				ChannelID:         types.ChannelID{Name: "rw-channel"},
				AccessMode:        types.AccessModeRW,
				ResourceGroup:     "rg1",
				PChannelWriteTerm: 1,
				State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
			{
				ChannelID:         types.ChannelID{Name: "ro-channel", WALReplicaID: 2},
				AccessMode:        types.AccessModeRO,
				ResourceGroup:     "rg1",
				PChannelWriteTerm: 1,
				State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
			},
		}, walReplicaInfos(node1Assignment.WalReplicas))
		assert.Equal(t, types.ShardAssignmentInfo{
			PChannelAssignments: []types.PChannelShardAssignment{
				{
					PChannel: "ro-channel",
					Entries: []types.ShardAssignmentEntry{
						{CollectionID: 100, ShardIndex: 1, ReplicaID: 10},
					},
				},
			},
		}, types.NewShardAssignmentInfoFromProto(node1Assignment.GetShardAssignment()))

		node2Assignment := assignments[2]
		assert.NotNil(t, node2Assignment)
		assert.Empty(t, node2Assignment.Channels)
		assert.Empty(t, node2Assignment.SecondaryChannels)
		assert.Nil(t, assignments[3], "stale WAL replica owner must not be published")
		return nil
	})

	helper := &discoverGrpcServerHelper{
		StreamingCoordAssignmentService_AssignmentDiscoverServer: streamServer,
	}
	err := helper.SendFullAssignment(balancer.WatchChannelAssignmentsCallbackParam{
		Version:            typeutil.VersionInt64Pair{Global: 1, Local: 2},
		CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "pchannel"}},
		Relations: []types.PChannelInfoAssigned{
			{
				Channel: types.PChannelInfo{Name: "rw-channel", Term: 1, AccessMode: types.AccessModeRW},
				Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			},
			{
				Channel: types.PChannelInfo{Name: "ro-channel", Term: 2, AccessMode: types.AccessModeRO},
				Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			},
			{
				Channel: types.PChannelInfo{Name: "stale-rw-channel", Term: 3, AccessMode: types.AccessModeRW},
				Node:    types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"},
			},
		},
		WALReplicaRelations: []types.WALReplicaInfoAssigned{
			{
				Replica: types.WALReplicaInfo{
					ChannelID:         types.ChannelID{Name: "rw-channel"},
					AccessMode:        types.AccessModeRW,
					PChannelWriteTerm: 1,
					State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				Node: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			},
			{
				Replica: types.WALReplicaInfo{
					ChannelID:         types.ChannelID{Name: "ro-channel", WALReplicaID: 2},
					AccessMode:        types.AccessModeRO,
					ResourceGroup:     "rg1",
					PChannelWriteTerm: 1,
					State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				Node: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			},
			{
				Replica: types.WALReplicaInfo{
					ChannelID:         types.ChannelID{Name: "stale-ro-channel", WALReplicaID: 3},
					AccessMode:        types.AccessModeRO,
					ResourceGroup:     "rg1",
					PChannelWriteTerm: 1,
					State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
				},
				Node: types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"},
			},
		},
		ShardAssignments: map[int64]types.ShardAssignmentInfo{
			1: {
				PChannelAssignments: []types.PChannelShardAssignment{
					{
						PChannel: "ro-channel",
						Entries: []types.ShardAssignmentEntry{
							{CollectionID: 100, ShardIndex: 1, ReplicaID: 10},
						},
					},
				},
			},
			3: {
				PChannelAssignments: []types.PChannelShardAssignment{
					{
						PChannel: "stale-rw-channel",
						Entries: []types.ShardAssignmentEntry{
							{CollectionID: 200, ShardIndex: 0, ReplicaID: 20},
						},
					},
				},
			},
		},
	})
	assert.NoError(t, err)
}

func TestSendFullAssignmentPublishesWALReplicaOwnerResourceGroup(t *testing.T) {
	mc := mock_manager.NewMockManagerClient(t)
	mc.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg-old"},
	}, nil)
	resource.InitForTest(resource.OptStreamingManagerClient(mc))

	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	streamServer.EXPECT().Send(mock.Anything).RunAndReturn(func(resp *streamingpb.AssignmentDiscoverResponse) error {
		fullAssignment := resp.GetFullAssignment()
		assert.Len(t, fullAssignment.GetAssignments(), 1)
		assignment := fullAssignment.GetAssignments()[0]
		assert.Len(t, assignment.GetWalReplicas(), 1)

		replica := types.NewWALReplicaInfoFromProto(assignment.GetWalReplicas()[0])
		assert.Equal(t, types.WALReplicaInfo{
			ChannelID:         types.ChannelID{Name: "p0", WALReplicaID: 1},
			AccessMode:        types.AccessModeRO,
			ResourceGroup:     "rg-old",
			PChannelWriteTerm: 7,
			AssignmentEpoch:   3,
			State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
		}, replica)
		return nil
	})

	helper := &discoverGrpcServerHelper{
		StreamingCoordAssignmentService_AssignmentDiscoverServer: streamServer,
	}
	err := helper.SendFullAssignment(balancer.WatchChannelAssignmentsCallbackParam{
		Version:            typeutil.VersionInt64Pair{Global: 1, Local: 2},
		CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "pchannel"}},
		WALReplicaRelations: []types.WALReplicaInfoAssigned{
			{
				Replica: types.WALReplicaInfo{
					ChannelID:         types.ChannelID{Name: "p0", WALReplicaID: 1},
					AccessMode:        types.AccessModeRO,
					ResourceGroup:     "rg-new",
					PChannelWriteTerm: 7,
					AssignmentEpoch:   3,
					State:             streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING,
				},
				Node: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			},
		},
	})
	assert.NoError(t, err)
}

func pchannelNames(channels []*streamingpb.PChannelInfo) []string {
	names := make([]string, 0, len(channels))
	for _, channel := range channels {
		names = append(names, channel.GetName())
	}
	return names
}

func walReplicaInfos(replicas []*streamingpb.WALReplicaInfo) []types.WALReplicaInfo {
	infos := make([]types.WALReplicaInfo, 0, len(replicas))
	for _, replica := range replicas {
		infos = append(infos, types.NewWALReplicaInfoFromProto(replica))
	}
	return infos
}

func TestAssignmentDiscover(t *testing.T) {
	mc := mock_manager.NewMockManagerClient(t)
	mc.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg1"},
	}, nil)
	resource.InitForTest(resource.OptStreamingManagerClient(mc))
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		versions := []typeutil.VersionInt64Pair{
			{Global: 1, Local: 2},
			{Global: 1, Local: 3},
		}
		pchans := [][]types.PChannelInfoAssigned{
			{
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
			},
			{
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel2", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
			},
		}
		for i := 0; i < len(versions); i++ {
			cb(balancer.WatchChannelAssignmentsCallbackParam{
				Version:            versions[i],
				CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "pchannel"}},
				Relations:          pchans[i],
			})
		}
		<-ctx.Done()
		return context.Cause(ctx)
	})
	b.EXPECT().MarkAsUnavailable(mock.Anything, mock.Anything).Return(nil)
	b.EXPECT().MarkWALReplicasAsUnavailable(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	b.EXPECT().MarkWALPrimaryReplicaAsUnavailable(
		mock.Anything,
		types.ChannelID{Name: "pchannel", WALReplicaID: 2},
		int64(6),
	).Return(nil)

	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	k := 0
	reqs := []*streamingpb.AssignmentDiscoverRequest{
		{
			Command: &streamingpb.AssignmentDiscoverRequest_ReportError{
				ReportError: &streamingpb.ReportAssignmentErrorRequest{
					Pchannel: &streamingpb.PChannelInfo{
						Name: "pchannel",
						Term: 1,
					},
					Err: &streamingpb.StreamingError{
						Code: streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
					},
				},
			},
		},
		{
			Command: &streamingpb.AssignmentDiscoverRequest_ReportError{
				ReportError: &streamingpb.ReportAssignmentErrorRequest{
					Pchannel: &streamingpb.PChannelInfo{
						Name:       "pchannel",
						Term:       1,
						AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					},
					Err: &streamingpb.StreamingError{
						Code: streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
					},
					WalReplicaId:    2,
					AssignmentEpoch: 5,
				},
			},
		},
		{
			Command: &streamingpb.AssignmentDiscoverRequest_ReportError{
				ReportError: &streamingpb.ReportAssignmentErrorRequest{
					Pchannel: &streamingpb.PChannelInfo{
						Name:       "pchannel",
						Term:       2,
						AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					},
					Err: &streamingpb.StreamingError{
						Code: streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
					},
					WalReplicaId:    2,
					AssignmentEpoch: 6,
				},
			},
		},
		{
			Command: &streamingpb.AssignmentDiscoverRequest_Close{},
		},
	}
	streamServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.AssignmentDiscoverRequest, error) {
		if k >= len(reqs) {
			return nil, io.EOF
		}
		req := reqs[k]
		k++
		return req, nil
	})
	streamServer.EXPECT().Send(mock.Anything).Return(nil)
	ads := NewAssignmentDiscoverServer(b, streamServer)
	ads.Execute()
}

func TestAssignmentDiscoverReportsReadOnlyReplicaZeroAsWALReplicaError(t *testing.T) {
	mc := mock_manager.NewMockManagerClient(t)
	mc.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{}, nil).Maybe()
	resource.InitForTest(resource.OptStreamingManagerClient(mc))

	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return context.Cause(ctx)
	})
	b.EXPECT().MarkWALReplicasAsUnavailable(
		mock.Anything,
		[]types.ChannelID{{Name: "pchannel", WALReplicaID: 0}},
		int64(5),
	).Return(nil).Once()

	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	reqs := []*streamingpb.AssignmentDiscoverRequest{
		{
			Command: &streamingpb.AssignmentDiscoverRequest_ReportError{
				ReportError: &streamingpb.ReportAssignmentErrorRequest{
					Pchannel: &streamingpb.PChannelInfo{
						Name:       "pchannel",
						Term:       10,
						AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY,
					},
					Err: &streamingpb.StreamingError{
						Code: streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
					},
					WalReplicaId:    0,
					AssignmentEpoch: 5,
				},
			},
		},
		{Command: &streamingpb.AssignmentDiscoverRequest_Close{}},
	}
	index := 0
	streamServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.AssignmentDiscoverRequest, error) {
		if index >= len(reqs) {
			return nil, io.EOF
		}
		req := reqs[index]
		index++
		return req, nil
	})
	streamServer.EXPECT().Send(mock.Anything).Return(nil)

	ads := NewAssignmentDiscoverServer(b, streamServer)
	_ = ads.Execute()
}

func TestAssignmentDiscoverReportsReadWriteWALReplicaAsPrimaryReplicaError(t *testing.T) {
	mc := mock_manager.NewMockManagerClient(t)
	mc.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{}, nil).Maybe()
	resource.InitForTest(resource.OptStreamingManagerClient(mc))

	b := &discoverReportFakeBalancer{}
	streamServer := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	reqs := []*streamingpb.AssignmentDiscoverRequest{
		{
			Command: &streamingpb.AssignmentDiscoverRequest_ReportError{
				ReportError: &streamingpb.ReportAssignmentErrorRequest{
					Pchannel: &streamingpb.PChannelInfo{
						Name:       "pchannel",
						Term:       10,
						AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE,
					},
					Err: &streamingpb.StreamingError{
						Code: streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
					},
					WalReplicaId:    2,
					AssignmentEpoch: 6,
				},
			},
		},
		{Command: &streamingpb.AssignmentDiscoverRequest_Close{}},
	}
	index := 0
	streamServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.AssignmentDiscoverRequest, error) {
		if index >= len(reqs) {
			return nil, io.EOF
		}
		req := reqs[index]
		index++
		return req, nil
	})
	streamServer.EXPECT().Send(mock.Anything).Return(nil)

	ads := NewAssignmentDiscoverServer(b, streamServer)
	_ = ads.Execute()

	assert.Empty(t, b.markedLegacy)
	assert.Empty(t, b.markedReplicas)
	assert.Equal(t, types.ChannelID{Name: "pchannel", WALReplicaID: 2}, b.markedPrimaryReplica)
	assert.Equal(t, int64(6), b.markedPrimaryEpoch)
}

type discoverReportFakeBalancer struct {
	balancer.Balancer

	markedLegacy         []types.PChannelInfo
	markedReplicas       []types.ChannelID
	markedPrimaryReplica types.ChannelID
	markedPrimaryEpoch   int64
}

func (b *discoverReportFakeBalancer) WatchChannelAssignments(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
	<-ctx.Done()
	return context.Cause(ctx)
}

func (b *discoverReportFakeBalancer) MarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) error {
	b.markedLegacy = append(b.markedLegacy, pChannels...)
	return nil
}

func (b *discoverReportFakeBalancer) MarkWALReplicasAsUnavailable(ctx context.Context, replicas []types.ChannelID, assignmentEpoch int64) error {
	b.markedReplicas = append(b.markedReplicas, replicas...)
	return nil
}

func (b *discoverReportFakeBalancer) MarkWALPrimaryReplicaAsUnavailable(ctx context.Context, replicaID types.ChannelID, assignmentEpoch int64) error {
	b.markedPrimaryReplica = replicaID
	b.markedPrimaryEpoch = assignmentEpoch
	return nil
}
