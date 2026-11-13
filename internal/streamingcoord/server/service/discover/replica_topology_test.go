package discover

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type topologyNodeManager struct{ manager.ManagerClient }

func (*topologyNodeManager) GetAllStreamingNodes(context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
	panic("mockey")
}

type topologyDiscoverStream struct {
	streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer
}

func (*topologyDiscoverStream) Context() context.Context                           { panic("mockey") }
func (*topologyDiscoverStream) Send(*streamingpb.AssignmentDiscoverResponse) error { panic("mockey") }

func TestDiscoveryCarriesPreparingReplicaTopology(t *testing.T) {
	mockey.PatchConvey("full topology is independent of writer-ready assignments", t, func() {
		mockey.Mock((*topologyNodeManager).GetAllStreamingNodes).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1}}, 2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2}}}, nil).Build()
		resource.InitForTest(resource.OptStreamingManagerClient(&topologyNodeManager{}))
		mockey.Mock((*topologyDiscoverStream).Context).Return(t.Context()).Build()
		plans := map[string]*streamingpb.WALReplicaPlan{"p": {Revision: 3, WalTerm: 7, Replicas: []*streamingpb.WALReplicaAssignment{{ReplicaId: 1, Node: &streamingpb.StreamingNodeInfo{ServerId: 1}}, {ReplicaId: 2, Node: &streamingpb.StreamingNodeInfo{ServerId: 2}, AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY}}}}
		sent := false
		mockey.Mock((*topologyDiscoverStream).Send).To(func(response *streamingpb.AssignmentDiscoverResponse) error {
			sent = true
			full := response.GetFullAssignment()
			require.Len(t, full.Assignments, 2)
			for _, node := range full.Assignments {
				require.Empty(t, node.Channels)
			}
			require.True(t, proto.Equal(plans["p"], full.WalReplicaPlans["p"]))
			return nil
		}).Build()
		helper := &discoverGrpcServerHelper{StreamingCoordAssignmentService_AssignmentDiscoverServer: &topologyDiscoverStream{}}
		require.NoError(t, helper.SendFullAssignment(balancer.WatchChannelAssignmentsCallbackParam{WALReplicaPlans: plans}))
		require.True(t, sent)
	})
}
