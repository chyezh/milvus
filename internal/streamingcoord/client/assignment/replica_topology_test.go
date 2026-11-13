package assignment

import (
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type topologyAssignmentStream struct {
	streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverClient
}

func (*topologyAssignmentStream) Recv() (*streamingpb.AssignmentDiscoverResponse, error) {
	panic("mockey")
}

func TestReplicaTopologySurvivesPrimaryAssignmentGap(t *testing.T) {
	mockey.PatchConvey("read endpoints remain discoverable while writer assignment is absent", t, func() {
		plans := map[string]*streamingpb.WALReplicaPlan{"p": {Revision: 3, WalTerm: 8, Replicas: []*streamingpb.WALReplicaAssignment{{ReplicaId: 1, Node: &streamingpb.StreamingNodeInfo{ServerId: 1}}, {ReplicaId: 2, Node: &streamingpb.StreamingNodeInfo{ServerId: 2}, AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY}}}}
		received := false
		mockey.Mock((*topologyAssignmentStream).Recv).To(func() (*streamingpb.AssignmentDiscoverResponse, error) {
			if received {
				return nil, io.EOF
			}
			received = true
			return &streamingpb.AssignmentDiscoverResponse{Response: &streamingpb.AssignmentDiscoverResponse_FullAssignment{FullAssignment: &streamingpb.FullStreamingNodeAssignmentWithVersion{VersionByRevision: &streamingpb.VersionPair{Global: 1, Local: 2}, WalReplicaPlans: plans, Assignments: []*streamingpb.StreamingNodeAssignment{{Node: &streamingpb.StreamingNodeInfo{ServerId: 2}}}}}}, nil
		}).Build()
		watcher := newWatcher()
		client := &assignmentDiscoverClient{streamClient: &topologyAssignmentStream{}, w: watcher, exitCh: make(chan struct{}), clusterID: "test"}
		client.wg.Add(1)
		require.NoError(t, client.recvLoop())
		latest, err := watcher.GetLatestDiscover(t.Context())
		require.NoError(t, err)
		require.Len(t, latest.WALReplicaPlans, 1)
		require.True(t, proto.Equal(plans["p"], latest.WALReplicaPlans["p"]))
		require.Empty(t, latest.Assignments[2].Channels)
	})
}
