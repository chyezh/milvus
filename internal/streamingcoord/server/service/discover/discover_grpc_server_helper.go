package discover

import (
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/layout"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
)

// discoverGrpcServerHelper is a wrapped discover server of log messages.
type discoverGrpcServerHelper struct {
	streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer
}

// SendFullAssignment sends the full assignment to client.
func (h *discoverGrpcServerHelper) SendFullAssignment(v *util.VersionInt64Pair, nodeStatus map[int64]*layout.NodeStatus) error {
	fullAssignment := &streamingpb.FullStreamingNodeAssignmentWithVersion{
		Version: &streamingpb.VersionPair{
			Global: v.Global,
			Local:  v.Local,
		},
		Assignment: make([]*streamingpb.StreamingNodeAssignment, 0, len(nodeStatus)),
	}
	for _, node := range nodeStatus {
		channels := make([]*streamingpb.PChannelInfo, 0, len(node.Channels))
		for _, ch := range node.Channels {
			channels = append(channels, ch)
		}
		fullAssignment.Assignment = append(fullAssignment.Assignment, &streamingpb.StreamingNodeAssignment{
			ServerId: node.ServerID,
			Address:  node.Address,
			Channels: channels,
		})
	}
	return h.Send(&streamingpb.AssignmentDiscoverResponse{
		Response: &streamingpb.AssignmentDiscoverResponse_FullAssignment{
			FullAssignment: fullAssignment,
		},
	})
}
