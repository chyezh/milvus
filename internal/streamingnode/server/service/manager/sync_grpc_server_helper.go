package manager

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func newAssignmentSyncGRPCServerHelper(s streamingpb.StreamingNodeManagerService_SyncServer) *assignmentSyncGRPCServerHelper {
	return &assignmentSyncGRPCServerHelper{s}
}

// TODO: report the balance attributes to the streaming coord.
type assignmentSyncGRPCServerHelper struct {
	streamingpb.StreamingNodeManagerService_SyncServer
}

// SendAssignWithError sends the assign failed result to client.
func (h *assignmentSyncGRPCServerHelper) SendAssignResponse(resp walmanager.SyncResponse) error {
	return h.Send(&streamingpb.StreamingNodeManagerSyncResponse{
		Response: &streamingpb.StreamingNodeManagerSyncResponse_Assignment{
			Assignment: newProtoFromSyncResponse(resp),
		},
	})
}

// SendClosed sends the close response to client.
func (h *assignmentSyncGRPCServerHelper) SendClosed() error {
	return h.Send(&streamingpb.StreamingNodeManagerSyncResponse{
		Response: &streamingpb.StreamingNodeManagerSyncResponse_Close{
			Close: &streamingpb.SyncCloseResponse{},
		},
	})
}

// newProtoFromSyncResponse converts the SyncResponse to proto.
func newProtoFromSyncResponse(resp walmanager.SyncResponse) *streamingpb.SyncAssignmentResponse {
	protoResp := &streamingpb.SyncAssignmentResponse{
		Assigned:   make([]*streamingpb.SyncAssignmentResult, 0, len(resp.Assign)),
		Unassigned: make([]*streamingpb.PChannelInfo, 0, len(resp.Unassign)),
	}
	for _, assign := range resp.Assign {
		if assign.Err != nil {
			protoResp.Assigned = append(protoResp.Assigned, &streamingpb.SyncAssignmentResult{
				Pchannel: types.NewProtoFromPChannelInfo(assign.Channel),
				Error:    status.AsStreamingError(assign.Err).AsPBError(),
			})
			continue
		}
		protoResp.Assigned = append(protoResp.Assigned, &streamingpb.SyncAssignmentResult{
			Pchannel: types.NewProtoFromPChannelInfo(assign.Channel),
			Error:    nil,
		})
	}
	for _, unassign := range resp.Unassign {
		protoResp.Unassigned = append(protoResp.Unassigned, types.NewProtoFromPChannelInfo(unassign))
	}
	return protoResp
}
