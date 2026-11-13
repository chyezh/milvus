package handler

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/client/connection"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// WALReplicaDiscoveryClient resolves each desired member independently before
// opening its local View watch. An endpoint is only a runtime identity; it does
// not imply that any particular VChannel or QueryView is locally ready.
type WALReplicaDiscoveryClient interface {
	GetWALReplicaEndpoint(context.Context, string, *streamingpb.WALReplicaAssignment) (*viewpb.StreamingWorkNode, error)
}

func (hc *handlerClientImpl) GetWALReplicaEndpoint(ctx context.Context, pchannel string, member *streamingpb.WALReplicaAssignment) (*viewpb.StreamingWorkNode, error) {
	if !hc.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrClientClosed
	}
	defer hc.lifetime.Done()
	if pchannel == "" || member.GetReplicaId() == 0 || member.GetNode().GetServerId() <= 0 {
		return nil, status.NewChannelNotExist(pchannel)
	}
	if hc.replicaConn == nil {
		return nil, status.NewOnShutdownError("WAL replica session discovery is unavailable")
	}
	nodeID := member.Node.ServerId
	client := streamingpb.NewStreamingNodeReplicaServiceClient(connection.ForNode(hc.replicaConn, nodeID))
	endpoint, err := client.GetWALReplicaEndpoint(ctx, &streamingpb.WALReplicaEndpointRequest{Pchannel: pchannel, ReplicaId: member.ReplicaId, NodeId: nodeID})
	if err != nil {
		return nil, status.ConvertStreamingError("GetWALReplicaEndpoint", err)
	}
	if endpoint.GetPchannel() != pchannel || endpoint.GetNodeId() != nodeID || endpoint.GetWalReplicaId() != member.ReplicaId ||
		endpoint.GetWalReplicaRevision() == 0 || endpoint.GetRuntimeId() == 0 || endpoint.GetWalTerm() <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("WAL replica endpoint does not identify the requested member")
	}
	return endpoint, nil
}
