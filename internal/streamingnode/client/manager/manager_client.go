package manager

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/streamingnode/client/connection"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ManagerClient is the client to manage wal instances in all streamingnode.
// ManagerClient wraps the Session Service Discovery.
// Provides the ability to assign and remove wal instances for the channel on streaming node.
type ManagerClient interface {
	// WatchNodeChanged returns a channel that receive the signal that a streaming node change.
	WatchNodeChanged(ctx context.Context) (<-chan struct{}, error)

	// GetAllStreamingNodes fetches all streaming node info with resource group.
	// The result is fetch from service discovery, so there's no rpc call.
	// The resource group is obtained from the session's ServerLabels.
	GetAllStreamingNodes(ctx context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error)

	// CollectAllStatus collects every resource group when resourceGroupHint is empty.
	// An explicit hint selects that group, or a fallback group if it is absent.
	CollectAllStatus(ctx context.Context, resourceGroupHint string) (map[int64]*types.StreamingNodeStatus, error)

	// Assign a wal instance for the channel on streaming node of given server id.
	Assign(ctx context.Context, pchannel types.PChannelInfoAssigned) error

	// Remove the wal instance for the channel on streaming node of given server id.
	// Return types.ErrNotAlive if the streaming node is not in the session view; in that case the wal on it is not confirmed closed.
	Remove(ctx context.Context, pchannel types.PChannelInfoAssigned) error

	// Close closes the manager client.
	// It close the underlying connection, stop the node watcher and release all resources.
	Close()
}

// ReplicaPlanClient delivers the persisted intent to one member. Preparation
// runs on the node; this RPC only admits the plan and returns local progress.
type ReplicaPlanClient interface {
	ReconcileWALReplicas(context.Context, int64, *streamingpb.ReconcileWALReplicasRequest) (*streamingpb.ReconcileWALReplicasResponse, error)
}

// NewManagerClient creates a new manager client.
func NewManagerClient(etcdCli *clientv3.Client) ManagerClient {
	conn, rb := connection.NewSession(etcdCli,
		"milvus.proto.streaming.StreamingNodeManagerService",
		"milvus.proto.streaming.StreamingNodeReplicaService")
	return &managerClientImpl{
		lifetime: typeutil.NewLifetime(), stopped: make(chan struct{}), rb: rb,
		service:        lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingNodeManagerServiceClient),
		replicaService: lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingNodeReplicaServiceClient),
	}
}
