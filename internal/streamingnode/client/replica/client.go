package replica

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/streamingnode/client/connection"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// Client shares session discovery and connections across the node's followers.
// Each Source binds a particular primary assignment; discovering a newer source
// never silently retargets an RPC belonging to the old runtime.
type Client struct {
	conn lazygrpc.Conn
	rb   resolver.Builder
}

func NewClient(etcd *clientv3.Client) *Client {
	conn, rb := connection.NewSession(etcd, "milvus.proto.streaming.StreamingNodeReplicaService", "milvus.proto.view.QueryPlanService")
	return &Client{conn: conn, rb: rb}
}

func (c *Client) Close() {
	c.conn.Close()
	c.rb.Close()
}

type Source struct {
	primary types.PChannelInfoAssigned
	events  streamingpb.StreamingNodeReplicaServiceClient
	query   viewpb.QueryPlanServiceClient
}

func (c *Client) ForPrimary(primary types.PChannelInfoAssigned) *Source {
	conn := connection.ForNode(c.conn, primary.Node.ServerID)
	return &Source{primary: primary, events: streamingpb.NewStreamingNodeReplicaServiceClient(conn), query: viewpb.NewQueryPlanServiceClient(conn)}
}

func (s *Source) Events() streamingpb.StreamingNodeReplicaServiceClient { return s.events }

// ResolveMVCC only asks for consistency resolution. The primary's local View
// may still be preparing while this replica already has a query-ready View.
func (s *Source) ResolveMVCC(ctx context.Context, req *viewpb.GetQueryPlanRequest, _ string) (*viewpb.QueryPlanMVCC, error) {
	ctx = worknodehandler.EncodeQueryViewPChannelToOutgoingContext(ctx, s.primary.Channel)
	mvcc, err := s.query.ResolveQueryPlanMVCC(ctx, req)
	return mvcc, viewerror.ConvertViewError(viewpb.QueryPlanService_ResolveQueryPlanMVCC_FullMethodName, err)
}
