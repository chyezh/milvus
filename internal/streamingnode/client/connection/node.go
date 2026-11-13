package connection

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
)

// ForNode binds every unary and streaming call to one session identity. It
// deliberately does not fall back to another replica when that node is absent.
// The caller retains ownership of the shared connection.
func ForNode(conn lazygrpc.Conn, nodeID int64) grpc.ClientConnInterface {
	return &nodeConnection{conn: conn, nodeID: nodeID}
}

type nodeConnection struct {
	conn   lazygrpc.Conn
	nodeID int64
}

func (c *nodeConnection) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	conn, err := c.conn.GetConn(ctx)
	if err != nil {
		return err
	}
	return conn.Invoke(contextutil.WithPickServerID(ctx, c.nodeID), method, args, reply, opts...)
}

func (c *nodeConnection) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn, err := c.conn.GetConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.NewStream(contextutil.WithPickServerID(ctx, c.nodeID), desc, method, opts...)
}
