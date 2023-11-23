package lazyconn

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func NewLazyGRPCConn(dialer func(ctx context.Context) (*grpc.ClientConn, error)) *LazyGRPCConn {
	ctx, cancel := context.WithCancel(context.Background())

	conn := &LazyGRPCConn{
		ctx:         ctx,
		cancel:      cancel,
		initialized: make(chan struct{}),
		ready:       make(chan struct{}),

		dialer: dialer,
	}
	go conn.initialize()
	return conn
}

type LazyGRPCConn struct {
	ctx         context.Context
	cancel      context.CancelFunc
	initialized chan struct{}
	ready       chan struct{}

	dialer func(ctx context.Context) (*grpc.ClientConn, error)
	conn   *grpc.ClientConn
}

func (c *LazyGRPCConn) initialize() {
	go func() {
		defer close(c.initialized)
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				conn, err := c.dialer(c.ctx)
				if err != nil {
					log.Warn("async dial failed, retry...", zap.Error(err))
					continue
				}
				c.conn = conn
				close(c.ready)
				return
			}
		}
	}()
}

func (c *LazyGRPCConn) Get(ctx context.Context) (*grpc.ClientConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ready:
		return c.conn, nil
	case <-c.ctx.Done():
		return nil, status.NewOnShutdownError("close lazy grpc conn")
	}
}

func (c *LazyGRPCConn) Close() {
	c.cancel()
	<-c.initialized

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Warn("close underlying grpc conn fail", zap.Error(err))
		}
	}
}
