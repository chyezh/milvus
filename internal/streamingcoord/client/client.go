package client

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazyconn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
)

var _ Client = (*clientImpl)(nil)

// Client is the interface of log service client.
type Client interface {
	// Channel access channel service.
	Channel() ChannelService

	// Assignment access assignment service.
	Assignment() AssignmentService

	// State access state service.
	State() streamingpb.StreamingCoordStateServiceClient

	// Close close the client.
	Close()
}

type ChannelService interface {
	CreatePChannel(ctx context.Context, name string) error
	RemovePChannel(ctx context.Context, name string) error
}

type AssignmentService interface {
	AssignmentDiscover(ctx context.Context, cb func(*streamingpb.AssignmentDiscoverResponse) error) error

	ReportStreamingError(ctx context.Context, req *streamingpb.ReportStreamingErrorRequest) error
}

type clientImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]
	conn     *lazyconn.LazyGRPCConn
	rb       resolver.Builder
}

func (c *clientImpl) Channel() ChannelService {
	return &channelService{
		clientImpl: c,
	}
}

func (c *clientImpl) Assignment() AssignmentService {
	return &assignmentService{
		clientImpl: c,
	}
}

func (c *clientImpl) State() streamingpb.StreamingCoordStateServiceClient {
	return &stateService{
		clientImpl: c,
	}
}

func (c *clientImpl) Close() {
	c.lifetime.SetState(lifetime.Stopped)
	c.lifetime.Wait()
	c.lifetime.Close()
	c.conn.Close()
	c.rb.Close()
}

// getChannelService returns a channel service client.
func (c *clientImpl) getChannelService(ctx context.Context) (streamingpb.StreamingCoordChannelServiceClient, error) {
	conn, err := c.conn.Get(ctx)
	if err != nil {
		return nil, err
	}
	return streamingpb.NewStreamingCoordChannelServiceClient(conn), nil
}

// getAssignmentService returns a channel service client.
func (c *clientImpl) getAssignmentService(ctx context.Context) (streamingpb.StreamingCoordAssignmentServiceClient, error) {
	conn, err := c.conn.Get(ctx)
	if err != nil {
		return nil, err
	}
	return streamingpb.NewStreamingCoordAssignmentServiceClient(conn), nil
}

// getStateService returns a channel service client.
func (c *clientImpl) getStateService(ctx context.Context) (streamingpb.StreamingCoordStateServiceClient, error) {
	conn, err := c.conn.Get(ctx)
	if err != nil {
		return nil, err
	}
	return streamingpb.NewStreamingCoordStateServiceClient(conn), nil
}
