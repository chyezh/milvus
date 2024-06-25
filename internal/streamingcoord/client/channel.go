package client

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
)

type channelService struct {
	*clientImpl
}

func (c *channelService) CreatePChannel(ctx context.Context, name string) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("channel client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	service, err := c.getChannelService(ctx)
	if err != nil {
		return err
	}
	_, err = service.CreatePChannel(ctx, &streamingpb.CreatePChannelRequest{
		Pchannel: name,
	})
	return err
}

func (c *channelService) RemovePChannel(ctx context.Context, name string) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("channel client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	service, err := c.getChannelService(ctx)
	if err != nil {
		return err
	}
	_, err = service.RemovePChannel(ctx, &streamingpb.RemovePChannelRequest{
		Pchannel: name,
	})
	return err
}
